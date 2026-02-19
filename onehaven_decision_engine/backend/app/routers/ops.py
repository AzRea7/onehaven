# onehaven_decision_engine/backend/app/routers/ops.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc, func
from sqlalchemy.orm import Session

from ..auth import get_principal, require_operator
from ..db import get_db
from ..models import (
    Property,
    PropertyState,
    UnderwritingResult,
    PropertyChecklistItem,
    Inspection,
    InspectionItem,
    RehabTask,
    Lease,
    Transaction,
    Valuation,
    WorkflowEvent,
)
from ..services.property_state_machine import compute_and_persist_stage


router = APIRouter(prefix="/ops", tags=["ops"])


def _now_utc() -> datetime:
    return datetime.utcnow()


def _loads(s: Optional[str], default: Any):
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _stage_label(stage: str) -> str:
    return stage or "deal"


def _txn_bucket(txn_type: str) -> str:
    t = (txn_type or "").lower().strip()
    if t in {"income"}:
        return "income"
    if t in {"expense"}:
        return "expense"
    if t in {"capex"}:
        return "capex"
    return "other"


@dataclass(frozen=True)
class ChecklistProgress:
    total: int
    todo: int
    in_progress: int
    blocked: int
    done: int

    @property
    def pct_done(self) -> float:
        return 0.0 if self.total <= 0 else float(self.done) / float(self.total)


def _checklist_progress(db: Session, *, org_id: int, property_id: int) -> ChecklistProgress:
    rows = db.scalars(
        select(PropertyChecklistItem.status).where(
            PropertyChecklistItem.org_id == org_id,
            PropertyChecklistItem.property_id == property_id,
        )
    ).all()

    total = len(rows)
    todo = inprog = blocked = done = 0
    for s in rows:
        st = (s or "todo").lower()
        if st == "done":
            done += 1
        elif st == "in_progress":
            inprog += 1
        elif st == "blocked":
            blocked += 1
        else:
            todo += 1

    return ChecklistProgress(total=total, todo=todo, in_progress=inprog, blocked=blocked, done=done)


def _latest_inspection(db: Session, *, property_id: int) -> Optional[Inspection]:
    return db.scalar(
        select(Inspection).where(Inspection.property_id == property_id).order_by(desc(Inspection.inspection_date))
    )


def _open_failed_inspection_items(db: Session, *, property_id: int) -> int:
    # Count unresolved failed items across all inspections for this property.
    # A "resolved" item is failed=False OR has resolved_at set.
    return int(
        db.scalar(
            select(func.count(InspectionItem.id))
            .select_from(InspectionItem)
            .join(Inspection, Inspection.id == InspectionItem.inspection_id)
            .where(
                Inspection.property_id == property_id,
                InspectionItem.failed.is_(True),
                InspectionItem.resolved_at.is_(None),
            )
        )
        or 0
    )


def _active_lease(db: Session, *, org_id: int, property_id: int) -> Optional[Lease]:
    now = _now_utc()
    # Active lease: started and not ended (or end in future)
    return db.scalar(
        select(Lease)
        .where(
            Lease.org_id == org_id,
            Lease.property_id == property_id,
            Lease.start_date <= now,
            func.coalesce(Lease.end_date, now + timedelta(days=3650)) >= now,
        )
        .order_by(desc(Lease.start_date))
    )


def _cash_rollup(db: Session, *, org_id: int, property_id: int, days: int) -> dict[str, float]:
    since = _now_utc() - timedelta(days=days)
    txns = db.scalars(
        select(Transaction).where(
            Transaction.org_id == org_id,
            Transaction.property_id == property_id,
            Transaction.txn_date >= since,
        )
    ).all()

    out = {"income": 0.0, "expense": 0.0, "capex": 0.0, "other": 0.0, "net": 0.0}
    for t in txns:
        b = _txn_bucket(t.txn_type)
        out[b] += float(t.amount or 0.0)
    # net: income - expense - capex (ignore other)
    out["net"] = out["income"] - out["expense"] - out["capex"]
    return out


def _latest_valuation(db: Session, *, org_id: int, property_id: int) -> Optional[Valuation]:
    return db.scalar(
        select(Valuation)
        .where(Valuation.org_id == org_id, Valuation.property_id == property_id)
        .order_by(desc(Valuation.as_of))
    )


def _latest_underwriting(db: Session, *, org_id: int, property_id: int) -> Optional[UnderwritingResult]:
    # Find latest deal underwriting for this property (via deals join in SQL, but underwriting_results has deal_id only).
    # Cheapest approach without changing schema: look up deal ids from property.
    deal_ids = db.scalars(
        select(UnderwritingResult.deal_id)
        .join_from(UnderwritingResult, Property, Property.id == Property.id)
    ).all()  # noop; keep simple fallback below

    # Real approach: just query underwriting_results joined to deals (models exist)
    from ..models import Deal  # local import to avoid circulars

    return db.scalar(
        select(UnderwritingResult)
        .join(Deal, Deal.id == UnderwritingResult.deal_id)
        .where(UnderwritingResult.org_id == org_id, Deal.property_id == property_id)
        .order_by(desc(UnderwritingResult.created_at))
    )


def _rehab_summary(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    rows = db.scalars(
        select(RehabTask).where(RehabTask.org_id == org_id, RehabTask.property_id == property_id)
    ).all()
    total = len(rows)
    done = sum(1 for r in rows if (r.status or "todo").lower() == "done")
    blocked = sum(1 for r in rows if (r.status or "todo").lower() == "blocked")
    inprog = sum(1 for r in rows if (r.status or "todo").lower() == "in_progress")
    todo = total - done - blocked - inprog

    cost_est = 0.0
    for r in rows:
        if r.cost_estimate is not None:
            cost_est += float(r.cost_estimate)

    return {
        "total": total,
        "todo": todo,
        "in_progress": inprog,
        "blocked": blocked,
        "done": done,
        "cost_estimate_sum": cost_est,
    }


def _next_actions(
    *,
    stage: str,
    checklist: ChecklistProgress,
    open_failed_items: int,
    latest_insp: Optional[Inspection],
    rehab: dict[str, Any],
    active_lease: Optional[Lease],
    latest_val: Optional[Valuation],
) -> list[str]:
    actions: list[str] = []

    # Phase 3 closure: compliance + inspection readiness
    if checklist.total == 0:
        actions.append("Generate compliance checklist (Phase 3 start).")
    else:
        if checklist.done < checklist.total:
            actions.append(f"Complete checklist items ({checklist.done}/{checklist.total} done).")

    if latest_insp is None:
        actions.append("Create first inspection record (or schedule inspection).")
    else:
        if not bool(latest_insp.passed):
            actions.append("Resolve failed inspection items, then reinspect.")
        if open_failed_items > 0:
            actions.append(f"Resolve {open_failed_items} unresolved failed inspection items.")

    # Rehab closure
    if rehab.get("total", 0) == 0 and (checklist.total > 0 and checklist.done < checklist.total):
        actions.append("Generate rehab tasks from checklist gaps.")
    if rehab.get("todo", 0) + rehab.get("in_progress", 0) + rehab.get("blocked", 0) > 0:
        actions.append("Finish rehab tasks blocking readiness.")

    # Lease closure
    if active_lease is None and stage in {"compliance", "tenant", "cash", "equity"}:
        actions.append("Create lease once inspection passes + unit is ready.")

    # Equity closure
    if latest_val is None and stage in {"cash", "equity"}:
        actions.append("Add a valuation snapshot to unlock equity storytelling.")

    # Keep list sane
    return actions[:10]


@router.get("/property/{property_id}/summary")
def property_ops_summary(
    property_id: int,
    cash_days: int = Query(default=90, ge=7, le=365),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == p.org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    # Compute + persist stage (state machine becomes evidence-driven)
    stage_row: PropertyState = compute_and_persist_stage(db, org_id=p.org_id, property=prop)
    stage = _stage_label(stage_row.current_stage)

    checklist = _checklist_progress(db, org_id=p.org_id, property_id=property_id)
    latest_insp = _latest_inspection(db, property_id=property_id)
    open_failed_items = _open_failed_inspection_items(db, property_id=property_id)
    rehab = _rehab_summary(db, org_id=p.org_id, property_id=property_id)
    active_lease = _active_lease(db, org_id=p.org_id, property_id=property_id)

    cash_30 = _cash_rollup(db, org_id=p.org_id, property_id=property_id, days=30)
    cash_n = _cash_rollup(db, org_id=p.org_id, property_id=property_id, days=cash_days)

    val = _latest_valuation(db, org_id=p.org_id, property_id=property_id)
    equity = None
    if val is not None:
        est_val = float(val.estimated_value or 0.0)
        loan = float(val.loan_balance or 0.0)
        equity = {
            "as_of": val.as_of.isoformat() if val.as_of else None,
            "estimated_value": est_val,
            "loan_balance": loan,
            "estimated_equity": est_val - loan,
        }

    uw = _latest_underwriting(db, org_id=p.org_id, property_id=property_id)
    underwriting = None
    if uw is not None:
        underwriting = {
            "decision": uw.decision,
            "score": int(uw.score),
            "cash_flow": float(uw.cash_flow),
            "dscr": float(uw.dscr),
            "cash_on_cash": float(uw.cash_on_cash),
            "gross_rent_used": float(uw.gross_rent_used),
            "reasons": _loads(uw.reasons_json, []),
            "jurisdiction_reasons": _loads(uw.jurisdiction_reasons_json, []),
            "decision_version": uw.decision_version,
            "created_at": uw.created_at.isoformat() if uw.created_at else None,
        }

    next_actions = _next_actions(
        stage=stage,
        checklist=checklist,
        open_failed_items=open_failed_items,
        latest_insp=latest_insp,
        rehab=rehab,
        active_lease=active_lease,
        latest_val=val,
    )

    return {
        "property": {
            "id": prop.id,
            "address": prop.address,
            "city": prop.city,
            "state": prop.state,
            "zip": prop.zip,
            "bedrooms": prop.bedrooms,
            "bathrooms": prop.bathrooms,
            "square_feet": prop.square_feet,
            "year_built": prop.year_built,
        },
        "stage": stage,
        "stage_updated_at": stage_row.updated_at.isoformat() if stage_row.updated_at else None,
        "checklist_progress": {
            "total": checklist.total,
            "todo": checklist.todo,
            "in_progress": checklist.in_progress,
            "blocked": checklist.blocked,
            "done": checklist.done,
            "pct_done": checklist.pct_done,
        },
        "inspection": {
            "latest": (
                {
                    "id": latest_insp.id,
                    "inspection_date": latest_insp.inspection_date.isoformat() if latest_insp.inspection_date else None,
                    "passed": bool(latest_insp.passed),
                    "reinspect_required": bool(latest_insp.reinspect_required),
                    "notes": latest_insp.notes,
                }
                if latest_insp
                else None
            ),
            "open_failed_items": open_failed_items,
        },
        "rehab_summary": rehab,
        "lease": (
            {
                "id": active_lease.id,
                "start_date": active_lease.start_date.isoformat(),
                "end_date": active_lease.end_date.isoformat() if active_lease.end_date else None,
                "total_rent": float(active_lease.total_rent),
                "tenant_portion": float(active_lease.tenant_portion or 0.0) if active_lease.tenant_portion is not None else None,
                "housing_authority_portion": float(active_lease.housing_authority_portion or 0.0) if active_lease.housing_authority_portion is not None else None,
            }
            if active_lease
            else None
        ),
        "cash": {
            "last_30_days": cash_30,
            f"last_{cash_days}_days": cash_n,
        },
        "equity": equity,
        "underwriting": underwriting,
        "next_actions": next_actions,
    }


@router.post("/property/{property_id}/generate_rehab_tasks")
def generate_rehab_tasks_from_gaps(
    property_id: int,
    max_tasks: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    """
    Close the Phase-3 -> Phase-4 loop:
      checklist gaps + unresolved inspection fails -> rehab tasks

    This intentionally uses existing tables (rehab_tasks, checklist_items, inspection_items).
    No migration required.
    """
    require_operator(p)

    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == p.org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    created: list[dict[str, Any]] = []

    # 1) checklist gaps -> tasks
    gaps = db.scalars(
        select(PropertyChecklistItem).where(
            PropertyChecklistItem.org_id == p.org_id,
            PropertyChecklistItem.property_id == property_id,
            PropertyChecklistItem.status.in_(["todo", "in_progress", "blocked"]),
        )
    ).all()

    # 2) unresolved inspection fails -> tasks
    fail_items = db.scalars(
        select(InspectionItem)
        .join(Inspection, Inspection.id == InspectionItem.inspection_id)
        .where(
            Inspection.property_id == property_id,
            InspectionItem.failed.is_(True),
            InspectionItem.resolved_at.is_(None),
        )
    ).all()

    # Prevent duplicates by title
    existing_titles = set(
        t.title for t in db.scalars(select(RehabTask).where(RehabTask.org_id == p.org_id, RehabTask.property_id == property_id)).all()
    )

    def _add_task(title: str, *, category: str, inspection_relevant: bool, notes: Optional[str]) -> None:
        nonlocal created
        if len(created) >= max_tasks:
            return
        if title in existing_titles:
            return
        rt = RehabTask(
            org_id=p.org_id,
            property_id=property_id,
            title=title,
            category=category,
            inspection_relevant=inspection_relevant,
            status="todo",
            notes=notes,
            created_at=_now_utc(),
        )
        db.add(rt)
        db.flush()
        existing_titles.add(title)
        created.append({"id": rt.id, "title": rt.title, "category": rt.category})

    for g in gaps:
        title = f"[HQS] {g.item_code}: {g.description}"
        _add_task(title, category="compliance", inspection_relevant=True, notes=g.notes)

    for fi in fail_items:
        title = f"[INSPECTION] {fi.code}: fix + document"
        detail = fi.details or ""
        loc = fi.location or ""
        notes = (" ".join([loc, detail])).strip() or None
        _add_task(title, category="inspection", inspection_relevant=True, notes=notes)

    # Workflow event for traceability
    evt = WorkflowEvent(
        org_id=p.org_id,
        property_id=property_id,
        actor_user_id=p.user_id,
        event_type="rehab_tasks.generated_from_gaps",
        payload_json=json.dumps({"created": created}),
        created_at=_now_utc(),
    )
    db.add(evt)

    # Stage recompute
    compute_and_persist_stage(db, org_id=p.org_id, property=prop)

    db.commit()

    return {"created": created, "count": len(created)}
