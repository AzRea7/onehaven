from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.auth import get_principal, require_operator
from onehaven_platform.backend.src.db import get_db
from onehaven_platform.backend.src.models import (
    Deal,
    Inspection,
    InspectionItem,
    Lease,
    Property,
    PropertyChecklistItem,
    PropertyState,
    RehabTask,
    Transaction,
    UnderwritingResult,
    Valuation,
    WorkflowEvent,
)
from onehaven_platform.backend.src.services.dashboard_rollup_service import compute_rollups
from onehaven_platform.backend.src.services.state_machine_service import (
    compute_and_persist_stage,
    get_state_payload,
)
from onehaven_platform.backend.src.services.compliance_projection_service import build_workflow_summary

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


def _num(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def _txn_bucket(txn_type: str | None) -> str:
    t = (txn_type or "").lower().strip()
    if t in {"income", "rent", "hap", "voucher"}:
        return "income"
    if t in {"expense", "repair", "maintenance", "mortgage", "insurance", "tax"}:
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


def _latest_inspection(db: Session, *, org_id: int, property_id: int) -> Optional[Inspection]:
    return db.scalar(
        select(Inspection)
        .where(Inspection.org_id == org_id, Inspection.property_id == property_id)
        .order_by(desc(Inspection.inspection_date), desc(Inspection.id))
    )


def _open_failed_inspection_items(db: Session, *, org_id: int, property_id: int) -> int:
    rows = db.scalars(
        select(InspectionItem)
        .join(Inspection, Inspection.id == InspectionItem.inspection_id)
        .where(
            Inspection.org_id == org_id,
            Inspection.property_id == property_id,
            InspectionItem.failed.is_(True),
            InspectionItem.resolved_at.is_(None),
        )
    ).all()
    return len(rows)


def _latest_underwriting(db: Session, *, org_id: int, property_id: int) -> Optional[UnderwritingResult]:
    return db.scalar(
        select(UnderwritingResult)
        .join(Deal, Deal.id == UnderwritingResult.deal_id)
        .where(Deal.org_id == org_id, Deal.property_id == property_id)
        .order_by(desc(UnderwritingResult.created_at), desc(UnderwritingResult.id))
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
        cost_est += _num(getattr(r, "cost_estimate", None) or getattr(r, "estimated_cost", None))

    return {
        "total": total,
        "todo": todo,
        "in_progress": inprog,
        "blocked": blocked,
        "done": done,
        "cost_estimate_sum": round(cost_est, 2),
        "is_complete": total > 0 and todo == 0 and inprog == 0 and blocked == 0,
    }


def _active_lease(db: Session, *, org_id: int, property_id: int) -> Optional[Lease]:
    now = _now_utc()
    rows = db.scalars(
        select(Lease)
        .where(Lease.org_id == org_id, Lease.property_id == property_id)
        .order_by(desc(Lease.start_date), desc(Lease.id))
    ).all()

    for row in rows:
        start = getattr(row, "start_date", None)
        end = getattr(row, "end_date", None)

        if start and isinstance(start, datetime) and start > now:
            continue
        if start and isinstance(start, date) and start > now.date():
            continue
        if end and isinstance(end, datetime) and end < now:
            continue
        if end and isinstance(end, date) and end < now.date():
            continue
        return row
    return None


def _latest_valuation(db: Session, *, org_id: int, property_id: int) -> Optional[Valuation]:
    return db.scalar(
        select(Valuation)
        .where(Valuation.org_id == org_id, Valuation.property_id == property_id)
        .order_by(desc(Valuation.as_of), desc(Valuation.id))
    )


def _cash_rollup(db: Session, *, org_id: int, property_id: int, days: int) -> dict[str, float]:
    since = _now_utc() - timedelta(days=days)
    rows = db.scalars(
        select(Transaction)
        .where(Transaction.org_id == org_id, Transaction.property_id == property_id)
        .order_by(desc(Transaction.txn_date), desc(Transaction.id))
    ).all()

    income = expense = capex = 0.0
    for r in rows:
        dt = r.txn_date or _now_utc()
        if dt < since:
            continue
        amt = _num(r.amount)
        bucket = _txn_bucket(getattr(r, "txn_type", None))
        if bucket == "income":
            income += amt
        elif bucket == "expense":
            expense += abs(amt)
        elif bucket == "capex":
            capex += abs(amt)

    return {
        "income": round(income, 2),
        "expense": round(expense, 2),
        "capex": round(capex, 2),
        "net": round(income - expense - capex, 2),
    }


def _tenant_summary(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    now = _now_utc()
    leases = list(
        db.scalars(
            select(Lease)
            .where(Lease.org_id == org_id, Lease.property_id == property_id)
            .order_by(desc(Lease.start_date), desc(Lease.id))
        ).all()
    )

    active = []
    upcoming = []
    ended = []
    for l in leases:
        if l.start_date and isinstance(l.start_date, datetime) and l.start_date > now:
            upcoming.append(l)
        elif l.start_date and isinstance(l.start_date, date) and l.start_date > now.date():
            upcoming.append(l)
        elif l.end_date and isinstance(l.end_date, datetime) and l.end_date < now:
            ended.append(l)
        elif l.end_date and isinstance(l.end_date, date) and l.end_date < now.date():
            ended.append(l)
        else:
            active.append(l)

    current = active[0] if active else None
    occupancy_status = "vacant"
    if current:
        occupancy_status = "occupied"
    elif upcoming:
        occupancy_status = "leased_not_started"

    return {
        "occupancy_status": occupancy_status,
        "lease_count": len(leases),
        "active_lease_count": len(active),
        "upcoming_lease_count": len(upcoming),
        "ended_lease_count": len(ended),
        "active_lease": (
            {
                "id": current.id,
                "tenant_id": current.tenant_id,
                "start_date": current.start_date.isoformat() if current.start_date else None,
                "end_date": current.end_date.isoformat() if current.end_date else None,
                "total_rent": _num(current.total_rent),
                "tenant_portion": _num(current.tenant_portion) if current.tenant_portion is not None else None,
                "housing_authority_portion": _num(current.housing_authority_portion)
                if current.housing_authority_portion is not None
                else None,
                "hap_contract_status": current.hap_contract_status,
                "notes": current.notes,
            }
            if current
            else None
        ),
    }


def _equity_summary(db: Session, *, org_id: int, property_id: int) -> Optional[dict[str, Any]]:
    latest = _latest_valuation(db, org_id=org_id, property_id=property_id)
    if latest is None:
        return None

    estimated_value = _num(latest.estimated_value)
    loan_balance = _num(latest.loan_balance)
    estimated_equity = estimated_value - loan_balance
    ltv_pct = round((loan_balance / estimated_value) * 100.0, 2) if estimated_value > 0 else None

    return {
        "as_of": latest.as_of.isoformat() if latest.as_of else None,
        "estimated_value": round(estimated_value, 2),
        "loan_balance": round(loan_balance, 2),
        "estimated_equity": round(estimated_equity, 2),
        "ltv_pct": ltv_pct,
        "notes": latest.notes,
    }


def _decision_health(
    *,
    underwriting: Optional[dict[str, Any]],
    checklist: ChecklistProgress,
    rehab: dict[str, Any],
    inspection_latest: Optional[Inspection],
    open_failed_items: int,
    active_lease: Optional[Lease],
    cash_n: dict[str, float],
    equity: Optional[dict[str, Any]],
) -> dict[str, Any]:
    score = 0
    flags: list[str] = []
    warnings: list[str] = []

    if underwriting:
        score += 20
        decision = str(underwriting.get("decision") or "").upper()
        if decision == "GOOD":
            score += 10
            flags.append("underwriting_positive")
        elif decision == "REJECT":
            warnings.append("underwriting_negative")
        else:
            warnings.append("underwriting_review")
    else:
        warnings.append("missing_underwriting")

    if checklist.total > 0:
        score += round(checklist.pct_done * 20)
        if checklist.done == checklist.total:
            flags.append("checklist_complete")
        elif checklist.blocked > 0:
            warnings.append("checklist_blocked")
    else:
        warnings.append("missing_checklist")

    if rehab.get("total", 0) > 0:
        if rehab.get("is_complete"):
            score += 15
            flags.append("rehab_complete")
        else:
            warnings.append("rehab_incomplete")
    else:
        warnings.append("missing_rehab_tasks")

    if inspection_latest:
        if bool(getattr(inspection_latest, "passed", False)) and open_failed_items == 0:
            score += 15
            flags.append("inspection_clean")
        else:
            warnings.append("inspection_open")
    else:
        warnings.append("missing_inspection")

    if active_lease:
        score += 10
        flags.append("active_lease")
    else:
        warnings.append("no_active_lease")

    if float(cash_n.get("income", 0.0) or 0.0) > 0:
        score += 5
        flags.append("cashflow_observed")
    else:
        warnings.append("no_cash_income")

    if equity and float(equity.get("estimated_value", 0.0) or 0.0) > 0:
        score += 5
        flags.append("valuation_present")
    else:
        warnings.append("missing_valuation")

    band = "low"
    if score >= 75:
        band = "high"
    elif score >= 45:
        band = "medium"

    return {
        "score": max(0, min(100, score)),
        "band": band,
        "flags": flags,
        "warnings": warnings,
    }


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

    stage_row: PropertyState = compute_and_persist_stage(db, org_id=p.org_id, property=prop)
    state_payload = get_state_payload(db, org_id=p.org_id, property_id=property_id, recompute=True)
    workflow = build_workflow_summary(db, org_id=p.org_id, property_id=property_id, recompute=False)
    stage = state_payload.get("current_stage") or stage_row.current_stage or "deal"

    checklist = _checklist_progress(db, org_id=p.org_id, property_id=property_id)
    latest_insp = _latest_inspection(db, org_id=p.org_id, property_id=property_id)
    open_failed_items = _open_failed_inspection_items(db, org_id=p.org_id, property_id=property_id)
    rehab = _rehab_summary(db, org_id=p.org_id, property_id=property_id)

    tenant = _tenant_summary(db, org_id=p.org_id, property_id=property_id)
    active_lease = _active_lease(db, org_id=p.org_id, property_id=property_id)

    cash_30 = _cash_rollup(db, org_id=p.org_id, property_id=property_id, days=30)
    cash_n = _cash_rollup(db, org_id=p.org_id, property_id=property_id, days=cash_days)

    equity = _equity_summary(db, org_id=p.org_id, property_id=property_id)

    uw = _latest_underwriting(db, org_id=p.org_id, property_id=property_id)
    underwriting = None
    if uw:
        underwriting = {
            "id": uw.id,
            "decision": state_payload.get("normalized_decision"),
            "raw_decision": uw.decision,
            "score": uw.score,
            "dscr": uw.dscr,
            "cash_flow": uw.cash_flow,
            "gross_rent_used": getattr(uw, "gross_rent_used", None),
            "opex_used": getattr(uw, "opex_used", None),
            "reason_json": _loads(getattr(uw, "reason_json", None), {}),
            "created_at": uw.created_at.isoformat() if uw.created_at else None,
        }

    decision_health = _decision_health(
        underwriting=underwriting,
        checklist=checklist,
        rehab=rehab,
        inspection_latest=latest_insp,
        open_failed_items=open_failed_items,
        active_lease=active_lease,
        cash_n=cash_n,
        equity=equity,
    )

    counts = {
        "rehab_tasks_total": rehab.get("total", 0),
        "rehab_tasks_open": int(rehab.get("todo", 0)) + int(rehab.get("in_progress", 0)) + int(rehab.get("blocked", 0)),
        "checklist_total": checklist.total,
        "checklist_done": checklist.done,
        "inspection_open_failed_items": open_failed_items,
        "has_active_lease": active_lease is not None,
        "cash_days_window": cash_days,
        "has_valuation": equity is not None,
        "has_underwriting": underwriting is not None,
    }

    endgame = {
        "tenant_ready": tenant["occupancy_status"] in {"occupied", "leased_not_started"},
        "cash_live": cash_n["income"] > 0 or cash_n["expense"] > 0 or cash_n["capex"] > 0,
        "equity_tracked": equity is not None,
        "ops_maturity_score": (
            (25 if tenant["occupancy_status"] in {"occupied", "leased_not_started"} else 0)
            + (25 if cash_n["income"] > 0 else 0)
            + (25 if equity is not None else 0)
            + (25 if active_lease is not None else 0)
        ),
    }

    return {
        "property": {
            "id": prop.id,
            "address": prop.address,
            "city": prop.city,
            "state": prop.state,
            "zip": prop.zip,
            "county": getattr(prop, "county", None),
            "bedrooms": prop.bedrooms,
            "bathrooms": prop.bathrooms,
            "square_feet": prop.square_feet,
            "year_built": prop.year_built,
            "lat": getattr(prop, "lat", None),
            "lng": getattr(prop, "lng", None),
            "crime_score": getattr(prop, "crime_score", None),
            "offender_count": getattr(prop, "offender_count", None),
            "is_red_zone": getattr(prop, "is_red_zone", False),
        },
        "stage": stage,
        "stage_label": workflow.get("current_stage_label"),
        "normalized_decision": state_payload.get("normalized_decision"),
        "gate_status": state_payload.get("gate_status"),
        "gate": state_payload.get("gate"),
        "stage_completion_summary": state_payload.get("stage_completion_summary"),
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
        "tenant": tenant,
        "lease": tenant.get("active_lease"),
        "cash": {
            "last_30_days": cash_30,
            f"last_{cash_days}_days": cash_n,
        },
        "equity": equity,
        "underwriting": underwriting,
        "decision_health": decision_health,
        "counts": counts,
        "endgame": endgame,
        "constraints": state_payload.get("constraints", {}),
        "outstanding_tasks": state_payload.get("outstanding_tasks", {}),
        "next_actions": state_payload.get("next_actions", []),
        "workflow": workflow,
    }


def _rollup_args(
    *,
    db: Session,
    p,
    state: Optional[str],
    county: Optional[str],
    city: Optional[str],
    q: Optional[str],
    stage: Optional[str],
    decision: Optional[str],
    only_red_zone: bool,
    exclude_red_zone: bool,
    min_crime_score: Optional[float],
    max_crime_score: Optional[float],
    min_offender_count: Optional[int],
    max_offender_count: Optional[int],
    days: int,
    limit: int,
):
    return compute_rollups(
        db,
        org_id=p.org_id,
        days=days,
        limit=limit,
        state=state,
        county=county,
        city=city,
        q=q,
        stage=stage,
        decision=decision,
        only_red_zone=only_red_zone,
        exclude_red_zone=exclude_red_zone,
        min_crime_score=min_crime_score,
        max_crime_score=max_crime_score,
        min_offender_count=min_offender_count,
        max_offender_count=max_offender_count,
    )


@router.get("/rollups")
def ops_rollups(
    state: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    decision: Optional[str] = Query(default=None),
    only_red_zone: bool = Query(default=False),
    exclude_red_zone: bool = Query(default=False),
    min_crime_score: Optional[float] = Query(default=None),
    max_crime_score: Optional[float] = Query(default=None),
    min_offender_count: Optional[int] = Query(default=None),
    max_offender_count: Optional[int] = Query(default=None),
    days: int = Query(default=90, ge=7, le=365),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return _rollup_args(
        db=db,
        p=p,
        state=state,
        county=county,
        city=city,
        q=q,
        stage=stage,
        decision=decision,
        only_red_zone=only_red_zone,
        exclude_red_zone=exclude_red_zone,
        min_crime_score=min_crime_score,
        max_crime_score=max_crime_score,
        min_offender_count=min_offender_count,
        max_offender_count=max_offender_count,
        days=days,
        limit=limit,
    )


@router.get("/control-plane")
def ops_control_plane(
    state: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    decision: Optional[str] = Query(default=None),
    only_red_zone: bool = Query(default=False),
    exclude_red_zone: bool = Query(default=False),
    min_crime_score: Optional[float] = Query(default=None),
    max_crime_score: Optional[float] = Query(default=None),
    min_offender_count: Optional[int] = Query(default=None),
    max_offender_count: Optional[int] = Query(default=None),
    days: int = Query(default=90, ge=7, le=365),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    payload = _rollup_args(
        db=db,
        p=p,
        state=state,
        county=county,
        city=city,
        q=q,
        stage=stage,
        decision=decision,
        only_red_zone=only_red_zone,
        exclude_red_zone=exclude_red_zone,
        min_crime_score=min_crime_score,
        max_crime_score=max_crime_score,
        min_offender_count=min_offender_count,
        max_offender_count=max_offender_count,
        days=days,
        limit=limit,
    )
    payload["view"] = "control_plane"
    return payload


@router.get("/drilldown/{kind}")
def ops_drilldown(
    kind: str,
    state: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    decision: Optional[str] = Query(default=None),
    only_red_zone: bool = Query(default=False),
    exclude_red_zone: bool = Query(default=False),
    min_crime_score: Optional[float] = Query(default=None),
    max_crime_score: Optional[float] = Query(default=None),
    min_offender_count: Optional[int] = Query(default=None),
    max_offender_count: Optional[int] = Query(default=None),
    days: int = Query(default=90, ge=7, le=365),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    payload = _rollup_args(
        db=db,
        p=p,
        state=state,
        county=county,
        city=city,
        q=q,
        stage=stage,
        decision=decision,
        only_red_zone=only_red_zone,
        exclude_red_zone=exclude_red_zone,
        min_crime_score=min_crime_score,
        max_crime_score=max_crime_score,
        min_offender_count=min_offender_count,
        max_offender_count=max_offender_count,
        days=days,
        limit=limit,
    )
    payload["view"] = "drilldown"
    payload["kind"] = kind
    return payload


@router.get("/property/{property_id}/workflow")
def property_workflow_summary(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == p.org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    compute_and_persist_stage(db, org_id=p.org_id, property=prop)
    return build_workflow_summary(db, org_id=p.org_id, property_id=property_id, recompute=False)


@router.post("/property/{property_id}/generate_rehab_tasks")
def generate_rehab_tasks_from_gaps(
    property_id: int,
    max_tasks: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    require_operator(p)

    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == p.org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    created: list[dict[str, Any]] = []

    gaps = db.scalars(
        select(PropertyChecklistItem).where(
            PropertyChecklistItem.org_id == p.org_id,
            PropertyChecklistItem.property_id == property_id,
            PropertyChecklistItem.status.in_(["todo", "in_progress", "blocked"]),
        )
    ).all()

    fail_items = db.scalars(
        select(InspectionItem)
        .join(Inspection, Inspection.id == InspectionItem.inspection_id)
        .where(
            Inspection.org_id == p.org_id,
            Inspection.property_id == property_id,
            InspectionItem.failed.is_(True),
            InspectionItem.resolved_at.is_(None),
        )
    ).all()

    existing_titles = set(
        t.title
        for t in db.scalars(
            select(RehabTask).where(RehabTask.org_id == p.org_id, RehabTask.property_id == property_id)
        ).all()
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

    evt = WorkflowEvent(
        org_id=p.org_id,
        property_id=property_id,
        actor_user_id=p.user_id,
        event_type="rehab_tasks.generated_from_gaps",
        payload_json=json.dumps({"created": created}),
        created_at=_now_utc(),
    )
    db.add(evt)

    compute_and_persist_stage(db, org_id=p.org_id, property=prop)
    db.commit()

    return {
        "created": created,
        "count": len(created),
        "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, recompute=True),
    }
