from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, desc, func, select
from sqlalchemy.orm import Session

from ..domain.workflow.stages import STAGES, clamp_stage, gate_for_next_stage, stage_rank
from ..models import (
    Deal,
    Inspection,
    InspectionItem,
    Lease,
    Property,
    PropertyChecklistItem,
    PropertyState,
    RehabTask,
    Tenant,
    Transaction,
    UnderwritingResult,
    Valuation,
)
from ..services.policy_projection_service import build_property_compliance_brief


def _utcnow() -> datetime:
    return datetime.utcnow()


def _loads_json(s: Optional[str]) -> dict:
    if not s:
        return {}
    try:
        x = json.loads(s)
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def _dumps_json(x: Optional[dict]) -> str:
    try:
        return json.dumps(x or {}, separators=(",", ":"), sort_keys=True, default=str)
    except Exception:
        return "{}"


@dataclass(frozen=True)
class ChecklistProgress:
    total: int
    todo: int
    in_progress: int
    blocked: int
    failed: int
    done: int

    @property
    def pct_done(self) -> float:
        return 0.0 if self.total <= 0 else float(self.done) / float(self.total)

    def as_dict(self) -> dict:
        return {
            "total": self.total,
            "todo": self.todo,
            "in_progress": self.in_progress,
            "blocked": self.blocked,
            "failed": self.failed,
            "done": self.done,
            "pct_done": round(self.pct_done, 4),
        }


@dataclass(frozen=True)
class InspectionStatus:
    exists: bool
    latest_passed: bool
    open_failed_items: int
    latest_inspection_id: Optional[int] = None
    latest_inspection_date: Optional[str] = None

    def as_dict(self) -> dict:
        return {
            "exists": self.exists,
            "latest_passed": self.latest_passed,
            "open_failed_items": self.open_failed_items,
            "latest": {
                "id": self.latest_inspection_id,
                "inspection_date": self.latest_inspection_date,
                "passed": self.latest_passed,
            }
            if self.exists
            else None,
        }


def ensure_state_row(db: Session, *, org_id: int, property_id: int) -> PropertyState:
    row = db.scalar(
        select(PropertyState).where(
            PropertyState.org_id == org_id,
            PropertyState.property_id == property_id,
        )
    )
    if row:
        return row

    now = _utcnow()
    row = PropertyState(
        org_id=org_id,
        property_id=property_id,
        current_stage="import",
        constraints_json=_dumps_json({}),
        outstanding_tasks_json=_dumps_json({}),
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def _set_state(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    current_stage: str,
    constraints: Optional[dict[str, Any]] = None,
    outstanding_tasks: Optional[dict[str, Any]] = None,
) -> PropertyState:
    row = ensure_state_row(db, org_id=org_id, property_id=property_id)
    row.current_stage = clamp_stage(current_stage)
    row.constraints_json = _dumps_json(constraints)
    row.outstanding_tasks_json = _dumps_json(outstanding_tasks)
    row.updated_at = _utcnow()
    db.add(row)
    db.flush()
    return row


def advance_stage_if_needed(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    suggested_stage: str,
    constraints: Optional[dict[str, Any]] = None,
    outstanding_tasks: Optional[dict[str, Any]] = None,
) -> PropertyState:
    # Step 2 rule: current_stage should reflect current canonical truth,
    # not "furthest ever reached". Otherwise the guard becomes decorative.
    return _set_state(
        db,
        org_id=org_id,
        property_id=property_id,
        current_stage=suggested_stage,
        constraints=constraints,
        outstanding_tasks=outstanding_tasks,
    )


def _get_property(db: Session, *, org_id: int, property_id: int) -> Optional[Property]:
    return db.scalar(
        select(Property).where(
            Property.org_id == org_id,
            Property.id == property_id,
        )
    )


def _get_latest_deal(db: Session, *, org_id: int, property_id: int) -> Optional[Deal]:
    return db.scalar(
        select(Deal)
        .where(and_(Deal.org_id == org_id, Deal.property_id == property_id))
        .order_by(desc(Deal.updated_at), desc(Deal.id))
        .limit(1)
    )


def _get_latest_underwriting(db: Session, *, org_id: int, property_id: int) -> Optional[UnderwritingResult]:
    return db.scalar(
        select(UnderwritingResult)
        .join(Deal, Deal.id == UnderwritingResult.deal_id)
        .where(
            UnderwritingResult.org_id == org_id,
            Deal.property_id == property_id,
        )
        .order_by(desc(UnderwritingResult.created_at), desc(UnderwritingResult.id))
        .limit(1)
    )


def _derive_deal_decision(deal: Optional[Deal], uw: Optional[UnderwritingResult]) -> str:
    explicit = str(getattr(deal, "decision", "") or "").strip().lower()
    if explicit in {"buy", "pass", "watch", "reject"}:
        return explicit

    uw_decision = str(getattr(uw, "decision", "") or "").strip().upper()
    if uw_decision in {"BUY", "PASS"}:
        return "buy"
    if uw_decision in {"WATCH", "MAYBE"}:
        return "watch"
    if uw_decision in {"REJECT", "PASS_ON_DEAL"}:
        return "reject"
    return ""


def _has_acquisition_fields(deal: Optional[Deal]) -> bool:
    if not deal:
        return False
    has_price = getattr(deal, "purchase_price", None) is not None
    has_close = getattr(deal, "closing_date", None) is not None
    return bool(has_price and has_close)


def _compute_checklist_progress(db: Session, *, org_id: int, property_id: int) -> ChecklistProgress:
    rows = db.scalars(
        select(PropertyChecklistItem.status).where(
            PropertyChecklistItem.org_id == org_id,
            PropertyChecklistItem.property_id == property_id,
        )
    ).all()

    total = len(rows)
    todo = inprog = blocked = failed = done = 0

    for s in rows:
        st = (s or "todo").lower().strip()
        if st == "done":
            done += 1
        elif st == "failed":
            failed += 1
        elif st == "blocked":
            blocked += 1
        elif st == "in_progress":
            inprog += 1
        else:
            todo += 1

    return ChecklistProgress(
        total=total,
        todo=todo,
        in_progress=inprog,
        blocked=blocked,
        failed=failed,
        done=done,
    )


def _get_latest_inspection(db: Session, *, org_id: int, property_id: int) -> Optional[Inspection]:
    return db.scalar(
        select(Inspection)
        .where(
            Inspection.org_id == org_id,
            Inspection.property_id == property_id,
        )
        .order_by(desc(Inspection.inspection_date), desc(Inspection.id))
        .limit(1)
    )


def _open_failed_inspection_items(db: Session, *, org_id: int, property_id: int) -> int:
    return int(
        db.scalar(
            select(func.count(InspectionItem.id))
            .select_from(InspectionItem)
            .join(Inspection, Inspection.id == InspectionItem.inspection_id)
            .where(
                Inspection.org_id == org_id,
                Inspection.property_id == property_id,
                InspectionItem.failed.is_(True),
                InspectionItem.resolved_at.is_(None),
            )
        )
        or 0
    )


def _compute_inspection_status(db: Session, *, org_id: int, property_id: int) -> InspectionStatus:
    insp = _get_latest_inspection(db, org_id=org_id, property_id=property_id)
    if not insp:
        return InspectionStatus(
            exists=False,
            latest_passed=False,
            open_failed_items=0,
            latest_inspection_id=None,
            latest_inspection_date=None,
        )

    open_failed = _open_failed_inspection_items(db, org_id=org_id, property_id=property_id)
    passed = bool(getattr(insp, "passed", False)) and open_failed == 0

    inspection_date = None
    if getattr(insp, "inspection_date", None):
        try:
            inspection_date = insp.inspection_date.isoformat()
        except Exception:
            inspection_date = str(insp.inspection_date)

    return InspectionStatus(
        exists=True,
        latest_passed=passed,
        open_failed_items=open_failed,
        latest_inspection_id=getattr(insp, "id", None),
        latest_inspection_date=inspection_date,
    )


def _rehab_summary(db: Session, *, org_id: int, property_id: int) -> dict:
    rows = db.scalars(
        select(RehabTask).where(
            RehabTask.org_id == org_id,
            RehabTask.property_id == property_id,
        )
    ).all()

    total = len(rows)
    done = 0
    blocked = 0
    open_count = 0

    for r in rows:
        st = (r.status or "todo").strip().lower()
        if st == "done":
            done += 1
        else:
            open_count += 1
        if st == "blocked":
            blocked += 1

    return {
        "total": total,
        "done": done,
        "open": open_count,
        "blocked": blocked,
        "has_plan_tasks": total > 0,
        "has_open_tasks": open_count > 0,
        "has_blockers": blocked > 0,
    }


def _has_any_tenant(db: Session, *, org_id: int) -> bool:
    row = db.scalar(select(Tenant.id).where(Tenant.org_id == org_id).limit(1))
    return row is not None


def _has_any_lease(db: Session, *, org_id: int, property_id: int) -> bool:
    row = db.scalar(
        select(Lease.id).where(
            Lease.org_id == org_id,
            Lease.property_id == property_id,
        ).limit(1)
    )
    return row is not None


def _has_active_lease(db: Session, *, org_id: int, property_id: int) -> bool:
    now = _utcnow()
    far_future = now + timedelta(days=3650)

    lease = db.scalar(
        select(Lease)
        .where(
            Lease.org_id == org_id,
            Lease.property_id == property_id,
            Lease.start_date <= now,
            func.coalesce(Lease.end_date, far_future) >= now,
        )
        .order_by(desc(Lease.start_date), desc(Lease.id))
        .limit(1)
    )
    return lease is not None


def _last_txn_date(db: Session, *, org_id: int, property_id: int) -> Optional[datetime]:
    return db.scalar(
        select(func.max(Transaction.txn_date)).where(
            Transaction.org_id == org_id,
            Transaction.property_id == property_id,
        )
    )


def _has_cash_txns(db: Session, *, org_id: int, property_id: int) -> bool:
    txn_id = db.scalar(
        select(Transaction.id)
        .where(Transaction.org_id == org_id, Transaction.property_id == property_id)
        .limit(1)
    )
    return txn_id is not None


def _latest_valuation(db: Session, *, org_id: int, property_id: int) -> Optional[Valuation]:
    return db.scalar(
        select(Valuation)
        .where(
            Valuation.org_id == org_id,
            Valuation.property_id == property_id,
        )
        .order_by(desc(Valuation.as_of), desc(Valuation.id))
        .limit(1)
    )


def _valuation_is_due(val: Optional[Valuation], *, cadence_days: int = 180) -> bool:
    if val is None or getattr(val, "as_of", None) is None:
        return True

    as_of = val.as_of
    if isinstance(as_of, datetime):
        as_of = as_of.date()

    today = _utcnow().date()

    try:
        return (today - as_of).days >= cadence_days
    except Exception:
        return True


def _rent_expected_proxy(db: Session, *, org_id: int, property_id: int) -> float:
    now = _utcnow()
    far_future = now + timedelta(days=3650)

    lease = db.scalar(
        select(Lease)
        .where(
            Lease.org_id == org_id,
            Lease.property_id == property_id,
            Lease.start_date <= now,
            func.coalesce(Lease.end_date, far_future) >= now,
        )
        .order_by(desc(Lease.start_date), desc(Lease.id))
        .limit(1)
    )
    if not lease:
        return 0.0

    try:
        return float(getattr(lease, "total_rent", 0.0) or 0.0)
    except Exception:
        return 0.0


def _rent_collected_last_30(db: Session, *, org_id: int, property_id: int) -> float:
    end = _utcnow()
    start = end - timedelta(days=30)

    s = db.scalar(
        select(func.coalesce(func.sum(Transaction.amount), 0.0))
        .where(Transaction.org_id == org_id, Transaction.property_id == property_id)
        .where(Transaction.txn_date >= start, Transaction.txn_date <= end)
        .where(func.lower(Transaction.txn_type).in_(["rent", "income"]))
    )

    try:
        return float(s or 0.0)
    except Exception:
        return 0.0


def derive_stage_and_constraints(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> tuple[str, Dict[str, Any], Dict[str, Any], List[str]]:
    next_actions: List[str] = []
    constraints: Dict[str, Any] = {}
    tasks: Dict[str, Any] = {}

    prop = _get_property(db, org_id=org_id, property_id=property_id)
    if prop is None:
        return "import", {"missing_property": True}, {"property": {"missing": True}}, ["Create or import the property first."]

    deal = _get_latest_deal(db, org_id=org_id, property_id=property_id)
    uw = _get_latest_underwriting(db, org_id=org_id, property_id=property_id)
    decision = _derive_deal_decision(deal, uw)

    checklist = _compute_checklist_progress(db, org_id=org_id, property_id=property_id)
    insp = _compute_inspection_status(db, org_id=org_id, property_id=property_id)
    rehab = _rehab_summary(db, org_id=org_id, property_id=property_id)
    has_any_lease = _has_any_lease(db, org_id=org_id, property_id=property_id)
    has_active_lease = _has_active_lease(db, org_id=org_id, property_id=property_id)
    has_cash_txns = _has_cash_txns(db, org_id=org_id, property_id=property_id)
    last_txn = _last_txn_date(db, org_id=org_id, property_id=property_id)
    valuation = _latest_valuation(db, org_id=org_id, property_id=property_id)

    policy_brief = build_property_compliance_brief(
        db,
        org_id=None,
        state=prop.state or "MI",
        county=getattr(prop, "county", None),
        city=prop.city,
        pha_name=None,
    )
    tasks["policy"] = {
        "production_readiness": policy_brief.get("coverage", {}).get("production_readiness"),
        "blocking_items": policy_brief.get("blocking_items", []),
        "required_actions": policy_brief.get("required_actions", []),
    }

    if not deal:
        constraints["missing_deal"] = True
        tasks["deal"] = {"missing": True}
        next_actions.append("Create a deal for this property.")
        return "import", constraints, tasks, next_actions

    tasks["deal"] = {
        "deal_id": getattr(deal, "id", None),
        "decision": decision or None,
        "has_underwriting": uw is not None,
    }

    if not uw:
        constraints["missing_underwriting"] = True
        next_actions.append("Run underwriting evaluation for the deal.")
        return "deal", constraints, tasks, next_actions

    if decision in {"", "watch"}:
        constraints["decision_pending"] = True
        next_actions.append("Finalize decision for this deal (BUY / WATCH / PASS).")
        return "decision", constraints, tasks, next_actions

    if decision in {"reject", "pass"}:
        constraints["decision_not_buy"] = decision
        next_actions.append("Deal is not BUY-approved. Revise assumptions or archive it.")
        return "decision", constraints, tasks, next_actions

    tasks["acquisition"] = {
        "purchase_price": getattr(deal, "purchase_price", None),
        "closing_date": getattr(deal, "closing_date", None).isoformat() if getattr(deal, "closing_date", None) else None,
        "loan_amount": getattr(deal, "loan_amount", None),
    }

    if not _has_acquisition_fields(deal):
        constraints["missing_acquisition_fields"] = {
            "purchase_price": getattr(deal, "purchase_price", None) is not None,
            "closing_date": getattr(deal, "closing_date", None) is not None,
        }
        next_actions.append("Add acquisition fields: purchase price and closing date.")
        return "acquisition", constraints, tasks, next_actions

    tasks["rehab"] = rehab

    if not rehab["has_plan_tasks"]:
        constraints["missing_rehab_plan"] = True
        next_actions.append("Create rehab plan tasks.")
        return "rehab_plan", constraints, tasks, next_actions

    if rehab["has_blockers"]:
        constraints["rehab_blockers_open"] = rehab["blocked"]
        next_actions.append(f"Clear blocked rehab tasks ({rehab['blocked']}).")
        return "rehab_exec", constraints, tasks, next_actions

    if rehab["has_open_tasks"]:
        constraints["rehab_open_tasks"] = rehab["open"]
        next_actions.append(f"Complete rehab execution tasks ({rehab['open']} open).")
        return "rehab_exec", constraints, tasks, next_actions

    tasks["compliance"] = {
        "checklist": checklist.as_dict(),
        "inspection": insp.as_dict(),
    }

    if checklist.total == 0:
        constraints["missing_checklist"] = True
        next_actions.append("Generate compliance checklist.")
        return "compliance", constraints, tasks, next_actions

    if checklist.failed > 0:
        constraints["checklist_failed_items"] = checklist.failed
        next_actions.append(f"Resolve failed checklist items ({checklist.failed}).")
        return "compliance", constraints, tasks, next_actions

    if checklist.blocked > 0:
        constraints["checklist_blocked_items"] = checklist.blocked
        next_actions.append(f"Unblock checklist items ({checklist.blocked}).")
        return "compliance", constraints, tasks, next_actions

    if checklist.pct_done < 0.95:
        constraints["checklist_incomplete"] = checklist.as_dict()
        next_actions.append(f"Finish compliance checklist ({max(0, checklist.total - checklist.done)} remaining).")
        return "compliance", constraints, tasks, next_actions

    if not insp.exists:
        constraints["missing_inspection"] = True
        next_actions.append("Create or record inspection.")
        return "compliance", constraints, tasks, next_actions

    if not insp.latest_passed:
        constraints["inspection_not_passed"] = insp.as_dict()
        if insp.open_failed_items > 0:
            next_actions.append(f"Resolve open inspection fails ({insp.open_failed_items}).")
        else:
            next_actions.append("Mark inspection passed or schedule reinspection.")
        return "compliance", constraints, tasks, next_actions

    # Tenant stage = compliance passed, but no lease yet.
    if not has_any_lease:
        constraints["missing_lease"] = True
        tasks["tenant"] = {"needs_lease": True}
        next_actions.append("Create tenant and lease.")
        return "tenant", constraints, tasks, next_actions

    if not has_active_lease:
        constraints["lease_not_active"] = True
        tasks["lease"] = {"needs_active_lease": True}
        next_actions.append("Activate a lease for this property.")
        return "lease", constraints, tasks, next_actions

    tasks["cash"] = {
        "has_transactions": has_cash_txns,
        "last_txn_date": last_txn.isoformat() if last_txn else None,
    }

    if not has_cash_txns:
        constraints["no_transactions"] = True
        next_actions.append("Add cash transactions (rent / expenses).")
        return "cash", constraints, tasks, next_actions

    expected = _rent_expected_proxy(db, org_id=org_id, property_id=property_id)
    collected = _rent_collected_last_30(db, org_id=org_id, property_id=property_id)
    tasks["cash"]["rent_expected_proxy"] = round(expected, 2)
    tasks["cash"]["rent_collected_last_30"] = round(collected, 2)

    if expected > 0 and collected + 1e-6 < expected:
        constraints["rent_reconciliation_gap"] = {
            "expected_proxy": round(expected, 2),
            "collected_last_30": round(collected, 2),
            "gap": round(expected - collected, 2),
        }
        next_actions.append(
            f"Reconcile rent collection gap (~${expected:.0f} expected vs ${collected:.0f} collected in last 30 days)."
        )
        return "cash", constraints, tasks, next_actions

    tasks["equity"] = {
        "has_valuation": valuation is not None,
        "latest_as_of": valuation.as_of.isoformat() if valuation and valuation.as_of else None,
    }

    if valuation is None:
        constraints["missing_valuation"] = True
        next_actions.append("Add a valuation snapshot.")
        return "equity", constraints, tasks, next_actions

    if _valuation_is_due(valuation, cadence_days=180):
        constraints["valuation_due"] = {
            "cadence_days": 180,
            "latest_as_of": valuation.as_of.isoformat() if valuation and valuation.as_of else None,
        }
        next_actions.append("Valuation is stale. Add a new valuation snapshot.")
        return "equity", constraints, tasks, next_actions

    return "equity", constraints, tasks, next_actions


def sync_property_state(db: Session, *, org_id: int, property_id: int) -> PropertyState:
    suggested, constraints, tasks, _ = derive_stage_and_constraints(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    return advance_stage_if_needed(
        db,
        org_id=org_id,
        property_id=property_id,
        suggested_stage=suggested,
        constraints=constraints,
        outstanding_tasks=tasks,
    )


def get_state_payload(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    recompute: bool = True,
) -> Dict[str, Any]:
    row = (
        sync_property_state(db, org_id=org_id, property_id=property_id)
        if recompute
        else ensure_state_row(db, org_id=org_id, property_id=property_id)
    )

    suggested, constraints_live, tasks_live, next_actions = derive_stage_and_constraints(
        db,
        org_id=org_id,
        property_id=property_id,
    )

    cur = clamp_stage(getattr(row, "current_stage", "import"))

    return {
        "property_id": property_id,
        "current_stage": cur,
        "current_stage_rank": stage_rank(cur),
        "suggested_stage": suggested,
        "suggested_stage_rank": stage_rank(suggested),
        "all_stages": list(STAGES),
        "constraints": constraints_live if recompute else _loads_json(getattr(row, "constraints_json", None)),
        "outstanding_tasks": tasks_live if recompute else _loads_json(getattr(row, "outstanding_tasks_json", None)),
        "next_actions": next_actions,
        "updated_at": getattr(row, "updated_at", None).isoformat() if getattr(row, "updated_at", None) else None,
    }


def get_transition_payload(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> Dict[str, Any]:
    prop = _get_property(db, org_id=org_id, property_id=property_id)
    deal = _get_latest_deal(db, org_id=org_id, property_id=property_id)
    uw = _get_latest_underwriting(db, org_id=org_id, property_id=property_id)
    decision = _derive_deal_decision(deal, uw)
    rehab = _rehab_summary(db, org_id=org_id, property_id=property_id)
    insp = _compute_inspection_status(db, org_id=org_id, property_id=property_id)
    checklist = _compute_checklist_progress(db, org_id=org_id, property_id=property_id)
    has_any_lease = _has_any_lease(db, org_id=org_id, property_id=property_id)
    has_active_lease = _has_active_lease(db, org_id=org_id, property_id=property_id)
    has_cash_txns = _has_cash_txns(db, org_id=org_id, property_id=property_id)
    valuation = _latest_valuation(db, org_id=org_id, property_id=property_id)

    state = get_state_payload(db, org_id=org_id, property_id=property_id, recompute=True)
    cur = clamp_stage(state["current_stage"])

    gate = gate_for_next_stage(
        current_stage=cur,
        has_property=prop is not None,
        has_deal=deal is not None,
        has_underwriting=uw is not None,
        decision_is_buy=(decision == "buy"),
        has_acquisition_fields=_has_acquisition_fields(deal),
        has_rehab_plan_tasks=rehab["has_plan_tasks"],
        rehab_blockers_open=rehab["has_blockers"],
        rehab_open_tasks=rehab["has_open_tasks"],
        compliance_passed=(checklist.total > 0 and checklist.failed == 0 and checklist.blocked == 0 and checklist.pct_done >= 0.95 and insp.latest_passed),
        tenant_selected=has_any_lease,
        lease_active=has_active_lease,
        has_cash_txns=has_cash_txns,
        has_valuation=valuation is not None,
    )

    return {
        "property_id": property_id,
        "current_stage": cur,
        "suggested_stage": state["suggested_stage"],
        "gate": {
            "ok": gate.ok,
            "blocked_reason": gate.blocked_reason,
            "allowed_next_stage": gate.allowed_next_stage,
        },
        "next_actions": state["next_actions"],
        "constraints": state["constraints"],
        "outstanding_tasks": state["outstanding_tasks"],
    }


def compute_and_persist_stage(db: Session, *, org_id: int, property: Property) -> PropertyState:
    row = sync_property_state(db, org_id=org_id, property_id=property.id)
    db.commit()
    db.refresh(row)
    return row
