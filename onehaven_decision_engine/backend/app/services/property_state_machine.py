# backend/app/services/property_state_machine.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Optional, Any, Dict, List, Tuple

from sqlalchemy import select, func, and_
from sqlalchemy.orm import Session

from ..models import (
    PropertyState,
    Deal,
    UnderwritingResult,
    ComplianceChecklist,
    ComplianceChecklistItem,
    Inspection,
    InspectionItem,
    RehabTask,
    Lease,
    CashTransaction,
    Valuation,
)

# -----------------------------------------------------------------------------
# Property State Machine (Phase 4 loop-closer)
# -----------------------------------------------------------------------------
# Goal:
#   Maintain a single row per property that answers:
#     - What "stage" is this property in right now?
#     - What constraints prevent advancing?
#     - What tasks are outstanding (at a high level)?
#
# This file intentionally does NOT enforce business rules in routes directly.
# It provides a consistent derived "truth" row that routes /ops and UI can use.
# -----------------------------------------------------------------------------

STAGE_ORDER = ["deal", "rehab", "compliance", "tenant", "cash", "equity"]

# If you later want finer states, keep current_stage coarse and add
# constraints/outstanding detail. Don’t explode stage enums too early.


def _utcnow() -> datetime:
    return datetime.utcnow()


def _stage_rank(stage: str) -> int:
    try:
        return STAGE_ORDER.index(stage)
    except ValueError:
        return 0


def _clamp_stage(stage: str) -> str:
    s = (stage or "deal").strip().lower()
    return s if s in STAGE_ORDER else "deal"


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
        return json.dumps(x or {}, separators=(",", ":"), sort_keys=True)
    except Exception:
        return "{}"


@dataclass(frozen=True)
class ChecklistProgress:
    total: int
    done: int
    failed: int
    blocked: int
    in_progress: int

    @property
    def pct_done(self) -> float:
        if self.total <= 0:
            return 0.0
        return float(self.done) / float(self.total)

    def as_dict(self) -> dict:
        return {
            "total": self.total,
            "done": self.done,
            "failed": self.failed,
            "blocked": self.blocked,
            "in_progress": self.in_progress,
            "pct_done": self.pct_done,
        }


@dataclass(frozen=True)
class InspectionStatus:
    exists: bool
    latest_passed: bool
    open_failed_items: int

    def as_dict(self) -> dict:
        return {
            "exists": self.exists,
            "latest_passed": self.latest_passed,
            "open_failed_items": self.open_failed_items,
        }


@dataclass(frozen=True)
class LeaseStatus:
    has_active_lease: bool
    active_lease_id: Optional[int]

    def as_dict(self) -> dict:
        return {
            "has_active_lease": self.has_active_lease,
            "active_lease_id": self.active_lease_id,
        }


@dataclass(frozen=True)
class CashStatus:
    last_txn_at: Optional[datetime]
    last_30d_net: Optional[float]
    last_90d_net: Optional[float]

    def as_dict(self) -> dict:
        return {
            "last_txn_at": self.last_txn_at.isoformat() if self.last_txn_at else None,
            "last_30d_net": self.last_30d_net,
            "last_90d_net": self.last_90d_net,
        }


@dataclass(frozen=True)
class EquityStatus:
    latest_valuation_at: Optional[date]
    estimated_value: Optional[float]
    loan_balance: Optional[float]
    estimated_equity: Optional[float]

    def as_dict(self) -> dict:
        return {
            "latest_valuation_at": self.latest_valuation_at.isoformat()
            if self.latest_valuation_at
            else None,
            "estimated_value": self.estimated_value,
            "loan_balance": self.loan_balance,
            "estimated_equity": self.estimated_equity,
        }


# -----------------------------------------------------------------------------
# Core row helpers
# -----------------------------------------------------------------------------


def ensure_state_row(db: Session, *, org_id: int, property_id: int) -> PropertyState:
    """
    Ensure a PropertyState row exists. Default stage is 'deal'.
    """
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
        current_stage="deal",
        constraints_json=_dumps_json({}),
        outstanding_tasks_json=_dumps_json({}),
        updated_at=now,
    )
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
    """
    Conservative stage advance:
      - never moves backward
      - only updates to a higher-ranked stage
      - can update constraints/tasks in place
    """
    row = ensure_state_row(db, org_id=org_id, property_id=property_id)

    cur = _clamp_stage(str(row.current_stage or "deal"))
    nxt = _clamp_stage(suggested_stage)

    if _stage_rank(nxt) > _stage_rank(cur):
        row.current_stage = nxt

    if constraints is not None:
        row.constraints_json = _dumps_json(constraints)

    if outstanding_tasks is not None:
        row.outstanding_tasks_json = _dumps_json(outstanding_tasks)

    row.updated_at = _utcnow()
    db.add(row)
    db.flush()
    return row


# -----------------------------------------------------------------------------
# Derivation logic (this is the “closing loops” piece)
# -----------------------------------------------------------------------------


def _get_latest_deal(db: Session, *, org_id: int, property_id: int) -> Optional[Deal]:
    # If your schema allows multiple deals per property, choose latest by id.
    return db.scalar(
        select(Deal)
        .where(and_(Deal.org_id == org_id, Deal.property_id == property_id))
        .order_by(Deal.id.desc())
        .limit(1)
    )


def _get_latest_underwriting(
    db: Session, *, org_id: int, property_id: int
) -> Optional[UnderwritingResult]:
    return db.scalar(
        select(UnderwritingResult)
        .where(
            and_(
                UnderwritingResult.org_id == org_id,
                UnderwritingResult.property_id == property_id,
            )
        )
        .order_by(UnderwritingResult.id.desc())
        .limit(1)
    )


def _get_latest_checklist(
    db: Session, *, org_id: int, property_id: int
) -> Optional[ComplianceChecklist]:
    return db.scalar(
        select(ComplianceChecklist)
        .where(
            and_(
                ComplianceChecklist.org_id == org_id,
                ComplianceChecklist.property_id == property_id,
            )
        )
        .order_by(ComplianceChecklist.id.desc())
        .limit(1)
    )


def _compute_checklist_progress(
    db: Session, *, org_id: int, checklist_id: int
) -> ChecklistProgress:
    items: List[Tuple[str, int]] = db.execute(
        select(
            func.lower(ComplianceChecklistItem.status).label("st"),
            func.count(ComplianceChecklistItem.id),
        )
        .where(
            and_(
                ComplianceChecklistItem.org_id == org_id,
                ComplianceChecklistItem.checklist_id == checklist_id,
            )
        )
        .group_by(func.lower(ComplianceChecklistItem.status))
    ).all()

    counts = {st or "todo": int(c) for (st, c) in items}
    # Treat null/unknown as todo
    total = sum(counts.values())
    done = counts.get("done", 0)
    failed = counts.get("failed", 0)
    blocked = counts.get("blocked", 0)
    in_progress = counts.get("in_progress", 0) + counts.get("in progress", 0)

    return ChecklistProgress(
        total=total, done=done, failed=failed, blocked=blocked, in_progress=in_progress
    )


def _get_latest_inspection(
    db: Session, *, org_id: int, property_id: int
) -> Optional[Inspection]:
    return db.scalar(
        select(Inspection)
        .where(and_(Inspection.org_id == org_id, Inspection.property_id == property_id))
        .order_by(Inspection.id.desc())
        .limit(1)
    )


def _compute_inspection_status(
    db: Session, *, org_id: int, inspection: Optional[Inspection]
) -> InspectionStatus:
    if not inspection:
        return InspectionStatus(exists=False, latest_passed=False, open_failed_items=0)

    # Interpret "passed" field robustly (some schemas use outcome/status)
    passed = bool(getattr(inspection, "passed", False))
    # Count unresolved failed items (status == failed and not resolved)
    # Adapt to your schema fields:
    #  - InspectionItem.status: failed/done/etc.
    #  - InspectionItem.resolved_at or is_resolved
    # If your model differs, adjust here.
    q = select(func.count(InspectionItem.id)).where(
        and_(
            InspectionItem.org_id == org_id,
            InspectionItem.inspection_id == inspection.id,
            func.lower(InspectionItem.status) == "failed",
            # Try both styles; if column missing, SQLAlchemy will throw at import time.
            # If your schema does not have resolved_at, remove this and use your field.
            getattr(InspectionItem, "resolved_at", None) == None,  # noqa: E711
        )
    )

    open_failed = 0
    try:
        open_failed = int(db.scalar(q) or 0)
    except Exception:
        # Fallback if resolved_at isn't present in your schema: just count failed
        open_failed = int(
            db.scalar(
                select(func.count(InspectionItem.id)).where(
                    and_(
                        InspectionItem.org_id == org_id,
                        InspectionItem.inspection_id == inspection.id,
                        func.lower(InspectionItem.status) == "failed",
                    )
                )
            )
            or 0
        )

    # If there are open failed items, treat as not passed even if inspection.passed is True
    if open_failed > 0:
        passed = False

    return InspectionStatus(exists=True, latest_passed=passed, open_failed_items=open_failed)


def _compute_rehab_open_count(db: Session, *, org_id: int, property_id: int) -> int:
    # Assume RehabTask.status has values like todo/in_progress/done/blocked
    # Open = not done
    return int(
        db.scalar(
            select(func.count(RehabTask.id)).where(
                and_(
                    RehabTask.org_id == org_id,
                    RehabTask.property_id == property_id,
                    func.lower(RehabTask.status) != "done",
                )
            )
        )
        or 0
    )


def _compute_lease_status(
    db: Session, *, org_id: int, property_id: int
) -> LeaseStatus:
    # Active lease: start_date <= today and (end_date is null or end_date >= today)
    today = date.today()
    lease = db.scalar(
        select(Lease)
        .where(
            and_(
                Lease.org_id == org_id,
                Lease.property_id == property_id,
                Lease.start_date <= today,
                (Lease.end_date == None) | (Lease.end_date >= today),  # noqa: E711
            )
        )
        .order_by(Lease.id.desc())
        .limit(1)
    )
    return LeaseStatus(has_active_lease=lease is not None, active_lease_id=getattr(lease, "id", None))


def _sum_cash_window(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    days: int,
) -> Optional[float]:
    # Interpret CashTransaction.amount signed:
    # - If you store expenses as negative amounts already, just sum.
    # - If you store txn_type and amount always positive, you should adjust here.
    start = _utcnow() - timedelta(days=days)
    # Many schemas use txn_date date not datetime; comparing with datetime still works in PG.
    total = db.scalar(
        select(func.coalesce(func.sum(CashTransaction.amount), 0.0)).where(
            and_(
                CashTransaction.org_id == org_id,
                CashTransaction.property_id == property_id,
                CashTransaction.txn_date >= start.date(),
            )
        )
    )
    try:
        return float(total)
    except Exception:
        return None


def _compute_cash_status(
    db: Session, *, org_id: int, property_id: int
) -> CashStatus:
    last_txn_at = db.scalar(
        select(func.max(CashTransaction.txn_date)).where(
            and_(
                CashTransaction.org_id == org_id,
                CashTransaction.property_id == property_id,
            )
        )
    )
    # last_txn_at might be a date; normalize to datetime-ish for consistent serialization
    if isinstance(last_txn_at, date) and not isinstance(last_txn_at, datetime):
        last_txn_at_dt = datetime.combine(last_txn_at, datetime.min.time())
    else:
        last_txn_at_dt = last_txn_at

    last_30 = _sum_cash_window(db, org_id=org_id, property_id=property_id, days=30)
    last_90 = _sum_cash_window(db, org_id=org_id, property_id=property_id, days=90)
    return CashStatus(last_txn_at=last_txn_at_dt, last_30d_net=last_30, last_90d_net=last_90)


def _compute_equity_status(
    db: Session, *, org_id: int, property_id: int
) -> EquityStatus:
    v = db.scalar(
        select(Valuation)
        .where(and_(Valuation.org_id == org_id, Valuation.property_id == property_id))
        .order_by(Valuation.id.desc())
        .limit(1)
    )
    if not v:
        return EquityStatus(
            latest_valuation_at=None,
            estimated_value=None,
            loan_balance=None,
            estimated_equity=None,
        )

    as_of = getattr(v, "as_of", None)
    if isinstance(as_of, datetime):
        as_of_d = as_of.date()
    else:
        as_of_d = as_of if isinstance(as_of, date) else None

    est_value = getattr(v, "estimated_value", None)
    loan = getattr(v, "loan_balance", None)

    eq = None
    try:
        if est_value is not None and loan is not None:
            eq = float(est_value) - float(loan)
    except Exception:
        eq = None

    return EquityStatus(
        latest_valuation_at=as_of_d,
        estimated_value=float(est_value) if est_value is not None else None,
        loan_balance=float(loan) if loan is not None else None,
        estimated_equity=eq,
    )


def derive_stage_and_constraints(
    db: Session, *, org_id: int, property_id: int
) -> Tuple[str, Dict[str, Any], Dict[str, Any], List[str]]:
    """
    Derive:
      - suggested_stage: one of STAGE_ORDER
      - constraints: machine-readable blockers
      - outstanding_tasks: machine-readable "work left"
      - next_actions: human-readable short actions (UI-friendly)
    """
    next_actions: List[str] = []
    constraints: Dict[str, Any] = {}
    tasks: Dict[str, Any] = {}

    deal = _get_latest_deal(db, org_id=org_id, property_id=property_id)
    uw = _get_latest_underwriting(db, org_id=org_id, property_id=property_id)

    checklist = _get_latest_checklist(db, org_id=org_id, property_id=property_id)
    cp = None
    if checklist:
        cp = _compute_checklist_progress(db, org_id=org_id, checklist_id=checklist.id)

    inspection = _get_latest_inspection(db, org_id=org_id, property_id=property_id)
    insp = _compute_inspection_status(db, org_id=org_id, inspection=inspection)

    rehab_open = _compute_rehab_open_count(db, org_id=org_id, property_id=property_id)
    lease_status = _compute_lease_status(db, org_id=org_id, property_id=property_id)
    cash_status = _compute_cash_status(db, org_id=org_id, property_id=property_id)
    equity_status = _compute_equity_status(db, org_id=org_id, property_id=property_id)

    # ---------------------------------------------------------
    # Stage logic (coarse, conservative)
    #
    # The idea:
    #   Deal stage: no deal or underwriting not run.
    #   Rehab stage: deal exists, but rehab tasks exist and not done.
    #   Compliance stage: rehab is basically done, but checklist/inspection not passed.
    #   Tenant stage: compliance passed but no active lease.
    #   Cash stage: active lease but no/low transaction reality.
    #   Equity stage: cash stable and valuations exist.
    # ---------------------------------------------------------

    # 1) Deal prerequisites
    if not deal:
        constraints["missing_deal"] = True
        next_actions.append("Create a deal for this property.")
        suggested = "deal"
        tasks["deal"] = {"missing": True}
        return suggested, constraints, tasks, next_actions

    # Underwriting is not a hard prerequisite for leaving "deal", but it is a practical one.
    if not uw:
        constraints["missing_underwriting"] = True
        next_actions.append("Run evaluate to generate underwriting result.")
        suggested = "deal"
        tasks["deal"] = {"needs_underwriting": True}
        return suggested, constraints, tasks, next_actions

    # If underwriting says REJECT, keep stage at deal (you can override later if you want a "dead" stage)
    decision = str(getattr(uw, "decision", "") or "").upper()
    if decision == "REJECT":
        constraints["rejected_by_underwriting"] = True
        next_actions.append("Underwriting rejected this deal. Adjust inputs or archive.")
        tasks["deal"] = {"rejected": True}
        return "deal", constraints, tasks, next_actions

    # 2) Rehab stage if rehab tasks exist and are not done
    if rehab_open > 0:
        tasks["rehab"] = {"open_tasks": rehab_open}
        next_actions.append(f"Complete rehab tasks ({rehab_open} open).")
        # Even if compliance exists, we don’t advance past rehab until rehab is done.
        suggested = "rehab"
        return suggested, constraints, tasks, next_actions

    # 3) Compliance stage until checklist is mostly done and inspection passes
    if not checklist:
        constraints["missing_checklist"] = True
        next_actions.append("Generate a compliance checklist.")
        tasks["compliance"] = {"needs_checklist": True}
        return "compliance", constraints, tasks, next_actions

    if cp:
        tasks["compliance"] = {
            "checklist": cp.as_dict(),
            "inspection": insp.as_dict(),
        }

        # Hard blockers:
        if cp.failed > 0:
            constraints["checklist_failed_items"] = cp.failed
            next_actions.append(f"Resolve failed checklist items ({cp.failed}).")
        if cp.blocked > 0:
            constraints["checklist_blocked_items"] = cp.blocked
            next_actions.append(f"Unblock checklist items ({cp.blocked}).")
        if cp.pct_done < 0.95:
            # 95% threshold: you can change this (or make it config-driven).
            constraints["checklist_incomplete"] = cp.as_dict()
            remaining = max(0, cp.total - cp.done)
            next_actions.append(f"Work checklist to completion ({remaining} remaining).")

    # Inspection reality:
    # - If there are open failed inspection items, stay in compliance
    # - If no inspection exists, prompt schedule
    if not insp.exists:
        constraints["missing_inspection"] = True
        next_actions.append("Schedule an inspection (none recorded yet).")
        return "compliance", constraints, tasks, next_actions

    if not insp.latest_passed:
        constraints["inspection_not_passed"] = insp.as_dict()
        if insp.open_failed_items > 0:
            next_actions.append(
                f"Fix inspection fail points ({insp.open_failed_items} open)."
            )
        else:
            next_actions.append("Reinspect: latest inspection not marked passed.")
        return "compliance", constraints, tasks, next_actions

    # If we got here, compliance is effectively "ready".
    # 4) Tenant stage until there is an active lease
    if not lease_status.has_active_lease:
        constraints["missing_active_lease"] = True
        next_actions.append("Create/activate a lease (no active lease).")
        tasks["tenant"] = {"needs_lease": True}
        return "tenant", constraints, tasks, next_actions

    # 5) Cash stage until we see transaction reality (or you can require rent payments)
    # For now: if no txns in last 90 days, remain in cash with action.
    tasks["cash"] = {"cash_status": cash_status.as_dict()}
    if cash_status.last_txn_at is None:
        constraints["no_transactions"] = True
        next_actions.append("Add transactions (rent, expenses) to start cash tracking.")
        return "cash", constraints, tasks, next_actions

    # 6) Equity stage requires at least one valuation
    tasks["equity"] = {"equity_status": equity_status.as_dict()}
    if equity_status.latest_valuation_at is None:
        constraints["missing_valuation"] = True
        next_actions.append("Add a valuation to track equity.")
        return "equity", constraints, tasks, next_actions

    # If valuations exist and lease exists and transactions exist, you're at equity stage.
    return "equity", constraints, tasks, next_actions


def sync_property_state(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> PropertyState:
    """
    Public API:
      - derive stage/constraints/tasks
      - advance stage conservatively (never backwards)
      - persist constraints/tasks
    This is what your /ops endpoints should call.
    """
    suggested, constraints, tasks, _next_actions = derive_stage_and_constraints(
        db, org_id=org_id, property_id=property_id
    )

    # Always update constraints/tasks, stage advances only if higher
    row = advance_stage_if_needed(
        db,
        org_id=org_id,
        property_id=property_id,
        suggested_stage=suggested,
        constraints=constraints,
        outstanding_tasks=tasks,
    )
    return row


def get_state_payload(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    recompute: bool = True,
) -> Dict[str, Any]:
    """
    Helper for ops endpoints:
      - returns a JSON-friendly payload of current stage + constraints + tasks
      - can recompute (sync) first for freshness
    """
    row = sync_property_state(db, org_id=org_id, property_id=property_id) if recompute else ensure_state_row(
        db, org_id=org_id, property_id=property_id
    )

    constraints = _loads_json(getattr(row, "constraints_json", None))
    tasks = _loads_json(getattr(row, "outstanding_tasks_json", None))

    # Also return human next_actions derived from current signals (UI-friendly)
    suggested, constraints_live, tasks_live, next_actions = derive_stage_and_constraints(
        db, org_id=org_id, property_id=property_id
    )

    # If recompute=False, these may differ; recompute=True keeps them aligned.
    # We choose to trust the "live" next_actions always.
    return {
        "property_id": property_id,
        "current_stage": _clamp_stage(getattr(row, "current_stage", "deal")),
        "suggested_stage": suggested,
        "constraints": constraints if not recompute else constraints_live,
        "outstanding_tasks": tasks if not recompute else tasks_live,
        "next_actions": next_actions,
        "updated_at": getattr(row, "updated_at", None).isoformat()
        if getattr(row, "updated_at", None)
        else None,
    }
