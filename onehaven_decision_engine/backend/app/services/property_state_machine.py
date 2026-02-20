# backend/app/services/property_state_machine.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, desc, func, select
from sqlalchemy.orm import Session

from ..models import (
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
)

STAGE_ORDER = ["deal", "rehab", "compliance", "tenant", "cash", "equity"]


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
      - only updates to higher-ranked stage
      - always updates constraints/tasks if provided
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
# Derivation helpers
# -----------------------------------------------------------------------------
def _get_latest_deal(db: Session, *, org_id: int, property_id: int) -> Optional[Deal]:
    return db.scalar(
        select(Deal)
        .where(and_(Deal.org_id == org_id, Deal.property_id == property_id))
        .order_by(desc(Deal.id))
        .limit(1)
    )


def _get_latest_underwriting(db: Session, *, org_id: int, property_id: int) -> Optional[UnderwritingResult]:
    return db.scalar(
        select(UnderwritingResult)
        .join(Deal, Deal.id == UnderwritingResult.deal_id)
        .where(UnderwritingResult.org_id == org_id, Deal.property_id == property_id)
        .order_by(desc(UnderwritingResult.created_at), desc(UnderwritingResult.id))
        .limit(1)
    )


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

    return ChecklistProgress(total=total, todo=todo, in_progress=inprog, blocked=blocked, failed=failed, done=done)


def _get_latest_inspection(db: Session, *, property_id: int) -> Optional[Inspection]:
    return db.scalar(
        select(Inspection)
        .where(Inspection.property_id == property_id)
        .order_by(desc(Inspection.inspection_date), desc(Inspection.id))
        .limit(1)
    )


def _open_failed_inspection_items(db: Session, *, property_id: int) -> int:
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


def _compute_inspection_status(db: Session, *, property_id: int) -> InspectionStatus:
    insp = _get_latest_inspection(db, property_id=property_id)
    if not insp:
        return InspectionStatus(exists=False, latest_passed=False, open_failed_items=0)

    open_failed = _open_failed_inspection_items(db, property_id=property_id)
    passed = bool(getattr(insp, "passed", False))
    if open_failed > 0:
        passed = False

    return InspectionStatus(exists=True, latest_passed=passed, open_failed_items=open_failed)


def _rehab_open_count(db: Session, *, org_id: int, property_id: int) -> int:
    return int(
        db.scalar(
            select(func.count(RehabTask.id)).where(
                RehabTask.org_id == org_id,
                RehabTask.property_id == property_id,
                func.lower(RehabTask.status) != "done",
            )
        )
        or 0
    )


def _has_active_lease(db: Session, *, org_id: int, property_id: int) -> bool:
    """
    IMPORTANT FIX:
    - Lease.start_date / end_date are DateTime in models.
    - The previous version compared them to a date(), which can break on some DBs/drivers.
    We compare using datetime bounds instead.
    """
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


def _latest_valuation(db: Session, *, org_id: int, property_id: int) -> Optional[Valuation]:
    return db.scalar(
        select(Valuation)
        .where(Valuation.org_id == org_id, Valuation.property_id == property_id)
        .order_by(desc(Valuation.as_of), desc(Valuation.id))
        .limit(1)
    )


# -----------------------------------------------------------------------------
# Main derivation
# -----------------------------------------------------------------------------
def derive_stage_and_constraints(
    db: Session, *, org_id: int, property_id: int
) -> Tuple[str, Dict[str, Any], Dict[str, Any], List[str]]:
    """
    Returns:
      (suggested_stage, constraints_dict, tasks_dict, next_actions_human)
    """
    next_actions: List[str] = []
    constraints: Dict[str, Any] = {}
    tasks: Dict[str, Any] = {}

    deal = _get_latest_deal(db, org_id=org_id, property_id=property_id)
    uw = _get_latest_underwriting(db, org_id=org_id, property_id=property_id)

    checklist = _compute_checklist_progress(db, org_id=org_id, property_id=property_id)
    insp = _compute_inspection_status(db, property_id=property_id)
    rehab_open = _rehab_open_count(db, org_id=org_id, property_id=property_id)
    has_lease = _has_active_lease(db, org_id=org_id, property_id=property_id)
    last_txn = _last_txn_date(db, org_id=org_id, property_id=property_id)
    val = _latest_valuation(db, org_id=org_id, property_id=property_id)

    # Stage 1: deal prerequisites
    if not deal:
        constraints["missing_deal"] = True
        tasks["deal"] = {"missing": True}
        next_actions.append("Create a deal for this property.")
        return "deal", constraints, tasks, next_actions

    if not uw:
        constraints["missing_underwriting"] = True
        tasks["deal"] = {"needs_underwriting": True}
        next_actions.append("Run evaluate to generate underwriting result.")
        return "deal", constraints, tasks, next_actions

    decision = str(getattr(uw, "decision", "") or "").upper().strip()
    if decision == "REJECT":
        constraints["rejected_by_underwriting"] = True
        tasks["deal"] = {"rejected": True}
        next_actions.append("Underwriting rejected this deal. Adjust inputs or archive.")
        return "deal", constraints, tasks, next_actions

    # Stage 2: rehab if rehab tasks open
    if rehab_open > 0:
        tasks["rehab"] = {"open_tasks": rehab_open}
        next_actions.append(f"Complete rehab tasks ({rehab_open} open).")
        return "rehab", constraints, tasks, next_actions

    # Stage 3: compliance until checklist + inspection pass
    if checklist.total == 0:
        constraints["missing_checklist"] = True
        tasks["compliance"] = {"needs_checklist": True}
        next_actions.append("Generate compliance checklist (no checklist items found).")
        return "compliance", constraints, tasks, next_actions

    tasks["compliance"] = {"checklist": checklist.as_dict(), "inspection": insp.as_dict()}

    if checklist.failed > 0:
        constraints["checklist_failed_items"] = checklist.failed
        next_actions.append(f"Resolve failed checklist items ({checklist.failed}).")

    if checklist.blocked > 0:
        constraints["checklist_blocked_items"] = checklist.blocked
        next_actions.append(f"Unblock checklist items ({checklist.blocked}).")

    if checklist.pct_done < 0.95:
        constraints["checklist_incomplete"] = checklist.as_dict()
        remaining = max(0, checklist.total - checklist.done)
        next_actions.append(f"Work checklist to completion ({remaining} remaining).")

    if not insp.exists:
        constraints["missing_inspection"] = True
        next_actions.append("Create first inspection record (or schedule inspection).")
        return "compliance", constraints, tasks, next_actions

    if not insp.latest_passed:
        constraints["inspection_not_passed"] = insp.as_dict()
        if insp.open_failed_items > 0:
            next_actions.append(f"Resolve {insp.open_failed_items} inspection fail items.")
        else:
            next_actions.append("Mark inspection passed or schedule reinspect.")
        return "compliance", constraints, tasks, next_actions

    # Stage 4: tenant until active lease exists
    if not has_lease:
        constraints["missing_active_lease"] = True
        tasks["tenant"] = {"needs_lease": True}
        next_actions.append("Create/activate a lease (no active lease).")
        return "tenant", constraints, tasks, next_actions

    # Stage 5: cash until transactions exist
    tasks["cash"] = {"last_txn_date": last_txn.isoformat() if last_txn else None}
    if last_txn is None:
        constraints["no_transactions"] = True
        next_actions.append("Add transactions (rent, expenses) to start cash tracking.")
        return "cash", constraints, tasks, next_actions

    # Stage 6: equity until valuations exist
    tasks["equity"] = {"has_valuation": val is not None}
    if val is None:
        constraints["missing_valuation"] = True
        next_actions.append("Add a valuation snapshot to track equity.")
        return "equity", constraints, tasks, next_actions

    return "equity", constraints, tasks, next_actions


def sync_property_state(db: Session, *, org_id: int, property_id: int) -> PropertyState:
    suggested, constraints, tasks, _ = derive_stage_and_constraints(db, org_id=org_id, property_id=property_id)
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
    """
    UI-friendly payload: includes next_actions.
    """
    row = sync_property_state(db, org_id=org_id, property_id=property_id) if recompute else ensure_state_row(
        db, org_id=org_id, property_id=property_id
    )

    suggested, constraints_live, tasks_live, next_actions = derive_stage_and_constraints(
        db, org_id=org_id, property_id=property_id
    )

    return {
        "property_id": property_id,
        "current_stage": _clamp_stage(getattr(row, "current_stage", "deal")),
        "suggested_stage": suggested,
        "constraints": constraints_live if recompute else _loads_json(getattr(row, "constraints_json", None)),
        "outstanding_tasks": tasks_live if recompute else _loads_json(getattr(row, "outstanding_tasks_json", None)),
        "next_actions": next_actions,
        "updated_at": getattr(row, "updated_at", None).isoformat() if getattr(row, "updated_at", None) else None,
    }


# -----------------------------------------------------------------------------
# Backward-compatible function used by ops.py
# -----------------------------------------------------------------------------
def compute_and_persist_stage(db: Session, *, org_id: int, property: Property) -> PropertyState:
    """
    This is the exact function ops.py imports.
    It recomputes and persists stage/constraints/tasks.
    """
    row = sync_property_state(db, org_id=org_id, property_id=property.id)
    db.commit()
    db.refresh(row)
    return row