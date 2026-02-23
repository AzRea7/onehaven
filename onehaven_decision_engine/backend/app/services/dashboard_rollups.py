# backend/app/services/dashboard_rollups.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from ..models import Lease, Property, RehabTask, Transaction, Valuation, PropertyChecklistItem, Inspection


@dataclass(frozen=True)
class PortfolioRollup:
    properties: int
    compliance_counts: dict[str, int]
    rehab_open_tasks: int
    rehab_blocked_tasks: int
    cash_last_30_net: float
    rent_expected_last_30: float
    rent_collected_last_30: float
    valuation_due: int


def _today_utc_date() -> date:
    return datetime.utcnow().date()


def _last_30_window() -> tuple[date, date]:
    end = _today_utc_date()
    start = end - timedelta(days=30)
    return start, end


def _active_lease_monthly_rent(db: Session, *, org_id: int, property_id: int, as_of: date) -> float:
    """
    Uses your leases table as the source of expected rent.

    Assumptions consistent with your schema/migrations:
    - Lease.start_date exists
    - Lease.end_date may be null
    - Lease.total_rent is a monthly rent number (common in your repo naming)
    """
    lease = db.scalar(
        select(Lease)
        .where(Lease.org_id == org_id, Lease.property_id == property_id)
        .where(Lease.start_date <= as_of)
        .where((Lease.end_date.is_(None)) | (Lease.end_date >= as_of))
        .order_by(desc(Lease.id))
        .limit(1)
    )
    if not lease:
        return 0.0
    try:
        return float(lease.total_rent or 0.0)
    except Exception:
        return 0.0


def _rent_collected_last_30(db: Session, *, org_id: int, property_id: int, start: date, end: date) -> float:
    """
    Transactions are the source of collected rent.
    This assumes:
    - Transaction.txn_date exists
    - Transaction.txn_type exists (you already index it in migrations)
    - Transaction.amount exists
    """
    s = db.scalar(
        select(func.coalesce(func.sum(Transaction.amount), 0.0))
        .where(Transaction.org_id == org_id, Transaction.property_id == property_id)
        .where(Transaction.txn_date >= start, Transaction.txn_date <= end)
        .where(func.lower(Transaction.txn_type) == "rent")
    )
    try:
        return float(s or 0.0)
    except Exception:
        return 0.0


def _net_last_30(db: Session, *, org_id: int, start: date, end: date) -> float:
    """
    Portfolio net = sum(amount) over last 30 days.
    Your system can treat expenses as negative rows or separate txn_type conventions.
    We do not guess sign; we sum amounts exactly as stored.
    """
    s = db.scalar(
        select(func.coalesce(func.sum(Transaction.amount), 0.0))
        .where(Transaction.org_id == org_id)
        .where(Transaction.txn_date >= start, Transaction.txn_date <= end)
    )
    try:
        return float(s or 0.0)
    except Exception:
        return 0.0


def _valuation_is_due(db: Session, *, org_id: int, property_id: int, as_of: date, cadence_days: int = 180) -> bool:
    """
    Enforced valuation cadence (Phase 4 DoD):
    - If no valuation exists -> due
    - If latest valuation.as_of older than cadence_days -> due
    """
    v = db.scalar(
        select(Valuation)
        .where(Valuation.org_id == org_id, Valuation.property_id == property_id)
        .order_by(desc(Valuation.as_of))
        .limit(1)
    )
    if not v or not getattr(v, "as_of", None):
        return True
    try:
        last = v.as_of
        if isinstance(last, datetime):
            last = last.date()
        age = (as_of - last).days
        return age >= cadence_days
    except Exception:
        return True


def compute_portfolio_rollup(db: Session, *, org_id: int, state: str = "MI", limit: int = 2000) -> dict[str, Any]:
    """
    Phase 4: portfolio-grade rollups (single source of truth inputs):
    - compliance status distribution
    - rehab open/blocked
    - cash last 30 net
    - expected vs collected rent last 30
    - valuation cadence due count
    """
    prop_ids = [
        r[0]
        for r in db.execute(
            select(Property.id)
            .where(Property.org_id == org_id, Property.state == state)
            .order_by(desc(Property.id))
            .limit(limit)
        ).all()
    ]

    start, end = _last_30_window()

    # Compliance counts
    # We define compliance as:
    # - "passed" if latest inspection passed AND no failed checklist items
    # - "failing" if latest inspection failed OR any failed items
    # - "unknown" if no inspection + no checklist items
    compliance = {"passed": 0, "failing": 0, "unknown": 0}

    rehab_open = 0
    rehab_blocked = 0

    rent_expected = 0.0
    rent_collected = 0.0

    valuation_due = 0
    as_of = _today_utc_date()

    for pid in prop_ids:
        # ---- Compliance ----
        latest_insp = db.scalar(
            select(Inspection)
            .where(Inspection.org_id == org_id, Inspection.property_id == pid)
            .order_by(desc(Inspection.id))
            .limit(1)
        )
        failed_items = db.scalar(
            select(func.count())
            .select_from(PropertyChecklistItem)
            .where(PropertyChecklistItem.org_id == org_id, PropertyChecklistItem.property_id == pid)
            .where(func.lower(PropertyChecklistItem.status) == "failed")
        )

        has_any_checklist = db.scalar(
            select(func.count())
            .select_from(PropertyChecklistItem)
            .where(PropertyChecklistItem.org_id == org_id, PropertyChecklistItem.property_id == pid)
        )

        insp_pass = bool(getattr(latest_insp, "passed", False)) if latest_insp else False

        if (has_any_checklist or 0) == 0 and not latest_insp:
            compliance["unknown"] += 1
        elif insp_pass and (failed_items or 0) == 0:
            compliance["passed"] += 1
        else:
            compliance["failing"] += 1

        # ---- Rehab tasks ----
        open_count = db.scalar(
            select(func.count())
            .select_from(RehabTask)
            .where(RehabTask.org_id == org_id, RehabTask.property_id == pid)
            .where(func.lower(RehabTask.status).in_(["todo", "in_progress"]))
        )
        blocked_count = db.scalar(
            select(func.count())
            .select_from(RehabTask)
            .where(RehabTask.org_id == org_id, RehabTask.property_id == pid)
            .where(func.lower(RehabTask.status) == "blocked")
        )
        rehab_open += int(open_count or 0)
        rehab_blocked += int(blocked_count or 0)

        # ---- Cash reconciliation ----
        monthly = _active_lease_monthly_rent(db, org_id=org_id, property_id=pid, as_of=as_of)
        rent_expected += float(monthly or 0.0)  # "expected this month-ish"
        rent_collected += _rent_collected_last_30(db, org_id=org_id, property_id=pid, start=start, end=end)

        # ---- Valuation cadence ----
        if _valuation_is_due(db, org_id=org_id, property_id=pid, as_of=as_of, cadence_days=180):
            valuation_due += 1

    net_30 = _net_last_30(db, org_id=org_id, start=start, end=end)

    return {
        "properties": len(prop_ids),
        "compliance_counts": compliance,
        "rehab": {"open_tasks": rehab_open, "blocked_tasks": rehab_blocked},
        "cash": {
            "net_last_30": round(float(net_30 or 0.0), 2),
            "rent_expected_proxy": round(float(rent_expected or 0.0), 2),
            "rent_collected_last_30": round(float(rent_collected or 0.0), 2),
            "rent_gap": round(float((rent_expected or 0.0) - (rent_collected or 0.0)), 2),
        },
        "equity": {"valuation_due_count": int(valuation_due)},
        "window": {"start": str(start), "end": str(end)},
    }
