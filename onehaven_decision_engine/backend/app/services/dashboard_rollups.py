from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.models import (
    Property,
    Deal,
    UnderwritingResult,
    RehabTask,
    Lease,
    Transaction,
    Valuation,
    PropertyState,
)


def _dt(v: Any) -> Optional[datetime]:
    return v if isinstance(v, datetime) else None


def _num(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _as_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def compute_rollups(
    db: Session,
    *,
    org_id: int,
    days: int = 90,
    limit: int = 50,
) -> dict[str, Any]:
    """
    Dashboard rollups (Phase 4 ops visibility) used by routers/dashboard.py.

    Design goals:
    - Deterministic.
    - Defensive against partial schemas.
    - Cheap-ish queries (no N+1).
    - Returns a simple dict for JSON response.

    The dashboard page typically wants:
      - portfolio counts
      - recent activity
      - rehab backlog summary
      - cashflow snapshot (last N days)
      - equity snapshot (latest valuations)
      - stage distribution (if PropertyState exists)
    """
    now = datetime.utcnow()
    since = now - timedelta(days=int(days))

    # --- Property counts ---
    property_count = db.scalar(
        select(func.count()).select_from(Property).where(Property.org_id == org_id)
    ) or 0

    # --- Deals ---
    deal_count = db.scalar(
        select(func.count()).select_from(Deal).where(Deal.org_id == org_id)
    ) or 0

    # --- Latest underwriting results (count by decision) ---
    # If your schema doesn’t have decision field, this still won’t crash; we just return empty buckets.
    decision_buckets: dict[str, int] = {}
    try:
        rows = db.execute(
            select(UnderwritingResult.decision, func.count())
            .where(UnderwritingResult.org_id == org_id)
            .group_by(UnderwritingResult.decision)
        ).all()
        decision_buckets = {str(k): int(v) for (k, v) in rows if k is not None}
    except Exception:
        decision_buckets = {}

    # --- Rehab backlog ---
    rehab_total = db.scalar(
        select(func.count())
        .select_from(RehabTask)
        .where(RehabTask.org_id == org_id)
    ) or 0

    rehab_open = 0
    rehab_cost_open = 0.0
    try:
        rehab_open = db.scalar(
            select(func.count())
            .select_from(RehabTask)
            .where(RehabTask.org_id == org_id)
            .where(RehabTask.status.in_(["todo", "in_progress"]))
        ) or 0

        rehab_cost_open = _num(
            db.scalar(
                select(func.coalesce(func.sum(RehabTask.cost_estimate), 0.0))
                .where(RehabTask.org_id == org_id)
                .where(RehabTask.status.in_(["todo", "in_progress"]))
            )
        )
    except Exception:
        rehab_open = 0
        rehab_cost_open = 0.0

    # --- Cashflow in the last N days ---
    txn_count = 0
    net_cash = 0.0
    try:
        txn_count = db.scalar(
            select(func.count())
            .select_from(Transaction)
            .where(Transaction.org_id == org_id)
            .where(Transaction.occurred_at >= since)
        ) or 0

        # Convention:
        # - income positive
        # - expense negative
        # If your data uses separate type fields, you can refine later.
        net_cash = _num(
            db.scalar(
                select(func.coalesce(func.sum(Transaction.amount), 0.0))
                .where(Transaction.org_id == org_id)
                .where(Transaction.occurred_at >= since)
            )
        )
    except Exception:
        txn_count = 0
        net_cash = 0.0

    # --- Equity snapshot: latest valuation per property ---
    # Simple version: count valuations and latest valuation overall.
    valuation_count = 0
    latest_valuation = None
    try:
        valuation_count = db.scalar(
            select(func.count())
            .select_from(Valuation)
            .where(Valuation.org_id == org_id)
        ) or 0

        latest_valuation = db.scalar(
            select(Valuation)
            .where(Valuation.org_id == org_id)
            .order_by(Valuation.as_of.desc(), Valuation.id.desc())
            .limit(1)
        )
    except Exception:
        valuation_count = 0
        latest_valuation = None

    latest_valuation_out = None
    if latest_valuation is not None:
        latest_valuation_out = {
            "property_id": getattr(latest_valuation, "property_id", None),
            "as_of": getattr(latest_valuation, "as_of", None),
            "value": getattr(latest_valuation, "value", None),
            "source": getattr(latest_valuation, "source", None),
        }

    # --- Stage distribution (if PropertyState exists) ---
    stage_buckets: dict[str, int] = {}
    try:
        stage_rows = db.execute(
            select(PropertyState.current_stage, func.count())
            .where(PropertyState.org_id == org_id)
            .group_by(PropertyState.current_stage)
        ).all()
        stage_buckets = {str(k): int(v) for (k, v) in stage_rows if k is not None}
    except Exception:
        stage_buckets = {}

    # --- Recent properties list (used in dashboard tiles/cards) ---
    props = db.scalars(
        select(Property)
        .where(Property.org_id == org_id)
        .order_by(Property.id.desc())
        .limit(int(limit))
    ).all()

    property_rows = []
    for p in props:
        property_rows.append(
            {
                "id": getattr(p, "id", None),
                "address": getattr(p, "address", None),
                "city": getattr(p, "city", None),
                "state": getattr(p, "state", None),
                "zip": getattr(p, "zip", None),
                "bedrooms": getattr(p, "bedrooms", None),
                "bathrooms": getattr(p, "bathrooms", None),
                "sqft": getattr(p, "sqft", None),
                "created_at": getattr(p, "created_at", None),
            }
        )

    return {
        "ok": True,
        "as_of": now.isoformat(),
        "window_days": int(days),
        "counts": {
            "properties": int(property_count),
            "deals": int(deal_count),
            "rehab_tasks_total": int(rehab_total),
            "rehab_tasks_open": int(rehab_open),
            "transactions_window": int(txn_count),
            "valuations": int(valuation_count),
        },
        "buckets": {
            "decisions": decision_buckets,
            "stages": stage_buckets,
        },
        "sums": {
            "rehab_open_cost_estimate": rehab_cost_open,
            "net_cash_window": net_cash,
        },
        "latest": {
            "valuation": latest_valuation_out,
        },
        "properties": property_rows,
    }