from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.models import (
    Deal,
    Lease,
    Property,
    PropertyState,
    RehabTask,
    Transaction,
    UnderwritingResult,
    Valuation,
)


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


def _apply_property_filters(
    stmt,
    *,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    q: Optional[str] = None,
    only_red_zone: bool = False,
    exclude_red_zone: bool = False,
    min_crime_score: Optional[float] = None,
    max_crime_score: Optional[float] = None,
    min_offender_count: Optional[int] = None,
    max_offender_count: Optional[int] = None,
):
    if state:
        stmt = stmt.where(Property.state == state)
    if county:
        stmt = stmt.where(func.lower(Property.county) == county.lower())
    if city:
        stmt = stmt.where(func.lower(Property.city) == city.lower())

    if q:
        like = f"%{q.strip().lower()}%"
        stmt = stmt.where(
            func.lower(
                func.concat(
                    Property.address,
                    " ",
                    Property.city,
                    " ",
                    Property.state,
                    " ",
                    Property.zip,
                )
            ).like(like)
        )

    if only_red_zone:
        stmt = stmt.where(Property.is_red_zone.is_(True))
    elif exclude_red_zone:
        stmt = stmt.where(
            (Property.is_red_zone.is_(False)) | (Property.is_red_zone.is_(None))
        )

    if min_crime_score is not None:
        stmt = stmt.where(Property.crime_score.is_not(None))
        stmt = stmt.where(Property.crime_score >= float(min_crime_score))

    if max_crime_score is not None:
        stmt = stmt.where(Property.crime_score.is_not(None))
        stmt = stmt.where(Property.crime_score <= float(max_crime_score))

    if min_offender_count is not None:
        stmt = stmt.where(Property.offender_count.is_not(None))
        stmt = stmt.where(Property.offender_count >= int(min_offender_count))

    if max_offender_count is not None:
        stmt = stmt.where(Property.offender_count.is_not(None))
        stmt = stmt.where(Property.offender_count <= int(max_offender_count))

    return stmt


def compute_rollups(
    db: Session,
    *,
    org_id: int,
    days: int = 90,
    limit: int = 50,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    q: Optional[str] = None,
    stage: Optional[str] = None,
    only_red_zone: bool = False,
    exclude_red_zone: bool = False,
    min_crime_score: Optional[float] = None,
    max_crime_score: Optional[float] = None,
    min_offender_count: Optional[int] = None,
    max_offender_count: Optional[int] = None,
) -> dict[str, Any]:
    now = datetime.utcnow()
    since = now - timedelta(days=int(days))

    base_prop_stmt = select(Property).where(Property.org_id == org_id)
    base_prop_stmt = _apply_property_filters(
        base_prop_stmt,
        state=state,
        county=county,
        city=city,
        q=q,
        only_red_zone=only_red_zone,
        exclude_red_zone=exclude_red_zone,
        min_crime_score=min_crime_score,
        max_crime_score=max_crime_score,
        min_offender_count=min_offender_count,
        max_offender_count=max_offender_count,
    )

    props = db.scalars(
        base_prop_stmt.order_by(desc(Property.id)).limit(int(max(limit, 500)))
    ).all()

    if stage:
        state_rows = db.execute(
            select(PropertyState.property_id, PropertyState.current_stage).where(
                PropertyState.org_id == org_id
            )
        ).all()
        stage_map = {int(pid): (st or "").lower() for pid, st in state_rows}
        props = [p for p in props if stage_map.get(int(p.id), "") == stage.lower()]

    prop_ids = [int(p.id) for p in props]

    property_count = len(prop_ids)

    if not prop_ids:
        return {
            "ok": True,
            "as_of": now.isoformat(),
            "window_days": int(days),
            "filters": {
                "state": state,
                "county": county,
                "city": city,
                "q": q,
                "stage": stage,
                "only_red_zone": only_red_zone,
                "exclude_red_zone": exclude_red_zone,
                "min_crime_score": min_crime_score,
                "max_crime_score": max_crime_score,
                "min_offender_count": min_offender_count,
                "max_offender_count": max_offender_count,
            },
            "counts": {
                "properties": 0,
                "deals": 0,
                "rehab_tasks_total": 0,
                "rehab_tasks_open": 0,
                "transactions_window": 0,
                "valuations": 0,
            },
            "buckets": {
                "decisions": {},
                "stages": {},
            },
            "sums": {
                "rehab_open_cost_estimate": 0.0,
                "net_cash_window": 0.0,
            },
            "latest": {
                "valuation": None,
            },
            "properties": [],
        }

    deal_count = db.scalar(
        select(func.count())
        .select_from(Deal)
        .where(Deal.org_id == org_id)
        .where(Deal.property_id.in_(prop_ids))
    ) or 0

    decision_buckets: dict[str, int] = {}
    try:
        rows = db.execute(
            select(UnderwritingResult.decision, func.count())
            .join(Deal, Deal.id == UnderwritingResult.deal_id)
            .where(UnderwritingResult.org_id == org_id)
            .where(Deal.org_id == org_id)
            .where(Deal.property_id.in_(prop_ids))
            .group_by(UnderwritingResult.decision)
        ).all()
        decision_buckets = {str(k): int(v) for (k, v) in rows if k is not None}
    except Exception:
        decision_buckets = {}

    rehab_total = db.scalar(
        select(func.count())
        .select_from(RehabTask)
        .where(RehabTask.org_id == org_id)
        .where(RehabTask.property_id.in_(prop_ids))
    ) or 0

    rehab_open = 0
    rehab_cost_open = 0.0
    try:
        rehab_open = db.scalar(
            select(func.count())
            .select_from(RehabTask)
            .where(RehabTask.org_id == org_id)
            .where(RehabTask.property_id.in_(prop_ids))
            .where(RehabTask.status.in_(["todo", "in_progress"]))
        ) or 0

        rehab_cost_open = _num(
            db.scalar(
                select(func.coalesce(func.sum(RehabTask.cost_estimate), 0.0))
                .where(RehabTask.org_id == org_id)
                .where(RehabTask.property_id.in_(prop_ids))
                .where(RehabTask.status.in_(["todo", "in_progress"]))
            )
        )
    except Exception:
        rehab_open = 0
        rehab_cost_open = 0.0

    txn_count = 0
    net_cash = 0.0
    try:
        txn_count = db.scalar(
            select(func.count())
            .select_from(Transaction)
            .where(Transaction.org_id == org_id)
            .where(Transaction.property_id.in_(prop_ids))
            .where(Transaction.txn_date >= since)
        ) or 0

        net_cash = _num(
            db.scalar(
                select(func.coalesce(func.sum(Transaction.amount), 0.0))
                .where(Transaction.org_id == org_id)
                .where(Transaction.property_id.in_(prop_ids))
                .where(Transaction.txn_date >= since)
            )
        )
    except Exception:
        txn_count = 0
        net_cash = 0.0

    valuation_count = 0
    latest_valuation = None
    try:
        valuation_count = db.scalar(
            select(func.count())
            .select_from(Valuation)
            .where(Valuation.org_id == org_id)
            .where(Valuation.property_id.in_(prop_ids))
        ) or 0

        latest_valuation = db.scalar(
            select(Valuation)
            .where(Valuation.org_id == org_id)
            .where(Valuation.property_id.in_(prop_ids))
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
            "estimated_value": getattr(latest_valuation, "estimated_value", None),
            "loan_balance": getattr(latest_valuation, "loan_balance", None),
            "notes": getattr(latest_valuation, "notes", None),
        }

    stage_buckets: dict[str, int] = {}
    try:
        stage_rows = db.execute(
            select(PropertyState.current_stage, func.count())
            .where(PropertyState.org_id == org_id)
            .where(PropertyState.property_id.in_(prop_ids))
            .group_by(PropertyState.current_stage)
        ).all()
        stage_buckets = {str(k): int(v) for (k, v) in stage_rows if k is not None}
    except Exception:
        stage_buckets = {}

    property_rows = []
    for p in props[: int(limit)]:
        property_rows.append(
            {
                "id": getattr(p, "id", None),
                "address": getattr(p, "address", None),
                "city": getattr(p, "city", None),
                "state": getattr(p, "state", None),
                "county": getattr(p, "county", None),
                "zip": getattr(p, "zip", None),
                "bedrooms": getattr(p, "bedrooms", None),
                "bathrooms": getattr(p, "bathrooms", None),
                "square_feet": getattr(p, "square_feet", None),
                "crime_score": getattr(p, "crime_score", None),
                "offender_count": getattr(p, "offender_count", None),
                "is_red_zone": getattr(p, "is_red_zone", None),
                "created_at": getattr(p, "created_at", None),
            }
        )

    return {
        "ok": True,
        "as_of": now.isoformat(),
        "window_days": int(days),
        "filters": {
            "state": state,
            "county": county,
            "city": city,
            "q": q,
            "stage": stage,
            "only_red_zone": only_red_zone,
            "exclude_red_zone": exclude_red_zone,
            "min_crime_score": min_crime_score,
            "max_crime_score": max_crime_score,
            "min_offender_count": min_offender_count,
            "max_offender_count": max_offender_count,
        },
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
