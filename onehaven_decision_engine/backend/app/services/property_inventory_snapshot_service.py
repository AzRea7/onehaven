from __future__ import annotations

import logging
import time
from typing import Any, Optional

from sqlalchemy import desc, func, or_, select
from sqlalchemy.orm import Session

from ..models import Deal, Property, UnderwritingResult
from ..services.property_state_machine import get_state_payload
from ..services.runtime_metrics import METRICS

log = logging.getLogger("onehaven.inventory_snapshot")


def _safe_float(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def _asking_price(prop: Property, deal: Deal | None) -> float | None:
    for attr in ("asking_price", "list_price", "price", "offer_price", "purchase_price"):
        if deal is not None and getattr(deal, attr, None) is not None:
            return _safe_float(getattr(deal, attr, None))
    for attr in ("asking_price", "list_price", "price"):
        if getattr(prop, attr, None) is not None:
            return _safe_float(getattr(prop, attr, None))
    return None


def _latest_deal(db: Session, *, org_id: int, property_id: int) -> Deal | None:
    return db.scalar(
        select(Deal)
        .where(Deal.org_id == org_id, Deal.property_id == property_id)
        .order_by(desc(Deal.updated_at), desc(Deal.id))
        .limit(1)
    )


def _latest_uw(db: Session, *, org_id: int, property_id: int) -> UnderwritingResult | None:
    return db.scalar(
        select(UnderwritingResult)
        .join(Deal, Deal.id == UnderwritingResult.deal_id)
        .where(UnderwritingResult.org_id == org_id, Deal.property_id == property_id)
        .order_by(desc(UnderwritingResult.created_at), desc(UnderwritingResult.id))
        .limit(1)
    )


def _normalized_query_stmt(
    *,
    org_id: int,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    q: Optional[str] = None,
    assigned_user_id: Optional[int] = None,
):
    stmt = select(Property).where(Property.org_id == org_id)

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
                    func.coalesce(Property.address, ""),
                    " ",
                    func.coalesce(Property.city, ""),
                    " ",
                    func.coalesce(Property.state, ""),
                    " ",
                    func.coalesce(Property.zip, ""),
                )
            ).like(like)
        )

    if assigned_user_id is not None:
        candidate_columns = [
            "assigned_user_id",
            "owner_user_id",
            "manager_user_id",
            "agent_user_id",
            "acquisition_user_id",
        ]
        clauses = []
        for col_name in candidate_columns:
            if hasattr(Property, col_name):
                clauses.append(getattr(Property, col_name) == assigned_user_id)
        if clauses:
            stmt = stmt.where(or_(*clauses))

    return stmt.order_by(desc(Property.id))


def infer_snapshot_completeness(snapshot: dict[str, Any]) -> str:
    strong_signals = [
        snapshot.get("asking_price") is not None,
        snapshot.get("market_rent_estimate") is not None,
        snapshot.get("projected_monthly_cashflow") is not None,
        snapshot.get("dscr") is not None,
        bool(snapshot.get("normalized_address")),
        snapshot.get("lat") is not None and snapshot.get("lng") is not None,
    ]
    count = len([x for x in strong_signals if x])

    if count == len(strong_signals):
        return "COMPLETE"
    if count >= 3:
        return "PARTIAL"
    return "MISSING"


def build_property_inventory_snapshot(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    prop = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    if prop is None:
        raise ValueError("property not found")

    deal = _latest_deal(db, org_id=org_id, property_id=property_id)
    uw = _latest_uw(db, org_id=org_id, property_id=property_id)
    state_payload = get_state_payload(db, org_id=org_id, property_id=property_id, recompute=False)

    rent_assumption = getattr(prop, "rent_assumption", None)
    if isinstance(rent_assumption, list):
        rent_row = rent_assumption[0] if rent_assumption else None
    else:
        rent_row = rent_assumption

    snapshot = {
        "property_id": int(prop.id),
        "org_id": int(org_id),
        "address": getattr(prop, "address", None),
        "normalized_address": getattr(prop, "normalized_address", None),
        "city": getattr(prop, "city", None),
        "county": getattr(prop, "county", None),
        "state": getattr(prop, "state", None),
        "zip": getattr(prop, "zip", None),
        "lat": _safe_float(getattr(prop, "lat", None)),
        "lng": _safe_float(getattr(prop, "lng", None)),
        "geocode_confidence": _safe_float(getattr(prop, "geocode_confidence", None)),
        "crime_score": _safe_float(getattr(prop, "crime_score", None)),
        "offender_count": _safe_int(getattr(prop, "offender_count", None), 0),
        "is_red_zone": bool(getattr(prop, "is_red_zone", False)),
        "property_type": getattr(prop, "property_type", None),
        "bedrooms": _safe_int(getattr(prop, "bedrooms", None), 0),
        "bathrooms": _safe_float(getattr(prop, "bathrooms", None)),
        "square_feet": _safe_float(getattr(prop, "square_feet", None)),
        "asking_price": _asking_price(prop, deal),
        "market_rent_estimate": _safe_float(getattr(rent_row, "market_rent_estimate", None)) if rent_row else None,
        "approved_rent_ceiling": _safe_float(getattr(rent_row, "approved_rent_ceiling", None)) if rent_row else None,
        "section8_fmr": _safe_float(getattr(rent_row, "section8_fmr", None)) if rent_row else None,
        "projected_monthly_cashflow": _safe_float(getattr(uw, "cash_flow", None)) if uw else None,
        "dscr": _safe_float(getattr(uw, "dscr", None)) if uw else None,
        "current_stage": state_payload.get("current_stage"),
        "current_stage_label": state_payload.get("current_stage_label"),
        "current_pane": state_payload.get("current_pane"),
        "current_pane_label": state_payload.get("current_pane_label"),
        "normalized_decision": state_payload.get("normalized_decision"),
        "gate_status": state_payload.get("gate_status"),
        "route_reason": state_payload.get("route_reason"),
        "next_actions": state_payload.get("next_actions") or [],
        "blockers": (state_payload.get("outstanding_tasks") or {}).get("blockers") or [],
        "updated_at": state_payload.get("updated_at") or getattr(prop, "updated_at", None),
    }

    snapshot["completeness"] = infer_snapshot_completeness(snapshot)
    snapshot["is_fully_enriched"] = snapshot["completeness"] == "COMPLETE"

    duration_ms = round((time.perf_counter() - t0) * 1000, 2)
    METRICS.observe_ms("inventory_snapshot_build_ms", duration_ms, labels={"org_id": org_id})
    METRICS.inc("inventory_snapshot_build_count", labels={"org_id": org_id})

    return snapshot


def build_inventory_snapshots_for_scope(
    db: Session,
    *,
    org_id: int,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    q: Optional[str] = None,
    assigned_user_id: Optional[int] = None,
    limit: int = 100,
) -> dict[str, Any]:
    t0 = time.perf_counter()

    stmt = _normalized_query_stmt(
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        q=q,
        assigned_user_id=assigned_user_id,
    )

    query_t0 = time.perf_counter()
    props = list(db.scalars(stmt.limit(limit)).all())
    query_ms = round((time.perf_counter() - query_t0) * 1000, 2)

    rows: list[dict[str, Any]] = []
    skipped_errors = 0

    build_t0 = time.perf_counter()
    for prop in props:
        try:
            rows.append(
                build_property_inventory_snapshot(
                    db,
                    org_id=org_id,
                    property_id=int(prop.id),
                )
            )
        except Exception:
            skipped_errors += 1
            log.exception(
                "inventory_snapshot_row_failed",
                extra={"org_id": org_id, "property_id": int(getattr(prop, "id", 0) or 0)},
            )

    build_ms = round((time.perf_counter() - build_t0) * 1000, 2)
    total_ms = round((time.perf_counter() - t0) * 1000, 2)

    METRICS.observe_ms("inventory_snapshot_scope_query_ms", query_ms, labels={"org_id": org_id})
    METRICS.observe_ms("inventory_snapshot_scope_build_ms", build_ms, labels={"org_id": org_id})
    METRICS.observe_ms("inventory_snapshot_scope_total_ms", total_ms, labels={"org_id": org_id})

    log.info(
        "inventory_snapshot_scope_complete",
        extra={
            "event": "inventory_snapshot_scope_complete",
            "org_id": org_id,
            "state": state,
            "county": county,
            "city": city,
            "q": q,
            "assigned_user_id": assigned_user_id,
            "limit": limit,
            "query_rows": len(props),
            "returned_rows": len(rows),
            "skipped_errors": skipped_errors,
            "query_ms": query_ms,
            "build_ms": build_ms,
            "total_ms": total_ms,
        },
    )

    return {
        "rows": rows,
        "count": len(rows),
        "meta": {
            "query_rows": len(props),
            "returned_rows": len(rows),
            "skipped_errors": skipped_errors,
            "query_ms": query_ms,
            "build_ms": build_ms,
            "total_ms": total_ms,
        },
    }