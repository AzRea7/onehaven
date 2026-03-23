from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import desc, or_, select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import Property, RentAssumption


def should_run_inline_rent_refresh() -> bool:
    return bool(getattr(settings, "ingestion_enable_inline_rent_refresh", False))


def should_queue_rent_refresh_after_sync() -> bool:
    return bool(getattr(settings, "ingestion_queue_rent_refresh_after_sync", True))


def publish_without_rent() -> bool:
    return bool(getattr(settings, "ingestion_publish_without_rent", True))


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _property_has_required_basics(prop: Property) -> bool:
    if bool(getattr(settings, "investor_require_address", True)):
        if not str(getattr(prop, "address", "") or "").strip():
            return False

    if bool(getattr(settings, "investor_require_price", True)):
        price = _safe_float(getattr(prop, "price", None) or getattr(prop, "asking_price", None))
        if price is None or price <= 0:
            return False

    if bool(getattr(settings, "investor_require_geo", False)):
        lat = _safe_float(getattr(prop, "lat", None))
        lng = _safe_float(getattr(prop, "lng", None))
        if lat is None or lng is None:
            return False

    return True


def property_is_publishable(prop: Property) -> bool:
    return _property_has_required_basics(prop)


def _latest_rent_assumption(db: Session, *, org_id: int, property_id: int) -> RentAssumption | None:
    return db.scalar(
        select(RentAssumption)
        .where(
            RentAssumption.org_id == int(org_id),
            RentAssumption.property_id == int(property_id),
        )
        .order_by(desc(RentAssumption.id))
    )


def property_needs_rent_refresh(db: Session, *, org_id: int, property_id: int) -> bool:
    ra = _latest_rent_assumption(db, org_id=org_id, property_id=property_id)
    if ra is None:
        return True

    market_rent = _safe_float(getattr(ra, "market_rent_estimate", None))
    if market_rent is None or market_rent <= 0:
        return True

    stale_after = int(getattr(settings, "ingestion_rent_refresh_stale_after_hours", 24 * 7))
    updated_at = getattr(ra, "updated_at", None) or getattr(ra, "created_at", None)
    if updated_at is None:
        return True

    return updated_at < datetime.utcnow() - timedelta(hours=stale_after)


def _candidate_score(prop: Property) -> float:
    """
    Cheap score to prioritize which properties deserve paid rent refresh first.
    No external APIs here.
    """
    score = 0.0

    price = _safe_float(getattr(prop, "price", None) or getattr(prop, "asking_price", None))
    beds = _safe_float(getattr(prop, "bedrooms", None))
    baths = _safe_float(getattr(prop, "bathrooms", None))

    if price is not None:
        if 30000 <= price <= 160000:
            score += 30
        elif 160000 < price <= 220000:
            score += 15

    if beds is not None:
        if beds >= 3:
            score += 20
        elif beds >= 2:
            score += 10

    if baths is not None and baths >= 1:
        score += 10

    city = str(getattr(prop, "city", "") or "").strip().lower()
    county = str(getattr(prop, "county", "") or "").strip().lower()

    if county in {"wayne", "oakland", "macomb"}:
        score += 15
    if city in {"detroit", "dearborn", "southfield", "warren", "pontiac"}:
        score += 10

    lat = _safe_float(getattr(prop, "lat", None))
    lng = _safe_float(getattr(prop, "lng", None))
    if lat is not None and lng is not None:
        score += 5

    return score


def list_properties_for_budgeted_rent_refresh(
    db: Session,
    *,
    org_id: int,
    limit: int,
) -> list[int]:
    rows = db.scalars(
        select(Property)
        .where(Property.org_id == int(org_id))
        .order_by(desc(Property.id))
        .limit(max(50, int(limit) * 10))
    ).all()

    candidates: list[tuple[float, int]] = []

    for prop in rows:
        if not property_is_publishable(prop):
            continue
        if not property_needs_rent_refresh(db, org_id=org_id, property_id=int(prop.id)):
            continue

        score = _candidate_score(prop)
        candidates.append((score, int(prop.id)))

    candidates.sort(key=lambda x: (-x[0], -x[1]))
    return [pid for _, pid in candidates[: max(0, int(limit))]]