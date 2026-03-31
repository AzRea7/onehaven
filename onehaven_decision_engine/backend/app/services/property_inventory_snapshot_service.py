from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import desc, func, or_, select, text
from sqlalchemy.orm import Session

from ..config import settings
from ..domain.underwriting import compute_trustworthy_investment_metrics, compute_monthly_housing_costs
from .property_tax_enrichment_service import get_property_tax_context
from .property_insurance_enrichment_service import get_property_insurance_context
from ..models import Deal, RentAssumption, Property, UnderwritingResult
from ..services.property_state_machine import get_state_payload
from ..services.risk_scoring import compute_risk_adjusted_score
from ..services.runtime_metrics import METRICS
from .acquisition_tag_service import list_tags_for_properties

log = logging.getLogger("onehaven.inventory_snapshot")

DEFAULT_NEW_THRESHOLD_DAYS = 1.0
DEFAULT_FRESH_THRESHOLD_DAYS = 7.0
DEFAULT_WARM_THRESHOLD_DAYS = 21.0
DEFAULT_STALE_THRESHOLD_DAYS = 45.0
DEFAULT_VERY_STALE_THRESHOLD_DAYS = 90.0


def _safe_float(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int | None = None) -> int | None:
    try:
        if v is None:
            return default
        return int(float(v))
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


def _market_rent_estimate_from_rent_row(rent_row: Any) -> float | None:
    if rent_row is None:
        return None
    return _safe_float(getattr(rent_row, "market_rent_estimate", None))


def _rent_used_from_rent_row(rent_row: Any) -> float | None:
    if rent_row is None:
        return None
    return _safe_float(getattr(rent_row, "rent_used", None))


def _rent_reasonableness_comp_from_rent_row(rent_row: Any) -> float | None:
    if rent_row is None:
        return None
    return _safe_float(getattr(rent_row, "rent_reasonableness_comp", None))


def _market_reference_rent_from_snapshot(snapshot: dict[str, Any]) -> float | None:
    return _safe_float(snapshot.get("market_reference_rent"))


def _monthly_debt_service_from_uw(uw: Any) -> float | None:
    if uw is None:
        return None
    return _safe_float(getattr(uw, "monthly_debt_service", None))


def _canonical_rent_gap(
    *,
    market_rent_estimate: float | None,
    monthly_debt_service: float | None,
) -> float | None:
    if market_rent_estimate is None or monthly_debt_service is None:
        return None
    return round(float(market_rent_estimate) - float(monthly_debt_service), 2)


def _market_rent_estimate_from_snapshot(snapshot: dict[str, Any]) -> float | None:
    rent_assumption = snapshot.get("rent_assumption")
    if isinstance(rent_assumption, dict):
        return _safe_float(
            snapshot.get("market_rent_estimate"),
            _safe_float(rent_assumption.get("market_rent_estimate")),
        )
    return _safe_float(snapshot.get("market_rent_estimate"))


def _section8_rent_used_from_snapshot(snapshot: dict[str, Any]) -> float | None:
    acquisition_meta = snapshot.get("acquisition_metadata") or {}
    return _safe_float(
        snapshot.get("rent_used"),
        _safe_float(
            snapshot.get("approved_rent_ceiling"),
            _safe_float(
                acquisition_meta.get("rent_used"),
                _safe_float(acquisition_meta.get("listing_price")),
            ),
        ),
    )


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

def _latest_rent_assumption(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> RentAssumption | None:
    return db.scalar(
        select(RentAssumption)
        .where(
            RentAssumption.org_id == org_id,
            RentAssumption.property_id == property_id,
        )
        .order_by(desc(RentAssumption.created_at), desc(RentAssumption.id))
        .limit(1)
    )

def _attr_float(obj: Any, *names: str) -> float | None:
    if obj is None:
        return None
    for name in names:
        if hasattr(obj, name):
            value = getattr(obj, name, None)
            parsed = _safe_float(value, None)
            if parsed is not None:
                return parsed
    return None


def _attr_int(obj: Any, *names: str) -> int | None:
    if obj is None:
        return None
    for name in names:
        if hasattr(obj, name):
            value = getattr(obj, name, None)
            parsed = _safe_int(value, None)
            if parsed is not None:
                return parsed
    return None


def _settings_interest_rate() -> float:
    return float(
        getattr(settings, "dscr_interest_rate", None)
        or getattr(settings, "interest_rate", None)
        or 0.07
    )

def _settings_utilities_monthly() -> float:
    return float(getattr(settings, "utilities_monthly_default", 0.0) or 0.0)


def _settings_term_years() -> int:
    return int(
        getattr(settings, "dscr_term_years", None)
        or getattr(settings, "term_years", None)
        or 30
    )


def _settings_down_payment_pct() -> float:
    return float(
        getattr(settings, "down_payment_pct", None)
        or getattr(settings, "dscr_down_payment_pct", None)
        or 0.20
    )


def _resolve_tax_rate_annual(
    *,
    prop: Property,
    deal: Deal | None,
    uw: UnderwritingResult | None,
    asking_price: float | None,
) -> float | None:
    direct = (
        _attr_float(uw, "tax_rate_annual", "property_tax_rate_annual")
        or _attr_float(deal, "tax_rate_annual", "property_tax_rate_annual")
        or _attr_float(prop, "tax_rate_annual", "property_tax_rate_annual")
    )
    if direct is not None:
        return direct

    taxes_monthly = (
        _attr_float(uw, "monthly_taxes", "taxes_monthly")
        or _attr_float(deal, "monthly_taxes", "taxes_monthly")
        or _attr_float(prop, "monthly_taxes", "taxes_monthly")
        or _safe_float(getattr(settings, "taxes_monthly_default", None), None)
    )
    if taxes_monthly is None or asking_price is None or asking_price <= 0:
        return None
    return float(taxes_monthly) * 12.0 / float(asking_price)


def _resolve_insurance_annual(
    *,
    prop: Property,
    deal: Deal | None,
    uw: UnderwritingResult | None,
) -> float | None:
    direct = (
        _attr_float(uw, "insurance_annual", "annual_insurance")
        or _attr_float(deal, "insurance_annual", "annual_insurance")
        or _attr_float(prop, "insurance_annual", "annual_insurance")
    )
    if direct is not None:
        return direct

    monthly_insurance = (
        _attr_float(uw, "monthly_insurance", "insurance_monthly")
        or _attr_float(deal, "monthly_insurance", "insurance_monthly")
        or _attr_float(prop, "monthly_insurance", "insurance_monthly")
        or _safe_float(getattr(settings, "insurance_monthly_default", None), None)
    )
    if monthly_insurance is None:
        return None
    return float(monthly_insurance) * 12.0


def _compute_housing_cost_bundle(
    *,
    db: Session,
    prop: Property,
    deal: Deal | None,
    uw: UnderwritingResult | None,
    asking_price: float | None,
) -> dict[str, float | None]:
    interest_rate = (
        _attr_float(uw, "interest_rate", "annual_interest_rate", "loan_interest_rate")
        or _attr_float(deal, "interest_rate", "annual_interest_rate", "loan_interest_rate")
        or _settings_interest_rate()
    )
    term_years = (
        _attr_int(uw, "term_years", "loan_term_years")
        or _attr_int(deal, "term_years", "loan_term_years")
        or _settings_term_years()
    )
    down_payment_pct = (
        _attr_float(uw, "down_payment_pct")
        or _attr_float(deal, "down_payment_pct")
        or _settings_down_payment_pct()
    )

    tax_ctx = get_property_tax_context(db, org_id=int(getattr(prop, "org_id")), property_id=int(getattr(prop, "id")))
    insurance_ctx = get_property_insurance_context(db, org_id=int(getattr(prop, "org_id")), property_id=int(getattr(prop, "id")))

    tax_rate_annual = tax_ctx.get("property_tax_rate_annual") or _resolve_tax_rate_annual(
        prop=prop,
        deal=deal,
        uw=uw,
        asking_price=asking_price,
    )
    taxes_annual = tax_ctx.get("property_tax_annual")
    insurance_annual = insurance_ctx.get("insurance_annual") or _resolve_insurance_annual(
        prop=prop,
        deal=deal,
        uw=uw,
    )

    bundle = compute_monthly_housing_costs(
        asking_price=asking_price,
        interest_rate=float(interest_rate),
        term_years=int(term_years),
        down_payment_pct=float(down_payment_pct),
        tax_rate_annual=tax_rate_annual,
        taxes_annual=taxes_annual,
        insurance_annual=insurance_annual,
    )
    return {
        **bundle,
        "property_tax_annual": taxes_annual if taxes_annual is not None else (bundle.get("monthly_taxes") * 12.0 if bundle.get("monthly_taxes") is not None else None),
        "property_tax_rate_annual": tax_rate_annual,
        "property_tax_source": tax_ctx.get("property_tax_source"),
        "property_tax_confidence": tax_ctx.get("property_tax_confidence"),
        "property_tax_year": tax_ctx.get("property_tax_year"),
        "insurance_annual": insurance_annual,
        "insurance_source": insurance_ctx.get("insurance_source"),
        "insurance_confidence": insurance_ctx.get("insurance_confidence"),
    }


def _normalized_query_stmt(
    *,
    org_id: int,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    q: Optional[str] = None,
    assigned_user_id: Optional[int] = None,
    include_hidden: bool = False,
):
    stmt = select(Property).where(Property.org_id == org_id)

    if not include_hidden:
        if hasattr(Property, "listing_hidden"):
            stmt = stmt.where(Property.listing_hidden.is_(False))
        else:
            stmt = stmt.where(
                or_(
                    Property.acquisition_metadata_json.is_(None),
                    text("COALESCE(acquisition_metadata_json->>'listing_hidden', 'false') <> 'true'"),
                )
            )

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
                    " ",
                    func.coalesce(Property.county, ""),
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
        clauses = [
            getattr(Property, c) == assigned_user_id
            for c in candidate_columns
            if hasattr(Property, c)
        ]
        if clauses:
            stmt = stmt.where(or_(*clauses))

    return stmt.order_by(desc(Property.updated_at).nullslast(), desc(Property.id))


def _load_property_meta(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    row = db.execute(
        text(
            """
            SELECT acquisition_first_seen_at, acquisition_last_seen_at,
                   acquisition_source_provider, acquisition_source_slug, acquisition_source_record_id, acquisition_source_url,
                   completeness_geo_status, completeness_rent_status, completeness_rehab_status,
                   completeness_risk_status, completeness_jurisdiction_status, completeness_cashflow_status,
                   listing_status, listing_hidden, listing_hidden_reason,
                   listing_last_seen_at, listing_removed_at, listing_listed_at, listing_created_at,
                   listing_days_on_market, listing_price,
                   listing_mls_name, listing_mls_number, listing_type,
                   listing_zillow_url,
                   listing_agent_name, listing_agent_phone, listing_agent_email, listing_agent_website,
                   listing_office_name, listing_office_phone, listing_office_email,
                   crime_band, crime_source, crime_radius_miles, crime_incident_count,
                   crime_confidence, investment_area_band,
                   offender_band, offender_source,
                   risk_score, risk_band, risk_confidence,
                   acquisition_metadata_json
            FROM properties
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).fetchone()
    return dict(row._mapping) if row is not None else {}


def infer_snapshot_completeness(snapshot: dict[str, Any]) -> str:
    statuses = list((snapshot.get("completeness_status") or {}).values())
    if statuses and all(x in {"complete", "deferred"} for x in statuses):
        return "COMPLETE"
    if any(x == "complete" for x in statuses):
        return "PARTIAL"

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


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        raw = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _latest_seen_dt(snapshot: dict[str, Any]) -> datetime | None:
    return (
        _parse_dt(snapshot.get("source_updated_at"))
        or _parse_dt(snapshot.get("acquisition_last_seen_at"))
        or _parse_dt(snapshot.get("updated_at"))
        or _parse_dt(snapshot.get("created_at"))
    )


def get_freshness_thresholds() -> dict[str, float]:
    return {
        "new_days": float(getattr(settings, "inventory_new_threshold_days", DEFAULT_NEW_THRESHOLD_DAYS) or DEFAULT_NEW_THRESHOLD_DAYS),
        "fresh_days": float(getattr(settings, "inventory_fresh_threshold_days", DEFAULT_FRESH_THRESHOLD_DAYS) or DEFAULT_FRESH_THRESHOLD_DAYS),
        "warm_days": float(getattr(settings, "inventory_warm_threshold_days", DEFAULT_WARM_THRESHOLD_DAYS) or DEFAULT_WARM_THRESHOLD_DAYS),
        "stale_days": float(getattr(settings, "inventory_stale_threshold_days", DEFAULT_STALE_THRESHOLD_DAYS) or DEFAULT_STALE_THRESHOLD_DAYS),
        "very_stale_days": float(getattr(settings, "inventory_very_stale_threshold_days", DEFAULT_VERY_STALE_THRESHOLD_DAYS) or DEFAULT_VERY_STALE_THRESHOLD_DAYS),
    }


def compute_days_since_seen(snapshot: dict[str, Any]) -> float | None:
    latest = _latest_seen_dt(snapshot)
    if latest is None:
        return None
    return max(0.0, (_utcnow() - latest).total_seconds() / 86400.0)


def compute_freshness_bucket(snapshot: dict[str, Any]) -> str:
    age_days = compute_days_since_seen(snapshot)
    thresholds = get_freshness_thresholds()

    if age_days is None:
        return "unknown"
    if age_days <= thresholds["new_days"]:
        return "new"
    if age_days <= thresholds["fresh_days"]:
        return "fresh"
    if age_days <= thresholds["warm_days"]:
        return "warm"
    if age_days <= thresholds["stale_days"]:
        return "aging"
    if age_days <= thresholds["very_stale_days"]:
        return "stale"
    return "very_stale"


def compute_freshness_score(snapshot: dict[str, Any]) -> int:
    age_days = compute_days_since_seen(snapshot)
    thresholds = get_freshness_thresholds()

    if age_days is None:
        return 0
    if age_days <= thresholds["new_days"]:
        return 28
    if age_days <= thresholds["fresh_days"]:
        return 18
    if age_days <= thresholds["warm_days"]:
        return 10
    if age_days <= thresholds["stale_days"]:
        return 2
    if age_days <= thresholds["very_stale_days"]:
        return -10
    return -22


def _text_match_score(snapshot: dict[str, Any], q: str | None) -> int:
    if not q:
        return 0

    wanted = q.strip().lower()
    if not wanted:
        return 0

    haystack = " ".join(
        [
            str(snapshot.get("address") or ""),
            str(snapshot.get("city") or ""),
            str(snapshot.get("county") or ""),
            str(snapshot.get("state") or ""),
            str(snapshot.get("zip") or ""),
            str(snapshot.get("normalized_address") or ""),
        ]
    ).lower()

    if wanted in haystack:
        return 18

    parts = [x for x in wanted.split() if x]
    if not parts:
        return 0

    hits = sum(1 for token in parts if token in haystack)
    return min(16, hits * 4)


def _market_match_score(
    snapshot: dict[str, Any],
    *,
    state: str | None,
    county: str | None,
    city: str | None,
) -> int:
    score = 0

    snap_state = str(snapshot.get("state") or "").strip().lower()
    snap_county = str(snapshot.get("county") or "").strip().lower()
    snap_city = str(snapshot.get("city") or "").strip().lower()

    if state and snap_state == str(state).strip().lower():
        score += 8
    if county and snap_county == str(county).strip().lower():
        score += 18
    if city and snap_city == str(city).strip().lower():
        score += 24

    return score


def _buy_box_score(snapshot: dict[str, Any], search_context: dict[str, Any]) -> int:
    score = 0

    asking_price = _safe_float(snapshot.get("asking_price"))
    max_price = _safe_float(
        search_context.get("buy_box_max_price"),
        _safe_float(getattr(settings, "investor_buy_box_max_price", 200_000), 200_000.0),
    ) or 200_000.0

    property_type = str(snapshot.get("property_type") or "").strip().lower()

    allowed_property_types = search_context.get("buy_box_property_types")
    if not allowed_property_types:
        allowed_property_types = ["single_family", "multi_family"]
    allowed_property_types = {str(x).strip().lower() for x in allowed_property_types if str(x).strip()}

    max_units = _safe_int(
        search_context.get("buy_box_max_units"),
        _safe_int(getattr(settings, "investor_buy_box_max_units", 4), 4),
    ) or 4
    units = _safe_int(snapshot.get("units"), 1) or 1

    if asking_price is not None:
        if asking_price <= max_price:
            score += 14
        elif asking_price <= max_price * 1.15:
            score += 6
        elif asking_price <= max_price * 1.30:
            score += 1
        else:
            score -= 12
    else:
        score -= 10

    if property_type in allowed_property_types:
        score += 10
    elif property_type:
        score -= 8

    if units <= max_units:
        score += 5
    else:
        score -= 8

    return score


def _ranking_metrics(snapshot: dict[str, Any]) -> dict[str, Any]:
    projected_monthly_cashflow = _safe_float(snapshot.get("projected_monthly_cashflow"))
    dscr = _safe_float(snapshot.get("dscr"))
    risk_score = _safe_float(snapshot.get("risk_score"))
    rent_gap = _safe_float(snapshot.get("rent_gap"))

    score_parts = compute_risk_adjusted_score(
        projected_monthly_cashflow=projected_monthly_cashflow,
        dscr=dscr,
        rent_gap=rent_gap,
        risk_score=risk_score,
    )

    return {
        "market_rent_estimate": _safe_float(snapshot.get("market_rent_estimate")),
        "rent_reasonableness_comp": _safe_float(snapshot.get("rent_reasonableness_comp")),
        "market_reference_rent": _safe_float(snapshot.get("market_reference_rent")),
        "rent_used": _safe_float(snapshot.get("rent_used")),
        "loan_amount": _safe_float(snapshot.get("loan_amount")),
        "monthly_debt_service": _safe_float(snapshot.get("monthly_debt_service")),
        "monthly_taxes": _safe_float(snapshot.get("monthly_taxes")),
        "monthly_insurance": _safe_float(snapshot.get("monthly_insurance")),
        "monthly_housing_cost": _safe_float(snapshot.get("monthly_housing_cost")),
        "effective_gross_income": _safe_float(snapshot.get("effective_gross_income")),
        "variable_operating_expenses": _safe_float(snapshot.get("variable_operating_expenses")),
        "fixed_operating_expenses": _safe_float(snapshot.get("fixed_operating_expenses")),
        "operating_expenses": _safe_float(snapshot.get("operating_expenses")),
        "noi": _safe_float(snapshot.get("noi")),
        "projected_monthly_cashflow": projected_monthly_cashflow,
        "rent_gap": rent_gap,
        "dscr": dscr,
        **score_parts,
    }


def _data_quality_score(snapshot: dict[str, Any]) -> int:
    score = 0

    if snapshot.get("address"):
        score += 6
    else:
        score -= 10

    if snapshot.get("asking_price") is not None:
        score += 6
    else:
        score -= 8

    if snapshot.get("normalized_address"):
        score += 5

    if snapshot.get("lat") is not None and snapshot.get("lng") is not None:
        score += 8
    else:
        score -= 4

    if snapshot.get("market_rent_estimate") is not None:
        score += 8

    if snapshot.get("projected_monthly_cashflow") is not None:
        score += 8

    dscr = _safe_float(snapshot.get("dscr"))
    if dscr is not None:
        score += 8
        if dscr >= 1.2:
            score += 4

    return score


def _decision_score(snapshot: dict[str, Any]) -> int:
    value = str(snapshot.get("normalized_decision") or "").strip().upper()
    if value in {"GOOD", "GOOD_DEAL"}:
        return 18
    if value == "REVIEW":
        return 8
    if value == "REJECT":
        return -18
    return 0


def _completeness_score(snapshot: dict[str, Any]) -> int:
    completeness = str(snapshot.get("completeness") or "").upper()
    if completeness == "COMPLETE":
        return 12
    if completeness == "PARTIAL":
        return 5
    return -6


def _cashflow_score(snapshot: dict[str, Any]) -> int:
    cashflow = _safe_float(snapshot.get("projected_monthly_cashflow"))
    if cashflow is None:
        return 0
    if cashflow > 500:
        return 12
    if cashflow > 250:
        return 9
    if cashflow > 0:
        return 6
    if cashflow < -250:
        return -8
    if cashflow < 0:
        return -4
    return 0


def compute_inventory_relevance(
    snapshot: dict[str, Any],
    search_context: dict[str, Any] | None = None,
) -> float:
    ctx = dict(search_context or {})
    score = 0.0

    score += _market_match_score(
        snapshot,
        state=ctx.get("state"),
        county=ctx.get("county"),
        city=ctx.get("city"),
    )
    score += _text_match_score(snapshot, ctx.get("q"))
    score += _buy_box_score(snapshot, ctx)
    score += _data_quality_score(snapshot)
    score += _decision_score(snapshot)
    score += _completeness_score(snapshot)
    score += _cashflow_score(snapshot)
    score += compute_freshness_score(snapshot)

    age_days = compute_days_since_seen(snapshot)
    thresholds = get_freshness_thresholds()
    if age_days is not None:
        if age_days > thresholds["very_stale_days"]:
            score -= 18
        elif age_days > thresholds["stale_days"]:
            score -= 8

    if snapshot.get("is_new_this_sync"):
        score += 6
    if snapshot.get("is_recently_refreshed"):
        score += 4
    if snapshot.get("is_very_stale"):
        score -= 10

    return float(round(score, 2))


def apply_freshness_policy(snapshot: dict[str, Any]) -> dict[str, Any]:
    age_days = compute_days_since_seen(snapshot)
    thresholds = get_freshness_thresholds()
    bucket = compute_freshness_bucket(snapshot)

    snapshot["days_since_seen"] = age_days
    snapshot["freshness_bucket"] = bucket
    snapshot["freshness_score"] = compute_freshness_score(snapshot)
    snapshot["freshness_thresholds"] = thresholds
    snapshot["is_new_this_sync"] = bool(age_days is not None and age_days <= thresholds["new_days"])
    snapshot["is_recently_refreshed"] = bool(age_days is not None and age_days <= thresholds["fresh_days"])
    snapshot["is_stale"] = bool(age_days is not None and age_days > thresholds["stale_days"])
    snapshot["is_very_stale"] = bool(age_days is not None and age_days > thresholds["very_stale_days"])

    return snapshot


def build_property_inventory_snapshot(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    search_context: dict | None = None,
) -> dict[str, Any]:
    search_context = dict(search_context or {})
    t0 = time.perf_counter()

    prop = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    if prop is None:
        raise ValueError("property not found")

    deal = _latest_deal(db, org_id=org_id, property_id=int(prop.id))
    uw = _latest_uw(db, org_id=org_id, property_id=int(prop.id))
    state_payload = get_state_payload(db, org_id=org_id, property_id=int(prop.id), recompute=False)

    rent_row = _latest_rent_assumption(
        db,
        org_id=org_id,
        property_id=int(prop.id),
    )

    if rent_row is None:
        rel_value = getattr(prop, "rent_assumption", None)
        if isinstance(rel_value, list):
            rent_row = rel_value[0] if rel_value else None
        elif rel_value is not None:
            rent_row = rel_value

    asking_price = _asking_price(prop, deal)
    housing_costs = _compute_housing_cost_bundle(
        db=db,
        prop=prop,
        deal=deal,
        uw=uw,
        asking_price=asking_price,
    )

    market_rent_estimate = _market_rent_estimate_from_rent_row(rent_row)
    rent_reasonableness_comp = _rent_reasonableness_comp_from_rent_row(rent_row)
    rent_used = _rent_used_from_rent_row(rent_row)

    monthly_debt_service = housing_costs.get("monthly_debt_service") or _monthly_debt_service_from_uw(uw)

    live_metrics = compute_trustworthy_investment_metrics(
        rent_used=rent_used,
        market_rent_estimate=market_rent_estimate,
        rent_reasonableness_comp=rent_reasonableness_comp,
        monthly_debt_service=monthly_debt_service,
        monthly_taxes=housing_costs.get("monthly_taxes"),
        monthly_insurance=housing_costs.get("monthly_insurance"),
        monthly_housing_cost=housing_costs.get("monthly_housing_cost"),
        utilities_monthly=_settings_utilities_monthly(),
    )

    market_reference_rent = live_metrics.get("market_reference_rent")
    rent_gap = live_metrics.get("rent_gap")

    meta = _load_property_meta(db, org_id=org_id, property_id=int(prop.id))
    tags = list_tags_for_properties(db, org_id=org_id, property_ids=[int(prop.id)]).get(int(prop.id), [])

    snapshot = {
        "id": int(prop.id),
        "property_id": int(prop.id),
        "address": getattr(prop, "address", None),
        "city": getattr(prop, "city", None),
        "county": getattr(prop, "county", None),
        "state": getattr(prop, "state", None),
        "zip": getattr(prop, "zip", None),
        "normalized_address": getattr(prop, "normalized_address", None),
        "lat": getattr(prop, "lat", None),
        "lng": getattr(prop, "lng", None),
        "property_type": getattr(prop, "property_type", None),
        "units": getattr(prop, "units", None),
        "bedrooms": getattr(prop, "bedrooms", None),
        "bathrooms": getattr(prop, "bathrooms", None),
        "asking_price": asking_price,
        "market_rent_estimate": market_rent_estimate,
        "rent_reasonableness_comp": rent_reasonableness_comp,
        "market_reference_rent": market_reference_rent,
        "rent_used": live_metrics.get("gross_rent_used"),
        "monthly_debt_service": monthly_debt_service,
        "effective_gross_income": live_metrics.get("effective_gross_income"),
        "variable_operating_expenses": live_metrics.get("variable_operating_expenses"),
        "fixed_operating_expenses": live_metrics.get("fixed_operating_expenses"),
        "operating_expenses": live_metrics.get("operating_expenses"),
        "noi": live_metrics.get("noi"),
        "utilities_monthly": live_metrics.get("utilities_monthly"),
        "vacancy_rate_used": live_metrics.get("vacancy_rate_used"),
        "maintenance_rate_used": live_metrics.get("maintenance_rate_used"),
        "management_rate_used": live_metrics.get("management_rate_used"),
        "capex_rate_used": live_metrics.get("capex_rate_used"),
        "projected_monthly_cashflow": live_metrics.get("projected_monthly_cashflow"),
        "rent_gap": rent_gap,
        "loan_amount": housing_costs.get("loan_amount"),
        "monthly_taxes": housing_costs.get("monthly_taxes"),
        "monthly_insurance": housing_costs.get("monthly_insurance"),
        "monthly_housing_cost": housing_costs.get("monthly_housing_cost"),
        "property_tax_annual": housing_costs.get("property_tax_annual"),
        "property_tax_rate_annual": housing_costs.get("property_tax_rate_annual"),
        "property_tax_source": housing_costs.get("property_tax_source"),
        "property_tax_confidence": housing_costs.get("property_tax_confidence"),
        "property_tax_year": housing_costs.get("property_tax_year"),
        "insurance_annual": housing_costs.get("insurance_annual"),
        "insurance_source": housing_costs.get("insurance_source"),
        "insurance_confidence": housing_costs.get("insurance_confidence"),
        "dscr": live_metrics.get("dscr"),
        "underwriting_result_cash_flow": _safe_float(getattr(uw, "cash_flow", None), None) if uw else None,
        "underwriting_result_dscr": _safe_float(getattr(uw, "dscr", None), None) if uw else None,
        "section8_fmr": getattr(rent_row, "section8_fmr", None) if rent_row is not None else None,
        "approved_rent_ceiling": getattr(rent_row, "approved_rent_ceiling", None) if rent_row is not None else None,
        "rent_cap_reason": getattr(rent_row, "rent_cap_reason", None) if rent_row is not None and hasattr(rent_row, "rent_cap_reason") else None,
        "crime_score": getattr(prop, "crime_score", None),
        "crime_band": getattr(prop, "crime_band", None) or meta.get("crime_band"),
        "crime_source": getattr(prop, "crime_source", None) or meta.get("crime_source"),
        "crime_radius_miles": getattr(prop, "crime_radius_miles", None) or meta.get("crime_radius_miles"),
        "crime_incident_count": getattr(prop, "crime_incident_count", None) or meta.get("crime_incident_count"),
        "crime_confidence": getattr(prop, "crime_confidence", None) or meta.get("crime_confidence"),
        "investment_area_band": getattr(prop, "investment_area_band", None) or meta.get("investment_area_band"),
        "offender_count": getattr(prop, "offender_count", None),
        "offender_band": getattr(prop, "offender_band", None) or meta.get("offender_band"),
        "offender_source": getattr(prop, "offender_source", None) or meta.get("offender_source"),
        "risk_score": getattr(prop, "risk_score", None) or meta.get("risk_score"),
        "risk_band": getattr(prop, "risk_band", None) or meta.get("risk_band"),
        "risk_confidence": getattr(prop, "risk_confidence", None) or meta.get("risk_confidence"),
        "is_red_zone": getattr(prop, "is_red_zone", None),
        "normalized_decision": state_payload.get("normalized_decision"),
        "pane": state_payload.get("primary_pane") or state_payload.get("current_pane"),
        "current_workflow_stage": state_payload.get("current_stage"),
        "current_workflow_stage_label": state_payload.get("current_stage_label"),
        "next_actions": state_payload.get("next_actions") or [],
        "workflow_state": state_payload,
        "listing_status": meta.get("listing_status"),
        "listing_hidden": bool(meta.get("listing_hidden") or False),
        "listing_hidden_reason": meta.get("listing_hidden_reason"),
        "listing_last_seen_at": meta.get("listing_last_seen_at"),
        "listing_removed_at": meta.get("listing_removed_at"),
        "listing_listed_at": meta.get("listing_listed_at"),
        "listing_created_at": meta.get("listing_created_at"),
        "listing_days_on_market": meta.get("listing_days_on_market"),
        "listing_price": meta.get("listing_price"),
        "listing_mls_name": meta.get("listing_mls_name"),
        "listing_mls_number": meta.get("listing_mls_number"),
        "listing_type": meta.get("listing_type"),
        "listing_zillow_url": meta.get("listing_zillow_url"),
        "listing_agent_name": meta.get("listing_agent_name"),
        "listing_agent_phone": meta.get("listing_agent_phone"),
        "listing_agent_email": meta.get("listing_agent_email"),
        "listing_agent_website": meta.get("listing_agent_website"),
        "listing_office_name": meta.get("listing_office_name"),
        "listing_office_phone": meta.get("listing_office_phone"),
        "listing_office_email": meta.get("listing_office_email"),
        "tags": tags,
        "source_updated_at": getattr(prop, "source_updated_at", None),
        "updated_at": getattr(prop, "updated_at", None),
        "created_at": getattr(prop, "created_at", None),
        "acquisition_first_seen_at": meta.get("acquisition_first_seen_at"),
        "acquisition_last_seen_at": meta.get("acquisition_last_seen_at"),
        "acquisition_source": {
            "provider": meta.get("acquisition_source_provider"),
            "slug": meta.get("acquisition_source_slug"),
            "record_id": meta.get("acquisition_source_record_id"),
            "url": meta.get("acquisition_source_url"),
        },
        "acquisition_metadata": meta.get("acquisition_metadata_json") or {},
        "completeness_status": {
            "geo": meta.get("completeness_geo_status") or "missing",
            "rent": meta.get("completeness_rent_status") or "missing",
            "rehab": meta.get("completeness_rehab_status") or "missing",
            "risk": meta.get("completeness_risk_status") or "missing",
            "jurisdiction": meta.get("completeness_jurisdiction_status") or "missing",
            "cashflow": meta.get("completeness_cashflow_status") or "missing",
        },
    }

    ranking = _ranking_metrics(snapshot)
    snapshot.update(ranking)

    snapshot["completeness"] = infer_snapshot_completeness(snapshot)
    snapshot["is_fully_enriched"] = snapshot["completeness"] == "COMPLETE"

    freshness_score = compute_freshness_score(snapshot)
    text_match_score = _text_match_score(snapshot, search_context.get("q"))
    market_match_score = _market_match_score(
        snapshot,
        state=search_context.get("state"),
        county=search_context.get("county"),
        city=search_context.get("city"),
    )
    buy_box_score = _buy_box_score(snapshot, search_context)

    rank_score = (
        _safe_float(snapshot.get("risk_adjusted_score"), 0.0)
        + freshness_score
        + text_match_score
        + market_match_score
        + buy_box_score
    )

    snapshot["freshness_score"] = freshness_score
    snapshot["text_match_score"] = text_match_score
    snapshot["market_match_score"] = market_match_score
    snapshot["buy_box_score"] = buy_box_score
    snapshot["rank_score"] = round(rank_score, 2)

    snapshot = apply_freshness_policy(snapshot)

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
    include_hidden: bool = False,
) -> dict[str, Any]:
    t0 = time.perf_counter()

    overfetch = max(limit, min(1000, int(limit) * 3))
    stmt = _normalized_query_stmt(
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        q=q,
        assigned_user_id=assigned_user_id,
        include_hidden=include_hidden,
    )

    query_t0 = time.perf_counter()
    props = list(db.scalars(stmt.limit(overfetch)).all())
    query_ms = round((time.perf_counter() - query_t0) * 1000, 2)

    search_context = {
        "state": state,
        "county": county,
        "city": city,
        "q": q,
        "buy_box_max_price": getattr(settings, "investor_buy_box_max_price", 200_000),
        "buy_box_max_units": getattr(settings, "investor_buy_box_max_units", 4),
        "buy_box_property_types": getattr(
            settings,
            "investor_buy_box_property_types",
            ["single_family", "multi_family"],
        ),
    }

    rows: list[dict[str, Any]] = []
    skipped_errors = 0

    build_t0 = time.perf_counter()
    for prop in props:
        try:
            snapshot = build_property_inventory_snapshot(
                db,
                org_id=org_id,
                property_id=int(prop.id),
                search_context=search_context,
            )
            if not include_hidden and bool(snapshot.get("listing_hidden")):
                continue
            snapshot["relevance_score"] = compute_inventory_relevance(snapshot, search_context)
            rows.append(snapshot)
        except Exception:
            skipped_errors += 1
            log.exception(
                "inventory_snapshot_row_failed",
                extra={"org_id": org_id, "property_id": int(getattr(prop, "id", 0) or 0)},
            )

    rows.sort(
        key=lambda row: (
            _safe_float(row.get("rank_score"), -10**12),
            _safe_float(row.get("projected_monthly_cashflow"), -10**12),
            _safe_float(row.get("dscr"), -10**12),
            str(row.get("updated_at") or ""),
        ),
        reverse=True,
    )
    rows = rows[: max(1, int(limit))]

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
        "items": rows,
        "query_rows": len(props),
        "returned_rows": len(rows),
        "skipped_errors": skipped_errors,
        "query_ms": query_ms,
        "build_ms": build_ms,
        "total_ms": total_ms,
    }