from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any, Optional
from urllib.parse import quote

from pydantic import BaseModel, Field

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import asc, desc, func, select, text
from sqlalchemy.orm import Session, selectinload

from ..auth import get_principal
from ..config import settings
from ..db import get_db
from ..domain.jurisdiction_scoring import compute_friction
from ..models import (
    AppUser,
    Deal,
    JurisdictionRule,
    Lease,
    Property,
    PropertyChecklist,
    PropertyChecklistItem,
    RehabTask,
    RentAssumption,
    Transaction,
    UnderwritingResult,
    Valuation,
)
from ..schemas import (
    CeilingCandidate,
    ChecklistItemOut,
    ChecklistOut,
    DealOut,
    JurisdictionRuleOut,
    LeaseOut,
    PropertyCreate,
    PropertyOut,
    PropertyViewOut,
    RehabTaskOut,
    RentExplainOut,
    TransactionOut,
    UnderwritingResultOut,
    ValuationOut,
)
from ..services.geo_enrichment import enrich_property_geo
from ..services.property_state_machine import (
    compute_and_persist_stage,
    get_state_payload,
    normalize_decision_bucket,
)
from ..services.workflow_gate_service import build_workflow_summary
from ..services.acquisition_tag_service import list_property_tags, replace_property_tags
from ..services.property_inventory_snapshot_service import (
    build_property_inventory_snapshot,
    build_inventory_snapshots_for_scope,
)
from ..domain.rent_learning import recompute_rent_fields

router = APIRouter(prefix="/properties", tags=["properties"])
log = logging.getLogger("onehaven.properties")


class AcquisitionTagsIn(BaseModel):
    tags: list[str] = Field(default_factory=list)


def _principal_user_id(p: Any) -> int | None:
    for attr in ("user_id", "id"):
        value = getattr(p, attr, None)
        if value is not None:
            try:
                return int(value)
            except Exception:
                return None
    return None


def _property_acquisition_meta(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
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
               crime_band, crime_source, crime_method, crime_radius_miles, crime_area_sq_miles,
               crime_area_type, crime_incident_count, crime_weighted_incident_count,
               crime_nearest_incident_miles, crime_dataset_version, crime_confidence,
               investment_area_band, offender_band, offender_source, offender_radius_miles,
               nearest_offender_miles, risk_score, risk_band, risk_summary, risk_confidence, risk_last_computed_at,

               acquisition_metadata_json
        FROM properties
        WHERE org_id = :org_id AND id = :property_id
    """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).fetchone()
    return dict(row._mapping) if row is not None else {}


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _crime_label(score: Any) -> str:
    if score is None:
        return "UNKNOWN"
    value = _safe_float(score, 0.0)
    if value >= 80:
        return "HIGH"
    if value >= 45:
        return "MODERATE"
    return "LOW"




def _derive_zillow_listing_url(*, address: Any = None, city: Any = None, state: Any = None, zip_code: Any = None) -> str | None:
    raw_parts = [address, city, state, zip_code]
    parts = [str(part).strip() for part in raw_parts if str(part or '').strip()]
    if not parts:
        return None

    query = quote(', '.join(parts))
    return f"https://www.zillow.com/homes/{query}_rb/"


def _resolved_zillow_listing_url(
    *,
    stored_url: Any = None,
    address: Any = None,
    city: Any = None,
    state: Any = None,
    zip_code: Any = None,
) -> str | None:
    raw = str(stored_url or '').strip()
    if raw:
        return raw
    return _derive_zillow_listing_url(
        address=address,
        city=city,
        state=state,
        zip_code=zip_code,
    )

def _asking_price(prop: Property, deal: Deal | None) -> float | None:
    for attr in ("asking_price", "list_price", "price", "offer_price", "purchase_price"):
        if deal is not None and getattr(deal, attr, None) is not None:
            return _safe_float(getattr(deal, attr, None), 0.0)
    for attr in ("asking_price", "list_price", "price"):
        if getattr(prop, attr, None) is not None:
            return _safe_float(getattr(prop, attr, None), 0.0)
    return None


def _norm_city(s: str) -> str:
    return (s or "").strip().title()


def _norm_state(s: str) -> str:
    return (s or "MI").strip().upper()


def _maybe_geo_enrich_property(db: Session, *, org_id: int, property_id: int, force: bool = False) -> dict[str, Any]:
    key = os.getenv("GOOGLE_MAPS_API_KEY")
    try:
        return asyncio.run(
            enrich_property_geo(
                db,
                org_id=org_id,
                property_id=property_id,
                google_api_key=key,
                force=force,
            )
        )
    except RuntimeError:
        return {"ok": False, "error": "geo_enrichment_runtime_error"}
    except Exception as e:
        log.exception(
            "property_geo_enrich_failed",
            extra={"org_id": org_id, "property_id": property_id, "force": force},
        )
        return {"ok": False, "error": str(e)}


def _pick_jurisdiction_rule(db: Session, org_id: int, city: str, state: str) -> JurisdictionRule | None:
    city = _norm_city(city)
    state = _norm_state(state)

    jr = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id == org_id,
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    )
    if jr:
        return jr

    return db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id.is_(None),
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    )


def _latest_deal(db: Session, *, org_id: int, property_id: int) -> Deal | None:
    return db.scalar(
        select(Deal)
        .where(Deal.org_id == org_id, Deal.property_id == property_id)
        .order_by(desc(Deal.updated_at), desc(Deal.id))
        .limit(1)
    )


def _latest_underwriting(db: Session, *, org_id: int, property_id: int) -> UnderwritingResult | None:
    return db.scalar(
        select(UnderwritingResult)
        .join(Deal, Deal.id == UnderwritingResult.deal_id)
        .where(UnderwritingResult.org_id == org_id, Deal.property_id == property_id)
        .order_by(desc(UnderwritingResult.created_at), desc(UnderwritingResult.id))
        .limit(1)
    )


def _rent_explain_for_view(db: Session, *, org_id: int, property_id: int, strategy: str) -> RentExplainOut:
    ra = db.scalar(
        select(RentAssumption).where(
            RentAssumption.org_id == org_id,
            RentAssumption.property_id == property_id,
        )
    )
    if not ra:
        raise HTTPException(status_code=404, detail="rent assumption not found")

    ps = float(getattr(settings, "default_payment_standard_pct", getattr(settings, "payment_standard_pct", 1.0)) or 1.0)
    computed = recompute_rent_fields(
        db,
        property_id=property_id,
        strategy=strategy,
        payment_standard_pct=ps,
    )

    approved = computed.get("approved_rent_ceiling")
    calibrated_market_rent = computed.get("calibrated_market_rent")
    rent_used = computed.get("rent_used")
    cap_reason = str(computed.get("rent_cap_reason") or "missing_rent_inputs")
    explanation = computed.get("explanation")

    ceiling_candidates: list[CeilingCandidate] = []
    if approved is not None:
        ceiling_candidates.append(CeilingCandidate(type="approved_fmr_ceiling", value=float(approved)))
    if ra.rent_reasonableness_comp is not None and float(ra.rent_reasonableness_comp) > 0:
        ceiling_candidates.append(CeilingCandidate(type="rent_reasonableness_comp", value=float(ra.rent_reasonableness_comp)))

    return RentExplainOut(
        property_id=property_id,
        strategy=strategy,
        payment_standard_pct=ps,
        market_rent_estimate=ra.market_rent_estimate,
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        approved_rent_ceiling=approved,
        calibrated_market_rent=calibrated_market_rent,
        rent_used=rent_used,
        ceiling_candidates=ceiling_candidates,
        cap_reason=cap_reason,
        explanation=explanation,
        fmr_adjusted=approved,
        run_id=None,
        created_at=None,
    )


def _merge_checklist_state(db: Session, org_id: int, property_id: int, items: list[ChecklistItemOut]) -> list[ChecklistItemOut]:
    state_rows = db.scalars(
        select(PropertyChecklistItem).where(
            PropertyChecklistItem.org_id == org_id,
            PropertyChecklistItem.property_id == property_id,
        )
    ).all()
    by_code: dict[str, PropertyChecklistItem] = {r.item_code: r for r in state_rows}

    user_ids = {r.marked_by_user_id for r in state_rows if r.marked_by_user_id}
    users_by_id: dict[int, str] = {}
    if user_ids:
        for user in db.scalars(select(AppUser).where(AppUser.id.in_(list(user_ids)))).all():
            users_by_id[user.id] = user.email

    out: list[ChecklistItemOut] = []
    for item in items:
        state_row = by_code.get(item.item_code)
        if state_row:
            item.status = state_row.status
            item.marked_at = state_row.marked_at
            item.proof_url = state_row.proof_url
            item.notes = state_row.notes
            if state_row.marked_by_user_id:
                item.marked_by = users_by_id.get(state_row.marked_by_user_id)
        out.append(item)
    return out


def _extract_urls_from_any(value: Any, out: list[str]) -> None:
    if value is None:
        return

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return

        if s.startswith("http") and any(x in s.lower() for x in [".jpg", ".jpeg", ".png", ".webp", "image", "photo"]):
            out.append(s)
            return

        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                parsed = json.loads(s)
                _extract_urls_from_any(parsed, out)
            except Exception:
                pass

        for url in re.findall(r"https?://[^\s'\"<>]+", s):
            lo = url.lower()
            if any(x in lo for x in [".jpg", ".jpeg", ".png", ".webp", "zillowstatic", "photos.zillowstatic"]):
                out.append(url)
        return

    if isinstance(value, dict):
        for v in value.values():
            _extract_urls_from_any(v, out)
        return

    if isinstance(value, list):
        for item in value:
            _extract_urls_from_any(item, out)
        return


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for item in items:
        value = item.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _extract_zillow_photo_urls(source_raw_json: Optional[str]) -> list[str]:
    if not source_raw_json:
        return []

    try:
        raw = json.loads(source_raw_json)
    except Exception:
        return []

    urls: list[str] = []
    _extract_urls_from_any(raw, urls)

    cleaned = []
    for url in _dedupe_keep_order(urls):
        lo = url.lower()
        if any(tok in lo for tok in [".jpg", ".jpeg", ".png", ".webp", "zillowstatic", "photos.zillowstatic"]):
            cleaned.append(url)

    return cleaned[:50]


def _latest_zillow_deal(db: Session, *, org_id: int, property_id: int) -> Deal | None:
    return db.scalar(
        select(Deal)
        .where(
            Deal.org_id == org_id,
            Deal.property_id == property_id,
            Deal.source == "zillow",
        )
        .order_by(desc(Deal.id))
        .limit(1)
    )


def _photo_gallery_for_property(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    zdeal = _latest_zillow_deal(db, org_id=org_id, property_id=property_id)
    photos = _extract_zillow_photo_urls(getattr(zdeal, "source_raw_json", None)) if zdeal else []
    return {
        "cover_url": photos[0] if photos else None,
        "photos": photos,
        "count": len(photos),
        "source": "zillow_import" if photos else None,
    }


def _build_property_list_item(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    recompute_state: bool = False,
) -> dict[str, Any]:
    deal = _latest_deal(db, org_id=org_id, property_id=int(prop.id))
    uw = _latest_underwriting(db, org_id=org_id, property_id=int(prop.id))
    state_payload = get_state_payload(
        db,
        org_id=org_id,
        property_id=int(prop.id),
        recompute=bool(recompute_state),
    )
    workflow = build_workflow_summary(
        db,
        org_id=org_id,
        property_id=int(prop.id),
        recompute=False,
    )

    prop_payload = PropertyOut.model_validate(prop, from_attributes=True).model_dump()

    acquisition_meta = _property_acquisition_meta(db, org_id=org_id, property_id=int(prop.id))
    acquisition_tags = [row.get("tag") for row in list_property_tags(db, org_id=org_id, property_id=int(prop.id))]

    prop_payload.update(
        {
            "asking_price": _asking_price(prop, deal),
            "projected_monthly_cashflow": _safe_float(getattr(uw, "cash_flow", None), 0.0) if uw else None,
            "dscr": _safe_float(getattr(uw, "dscr", None), 0.0) if uw else None,
            "crime_score": getattr(prop, "crime_score", None),
            "crime_label": _crime_label(getattr(prop, "crime_score", None)),
            "crime_band": getattr(prop, "crime_band", None),
            "crime_source": getattr(prop, "crime_source", None),
            "crime_method": getattr(prop, "crime_method", None),
            "crime_radius_miles": getattr(prop, "crime_radius_miles", None),
            "crime_area_sq_miles": getattr(prop, "crime_area_sq_miles", None),
            "crime_area_type": getattr(prop, "crime_area_type", None),
            "crime_incident_count": getattr(prop, "crime_incident_count", None),
            "crime_weighted_incident_count": getattr(prop, "crime_weighted_incident_count", None),
            "crime_nearest_incident_miles": getattr(prop, "crime_nearest_incident_miles", None),
            "crime_dataset_version": getattr(prop, "crime_dataset_version", None),
            "crime_confidence": getattr(prop, "crime_confidence", None),
            "investment_area_band": getattr(prop, "investment_area_band", None),
            "offender_band": getattr(prop, "offender_band", None),
            "offender_source": getattr(prop, "offender_source", None),
            "offender_radius_miles": getattr(prop, "offender_radius_miles", None),
            "nearest_offender_miles": getattr(prop, "nearest_offender_miles", None),
            "risk_score": getattr(prop, "risk_score", None),
            "risk_band": getattr(prop, "risk_band", None),
            "risk_summary": getattr(prop, "risk_summary", None),
            "risk_confidence": getattr(prop, "risk_confidence", None),
            "risk_last_computed_at": getattr(prop, "risk_last_computed_at", None),
            "normalized_decision": state_payload.get("normalized_decision")
            or normalize_decision_bucket(getattr(uw, "decision", None) if uw else None),
            "current_workflow_stage": state_payload.get("current_stage"),
            "current_workflow_stage_label": state_payload.get("current_stage_label"),
            "gate_status": state_payload.get("gate_status"),
            "gate": state_payload.get("gate"),
            "stage_completion_summary": state_payload.get("stage_completion_summary"),
            "next_actions": state_payload.get("next_actions") or [],
            "workflow": workflow,
            "acquisition_tags": acquisition_tags,
            "acquisition_first_seen_at": acquisition_meta.get("acquisition_first_seen_at"),
            "acquisition_last_seen_at": acquisition_meta.get("acquisition_last_seen_at"),
            "acquisition_source": {
                "provider": acquisition_meta.get("acquisition_source_provider"),
                "slug": acquisition_meta.get("acquisition_source_slug"),
                "record_id": acquisition_meta.get("acquisition_source_record_id"),
                "url": acquisition_meta.get("acquisition_source_url"),
            },
            "completeness_status": {
                "geo": acquisition_meta.get("completeness_geo_status") or "missing",
                "rent": acquisition_meta.get("completeness_rent_status") or "missing",
                "rehab": acquisition_meta.get("completeness_rehab_status") or "missing",
                "risk": acquisition_meta.get("completeness_risk_status") or "missing",
                "jurisdiction": acquisition_meta.get("completeness_jurisdiction_status") or "missing",
                "cashflow": acquisition_meta.get("completeness_cashflow_status") or "missing",
            },
            "listing_zillow_url": _resolved_zillow_listing_url(
                stored_url=acquisition_meta.get("listing_zillow_url"),
                address=getattr(prop, "address", None),
                city=getattr(prop, "city", None),
                state=getattr(prop, "state", None),
                zip_code=getattr(prop, "zip", None),
            ),
        }
    )
    return prop_payload


def _sort_inventory_rows(rows: list[dict[str, Any]], wanted_sort: str) -> list[dict[str, Any]]:
    sort_key = str(wanted_sort or "rank_score").strip().lower()

    if sort_key in {"rank_score", "relevance"}:
        rows.sort(
            key=lambda item: (
                float(item.get("rank_score") or float("-inf")),
                float(item.get("risk_adjusted_score") or float("-inf")),
                float(item.get("projected_monthly_cashflow") or float("-inf")),
                float(item.get("dscr") or float("-inf")),
                float(item.get("freshness_score") or 0.0),
                int(item.get("id") or 0),
            ),
            reverse=True,
        )
    elif sort_key == "best_cashflow":
        rows.sort(
            key=lambda item: (
                float(item.get("projected_monthly_cashflow") or float("-inf")),
                float(item.get("rank_score") or float("-inf")),
                float(item.get("dscr") or float("-inf")),
                float(item.get("freshness_score") or 0.0),
                int(item.get("id") or 0),
            ),
            reverse=True,
        )
    elif sort_key == "best_dscr":
        rows.sort(
            key=lambda item: (
                float(item.get("dscr") or float("-inf")),
                float(item.get("rank_score") or float("-inf")),
                float(item.get("projected_monthly_cashflow") or float("-inf")),
                float(item.get("freshness_score") or 0.0),
                int(item.get("id") or 0),
            ),
            reverse=True,
        )
    elif sort_key == "best_rent_gap":
        rows.sort(
            key=lambda item: (
                float(item.get("rent_gap") or float("-inf")),
                float(item.get("rank_score") or float("-inf")),
                float(item.get("projected_monthly_cashflow") or float("-inf")),
                float(item.get("freshness_score") or 0.0),
                int(item.get("id") or 0),
            ),
            reverse=True,
        )
    elif sort_key == "lowest_risk":
        rows.sort(
            key=lambda item: (
                float(item.get("risk_score") or float("inf")),
                -float(item.get("rank_score") or float("-inf")),
                -float(item.get("projected_monthly_cashflow") or float("-inf")),
                -float(item.get("freshness_score") or 0.0),
                -int(item.get("id") or 0),
            )
        )
    elif sort_key == "lowest_price":
        rows.sort(
            key=lambda item: (
                float(item.get("asking_price") or float("inf")),
                -float(item.get("rank_score") or float("-inf")),
                -float(item.get("freshness_score") or 0.0),
                -int(item.get("id") or 0),
            )
        )
    elif sort_key == "highest_price":
        rows.sort(
            key=lambda item: (
                float(item.get("asking_price") or float("-inf")),
                float(item.get("rank_score") or float("-inf")),
                float(item.get("freshness_score") or 0.0),
                int(item.get("id") or 0),
            ),
            reverse=True,
        )
    elif sort_key == "newest":
        rows.sort(
            key=lambda item: (
                bool(item.get("is_new_this_sync")),
                bool(item.get("is_recently_refreshed")),
                str(
                    item.get("source_updated_at")
                    or item.get("acquisition_last_seen_at")
                    or item.get("updated_at")
                    or item.get("created_at")
                    or ""
                ),
                float(item.get("freshness_score") or 0.0),
                float(item.get("rank_score") or float("-inf")),
                int(item.get("id") or 0),
            ),
            reverse=True,
        )
    else:
        rows.sort(
            key=lambda item: (
                float(item.get("rank_score") or float("-inf")),
                float(item.get("risk_adjusted_score") or float("-inf")),
                float(item.get("projected_monthly_cashflow") or float("-inf")),
                float(item.get("dscr") or float("-inf")),
                float(item.get("freshness_score") or 0.0),
                -int(bool(item.get("is_stale"))),
                -int(bool(item.get("is_very_stale"))),
                int(item.get("id") or 0),
            ),
            reverse=True,
        )

    return rows


@router.post("", response_model=PropertyOut)
def create_property(payload: PropertyCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = Property(**payload.model_dump())
    row.org_id = p.org_id
    db.add(row)
    db.commit()
    db.refresh(row)

    if row.lat is None or row.lng is None or not row.county:
        _maybe_geo_enrich_property(db, org_id=p.org_id, property_id=int(row.id), force=False)
        db.refresh(row)

    return row


@router.get("", response_model=list[dict])
def list_properties(
    state: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    decision: Optional[str] = Query(default=None),
    only_red_zone: bool = Query(default=False),
    exclude_red_zone: bool = Query(default=False),
    min_crime_score: Optional[float] = Query(default=None),
    max_crime_score: Optional[float] = Query(default=None),
    min_offender_count: Optional[int] = Query(default=None),
    max_offender_count: Optional[int] = Query(default=None),
    hide_stale: bool = Query(default=False),
    hide_very_stale: bool = Query(default=False),
    freshness: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    include_hidden: bool = Query(default=False),
    deals_only: bool = Query(default=False),
    include_suppressed: bool = Query(default=False),
    sort: Optional[str] = Query(default="rank_score"),
):
    req_t0 = time.perf_counter()

    scope = build_inventory_snapshots_for_scope(
        db,
        org_id=int(p.org_id),
        state=state,
        county=county,
        city=city,
        q=q,
        assigned_user_id=_principal_user_id(p),
        limit=limit,
        include_hidden=include_hidden,
    )

    rows = list(scope.get("rows") or [])
    wanted_freshness = str(freshness or "").strip().lower() or None

    def keep(row: dict[str, Any]) -> bool:
        row_stage = str(row.get("current_workflow_stage") or "").strip().lower()
        row_decision = str(row.get("normalized_decision") or "").strip().upper()
        crime_score = row.get("crime_score")
        offender_count = row.get("offender_count")
        is_red_zone = row.get("is_red_zone")
        row_bucket = str(row.get("freshness_bucket") or "").strip().lower()

        if stage and row_stage != str(stage).strip().lower():
            return False

        if decision:
            wanted = normalize_decision_bucket(decision)
            normalized = normalize_decision_bucket(row_decision)
            if normalized != wanted:
                return False

        if deals_only:
            if include_suppressed:
                if str(row.get("deal_filter_status") or "").strip().lower() == "hidden":
                    return False
            else:
                if not bool(row.get("is_deal_candidate")):
                    return False

        if only_red_zone and not bool(is_red_zone):
            return False

        if exclude_red_zone and bool(is_red_zone):
            return False

        if min_crime_score is not None:
            if crime_score is None or float(crime_score) < float(min_crime_score):
                return False

        if max_crime_score is not None:
            if crime_score is None or float(crime_score) > float(max_crime_score):
                return False

        if min_offender_count is not None:
            if offender_count is None or int(offender_count) < int(min_offender_count):
                return False

        if max_offender_count is not None:
            if offender_count is None or int(offender_count) > int(max_offender_count):
                return False

        if hide_very_stale and bool(row.get("is_very_stale")):
            return False

        if hide_stale and bool(row.get("is_stale")):
            return False

        if wanted_freshness:
            if wanted_freshness == "new" and row_bucket != "new":
                return False
            if wanted_freshness == "fresh" and row_bucket not in {"new", "fresh"}:
                return False
            if wanted_freshness == "warm" and row_bucket not in {"new", "fresh", "warm"}:
                return False
            if wanted_freshness == "aging" and row_bucket != "aging":
                return False
            if wanted_freshness == "stale" and row_bucket != "stale":
                return False
            if wanted_freshness == "very_stale" and row_bucket != "very_stale":
                return False

        return True

    rows = [row for row in rows if keep(row)]

    if deals_only:
        if include_suppressed:
            rows = [
                row
                for row in rows
                if str(row.get("deal_filter_status") or "").strip().lower() != "hidden"
            ]
        else:
            rows = [row for row in rows if bool(row.get("is_deal_candidate"))]

    wanted_sort = str(sort or "rank_score").strip().lower()
    rows = _sort_inventory_rows(rows, wanted_sort)

    total_ms = round((time.perf_counter() - req_t0) * 1000, 2)

    log.info(
        "properties_list_complete_inventory_snapshot",
        extra={
            "org_id": p.org_id,
            "state": state,
            "city": city,
            "county": county,
            "q": q,
            "stage": stage,
            "decision": decision,
            "freshness": wanted_freshness,
            "hide_stale": hide_stale,
            "hide_very_stale": hide_very_stale,
            "sort": wanted_sort,
            "limit": limit,
            "returned_rows": len(rows),
            "total_ms": total_ms,
        },
    )

    return rows[: max(1, int(limit))]


@router.get("/{property_id}", response_model=PropertyOut)
def get_property(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    stmt = (
        select(Property)
        .where(Property.id == property_id)
        .where(Property.org_id == p.org_id)
        .options(selectinload(Property.rent_assumption), selectinload(Property.rent_comps))
    )
    row = db.execute(stmt).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Property not found")

    meta = _property_acquisition_meta(db, org_id=p.org_id, property_id=int(row.id))
    payload = PropertyOut.model_validate(row, from_attributes=True).model_dump()
    payload.update(
        {
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
            "listing_zillow_url": _resolved_zillow_listing_url(
                stored_url=meta.get("listing_zillow_url"),
                address=getattr(row, "address", None),
                city=getattr(row, "city", None),
                state=getattr(row, "state", None),
                zip_code=getattr(row, "zip", None),
            ),
            "listing_agent_name": meta.get("listing_agent_name"),
            "listing_agent_phone": meta.get("listing_agent_phone"),
            "listing_agent_email": meta.get("listing_agent_email"),
            "listing_agent_website": meta.get("listing_agent_website"),
            "listing_office_name": meta.get("listing_office_name"),
            "listing_office_phone": meta.get("listing_office_phone"),
            "listing_office_email": meta.get("listing_office_email"),
        }
    )
    return PropertyOut.model_validate(payload)


@router.post("/{property_id}/geo/enrich", response_model=dict)
def geo_enrich_property(
    property_id: int,
    force: bool = Query(default=False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.scalar(select(Property).where(Property.org_id == p.org_id, Property.id == property_id))
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    return _maybe_geo_enrich_property(
        db,
        org_id=p.org_id,
        property_id=int(property_id),
        force=bool(force),
    )


@router.get("/{property_id}/view", response_model=PropertyViewOut)
def property_view(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    stmt = (
        select(Property)
        .where(Property.id == property_id)
        .where(Property.org_id == p.org_id)
        .options(selectinload(Property.rent_assumption), selectinload(Property.rent_comps))
    )
    prop = db.execute(stmt).scalar_one_or_none()
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    deal = _latest_deal(db, org_id=p.org_id, property_id=int(prop.id))
    if not deal:
        raise HTTPException(status_code=404, detail="No deal found for property")

    jr = _pick_jurisdiction_rule(db, org_id=p.org_id, city=prop.city, state=prop.state)
    friction = compute_friction(jr)
    uw = _latest_underwriting(db, org_id=p.org_id, property_id=int(prop.id))
    meta = _property_acquisition_meta(db, org_id=p.org_id, property_id=int(prop.id))
    snapshot = build_property_inventory_snapshot(db, org_id=p.org_id, property_id=int(prop.id))

    checklist_row = db.scalar(
        select(PropertyChecklist)
        .where(PropertyChecklist.org_id == p.org_id, PropertyChecklist.property_id == prop.id)
        .order_by(desc(PropertyChecklist.id))
        .limit(1)
    )

    checklist_out: ChecklistOut | None = None
    if checklist_row:
        try:
            parsed = json.loads(checklist_row.items_json or "[]")
        except Exception:
            parsed = []

        items = [ChecklistItemOut(**x) for x in parsed if isinstance(x, dict)]
        items = _merge_checklist_state(db, org_id=p.org_id, property_id=prop.id, items=items)
        checklist_out = ChecklistOut(
            property_id=prop.id,
            city=prop.city,
            state=prop.state,
            items=items,
        )

    rent_explain = _rent_explain_for_view(
        db,
        org_id=p.org_id,
        property_id=prop.id,
        strategy=deal.strategy,
    )

    property_payload = PropertyOut.model_validate(prop, from_attributes=True).model_dump()

    property_payload.update(
        {
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
            "listing_zillow_url": _resolved_zillow_listing_url(
                stored_url=meta.get("listing_zillow_url"),
                address=getattr(prop, "address", None),
                city=getattr(prop, "city", None),
                state=getattr(prop, "state", None),
                zip_code=getattr(prop, "zip", None),
            ),
            "listing_agent_name": meta.get("listing_agent_name"),
            "listing_agent_phone": meta.get("listing_agent_phone"),
            "listing_agent_email": meta.get("listing_agent_email"),
            "listing_agent_website": meta.get("listing_agent_website"),
            "listing_office_name": meta.get("listing_office_name"),
            "listing_office_phone": meta.get("listing_office_phone"),
            "listing_office_email": meta.get("listing_office_email"),
        }
    )

    return PropertyViewOut(
        property=PropertyOut.model_validate(property_payload),
        deal=DealOut.model_validate(deal, from_attributes=True),
        rent_explain=rent_explain,
        jurisdiction_rule=JurisdictionRuleOut.model_validate(jr, from_attributes=True) if jr else None,
        jurisdiction_friction={
            "multiplier": getattr(friction, "multiplier", 1.0),
            "reasons": getattr(friction, "reasons", []),
        },
        last_underwriting_result=UnderwritingResultOut.model_validate(uw) if uw else None,
        checklist=checklist_out,
        inventory_snapshot=snapshot,
    )


@router.get("/{property_id}/bundle", response_model=dict)
def property_bundle(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    view = property_view(property_id=property_id, db=db, p=p)

    rehab = db.scalars(
        select(RehabTask)
        .where(RehabTask.org_id == p.org_id, RehabTask.property_id == property_id)
        .order_by(desc(RehabTask.id))
        .limit(500)
    ).all()

    leases = db.scalars(
        select(Lease)
        .where(Lease.org_id == p.org_id, Lease.property_id == property_id)
        .order_by(desc(Lease.id))
        .limit(300)
    ).all()

    txns = db.scalars(
        select(Transaction)
        .where(Transaction.org_id == p.org_id, Transaction.property_id == property_id)
        .order_by(desc(Transaction.id))
        .limit(1000)
    ).all()

    vals = db.scalars(
        select(Valuation)
        .where(Valuation.org_id == p.org_id, Valuation.property_id == property_id)
        .order_by(desc(Valuation.id))
        .limit(100)
    ).all()

    gallery = _photo_gallery_for_property(db, org_id=p.org_id, property_id=property_id)
    snapshot = build_property_inventory_snapshot(db, org_id=p.org_id, property_id=property_id)

    return {
        "view": view.model_dump(),
        "rehab_tasks": [RehabTaskOut.model_validate(x, from_attributes=True).model_dump() for x in rehab],
        "leases": [LeaseOut.model_validate(x, from_attributes=True).model_dump() for x in leases],
        "transactions": [TransactionOut.model_validate(x, from_attributes=True).model_dump() for x in txns],
        "valuations": [ValuationOut.model_validate(x, from_attributes=True).model_dump() for x in vals],
        "gallery": gallery,
        "inventory_snapshot": snapshot,
    }


@router.get("/{property_id}/tags", response_model=list[dict])
def property_tags(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.scalar(select(Property).where(Property.org_id == p.org_id, Property.id == property_id))
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")
    return list_property_tags(db, org_id=p.org_id, property_id=property_id)


@router.put("/{property_id}/tags", response_model=dict)
def update_property_tags(
    property_id: int,
    payload: AcquisitionTagsIn,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.scalar(select(Property).where(Property.org_id == p.org_id, Property.id == property_id))
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    tags = replace_property_tags(
        db,
        org_id=p.org_id,
        property_id=property_id,
        tags=payload.tags,
    )
    return {"ok": True, "property_id": property_id, "tags": tags}
