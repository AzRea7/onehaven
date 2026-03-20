from __future__ import annotations

import asyncio
import json
import os
import re
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import asc, desc, func, select
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
    PropertyState,
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

router = APIRouter(prefix="/properties", tags=["properties"])


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

    ps = float(settings.payment_standard_pct)

    fmr_adjusted = (
        float(ra.section8_fmr) * ps
        if (ra.section8_fmr is not None and float(ra.section8_fmr) > 0)
        else None
    )

    cap_reason = "none"
    ceiling_candidates: list[CeilingCandidate] = []

    if fmr_adjusted is not None:
        ceiling_candidates.append(CeilingCandidate(type="payment_standard", value=float(fmr_adjusted)))
    if ra.rent_reasonableness_comp is not None and float(ra.rent_reasonableness_comp) > 0:
        ceiling_candidates.append(CeilingCandidate(type="rent_reasonableness", value=float(ra.rent_reasonableness_comp)))

    if ra.approved_rent_ceiling is not None and float(ra.approved_rent_ceiling) > 0:
        cap_reason = "override"
    else:
        cands: list[tuple[str, float]] = []
        if fmr_adjusted is not None:
            cands.append(("fmr", float(fmr_adjusted)))
        if ra.rent_reasonableness_comp is not None and float(ra.rent_reasonableness_comp) > 0:
            cands.append(("comps", float(ra.rent_reasonableness_comp)))
        if cands:
            cap_reason = min(cands, key=lambda x: x[1])[0]

    return RentExplainOut(
        property_id=property_id,
        strategy=strategy,
        payment_standard_pct=ps,
        market_rent_estimate=ra.market_rent_estimate,
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        approved_rent_ceiling=ra.approved_rent_ceiling,
        calibrated_market_rent=None,
        rent_used=ra.rent_used,
        ceiling_candidates=ceiling_candidates,
        cap_reason=cap_reason,
        explanation=None,
        fmr_adjusted=fmr_adjusted,
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


def _build_property_list_item(db: Session, *, org_id: int, prop: Property) -> dict[str, Any]:
    deal = _latest_deal(db, org_id=org_id, property_id=int(prop.id))
    uw = _latest_underwriting(db, org_id=org_id, property_id=int(prop.id))
    state_payload = get_state_payload(db, org_id=org_id, property_id=int(prop.id), recompute=True)
    workflow = build_workflow_summary(db, org_id=org_id, property_id=int(prop.id), recompute=False)

    prop_payload = PropertyOut.model_validate(prop, from_attributes=True).model_dump()

    prop_payload.update(
        {
            "asking_price": _asking_price(prop, deal),
            "projected_monthly_cashflow": _safe_float(getattr(uw, "cash_flow", None), 0.0) if uw else None,
            "dscr": _safe_float(getattr(uw, "dscr", None), 0.0) if uw else None,
            "crime_score": getattr(prop, "crime_score", None),
            "crime_label": _crime_label(getattr(prop, "crime_score", None)),
            "normalized_decision": state_payload.get("normalized_decision")
            or normalize_decision_bucket(getattr(uw, "decision", None) if uw else None),
            "current_workflow_stage": state_payload.get("current_stage"),
            "current_workflow_stage_label": state_payload.get("current_stage_label"),
            "gate_status": state_payload.get("gate_status"),
            "gate": state_payload.get("gate"),
            "stage_completion_summary": state_payload.get("stage_completion_summary"),
            "next_actions": state_payload.get("next_actions") or [],
            "workflow": workflow,
        }
    )
    return prop_payload


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
    sort: Optional[str] = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    stmt = (
        select(Property)
        .where(Property.org_id == p.org_id)
        .options(selectinload(Property.rent_assumption), selectinload(Property.rent_comps))
    )

    if state:
        stmt = stmt.where(Property.state == state)
    if city:
        stmt = stmt.where(func.lower(Property.city) == city.lower())
    if county:
        stmt = stmt.where(func.lower(Property.county) == county.lower())

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
        stmt = stmt.where((Property.is_red_zone.is_(False)) | (Property.is_red_zone.is_(None)))

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

    if sort == "oldest":
        stmt = stmt.order_by(asc(Property.id))
    elif sort == "address_asc":
        stmt = stmt.order_by(asc(Property.address), desc(Property.id))
    elif sort == "address_desc":
        stmt = stmt.order_by(desc(Property.address), desc(Property.id))
    elif sort == "crime_desc":
        stmt = stmt.order_by(desc(Property.crime_score).nullslast(), desc(Property.id))
    elif sort == "crime_asc":
        stmt = stmt.order_by(asc(Property.crime_score).nullslast(), desc(Property.id))
    elif sort == "offenders_desc":
        stmt = stmt.order_by(desc(Property.offender_count).nullslast(), desc(Property.id))
    elif sort == "offenders_asc":
        stmt = stmt.order_by(asc(Property.offender_count).nullslast(), desc(Property.id))
    else:
        stmt = stmt.order_by(desc(Property.id))

    rows = db.scalars(stmt.limit(limit)).unique().all()

    wanted_stage = (stage or "").strip().lower() or None
    wanted_decision = normalize_decision_bucket(decision) if decision else None

    out: list[dict] = []
    for prop in rows:
        item = _build_property_list_item(db, org_id=p.org_id, prop=prop)

        if wanted_stage and str(item.get("current_workflow_stage") or "").lower() != wanted_stage:
            continue
        if wanted_decision and str(item.get("normalized_decision") or "").upper() != wanted_decision:
            continue

        out.append(item)

    return out


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
    return row


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
        checklist_out = ChecklistOut(property_id=prop.id, city=prop.city, state=prop.state, items=items)

    rent_explain = _rent_explain_for_view(db, org_id=p.org_id, property_id=prop.id, strategy=deal.strategy)

    return PropertyViewOut(
        property=PropertyOut.model_validate(prop, from_attributes=True),
        deal=DealOut.model_validate(deal, from_attributes=True),
        rent_explain=rent_explain,
        jurisdiction_rule=JurisdictionRuleOut.model_validate(jr, from_attributes=True) if jr else None,
        jurisdiction_friction={
            "multiplier": getattr(friction, "multiplier", 1.0),
            "reasons": getattr(friction, "reasons", []),
        },
        last_underwriting_result=UnderwritingResultOut.model_validate(uw) if uw else None,
        checklist=checklist_out,
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
        .limit(300)
    ).all()

    prop = view.property
    photo_gallery = _photo_gallery_for_property(db, org_id=p.org_id, property_id=property_id)

    return {
        "view": view.model_dump() if hasattr(view, "model_dump") else view,
        "geo": {
            "lat": getattr(prop, "lat", None),
            "lng": getattr(prop, "lng", None),
            "county": getattr(prop, "county", None),
            "is_red_zone": bool(getattr(prop, "is_red_zone", False)),
            "crime_density": getattr(prop, "crime_density", None),
            "crime_score": getattr(prop, "crime_score", None),
            "offender_count": getattr(prop, "offender_count", None),
        },
        "photo_gallery": photo_gallery,
        "rehab_tasks": [RehabTaskOut.model_validate(x, from_attributes=True).model_dump() for x in rehab],
        "leases": [LeaseOut.model_validate(x, from_attributes=True).model_dump() for x in leases],
        "transactions": [TransactionOut.model_validate(x, from_attributes=True).model_dump() for x in txns],
        "valuations": [ValuationOut.model_validate(x, from_attributes=True).model_dump() for x in vals],
    }


@router.get("/{property_id}/cockpit", response_model=dict)
def property_cockpit(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == p.org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    compute_and_persist_stage(db, org_id=p.org_id, property=prop)

    bundle = property_bundle(property_id=property_id, db=db, p=p)
    state_payload = get_state_payload(db, org_id=p.org_id, property_id=property_id, recompute=True)
    workflow = build_workflow_summary(db, org_id=p.org_id, property_id=property_id, recompute=False)

    return {
        **bundle,
        "workflow": workflow,
        "state": {
            "current_stage": state_payload.get("current_stage"),
            "current_stage_label": state_payload.get("current_stage_label"),
            "normalized_decision": state_payload.get("normalized_decision"),
            "gate_status": state_payload.get("gate_status"),
            "gate": state_payload.get("gate"),
            "constraints": state_payload.get("constraints", {}),
            "outstanding_tasks": state_payload.get("outstanding_tasks", {}),
            "next_actions": state_payload.get("next_actions", []),
            "stage_completion_summary": state_payload.get("stage_completion_summary"),
        },
    }
