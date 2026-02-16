from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, desc, or_
from sqlalchemy.orm import Session, selectinload

from ..auth import get_principal
from ..db import get_db
from ..models import Property, JurisdictionRule, Deal, UnderwritingResult, PropertyChecklist
from ..schemas import (
    PropertyCreate,
    PropertyOut,
    JurisdictionRuleOut,
    PropertyViewOut,
    DealOut,
    UnderwritingResultOut,
    RentExplainOut,
    ChecklistOut,
    ChecklistItemOut,
)
from ..domain.jurisdiction_scoring import compute_friction
from ..config import settings
from ..models import RentAssumption

router = APIRouter(prefix="/properties", tags=["properties"])


def _norm_city(s: str) -> str:
    return (s or "").strip().title()


def _norm_state(s: str) -> str:
    return (s or "MI").strip().upper()


def _pick_jurisdiction_rule(db: Session, org_id: int, city: str, state: str) -> JurisdictionRule | None:
    """
    Precedence:
      1) org-specific rule (JurisdictionRule.org_id == org_id)
      2) global rule (JurisdictionRule.org_id IS NULL)
    """
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


@router.post("", response_model=PropertyOut)
def create_property(payload: PropertyCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = Property(**payload.model_dump())
    row.org_id = p.org_id
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.get("/{property_id}", response_model=PropertyOut)
def get_property(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    stmt = (
        select(Property)
        .where(Property.id == property_id)
        .where(Property.org_id == p.org_id)
        .options(
            selectinload(Property.rent_assumption),
            selectinload(Property.rent_comps),
        )
    )
    row = db.execute(stmt).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Property not found")

    jr = _pick_jurisdiction_rule(db, org_id=p.org_id, city=row.city, state=row.state)

    out = PropertyOut.model_validate(row, from_attributes=True).model_dump()
    out["jurisdiction_rule"] = (
        JurisdictionRuleOut.model_validate(jr, from_attributes=True).model_dump() if jr else None
    )
    return out


def _rent_explain_for_view(db: Session, property_id: int, strategy: str) -> RentExplainOut:
    ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == property_id))
    if not ra:
        raise HTTPException(status_code=404, detail="rent assumption not found")

    ps = float(settings.payment_standard_pct)

    fmr_adjusted = (float(ra.section8_fmr) * ps) if (ra.section8_fmr is not None and float(ra.section8_fmr) > 0) else None

    cap_reason = "none"
    ceiling_candidates: list[dict] = []

    if fmr_adjusted is not None:
        ceiling_candidates.append({"type": "payment_standard", "value": fmr_adjusted})
    if ra.rent_reasonableness_comp is not None and float(ra.rent_reasonableness_comp) > 0:
        ceiling_candidates.append({"type": "rent_reasonableness", "value": float(ra.rent_reasonableness_comp)})

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
    )


@router.get("/{property_id}/view", response_model=PropertyViewOut)
def property_view(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    stmt = (
        select(Property)
        .where(Property.id == property_id)
        .where(Property.org_id == p.org_id)
        .options(
            selectinload(Property.rent_assumption),
            selectinload(Property.rent_comps),
        )
    )
    prop = db.execute(stmt).scalar_one_or_none()
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    d = db.scalar(
        select(Deal)
        .where(Deal.property_id == prop.id)
        .where(Deal.org_id == p.org_id)
        .order_by(desc(Deal.id))
        .limit(1)
    )
    if not d:
        raise HTTPException(status_code=404, detail="No deal found for property")

    jr = _pick_jurisdiction_rule(db, org_id=p.org_id, city=prop.city, state=prop.state)
    friction = compute_friction(jr)

    r = db.scalar(
        select(UnderwritingResult)
        .where(UnderwritingResult.deal_id == d.id)
        .where(UnderwritingResult.org_id == p.org_id)
        .order_by(desc(UnderwritingResult.id))
        .limit(1)
    )

    chk = db.scalar(
        select(PropertyChecklist)
        .where(PropertyChecklist.property_id == prop.id)
        .order_by(desc(PropertyChecklist.id))
        .limit(1)
    )
    checklist_out: ChecklistOut | None = None
    if chk:
        try:
            parsed = json.loads(chk.items_json or "[]")
        except Exception:
            parsed = []
        items = [ChecklistItemOut(**x) for x in parsed if isinstance(x, dict)]
        checklist_out = ChecklistOut(property_id=prop.id, city=prop.city, state=prop.state, items=items)

    rent_explain = _rent_explain_for_view(db, property_id=prop.id, strategy=d.strategy)

    return PropertyViewOut(
        property=PropertyOut.model_validate(prop, from_attributes=True),
        deal=DealOut.model_validate(d, from_attributes=True),
        rent_explain=rent_explain,
        jurisdiction_rule=JurisdictionRuleOut.model_validate(jr, from_attributes=True) if jr else None,
        jurisdiction_friction={
            "multiplier": getattr(friction, "multiplier", 1.0),
            "reasons": getattr(friction, "reasons", []),
        },
        last_underwriting_result=UnderwritingResultOut.model_validate(r) if r else None,
        checklist=checklist_out,
    )
