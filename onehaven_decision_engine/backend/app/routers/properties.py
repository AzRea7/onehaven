# backend/app/routers/properties.py
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, desc
from sqlalchemy.orm import Session, selectinload

from ..auth import get_principal
from ..db import get_db
from ..models import (
    Property,
    JurisdictionRule,
    Deal,
    UnderwritingResult,
    PropertyChecklist,
    PropertyChecklistItem,
    AppUser,
    RentAssumption,
    RehabTask,
    Lease,
    Transaction,
    Valuation,
)
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
    RehabTaskOut,
    LeaseOut,
    TransactionOut,
    ValuationOut,
)
from ..domain.jurisdiction_scoring import compute_friction
from ..config import settings

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
        .options(selectinload(Property.rent_assumption), selectinload(Property.rent_comps))
    )
    row = db.execute(stmt).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Property not found")

    jr = _pick_jurisdiction_rule(db, org_id=p.org_id, city=row.city, state=row.state)

    out = PropertyOut.model_validate(row, from_attributes=True).model_dump()
    out["jurisdiction_rule"] = JurisdictionRuleOut.model_validate(jr, from_attributes=True).model_dump() if jr else None
    return out


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
        fmr_adjusted=fmr_adjusted,
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
        for u in db.scalars(select(AppUser).where(AppUser.id.in_(list(user_ids)))).all():
            users_by_id[u.id] = u.email

    out: list[ChecklistItemOut] = []
    for i in items:
        s = by_code.get(i.item_code)
        if s:
            i.status = s.status
            i.marked_at = s.marked_at
            i.proof_url = s.proof_url
            i.notes = s.notes
            if s.marked_by_user_id:
                i.marked_by = users_by_id.get(s.marked_by_user_id)
        out.append(i)
    return out


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
        .where(PropertyChecklist.org_id == p.org_id, PropertyChecklist.property_id == prop.id)
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
        items = _merge_checklist_state(db, org_id=p.org_id, property_id=prop.id, items=items)
        checklist_out = ChecklistOut(property_id=prop.id, city=prop.city, state=prop.state, items=items)

    rent_explain = _rent_explain_for_view(db, org_id=p.org_id, property_id=prop.id, strategy=d.strategy)

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


@router.get("/{property_id}/bundle", response_model=dict)
def property_bundle(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    """
    Single-pane truth payload:
      - view (underwriting + rent explain + friction + checklist)
      - rehab tasks
      - leases
      - transactions
      - valuations
    """
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

    return {
      "view": view.model_dump() if hasattr(view, "model_dump") else view,
      "rehab_tasks": [RehabTaskOut.model_validate(x, from_attributes=True).model_dump() for x in rehab],
      "leases": [LeaseOut.model_validate(x, from_attributes=True).model_dump() for x in leases],
      "transactions": [TransactionOut.model_validate(x, from_attributes=True).model_dump() for x in txns],
      "valuations": [ValuationOut.model_validate(x, from_attributes=True).model_dump() for x in vals],
    }
