from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..db import get_db
from ..models import JurisdictionRule
from ..schemas import JurisdictionRuleUpsert, JurisdictionRuleOut

router = APIRouter(prefix="/jurisdictions", tags=["jurisdictions"])


@router.put("", response_model=JurisdictionRuleOut)
def upsert_jurisdiction(payload: JurisdictionRuleUpsert, db: Session = Depends(get_db)):
    jr = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.city == payload.city,
            JurisdictionRule.state == payload.state,
        )
    )

    typical = payload.typical_fail_points
    data = payload.model_dump(exclude={"typical_fail_points"})
    if typical is not None:
        data["typical_fail_points_json"] = json.dumps(typical, ensure_ascii=False)

    if jr is None:
        jr = JurisdictionRule(**data)
        db.add(jr)
    else:
        for k, v in data.items():
            setattr(jr, k, v)

    db.commit()
    db.refresh(jr)

    out = JurisdictionRuleOut(
        id=jr.id,
        city=jr.city,
        state=jr.state,
        rental_license_required=jr.rental_license_required,
        inspection_authority=jr.inspection_authority,
        typical_fail_points=json.loads(jr.typical_fail_points_json) if jr.typical_fail_points_json else None,
        registration_fee=jr.registration_fee,
        processing_days=jr.processing_days,
        tenant_waitlist_depth=jr.tenant_waitlist_depth,
    )
    return out


@router.get("/{state}/{city}", response_model=JurisdictionRuleOut)
def get_jurisdiction(state: str, city: str, db: Session = Depends(get_db)):
    jr = db.scalar(select(JurisdictionRule).where(JurisdictionRule.city == city, JurisdictionRule.state == state))
    if not jr:
        raise HTTPException(status_code=404, detail="Jurisdiction rule not found")

    return JurisdictionRuleOut(
        id=jr.id,
        city=jr.city,
        state=jr.state,
        rental_license_required=jr.rental_license_required,
        inspection_authority=jr.inspection_authority,
        typical_fail_points=json.loads(jr.typical_fail_points_json) if jr.typical_fail_points_json else None,
        registration_fee=jr.registration_fee,
        processing_days=jr.processing_days,
        tenant_waitlist_depth=jr.tenant_waitlist_depth,
    )
