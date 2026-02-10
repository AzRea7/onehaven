from __future__ import annotations

import json

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..db import get_db
from ..models import JurisdictionRule
from ..schemas import JurisdictionRuleUpsert, JurisdictionRuleOut

router = APIRouter(prefix="/jurisdictions", tags=["jurisdictions"])


def _decode_fail_points(jr: JurisdictionRule) -> list[str] | None:
    if not jr.typical_fail_points_json:
        return None
    try:
        v = json.loads(jr.typical_fail_points_json)
        return v if isinstance(v, list) else None
    except Exception:
        return None


@router.put("", response_model=JurisdictionRuleOut)
def upsert_jurisdiction(payload: JurisdictionRuleUpsert, db: Session = Depends(get_db)):
    jr = db.scalar(select(JurisdictionRule).where(
        JurisdictionRule.city == payload.city,
        JurisdictionRule.state == payload.state,
    ))

    data = payload.model_dump(exclude={"typical_fail_points"})
    if payload.typical_fail_points is not None:
        data["typical_fail_points_json"] = json.dumps(payload.typical_fail_points)

    if not jr:
        jr = JurisdictionRule(**data)
        db.add(jr)
    else:
        for k, v in data.items():
            setattr(jr, k, v)

    db.commit()
    db.refresh(jr)

    return JurisdictionRuleOut(
        id=jr.id,
        city=jr.city,
        state=jr.state,
        rental_license_required=jr.rental_license_required,
        inspection_frequency=getattr(jr, "inspection_frequency", None),
        inspection_authority=jr.inspection_authority,
        typical_fail_points=_decode_fail_points(jr),
        registration_fee=jr.registration_fee,
        processing_days=jr.processing_days,
        tenant_waitlist_depth=jr.tenant_waitlist_depth,
        jurisdiction_type=getattr(jr, "jurisdiction_type", None),
        notes=getattr(jr, "notes", None),
    )


@router.get("", response_model=list[JurisdictionRuleOut])
def list_jurisdictions(db: Session = Depends(get_db)):
    rows = db.scalars(select(JurisdictionRule).order_by(JurisdictionRule.city)).all()
    out: list[JurisdictionRuleOut] = []
    for jr in rows:
        out.append(JurisdictionRuleOut(
            id=jr.id,
            city=jr.city,
            state=jr.state,
            rental_license_required=jr.rental_license_required,
            inspection_frequency=getattr(jr, "inspection_frequency", None),
            inspection_authority=jr.inspection_authority,
            typical_fail_points=_decode_fail_points(jr),
            registration_fee=jr.registration_fee,
            processing_days=jr.processing_days,
            tenant_waitlist_depth=jr.tenant_waitlist_depth,
            jurisdiction_type=getattr(jr, "jurisdiction_type", None),
            notes=getattr(jr, "notes", None),
        ))
    return out
