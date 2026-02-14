from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..auth import get_principal, require_owner
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


def _to_out(jr: JurisdictionRule) -> JurisdictionRuleOut:
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


@router.put("", response_model=JurisdictionRuleOut)
def upsert_jurisdiction(payload: JurisdictionRuleUpsert, db: Session = Depends(get_db), p=Depends(get_principal)):
    # Global table: only owners can change it (prevents truth drift)
    require_owner(p)

    jr = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.city == payload.city,
            JurisdictionRule.state == payload.state,
        )
    )

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
    return _to_out(jr)


@router.get("", response_model=list[JurisdictionRuleOut])
def list_jurisdictions(
    city: str | None = Query(default=None),
    state: str = Query(default="MI"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = select(JurisdictionRule).order_by(JurisdictionRule.city)
    if city:
        q = q.where(JurisdictionRule.city == city, JurisdictionRule.state == state)
    rows = db.scalars(q).all()
    return [_to_out(jr) for jr in rows]


@router.get("/{jurisdiction_id}", response_model=JurisdictionRuleOut)
def get_jurisdiction(jurisdiction_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    jr = db.get(JurisdictionRule, jurisdiction_id)
    if not jr:
        raise HTTPException(status_code=404, detail="jurisdiction not found")
    return _to_out(jr)
