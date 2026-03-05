# backend/app/routers/jurisdiction_profiles.py
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.auth import get_principal, require_owner
from app.db import get_db
from app.policy_models import JurisdictionProfile
from app.schemas import (
    JurisdictionProfileIn,
    JurisdictionProfileOut,
    JurisdictionProfileResolveOut,
)
from app.services.jurisdiction_profile_service import (
    delete_profile,
    list_profiles,
    resolve_profile,
    upsert_profile,
    _loads,  # ok to reuse internal helper here
)

router = APIRouter(prefix="/jurisdiction-profiles", tags=["jurisdiction_profiles"])


def _row_to_out(r: JurisdictionProfile) -> JurisdictionProfileOut:
    return JurisdictionProfileOut(
        id=int(r.id),
        scope="global" if r.org_id is None else "org",
        org_id=r.org_id,
        state=r.state,
        county=r.county,
        city=r.city,
        friction_multiplier=float(r.friction_multiplier or 1.0),
        pha_name=r.pha_name,
        policy=_loads(getattr(r, "policy_json", None)),
        notes=r.notes,
        created_at=(r.created_at.isoformat() if getattr(r, "created_at", None) else None),
        updated_at=(r.updated_at.isoformat() if getattr(r, "updated_at", None) else None),
    )


@router.get("", response_model=list[JurisdictionProfileOut])
def get_profiles(
    include_global: bool = Query(True),
    state: str = Query("MI"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    rows = list_profiles(db, org_id=p.org_id, include_global=include_global, state=state)

    # stable-ish ordering: scope, specificity, name
    def k(o: JurisdictionProfile):
        scope = 0 if o.org_id is None else 1
        spec = 2 if (o.city or "").strip() else (1 if (o.county or "").strip() else 0)
        name = (o.city or o.county or o.state or "")
        return (scope, spec, name)

    rows.sort(key=k)
    return [_row_to_out(r) for r in rows]


@router.get("/resolve", response_model=JurisdictionProfileResolveOut)
def resolve(
    city: Optional[str] = Query(None),
    county: Optional[str] = Query(None),
    state: str = Query("MI"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    out = resolve_profile(db, org_id=p.org_id, city=city, county=county, state=state)
    return JurisdictionProfileResolveOut(**out)


@router.post("", response_model=JurisdictionProfileOut)
def upsert(
    payload: JurisdictionProfileIn,
    db: Session = Depends(get_db),
    p=Depends(require_owner),
):
    row = upsert_profile(
        db,
        org_id=p.org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        friction_multiplier=payload.friction_multiplier,
        pha_name=payload.pha_name,
        policy=payload.policy,
        notes=payload.notes,
    )
    return _row_to_out(row)


@router.delete("", response_model=dict)
def delete(
    state: str = Query("MI"),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    p=Depends(require_owner),
):
    n = delete_profile(db, org_id=p.org_id, state=state, county=county, city=city)
    if n == 0:
        raise HTTPException(status_code=404, detail="profile not found")
    return {"deleted": n}
