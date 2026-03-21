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
from app.services.jurisdiction_completeness_service import (
    profile_completeness_payload,
    recompute_profile_and_coverage,
)
from app.services.jurisdiction_profile_service import (
    _loads,  # ok to reuse internal helper here
    delete_profile,
    list_profiles,
    resolve_profile,
    upsert_profile,
)
from app.services.jurisdiction_task_mapper import map_profile_jurisdiction_task_dicts

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


def _profile_admin_payload(db: Session, r: JurisdictionProfile) -> dict:
    completeness = profile_completeness_payload(db, r)
    return {
        **_row_to_out(r).dict(),
        "completeness": completeness,
        "tasks": map_profile_jurisdiction_task_dicts(r),
        "required_categories": completeness.get("required_categories", []),
        "covered_categories": completeness.get("covered_categories", []),
        "missing_categories": completeness.get("missing_categories", []),
        "completeness_status": completeness.get("completeness_status"),
        "completeness_score": completeness.get("completeness_score"),
        "is_stale": completeness.get("is_stale"),
        "stale_reason": completeness.get("stale_reason"),
    }


@router.get("", response_model=list[dict])
def get_profiles(
    include_global: bool = Query(True),
    state: str = Query("MI"),
    recompute: bool = Query(False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    rows = list_profiles(db, org_id=p.org_id, include_global=include_global, state=state)

    def k(o: JurisdictionProfile):
        scope = 0 if o.org_id is None else 1
        spec = 2 if (o.city or "").strip() else (1 if (o.county or "").strip() else 0)
        name = (o.city or o.county or o.state or "")
        return (scope, spec, name)

    rows.sort(key=k)

    out: list[dict] = []
    for r in rows:
        if recompute:
            r, _ = recompute_profile_and_coverage(db, r, commit=False)
        out.append(_profile_admin_payload(db, r))

    if recompute:
        db.commit()

    return out


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


@router.get("/{profile_id}", response_model=dict)
def get_profile_detail(
    profile_id: int,
    recompute: bool = Query(False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = db.get(JurisdictionProfile, int(profile_id))
    if not row:
        raise HTTPException(status_code=404, detail="profile not found")

    if row.org_id is not None and row.org_id != p.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    if recompute:
        row, _ = recompute_profile_and_coverage(db, row, commit=True)

    return _profile_admin_payload(db, row)


@router.post("", response_model=dict)
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
    row, _ = recompute_profile_and_coverage(db, row, commit=True)
    return _profile_admin_payload(db, row)


@router.post("/{profile_id}/recompute", response_model=dict)
def recompute_profile(
    profile_id: int,
    db: Session = Depends(get_db),
    p=Depends(require_owner),
):
    row = db.get(JurisdictionProfile, int(profile_id))
    if not row:
        raise HTTPException(status_code=404, detail="profile not found")

    if row.org_id is not None and row.org_id != p.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    row, coverage = recompute_profile_and_coverage(db, row, commit=True)
    return {
        "ok": True,
        "profile": _profile_admin_payload(db, row),
        "coverage": {
            "id": getattr(coverage, "id", None),
            "coverage_status": getattr(coverage, "coverage_status", None),
            "production_readiness": getattr(coverage, "production_readiness", None),
            "completeness_status": getattr(coverage, "completeness_status", None),
            "is_stale": getattr(coverage, "is_stale", None),
        },
    }


@router.get("/{profile_id}/tasks", response_model=dict)
def get_profile_tasks(
    profile_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = db.get(JurisdictionProfile, int(profile_id))
    if not row:
        raise HTTPException(status_code=404, detail="profile not found")

    if row.org_id is not None and row.org_id != p.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    return {
        "ok": True,
        "jurisdiction_profile_id": int(row.id),
        "tasks": map_profile_jurisdiction_task_dicts(row),
    }


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