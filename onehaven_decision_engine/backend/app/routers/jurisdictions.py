# backend/app/routers/jurisdictions.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, func, or_, select
from sqlalchemy.orm import Session

from ..auth import get_principal, require_owner
from ..db import get_db
from ..domain.audit import emit_audit
from ..domain.jurisdiction_defaults import michigan_global_defaults
from ..models import JurisdictionRule, Property
from ..policy_models import JurisdictionProfile
from ..services.jurisdiction_completeness_service import (
    profile_completeness_payload,
    recompute_profile_and_coverage,
)
from ..services.jurisdiction_notification_service import notify_if_jurisdiction_stale
from ..services.jurisdiction_refresh_service import refresh_jurisdiction_profile

router = APIRouter(prefix="/jurisdictions", tags=["jurisdictions"])


def _json_loads(value, default=None):
    if default is None:
        default = {}
    if value in (None, "", [], {}):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _profile_policy_meta(profile: JurisdictionProfile | None) -> dict:
    if profile is None:
        return {}
    payload = _json_loads(getattr(profile, "policy_json", None), {})
    if not isinstance(payload, dict):
        return {}
    meta = payload.get("meta") or {}
    return meta if isinstance(meta, dict) else {}


def _profile_layers_payload(profile: JurisdictionProfile | None) -> list[dict]:
    if profile is None:
        return []
    meta = _profile_policy_meta(profile)
    layers = meta.get("resolved_layers") or meta.get("layers") or []
    if isinstance(layers, list) and layers:
        return [row for row in layers if isinstance(row, dict)]

    rows = [{"layer": "state", "label": f"{profile.state or 'MI'} statewide baseline", "matched": True}]
    if getattr(profile, "county", None):
        rows.append({"layer": "county", "label": f"{profile.county} county overlay", "matched": True})
    if getattr(profile, "city", None):
        rows.append({"layer": "city", "label": f"{profile.city} city overlay", "matched": True})
    if getattr(profile, "pha_name", None):
        rows.append({"layer": "housing_authority", "label": f"{profile.pha_name} overlay", "matched": True})
    if getattr(profile, "org_id", None) is not None:
        rows.append({"layer": "org_override", "label": "Org override", "matched": True})
    return rows


def _profile_resolution_payload(db: Session, profile: JurisdictionProfile | None) -> dict | None:
    if profile is None:
        return None
    completeness = profile_completeness_payload(db, profile)
    meta = _profile_policy_meta(profile)
    return {
        "profile_id": int(profile.id),
        "state": profile.state,
        "county": profile.county,
        "city": profile.city,
        "pha_name": getattr(profile, "pha_name", None),
        "resolved_rule_version": meta.get("resolved_rule_version") or meta.get("rule_version") or (profile.updated_at.isoformat() if getattr(profile, "updated_at", None) else None),
        "coverage_confidence": completeness.get("coverage_confidence") or meta.get("coverage_confidence") or ("high" if completeness.get("completeness_score", 0) >= 0.85 else "medium" if completeness.get("completeness_score", 0) >= 0.6 else "low"),
        "completeness_score": completeness.get("completeness_score"),
        "completeness_status": completeness.get("completeness_status"),
        "missing_local_rule_areas": completeness.get("missing_local_rule_areas") or completeness.get("missing_categories") or meta.get("missing_local_rule_areas") or [],
        "source_evidence": meta.get("source_evidence") or meta.get("evidence") or [],
        "last_refreshed": meta.get("last_refreshed") or completeness.get("last_refreshed") or (profile.updated_at.isoformat() if getattr(profile, "updated_at", None) else None),
        "is_stale": bool(completeness.get("is_stale")),
        "stale_reason": completeness.get("stale_reason") or meta.get("stale_reason"),
        "resolved_layers": _profile_layers_payload(profile),
    }


def _norm_city(v: str) -> str:
    return (v or "").strip().title()


def _norm_state(v: str) -> str:
    s = (v or "MI").strip().upper()
    return s if len(s) == 2 else "MI"


def _norm_county(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip().lower()
    return s or None


def _norm_text(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip()
    return s or None


def _has_col(model, name: str) -> bool:
    return hasattr(model, name)


def _row_to_dict(r: JurisdictionRule) -> dict:
    return {
        "id": r.id,
        "scope": "global" if r.org_id is None else "org",
        "org_id": r.org_id,
        "city": r.city,
        "state": r.state,
        "rental_license_required": r.rental_license_required,
        "inspection_authority": getattr(r, "inspection_authority", None),
        "inspection_frequency": getattr(r, "inspection_frequency", None),
        "typical_fail_points_json": getattr(r, "typical_fail_points_json", None),
        "registration_fee": getattr(r, "registration_fee", None),
        "fees_json": getattr(r, "fees_json", None),
        "processing_days": getattr(r, "processing_days", None),
        "tenant_waitlist_depth": getattr(r, "tenant_waitlist_depth", None),
        "notes": getattr(r, "notes", None),
        "updated_at": r.updated_at.isoformat() if getattr(r, "updated_at", None) else None,
        "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
    }


def _profile_query_for_scope(
    db: Session,
    *,
    org_id: int,
    state: str,
    county: Optional[str],
    city: Optional[str],
    include_global: bool = True,
):
    q = select(JurisdictionProfile).where(JurisdictionProfile.state == state)

    if include_global:
        q = q.where(
            (JurisdictionProfile.org_id == org_id)
            | (JurisdictionProfile.org_id.is_(None))
        )
    else:
        q = q.where(JurisdictionProfile.org_id == org_id)

    if county is None:
        q = q.where(JurisdictionProfile.county.is_(None))
    else:
        q = q.where(JurisdictionProfile.county == county)

    if city is None:
        q = q.where(JurisdictionProfile.city.is_(None))
    else:
        q = q.where(JurisdictionProfile.city == city)

    return q.order_by(desc(JurisdictionProfile.org_id), desc(JurisdictionProfile.id))


def _effective_profile(
    db: Session,
    *,
    org_id: int,
    state: str,
    county: Optional[str],
    city: Optional[str],
) -> JurisdictionProfile | None:
    rows = list(
        db.scalars(
            _profile_query_for_scope(
                db,
                org_id=org_id,
                state=state,
                county=county,
                city=city,
                include_global=True,
            )
        ).all()
    )
    if not rows:
        return None

    rows.sort(
        key=lambda r: (
            0 if getattr(r, "org_id", None) == org_id else 1,
            0 if getattr(r, "city", None) else 1,
            0 if getattr(r, "county", None) else 1,
            -(getattr(r, "id", 0) or 0),
        )
    )
    return rows[0]


@router.get("/rules", response_model=list[dict])
def list_rules(
    city: Optional[str] = Query(default=None),
    state: str = Query(default="MI"),
    scope: str = Query(default="all", description="all|org|global"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    state = _norm_state(state)
    city_norm = _norm_city(city) if city else None

    q = select(JurisdictionRule).where(or_(JurisdictionRule.org_id == p.org_id, JurisdictionRule.org_id.is_(None)))

    if scope == "org":
        q = select(JurisdictionRule).where(JurisdictionRule.org_id == p.org_id)
    elif scope == "global":
        q = select(JurisdictionRule).where(JurisdictionRule.org_id.is_(None))

    if city_norm:
        q = q.where(JurisdictionRule.city == city_norm, JurisdictionRule.state == state)
    else:
        q = q.where(JurisdictionRule.state == state)

    rows = list(db.scalars(q.order_by(desc(JurisdictionRule.org_id), JurisdictionRule.city)).all())
    return [_row_to_dict(r) for r in rows]


@router.get("/rule", response_model=dict)
def get_effective_rule(
    city: str,
    state: str = "MI",
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    city = _norm_city(city)
    state = _norm_state(state)

    org_row = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id == p.org_id,
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    )
    if org_row:
        return {"scope": "org", "rule": _row_to_dict(org_row)}

    global_row = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id.is_(None),
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    )
    if global_row:
        return {"scope": "global", "rule": _row_to_dict(global_row)}

    raise HTTPException(status_code=404, detail="No jurisdiction rule found (org or global).")


@router.post("/rule", response_model=dict)
def upsert_rule(
    payload: dict,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _owner=Depends(require_owner),
    scope: str = Query(default="org", description="org|global"),
):
    city = _norm_city(payload.get("city") or "")
    state = _norm_state(payload.get("state") or "MI")
    if not city:
        raise HTTPException(status_code=400, detail="city is required")

    org_id = None if scope == "global" else p.org_id

    existing = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id.is_(None) if org_id is None else (JurisdictionRule.org_id == org_id),
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    )

    before = _row_to_dict(existing) if existing else None

    data = dict(payload)
    data["city"] = city
    data["state"] = state

    for k in ["id", "org_id", "updated_at", "created_at", "scope"]:
        data.pop(k, None)

    if not _has_col(JurisdictionRule, "notes"):
        data.pop("notes", None)

    now = datetime.utcnow()

    if existing is None:
        row = JurisdictionRule(org_id=org_id, updated_at=now, created_at=now, **data)
        db.add(row)
        db.commit()
        db.refresh(row)

        emit_audit(
            db,
            org_id=p.org_id,
            actor_user_id=p.user_id,
            action="jurisdiction.create",
            entity_type="JurisdictionRule",
            entity_id=str(row.id),
            before=None,
            after=_row_to_dict(row),
        )
        db.commit()
        return {"ok": True, "id": row.id, "scope": "global" if row.org_id is None else "org"}

    for k, v in data.items():
        if hasattr(existing, k):
            setattr(existing, k, v)
    existing.updated_at = now
    db.commit()
    db.refresh(existing)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="jurisdiction.update",
        entity_type="JurisdictionRule",
        entity_id=str(existing.id),
        before=before,
        after=_row_to_dict(existing),
    )
    db.commit()

    return {"ok": True, "id": existing.id, "scope": "global" if existing.org_id is None else "org"}


@router.delete("/rule", response_model=dict)
def delete_rule(
    city: str,
    state: str = "MI",
    scope: str = Query(default="org", description="org|global"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _owner=Depends(require_owner),
):
    city = _norm_city(city)
    state = _norm_state(state)
    org_id = None if scope == "global" else p.org_id

    row = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id.is_(None) if org_id is None else (JurisdictionRule.org_id == org_id),
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    )
    if not row:
        raise HTTPException(status_code=404, detail="Rule not found")

    before = _row_to_dict(row)
    rid = row.id
    db.delete(row)
    db.commit()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="jurisdiction.delete",
        entity_type="JurisdictionRule",
        entity_id=str(rid),
        before=before,
        after=None,
    )
    db.commit()

    return {"ok": True, "deleted_id": rid}


@router.post("/seed", response_model=dict)
def seed_michigan_defaults(
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _owner=Depends(require_owner),
):
    now = datetime.utcnow()
    created = 0

    allow_notes = _has_col(JurisdictionRule, "notes")

    for d in michigan_global_defaults():
        row_kwargs = d.to_row_kwargs()
        city = _norm_city(row_kwargs.get("city", ""))
        state = _norm_state(row_kwargs.get("state", "MI"))
        if not city:
            continue

        exists = db.scalar(
            select(JurisdictionRule).where(
                JurisdictionRule.org_id.is_(None),
                JurisdictionRule.city == city,
                JurisdictionRule.state == state,
            )
        )
        if exists:
            continue

        insert_kwargs = dict(
            org_id=None,
            city=city,
            state=state,
            rental_license_required=bool(row_kwargs.get("rental_license_required", False)),
            inspection_authority=row_kwargs.get("inspection_authority"),
            inspection_frequency=row_kwargs.get("inspection_frequency"),
            typical_fail_points_json=row_kwargs.get("typical_fail_points_json") or "[]",
            registration_fee=row_kwargs.get("registration_fee"),
            processing_days=row_kwargs.get("processing_days"),
            tenant_waitlist_depth=row_kwargs.get("tenant_waitlist_depth"),
            created_at=now,
            updated_at=now,
        )
        if allow_notes:
            insert_kwargs["notes"] = row_kwargs.get("notes")

        row = JurisdictionRule(**insert_kwargs)
        db.add(row)
        created += 1

    db.commit()
    return {"ok": True, "created": created}


@router.get("/coverage", response_model=dict)
def coverage(
    state: str = Query(default="MI"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    state = _norm_state(state)

    pairs = db.execute(
        select(func.lower(Property.city).label("city_lc"), Property.state, func.lower(Property.county).label("county_lc"))
        .where(Property.org_id == p.org_id, Property.state == state)
        .group_by(func.lower(Property.city), Property.state, func.lower(Property.county))
    ).all()

    rows = []
    for city_lc, st, county_lc in pairs:
        city = _norm_city(city_lc)
        county = _norm_county(county_lc)

        org_rule = db.scalar(
            select(JurisdictionRule).where(
                JurisdictionRule.org_id == p.org_id,
                JurisdictionRule.city == city,
                JurisdictionRule.state == st,
            )
        )
        global_rule = db.scalar(
            select(JurisdictionRule).where(
                JurisdictionRule.org_id.is_(None),
                JurisdictionRule.city == city,
                JurisdictionRule.state == st,
            )
        )

        profile = _effective_profile(
            db,
            org_id=p.org_id,
            state=st,
            county=county,
            city=county and _norm_county(city.lower()) is None and None or city.lower(),
        )
        if profile is None:
            profile = _effective_profile(
                db,
                org_id=p.org_id,
                state=st,
                county=county,
                city=(city or "").strip().lower() or None,
            )

        completeness = None
        if profile is not None:
            completeness = profile_completeness_payload(db, profile)

        provenance = "org" if org_rule else ("global" if global_rule else "missing")
        rows.append(
            {
                "city": city,
                "county": county,
                "state": st,
                "has_org_rule": bool(org_rule),
                "has_global_fallback": bool(global_rule),
                "provenance": provenance,
                "jurisdiction_profile_id": getattr(profile, "id", None) if profile else None,
                "jurisdiction_completeness_status": completeness.get("completeness_status") if completeness else None,
                "jurisdiction_is_stale": completeness.get("is_stale") if completeness else None,
                "jurisdiction_missing_categories": completeness.get("missing_categories") if completeness else [],
            }
        )

    missing = [r for r in rows if r["provenance"] == "missing"]
    incomplete = [
        r for r in rows if r.get("jurisdiction_completeness_status") not in {None, "complete"}
    ]
    stale = [r for r in rows if bool(r.get("jurisdiction_is_stale"))]

    return {
        "state": state,
        "total_pairs": len(rows),
        "missing_rules": len(missing),
        "incomplete_jurisdictions": len(incomplete),
        "stale_jurisdictions": len(stale),
        "rows": sorted(rows, key=lambda r: (r["provenance"], r["city"])),
    }


@router.get("/{jurisdiction_profile_id}/completeness", response_model=dict)
def get_jurisdiction_completeness(
    jurisdiction_profile_id: int,
    recompute: bool = Query(True),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    if not profile:
        raise HTTPException(status_code=404, detail="Jurisdiction profile not found")

    if profile.org_id is not None and profile.org_id != p.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    if recompute:
        profile, coverage = recompute_profile_and_coverage(db, profile, commit=True)
    else:
        coverage = None

    payload = profile_completeness_payload(db, profile)
    return {
        "ok": True,
        "profile": payload,
        "coverage": {
            "id": getattr(coverage, "id", None),
            "coverage_status": getattr(coverage, "coverage_status", None),
            "production_readiness": getattr(coverage, "production_readiness", None),
            "completeness_status": getattr(coverage, "completeness_status", None),
            "is_stale": getattr(coverage, "is_stale", None),
        }
        if coverage is not None
        else None,
    }


@router.post("/{jurisdiction_profile_id}/refresh", response_model=dict)
def refresh_jurisdiction(
    jurisdiction_profile_id: int,
    force: bool = Query(False),
    db: Session = Depends(get_db),
    p=Depends(require_owner),
):
    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    if not profile:
        raise HTTPException(status_code=404, detail="Jurisdiction profile not found")

    if profile.org_id is not None and profile.org_id != p.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    result = refresh_jurisdiction_profile(
        db,
        jurisdiction_profile_id=int(jurisdiction_profile_id),
        reviewer_user_id=p.user_id,
        force=bool(force),
    )
    return result


@router.post("/{jurisdiction_profile_id}/notify-stale", response_model=dict)
def notify_stale_jurisdiction(
    jurisdiction_profile_id: int,
    force: bool = Query(False),
    db: Session = Depends(get_db),
    p=Depends(require_owner),
):
    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    if not profile:
        raise HTTPException(status_code=404, detail="Jurisdiction profile not found")

    if profile.org_id is not None and profile.org_id != p.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    return notify_if_jurisdiction_stale(
        db,
        profile=profile,
        force=bool(force),
    )


@router.get("/resolve/property/{property_id}", response_model=dict)
def resolve_property_jurisdiction(
    property_id: int,
    recompute: bool = Query(False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.get(Property, int(property_id))
    if not prop or getattr(prop, "org_id", None) != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    state = _norm_state(getattr(prop, "state", None) or "MI")
    county = _norm_county(getattr(prop, "county", None))
    city = _norm_text(getattr(prop, "city", None))

    profile = _effective_profile(
        db,
        org_id=p.org_id,
        state=state,
        county=county,
        city=(city.lower() if city else None),
    )
    if profile is None and city:
        profile = _effective_profile(
            db,
            org_id=p.org_id,
            state=state,
            county=county,
            city=city,
        )

    if profile is not None and recompute:
        profile, _ = recompute_profile_and_coverage(db, profile, commit=True)

    return {
        "ok": True,
        "property": {
            "id": int(prop.id),
            "address": getattr(prop, "address", None),
            "city": getattr(prop, "city", None),
            "county": getattr(prop, "county", None),
            "state": getattr(prop, "state", None),
        },
        "resolved_profile": _profile_resolution_payload(db, profile),
    }


@router.get("/coverage/property/{property_id}", response_model=dict)
def property_coverage_detail(
    property_id: int,
    recompute: bool = Query(False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.get(Property, int(property_id))
    if not prop or getattr(prop, "org_id", None) != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    state = _norm_state(getattr(prop, "state", None) or "MI")
    county = _norm_county(getattr(prop, "county", None))
    city = _norm_text(getattr(prop, "city", None))
    profile = _effective_profile(db, org_id=p.org_id, state=state, county=county, city=(city.lower() if city else None))
    if profile is None and city:
        profile = _effective_profile(db, org_id=p.org_id, state=state, county=county, city=city)
    if profile is not None and recompute:
        profile, _ = recompute_profile_and_coverage(db, profile, commit=True)

    profile_payload = _profile_resolution_payload(db, profile)
    if profile_payload is None:
        return {
            "ok": True,
            "property_id": int(prop.id),
            "coverage_status": "missing",
            "coverage_confidence": "low",
            "missing_local_rule_areas": ["statewide baseline", "county overlay", "city overlay"],
            "stale_warning": False,
            "resolved_layers": [],
            "source_evidence": [],
        }

    return {
        "ok": True,
        "property_id": int(prop.id),
        "coverage_status": profile_payload.get("completeness_status"),
        "coverage_confidence": profile_payload.get("coverage_confidence"),
        "missing_local_rule_areas": profile_payload.get("missing_local_rule_areas"),
        "stale_warning": bool(profile_payload.get("is_stale")),
        "stale_reason": profile_payload.get("stale_reason"),
        "resolved_rule_version": profile_payload.get("resolved_rule_version"),
        "resolved_layers": profile_payload.get("resolved_layers"),
        "source_evidence": profile_payload.get("source_evidence"),
        "last_refreshed": profile_payload.get("last_refreshed"),
    }
