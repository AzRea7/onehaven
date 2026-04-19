# backend/app/routers/jurisdiction_profiles.py
from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.auth import get_principal, require_owner
from app.db import get_db
from app.policy_models import JurisdictionProfile, PolicySource
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
    _loads,
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


def _policy_json_dict(row: JurisdictionProfile) -> dict[str, Any]:
    raw = _loads(getattr(row, "policy_json", None), {})
    return raw if isinstance(raw, dict) else {}


def _policy_meta(row: JurisdictionProfile) -> dict[str, Any]:
    policy = _policy_json_dict(row)
    meta = policy.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}
    return meta


def _profile_layers(row: JurisdictionProfile) -> list[dict[str, Any]]:
    meta = _policy_meta(row)
    layers = meta.get("resolved_layers") or meta.get("layers") or []
    if isinstance(layers, list) and layers:
        return [layer for layer in layers if isinstance(layer, dict)]

    derived: list[dict[str, Any]] = [
        {
            "layer": "state",
            "label": f"{row.state or 'MI'} statewide baseline",
            "matched": True,
            "scope": "global",
        }
    ]
    if getattr(row, "county", None):
        derived.append(
            {
                "layer": "county",
                "label": f"{row.county} county overlay",
                "matched": True,
                "scope": "global" if row.org_id is None else "org",
            }
        )
    if getattr(row, "city", None):
        derived.append(
            {
                "layer": "city",
                "label": f"{row.city} city overlay",
                "matched": True,
                "scope": "global" if row.org_id is None else "org",
            }
        )
    if getattr(row, "pha_name", None):
        derived.append(
            {
                "layer": "housing_authority",
                "label": f"{row.pha_name} overlay",
                "matched": True,
                "scope": "global" if row.org_id is None else "org",
            }
        )
    if row.org_id is not None:
        derived.append(
            {
                "layer": "org_override",
                "label": "Org override",
                "matched": True,
                "scope": "org",
            }
        )
    return derived


def _serialize_policy_source(row: PolicySource) -> dict[str, Any]:
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "title": getattr(row, "title", None),
        "publisher": getattr(row, "publisher", None),
        "url": getattr(row, "url", None),
        "source_kind": getattr(row, "source_kind", None),
        "is_authoritative": bool(getattr(row, "is_authoritative", False)),
        "freshness_status": getattr(row, "freshness_status", None),
        "last_verified_at": getattr(row, "last_verified_at", None).isoformat() if getattr(row, "last_verified_at", None) else None,
        "freshness_checked_at": getattr(row, "freshness_checked_at", None).isoformat() if getattr(row, "freshness_checked_at", None) else None,
        "retrieved_at": getattr(row, "retrieved_at", None).isoformat() if getattr(row, "retrieved_at", None) else None,
    }


def _category_matrix_from_completeness(db: Session, completeness: dict[str, Any]) -> list[dict[str, Any]]:
    category_details = completeness.get("category_details") or {}
    category_statuses = completeness.get("category_statuses") or {}
    required = list(completeness.get("required_categories") or [])
    extras = [cat for cat in category_details.keys() if cat not in required]
    ordered = required + sorted(extras)

    out: list[dict[str, Any]] = []
    covered = set(completeness.get("covered_categories") or [])
    missing = set(completeness.get("missing_categories") or [])
    stale = set(completeness.get("stale_categories") or [])
    inferred = set(completeness.get("inferred_categories") or [])
    conflicting = set(completeness.get("conflicting_categories") or [])
    req_set = set(required)
    for category in ordered:
        detail = category_details.get(category) or {}
        source_ids = [int(x) for x in (detail.get("source_ids") or []) if str(x).strip()]
        source_rows = []
        if source_ids:
            rows = list(db.query(PolicySource).filter(PolicySource.id.in_(source_ids)).all())
            row_map = {int(row.id): row for row in rows if getattr(row, "id", None) is not None}
            source_rows = [_serialize_policy_source(row_map[source_id]) for source_id in source_ids if source_id in row_map]

        out.append(
            {
                "category": category,
                "status": detail.get("status") or category_statuses.get(category) or "missing",
                "expected": category in req_set,
                "covered": category in covered,
                "missing": category in missing,
                "stale": category in stale,
                "inferred": category in inferred,
                "conflicting": category in conflicting,
                "latest_verified_at": detail.get("latest_verified_at"),
                "source_count": int(detail.get("source_count") or 0),
                "authoritative_source_count": int(detail.get("authoritative_source_count") or 0),
                "assertion_count": int(detail.get("assertion_count") or 0),
                "governed_assertion_count": int(detail.get("governed_assertion_count") or 0),
                "citation_count": int(detail.get("citation_count") or 0),
                "source_ids": source_ids,
                "assertion_ids": [int(x) for x in (detail.get("assertion_ids") or []) if str(x).strip()],
                "sources": source_rows,
            }
        )
    return out


def _coverage_matrix_payload(db: Session, row: JurisdictionProfile) -> dict[str, Any]:
    completeness = profile_completeness_payload(db, row)
    return {
        "jurisdiction_profile_id": int(row.id),
        "state": row.state,
        "county": row.county,
        "city": row.city,
        "pha_name": row.pha_name,
        "expected_categories": list(completeness.get("required_categories") or []),
        "covered_categories": list(completeness.get("covered_categories") or []),
        "missing_categories": list(completeness.get("missing_categories") or []),
        "stale_categories": list(completeness.get("stale_categories") or []),
        "inferred_categories": list(completeness.get("inferred_categories") or []),
        "conflicting_categories": list(completeness.get("conflicting_categories") or []),
        "category_matrix": _category_matrix_from_completeness(db, completeness),
    }


def _profile_admin_payload(db: Session, r: JurisdictionProfile) -> dict[str, Any]:
    completeness = profile_completeness_payload(db, r)
    meta = _policy_meta(r)
    source_evidence = meta.get("source_evidence") or meta.get("evidence") or []
    missing_local_rule_areas = (
        completeness.get("missing_local_rule_areas")
        or completeness.get("missing_categories")
        or meta.get("missing_local_rule_areas")
        or []
    )
    stale_reason = completeness.get("stale_reason") or meta.get("stale_reason")
    stale_warning = bool(completeness.get("is_stale") or stale_reason)
    coverage_confidence = (
        completeness.get("coverage_confidence")
        or meta.get("coverage_confidence")
        or meta.get("confidence")
        or (
            "high"
            if completeness.get("completeness_score", 0) >= 0.85
            else "medium"
            if completeness.get("completeness_score", 0) >= 0.6
            else "low"
        )
    )
    resolved_rule_version = (
        meta.get("resolved_rule_version")
        or meta.get("rule_version")
        or (
            getattr(r, "updated_at", None).isoformat()
            if getattr(r, "updated_at", None)
            else None
        )
    )
    return {
        **_row_to_out(r).dict(),
        "completeness": completeness,
        "tasks": map_profile_jurisdiction_task_dicts(r),
        "required_categories": completeness.get("required_categories", []),
        "covered_categories": completeness.get("covered_categories", []),
        "missing_categories": completeness.get("missing_categories", []),
        "stale_categories": completeness.get("stale_categories", []),
        "inferred_categories": completeness.get("inferred_categories", []),
        "conflicting_categories": completeness.get("conflicting_categories", []),
        "missing_local_rule_areas": missing_local_rule_areas,
        "completeness_status": completeness.get("completeness_status"),
        "completeness_score": completeness.get("completeness_score"),
        "coverage_confidence": coverage_confidence,
        "confidence_label": completeness.get("confidence_label") or coverage_confidence,
        "production_readiness": completeness.get("production_readiness"),
        "trustworthy_for_projection": bool(
            completeness.get("trustworthy_for_projection", False)
        ),
        "discovery_status": completeness.get("discovery_status"),
        "last_discovery_run": completeness.get("last_discovery_run"),
        "last_discovered_at": completeness.get("last_discovered_at"),
        "resolved_rule_version": resolved_rule_version,
        "source_evidence": source_evidence,
        "resolved_layers": _profile_layers(r),
        "last_refresh": completeness.get("last_refresh")
        or meta.get("last_refreshed")
        or (
            r.updated_at.isoformat()
            if getattr(r, "updated_at", None)
            else None
        ),
        "last_refreshed": completeness.get("last_refreshed")
        or meta.get("last_refreshed")
        or (
            r.updated_at.isoformat()
            if getattr(r, "updated_at", None)
            else None
        ),
        "last_verified_at": completeness.get("last_verified_at"),
        "is_stale": completeness.get("is_stale"),
        "stale_reason": stale_reason,
        "stale_warning": stale_warning,
        "coverage_matrix": _coverage_matrix_payload(db, r),
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
        name = o.city or o.county or o.state or ""
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


@router.get("/{profile_id}/coverage-matrix", response_model=dict)
def get_profile_coverage_matrix(
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

    return {"ok": True, **_coverage_matrix_payload(db, row)}


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


@router.get("/resolve/property/{property_id}", response_model=dict)
def resolve_for_property(
    property_id: int,
    recompute: bool = Query(False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    from app.models import Property

    prop = db.get(Property, int(property_id))
    if not prop or getattr(prop, "org_id", None) != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    out = resolve_profile(
        db,
        org_id=p.org_id,
        city=getattr(prop, "city", None),
        county=getattr(prop, "county", None),
        state=getattr(prop, "state", None) or "MI",
    )

    profile_id = out.get("profile_id")
    profile_row = db.get(JurisdictionProfile, int(profile_id)) if profile_id else None
    if profile_row is not None and recompute:
        profile_row, _ = recompute_profile_and_coverage(db, profile_row, commit=True)

    return {
        "ok": True,
        "property": {
            "id": int(prop.id),
            "state": getattr(prop, "state", None),
            "county": getattr(prop, "county", None),
            "city": getattr(prop, "city", None),
        },
        "resolved": out,
        "profile": _profile_admin_payload(db, profile_row) if profile_row is not None else None,
    }

# ---- Step 2 registry + source mapping admin overlays ----

def _load_registry_meta_for_profile(row: JurisdictionProfile) -> dict[str, Any]:
    policy = _policy_json_dict(row)
    meta = policy.get('meta') or {}
    if not isinstance(meta, dict):
        meta = {}
    registry = meta.get('registry') or {}
    return registry if isinstance(registry, dict) else {}


_step2_base_coverage_matrix_payload = _coverage_matrix_payload
_step2_base_profile_admin_payload = _profile_admin_payload


def _coverage_matrix_payload(db: Session, row: JurisdictionProfile) -> dict[str, Any]:
    payload = _step2_base_coverage_matrix_payload(db, row)
    registry = _load_registry_meta_for_profile(row)
    payload['official_website'] = registry.get('official_website')
    payload['onboarding_status'] = registry.get('onboarding_status')
    payload['registry_hierarchy'] = registry.get('registry_hierarchy')
    payload['source_family_matrix'] = registry.get('source_family_matrix') or []
    return payload


def _profile_admin_payload(db: Session, r: JurisdictionProfile) -> dict[str, Any]:
    payload = _step2_base_profile_admin_payload(db, r)
    registry = _load_registry_meta_for_profile(r)
    payload['official_website'] = registry.get('official_website')
    payload['onboarding_status'] = registry.get('onboarding_status')
    payload['registry_hierarchy'] = registry.get('registry_hierarchy')
    payload['source_family_matrix'] = registry.get('source_family_matrix') or []
    payload['registry_enabled'] = bool(registry)
    return payload
