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
from ..policy_models import JurisdictionProfile, PolicySource
from ..services.jurisdiction_completeness_service import (
    profile_completeness_payload,
    recompute_profile_and_coverage,
)
from ..services.jurisdiction_notification_service import notify_if_jurisdiction_stale
from ..services.jurisdiction_health_service import get_jurisdiction_health
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


def _serialize_policy_source(row: PolicySource) -> dict:
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "title": getattr(row, "title", None),
        "publisher": getattr(row, "publisher", None),
        "url": getattr(row, "url", None),
        "source_kind": getattr(row, "source_kind", None),
        "is_authoritative": bool(getattr(row, "is_authoritative", False)),
        "freshness_status": getattr(row, "freshness_status", None),
        "refresh_state": getattr(row, "refresh_state", None),
        "refresh_status_reason": getattr(row, "refresh_status_reason", None),
        "validation_state": getattr(row, "validation_state", None),
        "validation_reason": getattr(row, "validation_reason", None),
        "next_refresh_due_at": getattr(row, "next_refresh_due_at", None).isoformat() if getattr(row, "next_refresh_due_at", None) else None,
        "validation_due_at": getattr(row, "validation_due_at", None).isoformat() if getattr(row, "validation_due_at", None) else None,
        "last_verified_at": getattr(row, "last_verified_at", None).isoformat() if getattr(row, "last_verified_at", None) else None,
        "last_validated_at": getattr(row, "last_validated_at", None).isoformat() if getattr(row, "last_validated_at", None) else None,
    }


def _iso_dt(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _profile_source_rows(db: Session, profile: JurisdictionProfile | None) -> list[PolicySource]:
    if profile is None:
        return []

    q = db.query(PolicySource).filter(PolicySource.state == getattr(profile, "state", None))
    if getattr(profile, "county", None) is None:
        q = q.filter(PolicySource.county.is_(None))
    else:
        q = q.filter(PolicySource.county == getattr(profile, "county", None))
    if getattr(profile, "city", None) is None:
        q = q.filter(PolicySource.city.is_(None))
    else:
        q = q.filter(PolicySource.city == getattr(profile, "city", None))
    if getattr(profile, "pha_name", None) is None:
        q = q.filter(PolicySource.pha_name.is_(None))
    else:
        q = q.filter(PolicySource.pha_name == getattr(profile, "pha_name", None))

    if getattr(profile, "org_id", None) is None:
        q = q.filter(PolicySource.org_id.is_(None))
    else:
        q = q.filter((PolicySource.org_id == getattr(profile, "org_id", None)) | (PolicySource.org_id.is_(None)))

    return list(q.order_by(PolicySource.is_authoritative.desc(), PolicySource.id.asc()).all())


def _source_summary_for_profile(db: Session, profile: JurisdictionProfile | None) -> dict:
    rows = _profile_source_rows(db, profile)
    freshness_counts: dict[str, int] = {}
    refresh_state_counts: dict[str, int] = {}
    validation_state_counts: dict[str, int] = {}
    next_refresh_due_at: Optional[datetime] = None
    next_validation_due_at: Optional[datetime] = None
    latest_validated_at: Optional[datetime] = None
    latest_verified_at: Optional[datetime] = None
    items: list[dict] = []

    for row in rows:
        freshness = str(getattr(row, "freshness_status", None) or "unknown").strip().lower()
        refresh_state = str(getattr(row, "refresh_state", None) or "unknown").strip().lower()
        validation_state = str(getattr(row, "validation_state", None) or "unknown").strip().lower()
        freshness_counts[freshness] = freshness_counts.get(freshness, 0) + 1
        refresh_state_counts[refresh_state] = refresh_state_counts.get(refresh_state, 0) + 1
        validation_state_counts[validation_state] = validation_state_counts.get(validation_state, 0) + 1

        nr = getattr(row, "next_refresh_due_at", None)
        nv = getattr(row, "validation_due_at", None)
        lv = getattr(row, "last_validated_at", None)
        lver = getattr(row, "last_verified_at", None)
        if nr is not None and (next_refresh_due_at is None or nr < next_refresh_due_at):
            next_refresh_due_at = nr
        if nv is not None and (next_validation_due_at is None or nv < next_validation_due_at):
            next_validation_due_at = nv
        if lv is not None and (latest_validated_at is None or lv > latest_validated_at):
            latest_validated_at = lv
        if lver is not None and (latest_verified_at is None or lver > latest_verified_at):
            latest_verified_at = lver

        if freshness in {"stale", "unknown"} or refresh_state in {"blocked", "degraded", "failed", "validating"} or validation_state in {"ambiguous", "conflicting", "unsupported", "weak_support", "unknown"}:
            if len(items) < 8:
                items.append(_serialize_policy_source(row))

    return {
        "total": len(rows),
        "authoritative_count": sum(1 for row in rows if bool(getattr(row, "is_authoritative", False))),
        "freshness_counts": freshness_counts,
        "refresh_state_counts": refresh_state_counts,
        "validation_state_counts": validation_state_counts,
        "next_refresh_due_at": _iso_dt(next_refresh_due_at),
        "next_validation_due_at": _iso_dt(next_validation_due_at),
        "latest_validated_at": _iso_dt(latest_validated_at),
        "latest_verified_at": _iso_dt(latest_verified_at),
        "items": items,
    }


def _operational_status_payload(db: Session, profile: JurisdictionProfile | None, *, org_id: int | None = None) -> dict:
    if profile is None:
        return {
            "health_state": "missing",
            "refresh_state": "missing",
            "reliability_state": "unsafe_to_rely_on",
            "safe_to_rely_on": False,
            "trustworthy_for_projection": False,
            "review_required": True,
            "reasons": ["jurisdiction_profile_not_found"],
            "lockout": {"lockout_active": True, "lockout_reason": "jurisdiction_profile_not_found"},
            "next_actions": {"next_step": "create_or_refresh_profile"},
            "source_summary": _source_summary_for_profile(db, None),
        }

    health = get_jurisdiction_health(db, profile_id=int(profile.id), org_id=org_id)
    completeness = dict((health or {}).get("completeness") or {})
    lockout = dict((health or {}).get("lockout") or {})
    next_actions = dict((health or {}).get("next_actions") or {})
    source_summary = _source_summary_for_profile(db, profile)
    refresh_state = str((health or {}).get("refresh_state") or getattr(profile, "refresh_state", None) or "unknown")
    status_reason = str((health or {}).get("refresh_status_reason") or getattr(profile, "refresh_status_reason", None) or "").strip() or None

    trustworthy = bool(completeness.get("trustworthy_for_projection", False))
    missing = list(completeness.get("missing_categories") or [])
    stale = list(completeness.get("stale_categories") or [])
    conflicting = list(completeness.get("conflicting_categories") or [])
    inferred = list(completeness.get("inferred_categories") or [])
    lockout_active = bool(lockout.get("lockout_active"))

    reasons: list[str] = []
    if lockout.get("lockout_reason"):
        reasons.append(str(lockout.get("lockout_reason")))
    if status_reason and status_reason not in reasons:
        reasons.append(status_reason)
    if completeness.get("stale_reason"):
        reasons.append(str(completeness.get("stale_reason")))
    if missing:
        reasons.append(f"missing categories: {', '.join(missing[:6])}")
    if stale:
        reasons.append(f"stale categories: {', '.join(stale[:6])}")
    if conflicting:
        reasons.append(f"conflicting categories: {', '.join(conflicting[:6])}")
    if inferred and not trustworthy:
        reasons.append(f"inferred-only categories: {', '.join(inferred[:6])}")

    safe_to_rely_on = bool(
        trustworthy
        and not lockout_active
        and refresh_state == "healthy"
        and not stale
        and not conflicting
    )
    review_required = bool(not safe_to_rely_on and refresh_state in {"degraded", "pending", "validating", "healthy"})
    reliability_state = (
        "safe_to_rely_on"
        if safe_to_rely_on
        else "unsafe_to_rely_on"
        if lockout_active or refresh_state in {"blocked", "failed", "missing"}
        else "review_required"
    )

    return {
        "health_state": "blocked" if lockout_active else refresh_state,
        "refresh_state": refresh_state,
        "refresh_status_reason": status_reason,
        "reliability_state": reliability_state,
        "safe_to_rely_on": safe_to_rely_on,
        "trustworthy_for_projection": trustworthy,
        "review_required": review_required,
        "reasons": reasons,
        "lockout": lockout,
        "next_actions": next_actions,
        "source_summary": source_summary,
        "last_refresh_success_at": (health or {}).get("last_refresh_success_at"),
        "last_refresh_completed_at": (health or {}).get("last_refresh_completed_at"),
    }


def _category_matrix_from_completeness(db: Session, completeness: dict) -> list[dict]:
    details = completeness.get("category_details") or {}
    statuses = completeness.get("category_statuses") or {}
    required = list(completeness.get("required_categories") or [])
    ordered = required + [cat for cat in details.keys() if cat not in required]
    out = []
    req_set = set(required)
    covered = set(completeness.get("covered_categories") or [])
    missing = set(completeness.get("missing_categories") or [])
    stale = set(completeness.get("stale_categories") or [])
    inferred = set(completeness.get("inferred_categories") or [])
    conflicting = set(completeness.get("conflicting_categories") or [])
    for category in ordered:
        detail = details.get(category) or {}
        source_ids = [int(x) for x in (detail.get("source_ids") or []) if str(x).strip()]
        source_rows = []
        if source_ids:
            rows = list(db.query(PolicySource).filter(PolicySource.id.in_(source_ids)).all())
            row_map = {int(row.id): row for row in rows if getattr(row, "id", None) is not None}
            source_rows = [_serialize_policy_source(row_map[source_id]) for source_id in source_ids if source_id in row_map]
        out.append(
            {
                "category": category,
                "status": detail.get("status") or statuses.get(category) or "missing",
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


def _coverage_matrix_payload(db: Session, profile: JurisdictionProfile | None) -> dict | None:
    if profile is None:
        return None
    completeness = profile_completeness_payload(db, profile)
    return {
        "jurisdiction_profile_id": int(profile.id),
        "state": profile.state,
        "county": profile.county,
        "city": profile.city,
        "pha_name": profile.pha_name,
        "expected_categories": list(completeness.get("required_categories") or []),
        "covered_categories": list(completeness.get("covered_categories") or []),
        "missing_categories": list(completeness.get("missing_categories") or []),
        "stale_categories": list(completeness.get("stale_categories") or []),
        "inferred_categories": list(completeness.get("inferred_categories") or []),
        "conflicting_categories": list(completeness.get("conflicting_categories") or []),
        "category_matrix": _category_matrix_from_completeness(db, completeness),
    }


def _profile_resolution_payload(db: Session, profile: JurisdictionProfile | None) -> dict | None:
    if profile is None:
        return None
    completeness = profile_completeness_payload(db, profile)
    meta = _profile_policy_meta(profile)
    operational_status = _operational_status_payload(
        db,
        profile,
        org_id=getattr(profile, "org_id", None),
    )
    return {
        "profile_id": int(profile.id),
        "state": profile.state,
        "county": profile.county,
        "city": profile.city,
        "pha_name": getattr(profile, "pha_name", None),
        "resolved_rule_version": meta.get("resolved_rule_version")
        or meta.get("rule_version")
        or (profile.updated_at.isoformat() if getattr(profile, "updated_at", None) else None),
        "coverage_confidence": completeness.get("coverage_confidence")
        or meta.get("coverage_confidence")
        or (
            "high"
            if completeness.get("completeness_score", 0) >= 0.85
            else "medium"
            if completeness.get("completeness_score", 0) >= 0.6
            else "low"
        ),
        "confidence_label": completeness.get("confidence_label")
        or completeness.get("coverage_confidence")
        or meta.get("coverage_confidence"),
        "completeness_score": completeness.get("completeness_score"),
        "completeness_status": completeness.get("completeness_status"),
        "production_readiness": completeness.get("production_readiness"),
        "trustworthy_for_projection": bool(completeness.get("trustworthy_for_projection", False)),
        "missing_local_rule_areas": completeness.get("missing_local_rule_areas")
        or completeness.get("missing_categories")
        or meta.get("missing_local_rule_areas")
        or [],
        "missing_categories": completeness.get("missing_categories") or [],
        "stale_categories": completeness.get("stale_categories") or [],
        "inferred_categories": completeness.get("inferred_categories") or [],
        "conflicting_categories": completeness.get("conflicting_categories") or [],
        "covered_categories": completeness.get("covered_categories") or [],
        "required_categories": completeness.get("required_categories") or [],
        "source_evidence": meta.get("source_evidence") or meta.get("evidence") or [],
        "last_refresh": completeness.get("last_refresh")
        or meta.get("last_refreshed")
        or (profile.updated_at.isoformat() if getattr(profile, "updated_at", None) else None),
        "last_refreshed": completeness.get("last_refreshed")
        or meta.get("last_refreshed")
        or (profile.updated_at.isoformat() if getattr(profile, "updated_at", None) else None),
        "discovery_status": completeness.get("discovery_status") or meta.get("discovery_status"),
        "last_discovery_run": completeness.get("last_discovery_run") or meta.get("last_discovery_run"),
        "last_discovered_at": completeness.get("last_discovered_at") or meta.get("last_discovered_at"),
        "is_stale": bool(completeness.get("is_stale")),
        "stale_reason": completeness.get("stale_reason") or meta.get("stale_reason"),
        "resolved_layers": _profile_layers_payload(profile),
        "coverage_matrix": _coverage_matrix_payload(db, profile),
        "operational_status": operational_status,
        "health": operational_status,
        "lockout": operational_status.get("lockout"),
        "next_actions": operational_status.get("next_actions"),
        "safe_to_rely_on": operational_status.get("safe_to_rely_on"),
        "unsafe_reasons": operational_status.get("reasons"),
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
        q = q.where((JurisdictionProfile.org_id == org_id) | (JurisdictionProfile.org_id.is_(None)))
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
        return {"ok": True, "scope": scope, "rule": _row_to_dict(row)}

    for k, v in data.items():
        if hasattr(existing, k):
            setattr(existing, k, v)
    existing.updated_at = now
    db.add(existing)
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

    return {"ok": True, "scope": scope, "rule": _row_to_dict(existing)}


@router.get("/defaults", response_model=list[dict])
def defaults():
    return michigan_global_defaults()


@router.get("/coverage", response_model=dict)
def coverage(
    state: str = Query(default="MI"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    state = _norm_state(state)

    pairs = db.execute(
        select(
            func.lower(Property.city).label("city_lc"),
            Property.state,
            func.lower(Property.county).label("county_lc"),
        )
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
                "jurisdiction_completeness_score": completeness.get("completeness_score") if completeness else None,
                "jurisdiction_confidence_label": completeness.get("confidence_label") if completeness else None,
                "jurisdiction_discovery_status": completeness.get("discovery_status") if completeness else None,
                "jurisdiction_production_readiness": completeness.get("production_readiness") if completeness else None,
                "jurisdiction_is_stale": completeness.get("is_stale") if completeness else None,
                "jurisdiction_missing_categories": completeness.get("missing_categories") if completeness else [],
                "jurisdiction_stale_categories": completeness.get("stale_categories") if completeness else [],
                "jurisdiction_inferred_categories": completeness.get("inferred_categories") if completeness else [],
                "jurisdiction_conflicting_categories": completeness.get("conflicting_categories") if completeness else [],
                "jurisdiction_expected_categories": completeness.get("required_categories") if completeness else [],
                "jurisdiction_category_matrix": _category_matrix_from_completeness(db, completeness) if completeness else [],
            }
        )

    missing = [r for r in rows if r["provenance"] == "missing"]
    incomplete = [r for r in rows if r.get("jurisdiction_completeness_status") not in {None, "complete"}]
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
    operational_status = _operational_status_payload(
        db,
        profile,
        org_id=getattr(p, "org_id", None),
    )
    return {
        "ok": True,
        "profile": payload,
        "operational_status": operational_status,
        "health": operational_status,
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


@router.get("/{jurisdiction_profile_id}/coverage-matrix", response_model=dict)
def get_jurisdiction_coverage_matrix(
    jurisdiction_profile_id: int,
    recompute: bool = Query(False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    if not profile:
        raise HTTPException(status_code=404, detail="Jurisdiction profile not found")

    if profile.org_id is not None and profile.org_id != p.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    if recompute:
        profile, _ = recompute_profile_and_coverage(db, profile, commit=True)

    operational_status = _operational_status_payload(
        db,
        profile,
        org_id=getattr(p, "org_id", None),
    )
    return {
        "ok": True,
        **(_coverage_matrix_payload(db, profile) or {}),
        "operational_status": operational_status,
        "health": operational_status,
    }


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

    resolved_profile = _profile_resolution_payload(db, profile)
    return {
        "ok": True,
        "property": {
            "id": int(prop.id),
            "address": getattr(prop, "address", None),
            "city": getattr(prop, "city", None),
            "county": getattr(prop, "county", None),
            "state": getattr(prop, "state", None),
        },
        "resolved_profile": resolved_profile,
        "operational_status": (resolved_profile or {}).get("operational_status"),
        "safe_to_rely_on": (resolved_profile or {}).get("safe_to_rely_on"),
        "unsafe_reasons": (resolved_profile or {}).get("unsafe_reasons") or [],
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

    profile_payload = _profile_resolution_payload(db, profile)
    if profile_payload is None:
        return {
            "ok": True,
            "property_id": int(prop.id),
            "coverage_status": "missing",
            "coverage_confidence": "low",
            "confidence_label": "low",
            "production_readiness": "blocked",
            "trustworthy_for_projection": False,
            "missing_local_rule_areas": ["statewide baseline", "county overlay", "city overlay"],
            "missing_categories": ["statewide baseline", "county overlay", "city overlay"],
            "stale_categories": [],
            "inferred_categories": [],
            "conflicting_categories": [],
            "discovery_status": "not_started",
            "stale_warning": False,
            "resolved_layers": [],
            "source_evidence": [],
            "operational_status": _operational_status_payload(db, None, org_id=getattr(p, "org_id", None)),
            "safe_to_rely_on": False,
            "unsafe_reasons": ["jurisdiction_profile_not_found"],
        }

    return {
        "ok": True,
        "property_id": int(prop.id),
        "coverage_status": profile_payload.get("completeness_status"),
        "coverage_confidence": profile_payload.get("coverage_confidence"),
        "confidence_label": profile_payload.get("confidence_label"),
        "production_readiness": profile_payload.get("production_readiness"),
        "trustworthy_for_projection": profile_payload.get("trustworthy_for_projection"),
        "missing_local_rule_areas": profile_payload.get("missing_local_rule_areas"),
        "missing_categories": profile_payload.get("missing_categories"),
        "stale_categories": profile_payload.get("stale_categories"),
        "inferred_categories": profile_payload.get("inferred_categories"),
        "conflicting_categories": profile_payload.get("conflicting_categories"),
        "discovery_status": profile_payload.get("discovery_status"),
        "last_discovery_run": profile_payload.get("last_discovery_run"),
        "last_discovered_at": profile_payload.get("last_discovered_at"),
        "stale_warning": bool(profile_payload.get("is_stale")),
        "stale_reason": profile_payload.get("stale_reason"),
        "resolved_rule_version": profile_payload.get("resolved_rule_version"),
        "resolved_layers": profile_payload.get("resolved_layers"),
        "source_evidence": profile_payload.get("source_evidence"),
        "last_refresh": profile_payload.get("last_refresh"),
        "last_refreshed": profile_payload.get("last_refreshed"),
    }

@router.get("/health")
def jurisdiction_health(
    profile_id: int | None = Query(None),
    state: str | None = Query(None),
    county: str | None = Query(None),
    city: str | None = Query(None),
    pha_name: str | None = Query(None),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    result = get_jurisdiction_health(
        db,
        profile_id=profile_id,
        org_id=getattr(principal, "org_id", None),
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    if not result.get("ok"):
        return result
    profile = db.get(JurisdictionProfile, int(result.get("jurisdiction_profile_id"))) if result.get("jurisdiction_profile_id") else None
    operational_status = _operational_status_payload(
        db,
        profile,
        org_id=getattr(principal, "org_id", None),
    )
    result["source_summary"] = operational_status.get("source_summary")
    result["operational_status"] = operational_status
    result["safe_to_rely_on"] = operational_status.get("safe_to_rely_on")
    result["unsafe_reasons"] = operational_status.get("reasons") or []
    return result


@router.get("/{jurisdiction_profile_id}/visibility", response_model=dict)
def get_jurisdiction_visibility(
    jurisdiction_profile_id: int,
    recompute: bool = Query(False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    if not profile:
        raise HTTPException(status_code=404, detail="Jurisdiction profile not found")
    if profile.org_id is not None and profile.org_id != p.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    if recompute:
        profile, _ = recompute_profile_and_coverage(db, profile, commit=True)
    return {
        "ok": True,
        "jurisdiction_profile_id": int(profile.id),
        "resolved_profile": _profile_resolution_payload(db, profile),
        "coverage_matrix": _coverage_matrix_payload(db, profile),
        "health": get_jurisdiction_health(db, profile_id=int(profile.id), org_id=getattr(p, "org_id", None)),
        "operational_status": _operational_status_payload(db, profile, org_id=getattr(p, "org_id", None)),
    }


@router.post("/{profile_id}/refresh")
def force_refresh_jurisdiction(
    profile_id: int,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    return refresh_jurisdiction_profile(
        db,
        jurisdiction_profile_id=int(profile_id),
        reviewer_user_id=getattr(principal, "user_id", None),
        force=True,
    )


@router.post("/{profile_id}/notify-stale")
def notify_stale_jurisdiction(
    profile_id: int,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    profile = db.get(JurisdictionProfile, int(profile_id))
    if profile is None:
        raise HTTPException(status_code=404, detail="Jurisdiction profile not found")
    return notify_if_jurisdiction_stale(db, profile=profile, force=True)
