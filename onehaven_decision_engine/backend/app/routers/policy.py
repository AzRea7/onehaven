# backend/app/routers/policy.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import get_principal, require_owner
from app.db import get_db, rollback_quietly
from app.models import Property
from app.policy_models import JurisdictionProfile, PolicyAssertion, PolicySource
from app.services.jurisdiction_completeness_service import profile_completeness_payload
from app.services.jurisdiction_health_service import get_jurisdiction_health
from app.services.jurisdiction_notification_service import build_review_queue_entries, persist_review_queue_decision
from app.services.policy_catalog import (
    catalog_for_market,
    catalog_mi_authoritative,
    catalog_municipalities,
)
from app.services.policy_coverage_service import (
    compute_coverage_status,
    upsert_coverage_status,
)
from app.services.policy_extractor_service import (
    _assertion_type_for,
    _priority_for,
    _rule_family_for,
    _source_rank_for,
    extract_assertions_for_source,
)
from app.services.policy_pipeline_service import (
    cleanup_market,
    repair_market,
    run_market_pipeline,
)
from app.services.policy_projection_service import (
    build_property_compliance_brief,
    project_verified_assertions_to_profile,
)
from app.services.policy_review_service import (
    auto_verify_market_assertions,
    create_policy_override,
    list_policy_overrides,
    revoke_policy_override,
    supersede_replaced_assertions,
)
from app.services.policy_source_service import (
    collect_catalog_all_municipalities,
    collect_catalog_for_focus,
    collect_catalog_for_market,
    collect_url,
)

router = APIRouter(prefix="/policy", tags=["policy"])


class CollectIn(BaseModel):
    url: str
    state: Optional[str] = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    program_type: Optional[str] = None
    publisher: Optional[str] = None
    title: Optional[str] = None
    notes: Optional[str] = None
    org_scope: bool = False


class ExtractIn(BaseModel):
    source_id: int
    org_scope: bool = False


class ReviewIn(BaseModel):
    review_status: str = Field(
        ...,
        description="verified|rejected|reviewed|extracted|stale|needs_recheck|superseded",
    )
    confidence: Optional[float] = None
    value: Optional[Any] = None
    review_notes: Optional[str] = None
    verification_reason: Optional[str] = None
    stale_after: Optional[str] = None
    superseded_by_assertion_id: Optional[int] = None


class BatchReviewIn(BaseModel):
    assertion_ids: list[int]
    review_status: str
    confidence: Optional[float] = None
    review_notes: Optional[str] = None
    verification_reason: Optional[str] = None


class SourceBackedAssertionIn(BaseModel):
    source_id: int
    rule_key: str
    value: dict[str, Any]
    confidence: float = Field(0.95, ge=0.0, le=1.0)
    review_notes: str
    verification_reason: str = "official_source_review"
    org_scope: bool = False


class BuildProfileIn(BaseModel):
    state: str = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    org_scope: bool = False
    notes: Optional[str] = None


class CoverageUpsertIn(BaseModel):
    state: str = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    org_scope: bool = False
    notes: Optional[str] = None


class MarketIn(BaseModel):
    state: str = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    org_scope: bool = False
    focus: str = "se_mi_extended"


class MarketBuildIn(BaseModel):
    state: str = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    org_scope: bool = False
    focus: str = "se_mi_extended"
    notes: Optional[str] = None


class MarketCleanupIn(BaseModel):
    state: str = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    org_scope: bool = False
    archive_extracted_duplicates: bool = True
    focus: str = "se_mi_extended"


class MarketRepairIn(BaseModel):
    state: str = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    org_scope: bool = False
    focus: str = "se_mi_extended"
    archive_extracted_duplicates: bool = True


class JurisdictionReviewDecisionIn(BaseModel):
    reviewer_action: str
    reviewer_rationale: Optional[str] = None
    expires_at: Optional[str] = None


class PolicyOverrideIn(BaseModel):
    jurisdiction_profile_id: int | None = None
    assertion_id: int | None = None
    state: str | None = None
    county: str | None = None
    city: str | None = None
    pha_name: str | None = None
    program_type: str | None = None
    override_scope: str = "jurisdiction"
    override_type: str = "interim_operational_override"
    rule_key: str | None = None
    rule_category: str | None = None
    severity: str = "medium"
    carrying_critical_rule: bool = False
    trust_impact: str = "review_required"
    reason: str
    linked_evidence: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    expires_at: str | None = None


class PolicyOverrideRevokeIn(BaseModel):
    revoked_reason: str | None = None


class ManualPolicyMarketIn(BaseModel):
    state: str = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    focus: str = "se_mi_extended"
    org_scope: bool = False
    auto_activate: bool = True



def _manual_market_target_org_id(principal: Any, org_scope: bool) -> int | None:
    return getattr(principal, "org_id", None) if org_scope else None


def _manual_market_result(
    *,
    action: str,
    result: dict[str, Any],
) -> dict[str, Any]:
    return {
        "ok": True,
        "action": action,
        "manual": True,
        "result": result,
        "summary": {
            "discovery_result": result.get("discovery_result"),
            "refresh_summary": result.get("refresh_summary"),
            "refresh_state": result.get("refresh_state"),
            "lifecycle_result": result.get("lifecycle_result"),
            "recompute": result.get("recompute"),
            "review_queue": result.get("review_queue"),
            "health": ((result.get("pipeline_result") or {}).get("health") if isinstance(result.get("pipeline_result"), dict) else None) or result.get("health"),
        },
    }

def _loads(s: Optional[str], default: Any = None) -> Any:
    if default is None:
        default = {}
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _norm_state(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip().upper()
    return v or None


def _norm_lower(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip().lower()
    return v or None


def _norm_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip()
    return v or None



def _apply_profile_pha_filter(query, pha_name: Optional[str]):
    """
    JurisdictionProfile does not always have a pha_name column in this repo shape.
    Only filter on it when the mapped model actually exposes that attribute.
    """
    pha_attr = getattr(JurisdictionProfile, "pha_name", None)
    if pha_attr is None:
        return query
    if pha_name is not None:
        return query.filter(pha_attr == _norm_text(pha_name))
    return query.filter(pha_attr.is_(None))


def _market_sources_for_catalog(
    db,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None,
    focus: str,
):
    from app.policy_models import PolicySource
    from app.services.policy_catalog_admin_service import merged_catalog_for_market

    items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    urls = [item.url.strip() for item in items if item.url.strip()]
    if not urls:
        return []

    q = db.query(PolicySource).filter(PolicySource.url.in_(urls))
    if org_id is None:
        q = q.filter(PolicySource.org_id.is_(None))
    else:
        q = q.filter((PolicySource.org_id == org_id) | (PolicySource.org_id.is_(None)))

    return q.order_by(PolicySource.id.asc()).all()


def _normalize_collect_results(results: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in results:
        source = getattr(item, "source", item)
        if source is None:
            continue
        normalized.append(
            {
                "source": source,
                "changed": bool(getattr(item, "changed", False)),
                "fetch_ok": bool(getattr(item, "fetch_ok", True)),
                "fetch_error": getattr(item, "fetch_error", None),
            }
        )
    return normalized


@router.get("/catalog")
def get_catalog(
    focus: str = Query("se_mi_extended"),
    principal=Depends(get_principal),
):
    items = catalog_mi_authoritative(focus=focus)
    return {"focus": focus, "items": [item.__dict__ for item in items]}


@router.get("/catalog/municipalities")
def get_catalog_municipalities(
    focus: str = Query("se_mi_extended"),
    principal=Depends(get_principal),
):
    items = catalog_mi_authoritative(focus=focus)
    return {
        "focus": focus,
        "count": len(catalog_municipalities(items)),
        "items": catalog_municipalities(items),
    }


@router.post("/catalog/ingest")
def ingest_catalog(
    focus: str = Query("se_mi_extended"),
    org_scope: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if org_scope else None

    results = collect_catalog_for_focus(
        db,
        org_id=target_org_id,
        focus=focus,
    )

    payload = [
        {
            "source_id": r.source.id,
            "changed": r.changed,
            "url": r.source.url,
            "fetch_ok": r.fetch_ok,
            "fetch_error": r.fetch_error,
        }
        for r in results
    ]

    return {
        "ok": True,
        "focus": focus,
        "count": len(payload),
        "ok_count": sum(1 for r in payload if r["fetch_ok"]),
        "failed_count": sum(1 for r in payload if not r["fetch_ok"]),
        "results": payload,
    }


@router.post("/catalog/collect/market")
def collect_catalog_market(
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if payload.org_scope else None
    results = collect_catalog_for_market(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        focus=payload.focus,
    )
    normalized_results = _normalize_collect_results(results)
    return {
        "ok": True,
        "state": payload.state,
        "county": _norm_lower(payload.county),
        "city": _norm_lower(payload.city),
        "count": len(normalized_results),
        "ok_count": sum(1 for r in normalized_results if r["fetch_ok"]),
        "failed_count": sum(1 for r in normalized_results if not r["fetch_ok"]),
        "results": [
            {
                "source_id": int(r["source"].id),
                "url": r["source"].url,
                "changed": bool(r["changed"]),
                "fetch_ok": bool(r["fetch_ok"]),
                "fetch_error": r["fetch_error"],
            }
            for r in normalized_results
        ],
    }


@router.post("/catalog/collect/all")
def collect_catalog_all(
    focus: str = Query("se_mi_extended"),
    org_scope: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if org_scope else None
    return {
        "ok": True,
        **collect_catalog_all_municipalities(
            db,
            org_id=target_org_id,
            focus=focus,
        ),
    }


@router.post("/sources/collect")
def collect_source(
    payload: CollectIn,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if payload.org_scope else None

    res = collect_url(
        db,
        org_id=target_org_id,
        url=payload.url,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        program_type=payload.program_type,
        publisher=payload.publisher,
        title=payload.title,
        notes=payload.notes,
    )

    s = res.source
    return {
        "ok": True,
        "changed": res.changed,
        "fetch_ok": res.fetch_ok,
        "fetch_error": res.fetch_error,
        "source": {
            "id": s.id,
            "org_id": s.org_id,
            "state": s.state,
            "county": s.county,
            "city": s.city,
            "pha_name": s.pha_name,
            "program_type": s.program_type,
            "publisher": s.publisher,
            "title": s.title,
            "url": s.url,
            "content_type": s.content_type,
            "http_status": s.http_status,
            "retrieved_at": s.retrieved_at.isoformat() if s.retrieved_at else None,
            "content_sha256": s.content_sha256,
            "raw_path": s.raw_path,
            "notes": s.notes,
            "normalized_categories": _loads(getattr(s, "normalized_categories_json", None), []),
            "freshness_status": getattr(s, "freshness_status", None),
            "freshness_reason": getattr(s, "freshness_reason", None),
            "last_verified_at": s.last_verified_at.isoformat() if getattr(s, "last_verified_at", None) else None,
        },
    }


@router.get("/sources")
def list_sources(
    limit: int = Query(100, ge=1, le=500),
    state: Optional[str] = Query(None),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    program_type: Optional[str] = Query(None),
    include_global: bool = Query(True),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    q = db.query(PolicySource)

    if include_global:
        q = q.filter(
            (PolicySource.org_id == principal.org_id) | (PolicySource.org_id.is_(None))
        )
    else:
        q = q.filter(PolicySource.org_id == principal.org_id)

    if state:
        q = q.filter(PolicySource.state == _norm_state(state))
    if county:
        q = q.filter(PolicySource.county == _norm_lower(county))
    if city:
        q = q.filter(PolicySource.city == _norm_lower(city))
    if pha_name:
        q = q.filter(PolicySource.pha_name == pha_name)
    if program_type:
        q = q.filter(PolicySource.program_type == program_type)

    rows = q.order_by(PolicySource.retrieved_at.desc()).limit(limit).all()

    return {
        "items": [
            {
                "id": s.id,
                "org_id": s.org_id,
                "state": s.state,
                "county": s.county,
                "city": s.city,
                "pha_name": s.pha_name,
                "program_type": s.program_type,
                "publisher": s.publisher,
                "title": s.title,
                "url": s.url,
                "content_type": s.content_type,
                "http_status": s.http_status,
                "retrieved_at": s.retrieved_at.isoformat() if s.retrieved_at else None,
                "content_sha256": s.content_sha256,
                "raw_path": s.raw_path,
                "notes": s.notes,
                "normalized_categories": _loads(getattr(s, "normalized_categories_json", None), []),
                "freshness_status": getattr(s, "freshness_status", None),
                "freshness_reason": getattr(s, "freshness_reason", None),
                "freshness_checked_at": s.freshness_checked_at.isoformat() if getattr(s, "freshness_checked_at", None) else None,
                "last_verified_at": s.last_verified_at.isoformat() if getattr(s, "last_verified_at", None) else None,
                "refresh_state": getattr(s, "refresh_state", None),
                "refresh_status_reason": getattr(s, "refresh_status_reason", None),
                "validation_state": getattr(s, "validation_state", None),
                "validation_reason": getattr(s, "validation_reason", None),
                "validation_due_at": getattr(s, "validation_due_at", None).isoformat() if getattr(s, "validation_due_at", None) else None,
                "next_refresh_due_at": getattr(s, "next_refresh_due_at", None).isoformat() if getattr(s, "next_refresh_due_at", None) else None,
                "last_validated_at": getattr(s, "last_validated_at", None).isoformat() if getattr(s, "last_validated_at", None) else None,
            }
            for s in rows
        ]
    }


@router.post("/assertions/extract")
def extract_assertions(
    payload: ExtractIn,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    src = db.query(PolicySource).filter(PolicySource.id == payload.source_id).first()
    if not src:
        raise HTTPException(status_code=404, detail="Source not found")

    target_org_id = principal.org_id if payload.org_scope else None

    created = extract_assertions_for_source(
        db,
        source=src,
        org_id=target_org_id,
        org_scope=payload.org_scope,
    )
    return {"ok": True, "created": len(created), "ids": [a.id for a in created]}


@router.post("/extract/market")
def extract_market(
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if payload.org_scope else None

    rows = _market_sources_for_catalog(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        focus=payload.focus,
    )

    created_ids: list[int] = []
    results: list[dict] = []

    for src in rows:
        created = extract_assertions_for_source(
            db,
            source=src,
            org_id=target_org_id,
            org_scope=payload.org_scope,
        )
        created_ids.extend([a.id for a in created])
        results.append(
            {
                "source_id": src.id,
                "url": src.url,
                "created": len(created),
                "normalized_categories": sorted(
                    {
                        getattr(a, "normalized_category", None)
                        for a in created
                        if getattr(a, "normalized_category", None)
                    }
                ),
            }
        )

    return {
        "ok": True,
        "state": _norm_state(payload.state),
        "county": _norm_lower(payload.county),
        "city": _norm_lower(payload.city),
        "source_count": len(rows),
        "assertion_count_created": len(created_ids),
        "results": results,
    }


@router.post("/extract/all")
def extract_all(
    focus: str = Query("se_mi_extended"),
    org_scope: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    markets = catalog_municipalities(catalog_mi_authoritative(focus=focus))
    out: list[dict] = []
    total_created = 0

    for market in markets:
        payload = MarketIn(
            state=market["state"] or "MI",
            county=market["county"],
            city=market["city"],
            org_scope=org_scope,
            focus=focus,
        )
        resp = extract_market(payload=payload, db=db, principal=principal)
        total_created += int(resp["assertion_count_created"])
        out.append(
            {
                "state": resp["state"],
                "county": resp["county"],
                "city": resp["city"],
                "source_count": resp["source_count"],
                "assertion_count_created": resp["assertion_count_created"],
            }
        )

    return {
        "ok": True,
        "focus": focus,
        "municipality_count": len(out),
        "assertion_count_created": total_created,
        "markets": out,
    }


@router.get("/assertions")
def list_assertions(
    limit: int = Query(200, ge=1, le=500),
    review_status: Optional[str] = Query(None),
    rule_key: Optional[str] = Query(None),
    rule_family: Optional[str] = Query(None),
    assertion_type: Optional[str] = Query(None),
    normalized_category: Optional[str] = Query(None),
    coverage_status: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    program_type: Optional[str] = Query(None),
    include_global: bool = Query(True),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    q = db.query(PolicyAssertion)

    if include_global:
        q = q.filter(
            (PolicyAssertion.org_id == principal.org_id)
            | (PolicyAssertion.org_id.is_(None))
        )
    else:
        q = q.filter(PolicyAssertion.org_id == principal.org_id)

    if review_status:
        q = q.filter(PolicyAssertion.review_status == review_status)
    if rule_key:
        q = q.filter(PolicyAssertion.rule_key == rule_key)
    if rule_family:
        q = q.filter(PolicyAssertion.rule_family == rule_family)
    if assertion_type:
        q = q.filter(PolicyAssertion.assertion_type == assertion_type)
    if normalized_category:
        q = q.filter(PolicyAssertion.normalized_category == normalized_category)
    if coverage_status:
        q = q.filter(PolicyAssertion.coverage_status == coverage_status)
    if state:
        q = q.filter(PolicyAssertion.state == _norm_state(state))
    if county:
        q = q.filter(PolicyAssertion.county == _norm_lower(county))
    if city:
        q = q.filter(PolicyAssertion.city == _norm_lower(city))
    if pha_name:
        q = q.filter(PolicyAssertion.pha_name == pha_name)
    if program_type:
        q = q.filter(PolicyAssertion.program_type == program_type)

    rows = q.order_by(PolicyAssertion.extracted_at.desc()).limit(limit).all()

    return {
        "items": [
            {
                "id": a.id,
                "org_id": a.org_id,
                "source_id": a.source_id,
                "state": a.state,
                "county": a.county,
                "city": a.city,
                "pha_name": a.pha_name,
                "program_type": a.program_type,
                "rule_key": a.rule_key,
                "rule_family": a.rule_family,
                "assertion_type": a.assertion_type,
                "normalized_category": getattr(a, "normalized_category", None),
                "coverage_status": getattr(a, "coverage_status", None),
                "source_freshness_status": getattr(a, "source_freshness_status", None),
                "value": _loads(a.value_json, {}),
                "confidence": float(a.confidence or 0.0),
                "priority": a.priority,
                "source_rank": a.source_rank,
                "review_status": a.review_status,
                "review_notes": a.review_notes,
                "reviewed_by_user_id": a.reviewed_by_user_id,
                "verification_reason": a.verification_reason,
                "stale_after": a.stale_after.isoformat() if a.stale_after else None,
                "superseded_by_assertion_id": a.superseded_by_assertion_id,
            }
            for a in rows
        ]
    }


@router.post("/assertions/{assertion_id}/review")
def review_assertion(
    assertion_id: int,
    payload: ReviewIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    a = db.query(PolicyAssertion).filter(PolicyAssertion.id == assertion_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Assertion not found")

    if a.org_id is not None and a.org_id != principal.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    a.review_status = payload.review_status
    if payload.confidence is not None:
        a.confidence = float(payload.confidence)
    if payload.value is not None:
        a.value_json = json.dumps(payload.value, ensure_ascii=False)
    if payload.review_notes is not None:
        a.review_notes = payload.review_notes
    if payload.verification_reason is not None:
        a.verification_reason = payload.verification_reason
    if payload.stale_after is not None:
        a.stale_after = datetime.fromisoformat(payload.stale_after)
    if payload.superseded_by_assertion_id is not None:
        a.superseded_by_assertion_id = payload.superseded_by_assertion_id

    a.reviewed_by_user_id = principal.user_id
    a.reviewed_at = datetime.utcnow()

    db.commit()
    db.refresh(a)
    return {
        "ok": True,
        "id": a.id,
        "review_status": a.review_status,
        "normalized_category": getattr(a, "normalized_category", None),
        "coverage_status": getattr(a, "coverage_status", None),
    }


@router.post("/assertions/review/batch")
def review_assertions_batch(
    payload: BatchReviewIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    if not payload.assertion_ids:
        raise HTTPException(status_code=400, detail="assertion_ids required")

    rows = (
        db.query(PolicyAssertion)
        .filter(PolicyAssertion.id.in_(payload.assertion_ids))
        .all()
    )

    updated: list[int] = []
    now = datetime.utcnow()

    for a in rows:
        if a.org_id is not None and a.org_id != principal.org_id:
            continue
        a.review_status = payload.review_status
        if payload.confidence is not None:
            a.confidence = float(payload.confidence)
        if payload.review_notes is not None:
            a.review_notes = payload.review_notes
        if payload.verification_reason is not None:
            a.verification_reason = payload.verification_reason
        a.reviewed_by_user_id = principal.user_id
        a.reviewed_at = now
        updated.append(a.id)

    db.commit()
    return {
        "ok": True,
        "requested_count": len(payload.assertion_ids),
        "updated_count": len(updated),
        "updated_ids": updated,
    }


@router.post("/assertions/from-source")
def create_verified_assertion_from_source(
    payload: SourceBackedAssertionIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    src = db.query(PolicySource).filter(PolicySource.id == payload.source_id).first()
    if not src:
        raise HTTPException(status_code=404, detail="Source not found")

    if src.org_id is not None and src.org_id != principal.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    target_org_id = principal.org_id if payload.org_scope else None

    row = PolicyAssertion(
        org_id=target_org_id,
        source_id=src.id,
        state=src.state,
        county=src.county,
        city=src.city,
        pha_name=src.pha_name,
        program_type=src.program_type,
        rule_key=payload.rule_key,
        rule_family=_rule_family_for(payload.rule_key),
        assertion_type=_assertion_type_for(payload.rule_key),
        value_json=json.dumps(payload.value, ensure_ascii=False),
        confidence=float(payload.confidence),
        priority=_priority_for(payload.rule_key),
        source_rank=_source_rank_for(src),
        review_status="verified",
        review_notes=payload.review_notes,
        reviewed_by_user_id=principal.user_id,
        reviewed_at=datetime.utcnow(),
        verification_reason=payload.verification_reason,
        extracted_at=datetime.utcnow(),
    )

    db.add(row)
    db.commit()
    db.refresh(row)

    return {
        "ok": True,
        "assertion": {
            "id": row.id,
            "source_id": row.source_id,
            "rule_key": row.rule_key,
            "review_status": row.review_status,
            "confidence": row.confidence,
            "normalized_category": getattr(row, "normalized_category", None),
            "coverage_status": getattr(row, "coverage_status", None),
        },
    }


@router.post("/profiles/build")
def build_profile_from_verified_assertions(
    payload: BuildProfileIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if payload.org_scope else None

    row = project_verified_assertions_to_profile(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        notes=payload.notes,
    )

    return {
        "ok": True,
        "profile": {
            "id": row.id,
            "org_id": row.org_id,
            "state": row.state,
            "county": row.county,
            "city": row.city,
            "friction_multiplier": row.friction_multiplier,
            "pha_name": row.pha_name,
            "policy": _loads(row.policy_json, {}),
            "notes": row.notes,
            "completeness": profile_completeness_payload(db, row),
        },
    }


@router.post("/profiles/build/all")
def build_profiles_all(
    focus: str = Query("se_mi_extended"),
    org_scope: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if org_scope else None
    markets = catalog_municipalities(catalog_mi_authoritative(focus=focus))

    out: list[dict] = []
    ready = 0
    partial = 0

    for market in markets:
        row = project_verified_assertions_to_profile(
            db,
            org_id=target_org_id,
            state=market["state"] or "MI",
            county=market["county"],
            city=market["city"],
            notes=f"Projected from verified policy assertions for {market['city']}, {market['county']}, {market['state']}.",
        )
        policy = _loads(row.policy_json, {})
        coverage = policy.get("coverage", {})
        if coverage.get("production_readiness") == "ready":
            ready += 1
        else:
            partial += 1

        out.append(
            {
                "profile_id": row.id,
                "state": row.state,
                "county": row.county,
                "city": row.city,
                "friction_multiplier": row.friction_multiplier,
                "production_readiness": coverage.get("production_readiness"),
                "coverage_status": coverage.get("coverage_status"),
                "completeness_status": coverage.get("completeness_status"),
                "is_stale": coverage.get("is_stale"),
            }
        )

    return {
        "ok": True,
        "focus": focus,
        "municipality_count": len(out),
        "ready_count": ready,
        "partial_count": partial,
        "items": out,
    }


@router.get("/coverage")
def get_coverage_status(
    state: str = Query("MI"),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    org_scope: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if org_scope else None
    coverage = compute_coverage_status(
        db,
        org_id=target_org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    profile_query = db.query(JurisdictionProfile).filter(
        JurisdictionProfile.state == _norm_state(state),
        JurisdictionProfile.county == _norm_lower(county) if county is not None else JurisdictionProfile.county.is_(None),
        JurisdictionProfile.city == _norm_lower(city) if city is not None else JurisdictionProfile.city.is_(None),
        (JurisdictionProfile.org_id == target_org_id) | (JurisdictionProfile.org_id.is_(None)),
    )
    profile_query = _apply_profile_pha_filter(profile_query, pha_name)
    profile = profile_query.order_by(JurisdictionProfile.org_id.desc(), JurisdictionProfile.id.desc()).first()
    return {
        **coverage,
        "operational_status": _profile_operational_payload(db, profile, org_id=target_org_id),
    }


@router.get("/coverage/all")
def get_coverage_all(
    focus: str = Query("se_mi_extended"),
    org_scope: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if org_scope else None
    markets = catalog_municipalities(catalog_mi_authoritative(focus=focus))

    items = []
    for market in markets:
        coverage = compute_coverage_status(
            db,
            org_id=target_org_id,
            state=market["state"] or "MI",
            county=market["county"],
            city=market["city"],
            pha_name=None,
        )
        items.append(
            {
                "state": market["state"],
                "county": market["county"],
                "city": market["city"],
                **coverage,
            }
        )

    return {
        "ok": True,
        "focus": focus,
        "count": len(items),
        "items": items,
    }


@router.post("/coverage")
def refresh_coverage_status(
    payload: CoverageUpsertIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if payload.org_scope else None
    row = upsert_coverage_status(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        notes=payload.notes,
    )
    return {
        "ok": True,
        "coverage": {
            "id": row.id,
            "state": row.state,
            "county": row.county,
            "city": row.city,
            "pha_name": row.pha_name,
            "coverage_status": row.coverage_status,
            "production_readiness": row.production_readiness,
            "verified_rule_count": row.verified_rule_count,
            "source_count": row.source_count,
            "fetch_failure_count": row.fetch_failure_count,
            "stale_warning_count": row.stale_warning_count,
            "completeness_score": getattr(row, "completeness_score", None),
            "completeness_status": getattr(row, "completeness_status", None),
            "is_stale": getattr(row, "is_stale", None),
            "stale_reason": getattr(row, "stale_reason", None),
            "required_categories": _loads(getattr(row, "required_categories_json", None), []),
            "covered_categories": _loads(getattr(row, "covered_categories_json", None), []),
            "missing_categories": _loads(getattr(row, "missing_categories_json", None), []),
        },
    }


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
    }


def _category_matrix_from_completeness(db: Session, completeness: dict[str, Any]) -> list[dict[str, Any]]:
    details = completeness.get("category_details") or {}
    statuses = completeness.get("category_statuses") or {}
    required = list(completeness.get("required_categories") or [])
    ordered = required + [cat for cat in details.keys() if cat not in required]
    out: list[dict[str, Any]] = []
    required_set = set(required)
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
            rows = db.query(PolicySource).filter(PolicySource.id.in_(source_ids)).all()
            row_map = {int(row.id): row for row in rows if getattr(row, "id", None) is not None}
            source_rows = [_serialize_policy_source(row_map[source_id]) for source_id in source_ids if source_id in row_map]
        out.append(
            {
                "category": category,
                "status": detail.get("status") or statuses.get(category) or "missing",
                "expected": category in required_set,
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


def _coverage_matrix_payload(db: Session, profile: JurisdictionProfile | None) -> dict[str, Any] | None:
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


def _policy_meta_from_profile(profile: JurisdictionProfile | None) -> dict[str, Any]:
    if profile is None:
        return {}
    payload = _loads(getattr(profile, "policy_json", None), {})
    if not isinstance(payload, dict):
        return {}
    meta = payload.get("meta") or {}
    return meta if isinstance(meta, dict) else {}


def _profile_summary_payload(db: Session, profile: JurisdictionProfile | None) -> dict[str, Any] | None:
    if profile is None:
        return None
    completeness = profile_completeness_payload(db, profile)
    meta = _policy_meta_from_profile(profile)
    return {
        "id": profile.id,
        "state": profile.state,
        "county": profile.county,
        "city": profile.city,
        "pha_name": profile.pha_name,
        "policy": _loads(profile.policy_json, {}),
        "resolved_rule_version": meta.get("resolved_rule_version") or meta.get("rule_version") or (profile.updated_at.isoformat() if getattr(profile, "updated_at", None) else None),
        "coverage_confidence": completeness.get("coverage_confidence") or meta.get("coverage_confidence") or ("high" if completeness.get("completeness_score", 0) >= 0.85 else "medium" if completeness.get("completeness_score", 0) >= 0.6 else "low"),
        "missing_local_rule_areas": completeness.get("missing_local_rule_areas") or completeness.get("missing_categories") or meta.get("missing_local_rule_areas") or [],
        "source_evidence": meta.get("source_evidence") or meta.get("evidence") or [],
        "last_refreshed": meta.get("last_refreshed") or (profile.updated_at.isoformat() if getattr(profile, "updated_at", None) else None),
        "is_stale": bool(completeness.get("is_stale")),
        "stale_reason": completeness.get("stale_reason") or meta.get("stale_reason"),
        "completeness": completeness,
        "resolved_layers": meta.get("resolved_layers") or meta.get("layers") or [],
        "coverage_matrix": _coverage_matrix_payload(db, profile),
    }



def _profile_operational_payload(db: Session, profile: JurisdictionProfile | None, *, org_id: int | None = None) -> dict[str, Any]:
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
            "source_summary": {},
            "last_validation_at": None,
            "next_due_step": "create_or_refresh_profile",
            "lockout_causing_categories": [],
            "informational_gap_categories": [],
            "validation_pending_categories": [],
            "authority_gap_categories": [],
        }
    health = get_jurisdiction_health(db, profile_id=int(profile.id), org_id=org_id)
    return {
        "health_state": (health or {}).get("health_status") or (health or {}).get("operational_state") or "unknown",
        "refresh_state": (health or {}).get("refresh_state"),
        "refresh_status_reason": (health or {}).get("refresh_status_reason"),
        "reliability_state": "safe_to_rely_on" if (health or {}).get("safe_to_rely_on") else ("unsafe_to_rely_on" if ((health or {}).get("lockout") or {}).get("lockout_active") else "review_required"),
        "safe_to_rely_on": bool((health or {}).get("safe_to_rely_on") or (health or {}).get("safe_for_user_reliance")),
        "trustworthy_for_projection": bool((health or {}).get("safe_for_projection")),
        "review_required": bool((health or {}).get("review_required") or (health or {}).get("validation_pending_categories") or (health or {}).get("authority_gap_categories")),
        "reasons": list(((health or {}).get("lockout") or {}).get("informational_gap_categories") or []) or list((health or {}).get("completeness", {}).get("missing_categories") or []),
        "lockout": dict((health or {}).get("lockout") or {}),
        "next_actions": dict((health or {}).get("next_actions") or {}),
        "source_summary": dict((health or {}).get("sla_summary") or {}),
        "last_refresh_success_at": (health or {}).get("last_refresh_success_at"),
        "last_refresh_completed_at": (health or {}).get("last_refresh_completed_at"),
        "last_validation_at": (health or {}).get("last_validation_at"),
        "next_due_step": (health or {}).get("next_due_step"),
        "lockout_causing_categories": list((health or {}).get("lockout_causing_categories") or []),
        "informational_gap_categories": list((health or {}).get("informational_gap_categories") or []),
        "validation_pending_categories": list((health or {}).get("validation_pending_categories") or []),
        "authority_gap_categories": list((health or {}).get("authority_gap_categories") or []),
        "operational_reason": (health or {}).get("operational_reason"),
    }


@router.get("/profiles/{jurisdiction_profile_id}/coverage-matrix")
def get_policy_profile_coverage_matrix(
    jurisdiction_profile_id: int,
    recompute: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    if profile is None:
        raise HTTPException(status_code=404, detail="Jurisdiction profile not found")
    if profile.org_id is not None and profile.org_id != principal.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    if recompute:
        from app.services.jurisdiction_completeness_service import recompute_profile_and_coverage
        profile, _ = recompute_profile_and_coverage(db, profile, commit=True)
    return {"ok": True, **(_coverage_matrix_payload(db, profile) or {})}


@router.post("/market/coverage")
def get_market_coverage(
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if payload.org_scope else None
    coverage = compute_coverage_status(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
    )

    profile_query = db.query(JurisdictionProfile).filter(
        JurisdictionProfile.state == _norm_state(payload.state),
        JurisdictionProfile.county == _norm_lower(payload.county),
        JurisdictionProfile.city == _norm_lower(payload.city),
        (JurisdictionProfile.org_id == target_org_id) if target_org_id is not None else JurisdictionProfile.org_id.is_(None),
    )
    profile_query = _apply_profile_pha_filter(profile_query, payload.pha_name)
    profile = profile_query.order_by(JurisdictionProfile.id.desc()).first()

    profile_payload = _profile_summary_payload(db, profile)
    return {
        "ok": True,
        "market": {
            "state": _norm_state(payload.state),
            "county": _norm_lower(payload.county),
            "city": _norm_lower(payload.city),
            "pha_name": _norm_text(payload.pha_name),
        },
        "coverage": coverage,
        "profile": profile_payload,
        "coverage_confidence": (profile_payload or {}).get("coverage_confidence") or (coverage or {}).get("coverage_confidence"),
        "missing_local_rule_areas": (profile_payload or {}).get("missing_local_rule_areas") or (coverage or {}).get("missing_local_rule_areas") or [],
        "coverage_matrix": (profile_payload or {}).get("coverage_matrix"),
    }


@router.get("/brief")
def get_policy_brief(
    state: str = Query("MI"),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    org_scope: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if org_scope else None
    try:
        profile_query = db.query(JurisdictionProfile).filter(
            JurisdictionProfile.state == _norm_state(state),
            JurisdictionProfile.county == _norm_lower(county) if county is not None else JurisdictionProfile.county.is_(None),
            JurisdictionProfile.city == _norm_lower(city) if city is not None else JurisdictionProfile.city.is_(None),
            (JurisdictionProfile.org_id == target_org_id) | (JurisdictionProfile.org_id.is_(None)),
        )
        profile_query = _apply_profile_pha_filter(profile_query, pha_name)
        profile = profile_query.order_by(JurisdictionProfile.org_id.desc(), JurisdictionProfile.id.desc()).first()
    
        brief = build_property_compliance_brief(
            db,
            org_id=target_org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )
    
        profile_payload = _profile_summary_payload(db, profile)
        operational_status = _profile_operational_payload(db, profile, org_id=target_org_id)
        return {
            "ok": True,
            "market": {
                "state": _norm_state(state),
                "county": _norm_lower(county),
                "city": _norm_lower(city),
                "pha_name": _norm_text(pha_name),
            },
            "profile": profile_payload,
            "brief": brief,
            "coverage_confidence": (profile_payload or {}).get("coverage_confidence") or brief.get("coverage_confidence"),
            "missing_local_rule_areas": (profile_payload or {}).get("missing_local_rule_areas") or brief.get("missing_local_rule_areas") or [],
            "coverage_matrix": (profile_payload or {}).get("coverage_matrix"),
            "operational_status": operational_status,
            "operational_health": operational_status,
            "safe_to_rely_on": operational_status.get("safe_to_rely_on"),
            "unsafe_reasons": operational_status.get("reasons") or [],
            "lockout": operational_status.get("lockout"),
            "next_actions": operational_status.get("next_actions"),
        }
    except Exception as exc:
        rollback_quietly(db)
        raise HTTPException(status_code=409, detail=f"policy_brief_conflict: {type(exc).__name__}")


@router.get("/property/{property_id}/resolved-rules")
def get_property_resolved_rules(
    property_id: int,
    recompute_coverage: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    prop = db.get(Property, int(property_id))
    if not prop or getattr(prop, "org_id", None) != principal.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    target_org_id = principal.org_id
    state = _norm_state(getattr(prop, "state", None) or "MI")
    county = _norm_lower(getattr(prop, "county", None))
    city = _norm_lower(getattr(prop, "city", None))

    profile = db.query(JurisdictionProfile).filter(
        JurisdictionProfile.state == state,
        JurisdictionProfile.county == county,
        JurisdictionProfile.city == city,
        (JurisdictionProfile.org_id == target_org_id) | (JurisdictionProfile.org_id.is_(None)),
    ).order_by(JurisdictionProfile.org_id.desc(), JurisdictionProfile.id.desc()).first()

    if recompute_coverage:
        upsert_coverage_status(
            db,
            org_id=target_org_id,
            state=state,
            county=county,
            city=city,
            pha_name=getattr(prop, "pha_name", None),
            notes=f"Coverage refreshed for property {prop.id}",
        )
        if profile is not None:
            db.refresh(profile)

    brief = build_property_compliance_brief(
        db,
        org_id=target_org_id,
        state=state,
        county=county,
        city=city,
        pha_name=getattr(prop, "pha_name", None),
    )

    profile_payload = _profile_summary_payload(db, profile)
    operational_status = _profile_operational_payload(
        db,
        profile,
        org_id=target_org_id,
    )
    return {
        "ok": True,
        "property": {
            "id": int(prop.id),
            "address": getattr(prop, "address", None),
            "state": getattr(prop, "state", None),
            "county": getattr(prop, "county", None),
            "city": getattr(prop, "city", None),
        },
        "profile": profile_payload,
        "brief": brief,
        "resolved_rule_version": (profile_payload or {}).get("resolved_rule_version"),
        "coverage_confidence": (profile_payload or {}).get("coverage_confidence"),
        "missing_local_rule_areas": (profile_payload or {}).get("missing_local_rule_areas") or [],
        "stale_warning": bool((profile_payload or {}).get("is_stale")),
        "coverage_matrix": (profile_payload or {}).get("coverage_matrix"),
        "operational_status": operational_status,
        "operational_health": operational_status,
        "safe_to_rely_on": operational_status.get("safe_to_rely_on"),
        "unsafe_reasons": operational_status.get("reasons") or [],
        "lockout": operational_status.get("lockout"),
        "next_actions": operational_status.get("next_actions"),
    }

@router.get("/operational-health")
def policy_operational_health(
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
    operational_status = _profile_operational_payload(db, profile, org_id=getattr(principal, "org_id", None))
    result["operational_status"] = operational_status
    result["source_summary"] = operational_status.get("source_summary")
    result["safe_to_rely_on"] = operational_status.get("safe_to_rely_on")
    result["unsafe_reasons"] = operational_status.get("reasons") or []
    result["lockout_causing_categories"] = operational_status.get("lockout_causing_categories") or []
    result["informational_gap_categories"] = operational_status.get("informational_gap_categories") or []
    result["validation_pending_categories"] = operational_status.get("validation_pending_categories") or []
    result["authority_gap_categories"] = operational_status.get("authority_gap_categories") or []
    return result


@router.get("/sources/stale")
def list_stale_sources(
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    rows = (
        db.query(PolicySource)
        .filter((PolicySource.org_id == principal.org_id) | (PolicySource.org_id.is_(None)))
        .filter((PolicySource.freshness_status == "stale") | (PolicySource.refresh_state.in_(["blocked", "degraded", "failed"])))
        .order_by(PolicySource.next_refresh_due_at.asc().nullsfirst())
        .limit(limit)
        .all()
    )
    return {
        "items": [
            {
                "id": int(s.id),
                "title": s.title,
                "publisher": s.publisher,
                "url": s.url,
                "freshness_status": getattr(s, "freshness_status", None),
                "refresh_state": getattr(s, "refresh_state", None),
                "refresh_status_reason": getattr(s, "refresh_status_reason", None),
                "validation_state": getattr(s, "validation_state", None),
                "validation_reason": getattr(s, "validation_reason", None),
                "next_refresh_due_at": s.next_refresh_due_at.isoformat() if getattr(s, "next_refresh_due_at", None) else None,
                "validation_due_at": s.validation_due_at.isoformat() if getattr(s, "validation_due_at", None) else None,
            }
            for s in rows
        ]
    }


@router.get("/property/{property_id}/compliance-safety")
def get_property_policy_compliance_safety(
    property_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    property_row = db.get(Property, int(property_id))
    if property_row is None or int(getattr(property_row, "org_id", 0) or 0) != int(principal.org_id):
        raise HTTPException(status_code=404, detail="Property not found")
    brief = build_property_compliance_brief(db, org_id=principal.org_id, property_id=property_id, property=property_row)
    return {
        "ok": True,
        "property_id": int(property_id),
        "safe_to_rely_on": bool(brief.get("safe_to_rely_on")),
        "legally_unsafe": bool(brief.get("legally_unsafe")),
        "informationally_incomplete": bool(brief.get("informationally_incomplete")),
        "unsafe_reasons": list(brief.get("unsafe_reasons") or []),
        "informational_reasons": list(brief.get("informational_reasons") or []),
        "brief": brief,
    }


@router.get("/jurisdictions/review-queue")
def get_jurisdiction_review_queue(
    state: Optional[str] = Query(None),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    return build_review_queue_entries(
        db,
        org_id=getattr(principal, "org_id", None),
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )


@router.post("/jurisdictions/{profile_id}/review-decision")
def post_jurisdiction_review_decision(
    profile_id: int,
    payload: JurisdictionReviewDecisionIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    profile = db.get(JurisdictionProfile, int(profile_id))
    if profile is None:
        raise HTTPException(status_code=404, detail="Jurisdiction profile not found")
    if getattr(profile, "org_id", None) not in {None, getattr(principal, "org_id", None)}:
        raise HTTPException(status_code=403, detail="Forbidden")
    expires_at = datetime.fromisoformat(payload.expires_at) if payload.expires_at else None
    return persist_review_queue_decision(
        db,
        profile=profile,
        reviewer_user_id=getattr(principal, "user_id", None),
        reviewer_action=payload.reviewer_action,
        reviewer_rationale=payload.reviewer_rationale,
        expires_at=expires_at,
    )


@router.get("/overrides")
def get_policy_overrides(
    state: Optional[str] = Query(None),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    jurisdiction_profile_id: Optional[int] = Query(None),
    include_inactive: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    return list_policy_overrides(
        db,
        org_id=principal.org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        jurisdiction_profile_id=jurisdiction_profile_id,
        include_inactive=include_inactive,
    )


@router.post("/overrides")
def create_override(
    payload: PolicyOverrideIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    actor_user_id = getattr(principal, "user_id", None) or getattr(principal, "id", None)
    return create_policy_override(
        db,
        org_id=principal.org_id,
        created_by_user_id=int(actor_user_id) if actor_user_id is not None else None,
        jurisdiction_profile_id=payload.jurisdiction_profile_id,
        assertion_id=payload.assertion_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        program_type=payload.program_type,
        override_scope=payload.override_scope,
        override_type=payload.override_type,
        rule_key=payload.rule_key,
        rule_category=payload.rule_category,
        severity=payload.severity,
        carrying_critical_rule=payload.carrying_critical_rule,
        trust_impact=payload.trust_impact,
        reason=payload.reason,
        linked_evidence=payload.linked_evidence,
        metadata=payload.metadata,
        expires_at=payload.expires_at,
    )


@router.post("/overrides/{override_id}/revoke")
def revoke_override(
    override_id: int,
    payload: PolicyOverrideRevokeIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    actor_user_id = getattr(principal, "user_id", None) or getattr(principal, "id", None)
    return revoke_policy_override(
        db,
        override_id=int(override_id),
        revoked_reason=payload.revoked_reason,
        approved_by_user_id=int(actor_user_id) if actor_user_id is not None else None,
    )

@router.get("/manual/runbook")
def get_manual_policy_runbook(
    state: Optional[str] = Query(None),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    health = None
    if state:
        health = get_jurisdiction_health(
            db,
            org_id=getattr(principal, "org_id", None),
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )
    return {
        "ok": True,
        "manual_mode": True,
        "automation_enabled": False,
        "scope": {"state": state, "county": county, "city": city, "pha_name": pha_name},
        "next_due_step": (health or {}).get("next_actions", {}).get("next_step") if isinstance((health or {}).get("next_actions"), dict) else None,
        "health": health,
        "actions": [
            "discovery_refresh",
            "crawl_refresh",
            "validation_retry",
            "recompute",
            "health_recompute",
        ],
    }


@router.post("/manual/discovery-refresh")
def post_manual_discovery_refresh(
    payload: ManualPolicyMarketIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    result = run_market_pipeline(
        db,
        org_id=_manual_market_target_org_id(principal, payload.org_scope),
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        focus=payload.focus,
        reviewer_user_id=getattr(principal, "user_id", None),
        auto_activate=payload.auto_activate,
    )
    return _manual_market_result(action="discovery_refresh", result=result)


@router.post("/manual/crawl-refresh")
def post_manual_crawl_refresh(
    payload: ManualPolicyMarketIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    result = run_market_pipeline(
        db,
        org_id=_manual_market_target_org_id(principal, payload.org_scope),
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        focus=payload.focus,
        reviewer_user_id=getattr(principal, "user_id", None),
        auto_activate=payload.auto_activate,
    )
    return _manual_market_result(action="crawl_refresh", result=result)


@router.post("/manual/validation-retry")
def post_manual_validation_retry(
    payload: ManualPolicyMarketIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    result = run_market_pipeline(
        db,
        org_id=_manual_market_target_org_id(principal, payload.org_scope),
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        focus=payload.focus,
        reviewer_user_id=getattr(principal, "user_id", None),
        auto_activate=payload.auto_activate,
    )
    return _manual_market_result(action="validation_retry", result=result)


@router.post("/manual/recompute")
def post_manual_market_recompute(
    payload: ManualPolicyMarketIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    result = run_market_pipeline(
        db,
        org_id=_manual_market_target_org_id(principal, payload.org_scope),
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        focus=payload.focus,
        reviewer_user_id=getattr(principal, "user_id", None),
        auto_activate=payload.auto_activate,
    )
    return _manual_market_result(action="recompute", result=result)


@router.post("/manual/health-recompute")
def post_manual_market_health_recompute(
    payload: ManualPolicyMarketIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    health = get_jurisdiction_health(
        db,
        org_id=_manual_market_target_org_id(principal, payload.org_scope),
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
    )
    return {"ok": True, "manual": True, "action": "health_recompute", "health": health}
