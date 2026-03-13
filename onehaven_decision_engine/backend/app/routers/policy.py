from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import get_principal, require_owner
from app.db import get_db
from app.policy_models import PolicyAssertion, PolicySource
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
    return {
        "ok": True,
        "state": payload.state,
        "county": _norm_lower(payload.county),
        "city": _norm_lower(payload.city),
        "count": len(results),
        "ok_count": sum(1 for r in results if r.fetch_ok),
        "failed_count": sum(1 for r in results if not r.fetch_ok),
        "results": [
            {
                "source_id": r.source.id,
                "url": r.source.url,
                "changed": r.changed,
                "fetch_ok": r.fetch_ok,
                "fetch_error": r.fetch_error,
            }
            for r in results
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
    return {"ok": True, "id": a.id, "review_status": a.review_status}


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
    return compute_coverage_status(
        db,
        org_id=target_org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )


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
            "notes": row.notes,
        },
    }


@router.get("/brief")
def get_property_compliance_brief(
    state: str = Query("MI"),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    org_scope: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if org_scope else None
    return build_property_compliance_brief(
        db,
        org_id=target_org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )


@router.post("/market/seed")
def seed_market(
    payload: MarketIn,
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
        notes=f"Seeded market row for {payload.city or payload.county or payload.state}.",
    )
    return {
        "ok": True,
        "market": {
            "state": row.state,
            "county": row.county,
            "city": row.city,
            "pha_name": row.pha_name,
        },
        "coverage": {
            "id": row.id,
            "coverage_status": row.coverage_status,
            "production_readiness": row.production_readiness,
        },
    }


@router.post("/market/collect")
def collect_market_step(
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
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
    return {
        "ok": True,
        "market": {
            "state": _norm_state(payload.state),
            "county": _norm_lower(payload.county),
            "city": _norm_lower(payload.city),
            "pha_name": _norm_text(payload.pha_name),
        },
        "count": len(results),
        "ok_count": sum(1 for r in results if r.fetch_ok),
        "failed_count": sum(1 for r in results if not r.fetch_ok),
        "results": [
            {
                "source_id": r.source.id,
                "url": r.source.url,
                "changed": r.changed,
                "fetch_ok": r.fetch_ok,
                "fetch_error": r.fetch_error,
            }
            for r in results
        ],
    }


@router.post("/market/extract")
def extract_market_step(
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if payload.org_scope else None

    rows = _market_sources_for_catalog(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
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
            }
        )

    review_result = auto_verify_market_assertions(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        reviewer_user_id=principal.user_id,
    )
    supersede_result = supersede_replaced_assertions(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        reviewer_user_id=principal.user_id,
    )

    return {
        "ok": True,
        "market": {
            "state": _norm_state(payload.state),
            "county": _norm_lower(payload.county),
            "city": _norm_lower(payload.city),
            "pha_name": _norm_text(payload.pha_name),
        },
        "source_count": len(rows),
        "assertion_count_created": len(created_ids),
        "results": results,
        "review": {
            **review_result,
            **supersede_result,
        },
    }


@router.post("/market/build")
def build_market_step(
    payload: MarketBuildIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if payload.org_scope else None

    profile = project_verified_assertions_to_profile(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        notes=payload.notes or f"Projected from verified assertions for {payload.city or payload.county or payload.state}.",
    )
    coverage = upsert_coverage_status(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        notes=f"Coverage refreshed after build for {payload.city or payload.county or payload.state}.",
    )
    brief = build_property_compliance_brief(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
    )

    return {
        "ok": True,
        "profile": {
            "id": profile.id,
            "state": profile.state,
            "county": profile.county,
            "city": profile.city,
            "pha_name": profile.pha_name,
            "friction_multiplier": profile.friction_multiplier,
            "notes": profile.notes,
            "policy": _loads(profile.policy_json, {}),
        },
        "coverage": {
            "id": coverage.id,
            "coverage_status": coverage.coverage_status,
            "production_readiness": coverage.production_readiness,
            "verified_rule_count": coverage.verified_rule_count,
            "stale_warning_count": coverage.stale_warning_count,
        },
        "brief": brief,
    }


@router.post("/market/pipeline")
def run_market_pipeline_route(
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if payload.org_scope else None
    return run_market_pipeline(
        db,
        org_id=target_org_id,
        reviewer_user_id=principal.user_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        focus=payload.focus,
    )


@router.post("/market/cleanup-stale")
def cleanup_market_stale_route(
    payload: MarketCleanupIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if payload.org_scope else None
    return cleanup_market(
        db,
        org_id=target_org_id,
        reviewer_user_id=principal.user_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        archive_extracted_duplicates=payload.archive_extracted_duplicates,
        focus=payload.focus,
    )


@router.post("/market/repair")
def repair_market_route(
    payload: MarketRepairIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if payload.org_scope else None
    return repair_market(
        db,
        org_id=target_org_id,
        reviewer_user_id=principal.user_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        focus=payload.focus,
        archive_extracted_duplicates=payload.archive_extracted_duplicates,
    )
