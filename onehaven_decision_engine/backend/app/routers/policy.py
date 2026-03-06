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
from app.services.policy_catalog import catalog_mi_authoritative
from app.services.policy_coverage_service import (
    compute_coverage_status,
    upsert_coverage_status,
)
from app.services.policy_extractor_service import extract_assertions_for_source
from app.services.policy_projection_service import (
    build_property_compliance_brief,
    project_verified_assertions_to_profile,
)
from app.services.policy_source_service import collect_url

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


@router.get("/catalog")
def get_catalog(
    focus: str = Query("se_mi_extended"),
    principal=Depends(get_principal),
):
    items = catalog_mi_authoritative(focus=focus)
    return {"focus": focus, "items": [item.__dict__ for item in items]}


@router.post("/catalog/ingest")
def ingest_catalog(
    focus: str = Query("se_mi_extended"),
    org_scope: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    items = catalog_mi_authoritative(focus=focus)
    target_org_id = principal.org_id if org_scope else None

    results = []
    ok_count = 0
    failed_count = 0

    for item in items:
        try:
            res = collect_url(
                db,
                org_id=target_org_id,
                url=item.url,
                state=item.state,
                county=item.county,
                city=item.city,
                pha_name=item.pha_name,
                program_type=item.program_type,
                publisher=item.publisher,
                title=item.title,
                notes=item.notes,
            )
            results.append(
                {
                    "source_id": res.source.id,
                    "changed": res.changed,
                    "url": item.url,
                    "fetch_ok": res.fetch_ok,
                    "fetch_error": res.fetch_error,
                }
            )
            if res.fetch_ok:
                ok_count += 1
            else:
                failed_count += 1
        except Exception as e:
            failed_count += 1
            results.append(
                {
                    "source_id": None,
                    "changed": False,
                    "url": item.url,
                    "fetch_ok": False,
                    "fetch_error": f"{type(e).__name__}: {e}",
                }
            )

    return {
        "ok": True,
        "focus": focus,
        "count": len(results),
        "ok_count": ok_count,
        "failed_count": failed_count,
        "results": results,
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
            "retrieved_at": s.retrieved_at.isoformat(),
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
                "retrieved_at": s.retrieved_at.isoformat(),
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

    created = extract_assertions_for_source(
        db,
        source=src,
        org_id=principal.org_id,
        org_scope=payload.org_scope,
    )
    return {"ok": True, "created": len(created), "ids": [a.id for a in created]}


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
