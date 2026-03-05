# backend/app/routers/policy.py
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
from app.services.policy_extractor_service import extract_assertions_for_source
from app.services.policy_projection_service import project_verified_assertions_to_profile
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
    review_status: str = Field(..., description="verified|rejected|reviewed|extracted")
    confidence: Optional[float] = None
    value: Optional[Any] = None
    review_notes: Optional[str] = None


class BuildProfileIn(BaseModel):
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


@router.get("/catalog")
def get_catalog(
    focus: str = Query("se_mi"),
    principal=Depends(get_principal),
):
    items = catalog_mi_authoritative(focus=focus)
    return {
        "focus": focus,
        "items": [item.__dict__ for item in items],
    }


@router.post("/catalog/ingest")
def ingest_catalog(
    focus: str = Query("se_mi"),
    org_scope: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    items = catalog_mi_authoritative(focus=focus)
    target_org_id = principal.org_id if org_scope else None

    results = []
    for item in items:
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
            }
        )
    return {"ok": True, "count": len(results), "results": results}


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
    include_global: bool = Query(True),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    q = db.query(PolicySource)

    if include_global:
        q = q.filter((PolicySource.org_id == principal.org_id) | (PolicySource.org_id.is_(None)))
    else:
        q = q.filter(PolicySource.org_id == principal.org_id)

    if state:
        q = q.filter(PolicySource.state == state.strip().upper())
    if county:
        q = q.filter(PolicySource.county == county.strip().lower())
    if city:
        q = q.filter(PolicySource.city == city.strip().lower())

    rows = q.order_by(PolicySource.retrieved_at.desc()).limit(limit).all()

    return [
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
    state: Optional[str] = Query(None),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    include_global: bool = Query(True),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    q = db.query(PolicyAssertion)

    if include_global:
        q = q.filter((PolicyAssertion.org_id == principal.org_id) | (PolicyAssertion.org_id.is_(None)))
    else:
        q = q.filter(PolicyAssertion.org_id == principal.org_id)

    if review_status:
        q = q.filter(PolicyAssertion.review_status == review_status)
    if rule_key:
        q = q.filter(PolicyAssertion.rule_key == rule_key)
    if state:
        q = q.filter(PolicyAssertion.state == state.strip().upper())
    if county:
        q = q.filter(PolicyAssertion.county == county.strip().lower())
    if city:
        q = q.filter(PolicyAssertion.city == city.strip().lower())

    rows = q.order_by(PolicyAssertion.extracted_at.desc()).limit(limit).all()

    out = []
    for a in rows:
        out.append(
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
                "value": _loads(a.value_json, {}),
                "confidence": float(a.confidence or 0.0),
                "review_status": a.review_status,
                "review_notes": a.review_notes,
            }
        )
    return out


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
