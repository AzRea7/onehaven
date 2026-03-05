# backend/app/routers/policy_evidence.py
from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from app.auth import get_principal
from app.db import get_db
from app.policy_models import PolicyAssertion, PolicySource
from app.services.policy_evidence_service import collect_policy_source
from app.services.policy_extractor_v1 import extract_assertions_v1

router = APIRouter(prefix="/policy", tags=["policy"])


class CollectIn(BaseModel):
    url: str

    # optional scope
    state: Optional[str] = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    program_type: Optional[str] = None

    publisher: Optional[str] = None
    title: Optional[str] = None
    notes: Optional[str] = None


class PolicySourceOut(BaseModel):
    id: int
    org_id: Optional[int]
    state: Optional[str]
    county: Optional[str]
    city: Optional[str]
    pha_name: Optional[str]
    program_type: Optional[str]
    publisher: Optional[str]
    title: Optional[str]
    url: str
    content_type: Optional[str]
    content_hash: str
    raw_path: Optional[str]
    retrieved_at: str
    notes: Optional[str]


class ExtractIn(BaseModel):
    source_id: int
    org_scope: bool = True  # if True, assertions get org_id = principal.org_id else NULL


class PolicyAssertionOut(BaseModel):
    id: int
    org_id: Optional[int]
    source_id: int
    state: Optional[str]
    county: Optional[str]
    city: Optional[str]
    pha_name: Optional[str]
    program_type: Optional[str]
    rule_key: str
    value_json: str
    confidence: float
    review_status: str
    extracted_at: str
    review_notes: Optional[str]


class ReviewIn(BaseModel):
    review_status: str = Field(..., description="verified|rejected|extracted")
    confidence: Optional[float] = None
    value: Optional[Any] = None
    review_notes: Optional[str] = None


@router.post("/sources/collect")
def collect(
    payload: CollectIn,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    res = collect_policy_source(
        db,
        org_id=principal.org_id,
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
        "source": PolicySourceOut(
            id=s.id,
            org_id=s.org_id,
            state=s.state,
            county=s.county,
            city=s.city,
            pha_name=s.pha_name,
            program_type=s.program_type,
            publisher=s.publisher,
            title=s.title,
            url=s.url,
            content_type=s.content_type,
            content_hash=s.content_hash,
            raw_path=s.raw_path,
            retrieved_at=s.retrieved_at.isoformat(),
            notes=s.notes,
        ).model_dump(),
    }


@router.get("/sources")
def list_sources(
    limit: int = 100,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    q = (
        select(PolicySource)
        .where(PolicySource.org_id == principal.org_id)
        .order_by(desc(PolicySource.id))
        .limit(min(limit, 500))
    )
    rows = list(db.scalars(q).all())
    return {
        "items": [
            PolicySourceOut(
                id=s.id,
                org_id=s.org_id,
                state=s.state,
                county=s.county,
                city=s.city,
                pha_name=s.pha_name,
                program_type=s.program_type,
                publisher=s.publisher,
                title=s.title,
                url=s.url,
                content_type=s.content_type,
                content_hash=s.content_hash,
                raw_path=s.raw_path,
                retrieved_at=s.retrieved_at.isoformat(),
                notes=s.notes,
            ).model_dump()
            for s in rows
        ]
    }


@router.post("/assertions/extract")
def extract(
    payload: ExtractIn,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    org_id = principal.org_id if payload.org_scope else None
    out = extract_assertions_v1(db, org_id=org_id, source_id=payload.source_id)
    return {"ok": True, "created": len(out)}


@router.get("/assertions")
def list_assertions(
    review_status: Optional[str] = None,
    rule_key: Optional[str] = None,
    limit: int = 200,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    q = select(PolicyAssertion).where(
        (PolicyAssertion.org_id == principal.org_id) | (PolicyAssertion.org_id.is_(None))
    )
    if review_status:
        q = q.where(PolicyAssertion.review_status == review_status)
    if rule_key:
        q = q.where(PolicyAssertion.rule_key == rule_key)

    q = q.order_by(desc(PolicyAssertion.id)).limit(min(limit, 1000))
    rows = list(db.scalars(q).all())

    return {
        "items": [
            PolicyAssertionOut(
                id=a.id,
                org_id=a.org_id,
                source_id=a.source_id,
                state=a.state,
                county=a.county,
                city=a.city,
                pha_name=a.pha_name,
                program_type=a.program_type,
                rule_key=a.rule_key,
                value_json=a.value_json,
                confidence=float(a.confidence or 0.0),
                review_status=a.review_status,
                extracted_at=a.extracted_at.isoformat(),
                review_notes=a.review_notes,
            ).model_dump()
            for a in rows
        ]
    }


@router.post("/assertions/{assertion_id}/review")
def review(
    assertion_id: int,
    payload: ReviewIn,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    a = db.get(PolicyAssertion, assertion_id)
    if not a:
        return {"ok": False, "detail": "not found"}

    # IMPORTANT: only allow editing assertions visible to this org (org-owned or global)
    if not (a.org_id == principal.org_id or a.org_id is None):
        return {"ok": False, "detail": "forbidden"}

    a.review_status = payload.review_status
    if payload.confidence is not None:
        a.confidence = float(payload.confidence)
    if payload.value is not None:
        import json
        a.value_json = json.dumps(payload.value, ensure_ascii=False)
    if payload.review_notes is not None:
        a.review_notes = payload.review_notes

    db.commit()
    db.refresh(a)
    return {"ok": True}
