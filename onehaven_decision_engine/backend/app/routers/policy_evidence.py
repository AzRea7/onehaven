from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.auth import get_principal
from app.db import get_db
from app.policy_models import PolicyAssertion, PolicySource

router = APIRouter(prefix="/policy-evidence", tags=["policy-evidence"])

ARCHIVE_MARKER = "[archived_stale_source]"


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


def _is_archived_source(src: PolicySource) -> bool:
    return ARCHIVE_MARKER in (src.notes or "").lower()


@router.get("/market")
def evidence_for_market(
    state: str = Query("MI"),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    include_global: bool = Query(True),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    st = _norm_state(state) or "MI"
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    src_q = db.query(PolicySource)
    asr_q = db.query(PolicyAssertion)

    if include_global:
        src_q = src_q.filter(
            (PolicySource.org_id == principal.org_id) | (PolicySource.org_id.is_(None))
        )
        asr_q = asr_q.filter(
            (PolicyAssertion.org_id == principal.org_id)
            | (PolicyAssertion.org_id.is_(None))
        )
    else:
        src_q = src_q.filter(PolicySource.org_id == principal.org_id)
        asr_q = asr_q.filter(PolicyAssertion.org_id == principal.org_id)

    src_rows = src_q.filter(PolicySource.state == st).all()
    asr_rows = asr_q.filter(PolicyAssertion.state == st).all()

    source_items = []
    for s in src_rows:
        if s.county is not None and s.county != cnty:
            continue
        if s.city is not None and s.city != cty:
            continue
        if s.pha_name is not None and s.pha_name != pha:
            continue
        if _is_archived_source(s):
            continue

        source_items.append(
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
        )

    assertion_items = []
    for a in asr_rows:
        if a.county is not None and a.county != cnty:
            continue
        if a.city is not None and a.city != cty:
            continue
        if a.pha_name is not None and a.pha_name != pha:
            continue

        assertion_items.append(
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
                "value": a.value_json,
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
        )

    source_items.sort(key=lambda x: ((x.get("title") or ""), (x.get("url") or "")))
    assertion_items.sort(key=lambda x: ((x.get("rule_key") or ""), x.get("id") or 0))

    return {
        "ok": True,
        "market": {
            "state": st,
            "county": cnty,
            "city": cty,
            "pha_name": pha,
        },
        "sources": source_items,
        "assertions": assertion_items,
    }
