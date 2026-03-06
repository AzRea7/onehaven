from __future__ import annotations

import json
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.auth import get_principal
from app.db import get_db
from app.policy_models import PolicyAssertion, PolicySource
from app.services.policy_catalog import catalog_mi_authoritative, catalog_municipalities
from app.services.policy_coverage_service import compute_coverage_status

router = APIRouter(prefix="/policy-evidence", tags=["policy-evidence"])


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


@router.get("/market")
def get_market_evidence(
    state: str = Query("MI"),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    include_global: bool = Query(True),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    q_sources = db.query(PolicySource)
    q_assertions = db.query(PolicyAssertion)

    if include_global:
        q_sources = q_sources.filter((PolicySource.org_id == principal.org_id) | (PolicySource.org_id.is_(None)))
        q_assertions = q_assertions.filter((PolicyAssertion.org_id == principal.org_id) | (PolicyAssertion.org_id.is_(None)))
    else:
        q_sources = q_sources.filter(PolicySource.org_id == principal.org_id)
        q_assertions = q_assertions.filter(PolicyAssertion.org_id == principal.org_id)

    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)

    q_sources = q_sources.filter(PolicySource.state == st)
    q_assertions = q_assertions.filter(PolicyAssertion.state == st)

    if cnty is not None:
        q_sources = q_sources.filter(PolicySource.county == cnty)
        q_assertions = q_assertions.filter(PolicyAssertion.county == cnty)

    if cty is not None:
        q_sources = q_sources.filter(PolicySource.city == cty)
        q_assertions = q_assertions.filter(PolicyAssertion.city == cty)

    sources = q_sources.order_by(PolicySource.retrieved_at.desc()).all()
    assertions = q_assertions.order_by(PolicyAssertion.extracted_at.desc()).all()

    return {
        "market": {
            "state": st,
            "county": cnty,
            "city": cty,
        },
        "source_count": len(sources),
        "assertion_count": len(assertions),
        "sources": [
            {
                "id": s.id,
                "publisher": s.publisher,
                "title": s.title,
                "url": s.url,
                "http_status": s.http_status,
                "retrieved_at": s.retrieved_at.isoformat() if s.retrieved_at else None,
                "raw_path": s.raw_path,
                "notes": s.notes,
            }
            for s in sources
        ],
        "assertions": [
            {
                "id": a.id,
                "source_id": a.source_id,
                "rule_key": a.rule_key,
                "rule_family": a.rule_family,
                "assertion_type": a.assertion_type,
                "review_status": a.review_status,
                "confidence": float(a.confidence or 0.0),
                "value": _loads(a.value_json, {}),
                "review_notes": a.review_notes,
                "reviewed_at": a.reviewed_at.isoformat() if a.reviewed_at else None,
            }
            for a in assertions
        ],
    }


@router.get("/review-queue")
def get_review_queue(
    focus: str = Query("se_mi_extended"),
    org_scope: bool = Query(False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if org_scope else None
    markets = catalog_municipalities(catalog_mi_authoritative(focus=focus))

    rows = []
    for market in markets:
        st = market["state"] or "MI"
        cnty = market["county"]
        cty = market["city"]

        coverage = compute_coverage_status(
            db,
            org_id=target_org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=None,
        )

        q = db.query(PolicyAssertion).filter(PolicyAssertion.state == st)
        if target_org_id is None:
            q = q.filter(PolicyAssertion.org_id.is_(None))
        else:
            q = q.filter((PolicyAssertion.org_id == target_org_id) | (PolicyAssertion.org_id.is_(None)))

        if cnty is not None:
            q = q.filter(PolicyAssertion.county == cnty)
        if cty is not None:
            q = q.filter(PolicyAssertion.city == cty)

        assertions = q.all()

        rows.append(
            {
                "state": st,
                "county": cnty,
                "city": cty,
                "coverage_status": coverage.get("coverage_status"),
                "production_readiness": coverage.get("production_readiness"),
                "verified_rule_count": coverage.get("verified_rule_count"),
                "source_count": coverage.get("source_count"),
                "extracted_count": sum(1 for a in assertions if a.review_status == "extracted"),
                "verified_count": sum(1 for a in assertions if a.review_status == "verified"),
                "rejected_count": sum(1 for a in assertions if a.review_status == "rejected"),
                "needs_recheck_count": sum(1 for a in assertions if a.review_status == "needs_recheck"),
            }
        )

    return {
        "focus": focus,
        "count": len(rows),
        "items": rows,
    }