
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.policy_models import PolicyAssertion, PolicySource

ARCHIVE_MARKER = "[archived_stale_source]"


def _norm_state(v: Optional[str]) -> str:
    return (v or "MI").strip().upper()


def _norm_lower(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    out = str(v).strip().lower()
    return out or None


def _norm_text(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    out = str(v).strip()
    return out or None


def _loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        parsed = json.loads(value)
        return parsed if parsed is not None else default
    except Exception:
        return default


def _is_archived_source(source: PolicySource) -> bool:
    return ARCHIVE_MARKER in str(getattr(source, "notes", "") or "").lower()


def source_family_for_source(source: PolicySource | None) -> str:
    if source is None:
        return "unknown"
    source_type = str(getattr(source, "source_type", "") or "").strip().lower()
    publication_type = str(getattr(source, "publication_type", "") or "").strip().lower()
    notes = str(getattr(source, "notes", "") or "").lower()
    if "dataset" in notes or source_type in {"dataset", "catalog", "registry", "manual"}:
        return "dataset"
    if source_type in {"api", "feed", "program"}:
        return "api"
    if publication_type in {"pdf", "official_document"} or str(getattr(source, "url", "") or "").lower().endswith(".pdf"):
        return "artifact"
    return "web_source"


def evidence_role_for_source(source: PolicySource | None) -> str:
    if source is None:
        return "unknown"
    authority_use_type = str(getattr(source, "authority_use_type", "") or "").strip().lower()
    if authority_use_type == "binding":
        return "primary"
    if authority_use_type == "supporting":
        return "supporting"
    return "freshness_signal"


def source_evidence_record(source: PolicySource) -> dict[str, Any]:
    return {
        "source_id": int(getattr(source, "id", 0) or 0),
        "url": getattr(source, "url", None),
        "title": getattr(source, "title", None),
        "publisher": getattr(source, "publisher", None),
        "state": getattr(source, "state", None),
        "county": getattr(source, "county", None),
        "city": getattr(source, "city", None),
        "pha_name": getattr(source, "pha_name", None),
        "program_type": getattr(source, "program_type", None),
        "source_type": getattr(source, "source_type", None),
        "publication_type": getattr(source, "publication_type", None),
        "content_type": getattr(source, "content_type", None),
        "http_status": getattr(source, "http_status", None),
        "retrieved_at": getattr(source, "retrieved_at", None).isoformat() if getattr(source, "retrieved_at", None) else None,
        "last_verified_at": getattr(source, "last_verified_at", None).isoformat() if getattr(source, "last_verified_at", None) else None,
        "freshness_status": getattr(source, "freshness_status", None),
        "refresh_state": getattr(source, "refresh_state", None),
        "validation_state": getattr(source, "validation_state", None),
        "authority_tier": getattr(source, "authority_tier", None),
        "authority_rank": getattr(source, "authority_rank", None),
        "authority_use_type": getattr(source, "authority_use_type", None),
        "source_family": source_family_for_source(source),
        "evidence_role": evidence_role_for_source(source),
        "content_sha256": getattr(source, "content_sha256", None),
        "raw_path": getattr(source, "raw_path", None),
        "notes": getattr(source, "notes", None),
    }


def assertion_evidence_record(assertion: PolicyAssertion, source: PolicySource | None = None) -> dict[str, Any]:
    value_json = _loads(getattr(assertion, "value_json", None), {})
    citation_json = _loads(getattr(assertion, "citation_json", None), {})
    provenance_json = _loads(getattr(assertion, "rule_provenance_json", None), {})
    evidence_family = None
    if isinstance(value_json, dict):
        evidence_family = value_json.get("evidence_family")
    evidence_family = evidence_family or (provenance_json.get("evidence_family") if isinstance(provenance_json, dict) else None)
    evidence_family = evidence_family or (source_family_for_source(source) if source is not None else "unknown")
    return {
        "assertion_id": int(getattr(assertion, "id", 0) or 0),
        "source_id": getattr(assertion, "source_id", None),
        "state": getattr(assertion, "state", None),
        "county": getattr(assertion, "county", None),
        "city": getattr(assertion, "city", None),
        "pha_name": getattr(assertion, "pha_name", None),
        "program_type": getattr(assertion, "program_type", None),
        "rule_key": getattr(assertion, "rule_key", None),
        "rule_family": getattr(assertion, "rule_family", None),
        "rule_category": getattr(assertion, "rule_category", None) or getattr(assertion, "normalized_category", None),
        "assertion_type": getattr(assertion, "assertion_type", None),
        "confidence": float(getattr(assertion, "confidence", 0.0) or 0.0),
        "review_status": getattr(assertion, "review_status", None),
        "governance_state": getattr(assertion, "governance_state", None),
        "rule_status": getattr(assertion, "rule_status", None),
        "source_citation": getattr(assertion, "source_citation", None),
        "citation_json": citation_json,
        "rule_provenance_json": provenance_json,
        "value_json": value_json,
        "evidence_family": evidence_family,
        "evidence_role": evidence_role_for_source(source),
        "stale_after": getattr(assertion, "stale_after", None).isoformat() if getattr(assertion, "stale_after", None) else None,
        "superseded_by_assertion_id": getattr(assertion, "superseded_by_assertion_id", None),
    }


def _source_scope_match(source: PolicySource, *, county: str | None, city: str | None, pha_name: str | None) -> bool:
    if getattr(source, "county", None) is not None and getattr(source, "county", None) != county:
        return False
    if getattr(source, "city", None) is not None and getattr(source, "city", None) != city:
        return False
    if getattr(source, "pha_name", None) is not None and getattr(source, "pha_name", None) != pha_name:
        return False
    return True


def _assertion_scope_match(assertion: PolicyAssertion, *, county: str | None, city: str | None, pha_name: str | None) -> bool:
    if getattr(assertion, "county", None) is not None and getattr(assertion, "county", None) != county:
        return False
    if getattr(assertion, "city", None) is not None and getattr(assertion, "city", None) != city:
        return False
    if getattr(assertion, "pha_name", None) is not None and getattr(assertion, "pha_name", None) != pha_name:
        return False
    return True


def evidence_for_market(
    db: Session | None,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None,
    include_global: bool = True,
) -> dict[str, Any]:
    if db is None:
        return {
            "ok": True,
            "market": {"state": _norm_state(state), "county": _norm_lower(county), "city": _norm_lower(city), "pha_name": _norm_text(pha_name)},
            "sources": [],
            "assertions": [],
            "summary": {"source_count": 0, "assertion_count": 0, "service_role": "canonical_evidence_abstraction", "truth_model": "evidence_first"},
        }
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    src_stmt = select(PolicySource).where(PolicySource.state == st)
    asr_stmt = select(PolicyAssertion).where(PolicyAssertion.state == st)

    if include_global:
        if org_id is None:
            src_stmt = src_stmt.where(PolicySource.org_id.is_(None))
            asr_stmt = asr_stmt.where(PolicyAssertion.org_id.is_(None))
        else:
            src_stmt = src_stmt.where(or_(PolicySource.org_id == org_id, PolicySource.org_id.is_(None)))
            asr_stmt = asr_stmt.where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))
    else:
        src_stmt = src_stmt.where(PolicySource.org_id == org_id)
        asr_stmt = asr_stmt.where(PolicyAssertion.org_id == org_id)

    src_rows = list(db.scalars(src_stmt).all())
    asr_rows = list(db.scalars(asr_stmt).all())

    scoped_sources = [s for s in src_rows if _source_scope_match(s, county=cnty, city=cty, pha_name=pha) and not _is_archived_source(s)]
    source_map = {int(getattr(s, "id", 0) or 0): s for s in scoped_sources if getattr(s, "id", None) is not None}
    scoped_assertions = [a for a in asr_rows if _assertion_scope_match(a, county=cnty, city=cty, pha_name=pha)]

    source_items = [source_evidence_record(s) for s in scoped_sources]
    assertion_items = [assertion_evidence_record(a, source_map.get(int(getattr(a, "source_id", 0) or 0))) for a in scoped_assertions]

    family_counts: dict[str, int] = {}
    role_counts: dict[str, int] = {}
    for item in source_items:
        family = str(item.get("source_family") or "unknown")
        role = str(item.get("evidence_role") or "unknown")
        family_counts[family] = family_counts.get(family, 0) + 1
        role_counts[role] = role_counts.get(role, 0) + 1

    return {
        "ok": True,
        "market": {"state": st, "county": cnty, "city": cty, "pha_name": pha},
        "sources": source_items,
        "assertions": assertion_items,
        "summary": {
            "source_count": len(source_items),
            "assertion_count": len(assertion_items),
            "source_family_counts": family_counts,
            "evidence_role_counts": role_counts,
            "service_role": "canonical_evidence_abstraction",
            "truth_model": "evidence_first",
        },
    }


def evidence_summary_for_market(
    db: Session | None,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None,
    include_global: bool = True,
) -> dict[str, Any]:
    payload = evidence_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_global=include_global,
    )
    payload["service_role"] = "canonical_evidence_abstraction"
    payload["truth_model"] = "evidence_first"
    return payload
