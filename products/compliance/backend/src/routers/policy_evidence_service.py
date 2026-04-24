from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.policy_models import PolicyAssertion, PolicySource

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
    url = str(getattr(source, "url", "") or "").lower()
    if "dataset" in notes or source_type in {"dataset", "catalog", "registry", "manual"}:
        return "dataset"
    if source_type in {"api", "feed", "program"}:
        return "api"
    if publication_type in {"pdf", "official_document"} or url.endswith(".pdf"):
        return "artifact"
    return "web_source"


def evidence_role_for_source(source: PolicySource | None) -> str:
    if source is None:
        return "unknown"
    authority_use_type = _effective_source_use_type(source)
    if authority_use_type == "binding":
        return "primary"
    if authority_use_type == "supporting":
        return "supporting"
    return "freshness_signal"



def _effective_source_use_type(source: PolicySource | None) -> str:
    if source is None:
        return "weak"
    explicit = str(getattr(source, "authority_use_type", "") or "").strip().lower()
    if explicit:
        return explicit
    policy_json = _loads(getattr(source, "authority_policy_json", None), {})
    if isinstance(policy_json, dict):
        policy_use = str(policy_json.get("use_type", "") or "").strip().lower()
        if policy_use:
            return policy_use
    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
    if authority_tier == "authoritative_official":
        return "binding"
    if authority_tier in {"approved_official_supporting", "semi_authoritative_operational"}:
        return "supporting"
    return "weak"


def _source_validation_ok(source: PolicySource | None) -> bool:
    if source is None:
        return False
    refresh_state = str(getattr(source, "refresh_state", "") or "").strip().lower()
    freshness_status = str(getattr(source, "freshness_status", "") or "").strip().lower()
    if refresh_state in {"failed", "blocked"}:
        return False
    if freshness_status in {"fetch_failed", "error", "blocked"}:
        return False
    http_status = getattr(source, "http_status", None)
    try:
        http_code = int(http_status) if http_status is not None else None
    except Exception:
        http_code = None
    if http_code is not None and http_code >= 400:
        return False
    return True


def _source_truth_bucket(source: PolicySource | None) -> str:
    if source is None:
        return "unknown"
    authority_use_type = _effective_source_use_type(source)
    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
    if not _source_validation_ok(source):
        return "unusable"
    if authority_use_type == "binding" and authority_tier == "authoritative_official":
        return "binding"
    if authority_use_type in {"binding", "supporting"} or authority_tier in {"authoritative_official", "approved_official_supporting"}:
        return "supporting"
    return "weak"


def _source_truth_flags(source: PolicySource | None) -> dict[str, Any]:
    bucket = _source_truth_bucket(source)
    return {
        "truth_bucket": bucket,
        "usable_for_truth": bucket in {"binding", "supporting"},
        "binding_eligible": bucket == "binding",
        "supporting_only": bucket == "supporting",
        "freshness_only": bucket == "weak",
        "archived": bool(source is not None and _is_archived_source(source)),
    }


def source_evidence_record(source: PolicySource) -> dict[str, Any]:
    truth = _source_truth_flags(source)
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
        "authority_use_type": _effective_source_use_type(source),
        "source_family": source_family_for_source(source),
        "evidence_role": evidence_role_for_source(source),
        "content_sha256": getattr(source, "content_sha256", None),
        "raw_path": getattr(source, "raw_path", None),
        "notes": getattr(source, "notes", None),
        **truth,
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

    validation_state = str(getattr(assertion, "validation_state", None) or "").strip().lower()
    trust_state = str(getattr(assertion, "trust_state", None) or "").strip().lower()
    governance_state = str(getattr(assertion, "governance_state", None) or "").strip().lower()
    review_status = str(getattr(assertion, "review_status", None) or "").strip().lower()
    coverage_status = str(getattr(assertion, "coverage_status", None) or "").strip().lower()
    source_truth = _source_truth_flags(source)

    truth_bucket = "weak"
    if source_truth["binding_eligible"] and validation_state == "validated" and trust_state in {"validated", "trusted"}:
        truth_bucket = "binding"
    elif source_truth["usable_for_truth"] and validation_state == "validated":
        truth_bucket = "supporting"
    elif coverage_status in {"partial", "inferred", "candidate"} or trust_state in {"extracted", "needs_review", "downgraded"}:
        truth_bucket = "weak"

    projectable = bool(
        governance_state == "active"
        and review_status == "verified"
        and validation_state == "validated"
        and trust_state == "trusted"
        and coverage_status in {"verified", "covered", "active", "approved"}
        and getattr(assertion, "superseded_by_assertion_id", None) is None
    )

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
        "validation_state": getattr(assertion, "validation_state", None),
        "trust_state": getattr(assertion, "trust_state", None),
        "coverage_status": getattr(assertion, "coverage_status", None),
        "source_citation": getattr(assertion, "source_citation", None),
        "citation_json": citation_json,
        "rule_provenance_json": provenance_json,
        "value_json": value_json,
        "evidence_family": evidence_family,
        "evidence_role": evidence_role_for_source(source),
        "truth_bucket": truth_bucket,
        "usable_for_truth": truth_bucket in {"binding", "supporting"},
        "binding_eligible": truth_bucket == "binding",
        "projectable_truth": projectable,
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
    truth_bucket_counts: dict[str, int] = {}
    projectable_truth_count = 0

    for item in source_items:
        family = str(item.get("source_family") or "unknown")
        role = str(item.get("evidence_role") or "unknown")
        truth_bucket = str(item.get("truth_bucket") or "unknown")
        family_counts[family] = family_counts.get(family, 0) + 1
        role_counts[role] = role_counts.get(role, 0) + 1
        truth_bucket_counts[truth_bucket] = truth_bucket_counts.get(truth_bucket, 0) + 1

    for item in assertion_items:
        if bool(item.get("projectable_truth")):
            projectable_truth_count += 1

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
            "truth_bucket_counts": truth_bucket_counts,
            "projectable_truth_count": int(projectable_truth_count),
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


# --- surgical pdf/artifact truth overlay ---
def _artifact_backed_source(source: PolicySource | None) -> bool:
    if source is None:
        return False
    source_type = str(getattr(source, "source_type", "") or "").strip().lower()
    publication_type = str(getattr(source, "publication_type", "") or "").strip().lower()
    notes = str(getattr(source, "notes", "") or "").strip().lower()
    raw_path = str(getattr(source, "raw_path", "") or "").strip().lower()
    url = str(getattr(source, "url", "") or "").strip().lower()
    return bool(
        source_type in {"artifact", "dataset", "catalog", "manual"}
        or publication_type in {"pdf", "official_document"}
        or "artifact" in notes
        or "pdf" in notes
        or raw_path.endswith(".pdf")
        or url.endswith(".pdf")
    )


def _artifact_backed_assertion(assertion: PolicyAssertion, source: PolicySource | None = None) -> bool:
    for payload in (
        _loads(getattr(assertion, "value_json", None), {}),
        _loads(getattr(assertion, "citation_json", None), {}),
        _loads(getattr(assertion, "rule_provenance_json", None), {}),
    ):
        if isinstance(payload, dict):
            fam = str(payload.get("evidence_family", "") or "").strip().lower()
            raw_path = str(payload.get("raw_path", "") or "").strip().lower()
            title = str(payload.get("title", "") or "").strip().lower()
            if fam in {"artifact", "pdf"} or raw_path.endswith(".pdf") or title.endswith(".pdf"):
                return True
    return _artifact_backed_source(source)


_evidence_orig_assertion_evidence_record = assertion_evidence_record

def assertion_evidence_record(assertion: PolicyAssertion, source: PolicySource | None = None) -> dict[str, Any]:
    payload = dict(_evidence_orig_assertion_evidence_record(assertion, source))
    artifact_backed = _artifact_backed_assertion(assertion, source)
    payload["artifact_backed"] = bool(artifact_backed)

    validation_state = str(getattr(assertion, "validation_state", None) or "").strip().lower()
    trust_state = str(getattr(assertion, "trust_state", None) or "").strip().lower()
    governance_state = str(getattr(assertion, "governance_state", None) or "").strip().lower()
    review_status = str(getattr(assertion, "review_status", None) or "").strip().lower()
    coverage_status = str(getattr(assertion, "coverage_status", None) or "").strip().lower()

    projectable = bool(
        governance_state == "active"
        and review_status == "verified"
        and validation_state == "validated"
        and trust_state in {"validated", "trusted"}
        and coverage_status in {"verified", "covered", "active", "approved"}
        and getattr(assertion, "superseded_by_assertion_id", None) is None
        and (
            bool(payload.get("binding_eligible"))
            or bool(payload.get("usable_for_truth"))
            or artifact_backed
        )
    )
    payload["projectable_truth"] = projectable
    return payload


_evidence_orig_evidence_for_market = evidence_for_market

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
    payload = dict(_evidence_orig_evidence_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_global=include_global,
    ))
    assertions = list(payload.get("assertions") or [])
    summary = dict(payload.get("summary") or {})
    summary["artifact_backed_assertion_count"] = sum(1 for item in assertions if bool(item.get("artifact_backed")))
    summary["projectable_truth_count"] = sum(1 for item in assertions if bool(item.get("projectable_truth")))
    payload["summary"] = summary
    return payload
