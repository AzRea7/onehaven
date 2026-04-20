from __future__ import annotations

import os
import json
import re
from datetime import datetime


from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import expected_rule_universe_for_scope, normalize_category
from app.policy_models import PolicyAssertion, PolicySource, PolicySourceInventory
from app.services.policy_change_detection_service import compute_next_retry_due, determine_validation_refresh_state
from app.services.policy_source_service import _is_official_host


VALIDATION_STATE_VALIDATED = "validated"
VALIDATION_STATE_WEAK = "weak_support"
VALIDATION_STATE_AMBIGUOUS = "ambiguous"
VALIDATION_STATE_CONFLICTING = "conflicting"
VALIDATION_STATE_UNSUPPORTED = "unsupported"

TRUST_STATE_EXTRACTED = "extracted"
TRUST_STATE_VALIDATED = "validated"
TRUST_STATE_TRUSTED = "trusted"
TRUST_STATE_NEEDS_REVIEW = "needs_review"
TRUST_STATE_DOWNGRADED = "downgraded"

AUTHORITY_TIER_RANKS: dict[str, int] = {
    "derived_or_inferred": 25,
    "semi_authoritative_operational": 60,
    "approved_official_supporting": 85,
    "authoritative_official": 100,
}

CRITICAL_BINDING_CATEGORIES = {"registration", "inspection", "occupancy", "lead", "section8", "program_overlay", "safety"}


CRITICAL_VALIDATION_MIN_CITATION = 0.70
CRITICAL_VALIDATION_MIN_CONFIDENCE = 0.75
GENERAL_VALIDATION_MIN_CITATION = 0.35
GENERAL_VALIDATION_MIN_CONFIDENCE = 0.55

SOURCE_FETCH_FAILURE_STATES = {"fetch_failed", "error", "blocked"}
SOURCE_BLOCKED_STATES = {"blocked"}
SOURCE_HTTP_BLOCK_STATUSES = {401, 403, 405, 406, 407, 429, 451}
SOURCE_HTTP_DEAD_STATUSES = {404, 410}
OFFICIAL_HOST_ALLOWLIST = {
    "ecfr.gov",
    "www.ecfr.gov",
    "federalregister.gov",
    "www.federalregister.gov",
    "hud.gov",
    "www.hud.gov",
    "michigan.gov",
    "www.michigan.gov",
    "legislature.mi.gov",
    "www.legislature.mi.gov",
    "courts.michigan.gov",
    "www.courts.michigan.gov",
}

def _source_requires_revalidation(source: PolicySource | None) -> bool:
    return bool(getattr(source, "revalidation_required", False)) if source is not None else False


def _validation_support_summary(assertion: PolicyAssertion, source: PolicySource | None) -> dict[str, object]:
    authority_policy = _authority_policy_for_source(source)
    category_requirement = _category_authority_requirement(assertion)
    url_validation = _source_url_validation_summary(source)
    return {
        "authority_policy": authority_policy,
        "category_requirement": category_requirement,
        "critical_binding_required": bool(category_requirement.get("critical_binding_required")),
        "binding_sufficient": bool(authority_policy.get("binding_sufficient")) and bool(url_validation.get("binding_allowed")),
        "supporting_only": bool(authority_policy.get("supporting_only")),
        "unusable": (not bool(authority_policy.get("usable", True))) or (not bool(url_validation.get("trust_for_extraction"))),
        "requires_revalidation": _source_requires_revalidation(source),
        "url_validation": url_validation,
    }


def _host_from_url(url: str) -> str:
    raw = str(url or "").strip().lower()
    if "://" in raw:
        raw = raw.split("://", 1)[1]
    raw = raw.split("/", 1)[0].strip()
    if ":" in raw:
        raw = raw.split(":", 1)[0].strip()
    return raw


def _source_http_status(source: PolicySource | None) -> int | None:
    if source is None:
        return None
    raw = getattr(source, "http_status", None)
    try:
        return int(raw) if raw is not None else None
    except Exception:
        return None


def _source_failure_count(source: PolicySource | None) -> int:
    if source is None:
        return 0
    retry_count = getattr(source, "refresh_retry_count", None)
    if retry_count is not None:
        try:
            return int(retry_count)
        except Exception:
            pass
    outcome = _loads_dict(getattr(source, "last_refresh_outcome_json", None))
    validation_summary = _loads_dict(outcome.get("validation_summary"))
    for key in ("failure_count", "retry_count", "consecutive_failure_count"):
        try:
            return int(validation_summary.get(key) or 0)
        except Exception:
            continue
    return 0


def _host_looks_guessed(host: str) -> bool:
    host = str(host or "").strip().lower()
    if not host:
        return True
    guessed_patterns = [
        r"(^|\.)ci\.",
        r"(^|\.)co\.",
        r"(^|\.)cityof[a-z0-9-]+\.",
        r"(^|\.)countyof[a-z0-9-]+\.",
        r"(^|\.)housingauthorityof[a-z0-9-]+\.",
    ]
    if any(re.search(pat, host) for pat in guessed_patterns):
        return True
    bad_tokens = {
        "example.gov",
        "example.mi.us",
        "localhost",
        "127.0.0.1",
    }
    if host in bad_tokens:
        return True
    return False


def _official_host_allowed(url: str) -> bool:
    host = _host_from_url(url)
    if not host:
        return False
    if host in OFFICIAL_HOST_ALLOWLIST:
        return True
    if host.endswith(".gov"):
        return True
    if host.endswith(".mi.us"):
        return True
    return False


def _source_url_validation_summary(source: PolicySource | None) -> dict[str, object]:
    if source is None:
        return {
            "host": "",
            "url_allowed": False,
            "url_reason": "missing_source",
            "fetch_usable": False,
            "fetch_reason": "missing_source",
            "trust_for_extraction": False,
            "binding_allowed": False,
            "rejection_reasons": ["missing_source"],
            "http_status": None,
            "refresh_state": None,
            "freshness_status": None,
            "failure_count": 0,
            "looks_guessed": True,
        }

    url = str(getattr(source, "url", "") or "").strip()
    host = _host_from_url(url)
    freshness_status = str(getattr(source, "freshness_status", "") or "").strip().lower()
    refresh_state = str(getattr(source, "refresh_state", "") or "").strip().lower()
    refresh_reason = str(getattr(source, "refresh_status_reason", "") or getattr(source, "refresh_blocked_reason", "") or "").strip().lower()
    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
    http_status = _source_http_status(source)
    failure_count = _source_failure_count(source)
    looks_guessed = _host_looks_guessed(host)
    is_official_host = _official_host_allowed(url)

    rejection_reasons: list[str] = []
    if not url:
        rejection_reasons.append("missing_url")
    if looks_guessed:
        rejection_reasons.append("guessed_domain")
    if not is_official_host:
        rejection_reasons.append("non_official_host")
    if http_status in SOURCE_HTTP_DEAD_STATUSES:
        rejection_reasons.append("http_not_found")
    elif http_status is not None and http_status >= 400:
        rejection_reasons.append(f"http_status_{http_status}")
    if freshness_status in SOURCE_FETCH_FAILURE_STATES:
        rejection_reasons.append(freshness_status)
    if refresh_state in SOURCE_BLOCKED_STATES:
        rejection_reasons.append("refresh_blocked")
    if http_status in SOURCE_HTTP_BLOCK_STATUSES or "anti-bot" in refresh_reason or "antibot" in refresh_reason or "captcha" in refresh_reason:
        rejection_reasons.append("blocked_or_antibot")
    if failure_count >= 2 and freshness_status in SOURCE_FETCH_FAILURE_STATES:
        rejection_reasons.append("repeated_fetch_failed")

    url_allowed = bool(url) and is_official_host and not looks_guessed
    fetch_usable = url_allowed and not any(
        reason in {
            "http_not_found",
            "fetch_failed",
            "error",
            "blocked",
            "refresh_blocked",
            "blocked_or_antibot",
            "repeated_fetch_failed",
        } or reason.startswith("http_status_")
        for reason in rejection_reasons
    )
    trust_for_extraction = fetch_usable
    binding_allowed = trust_for_extraction and authority_tier == "authoritative_official"

    if not url_allowed:
        url_reason = "guessed_domain" if looks_guessed else "non_official_host"
    elif "http_not_found" in rejection_reasons:
        url_reason = "http_not_found"
    elif any(reason.startswith("http_status_") for reason in rejection_reasons):
        url_reason = next(reason for reason in rejection_reasons if reason.startswith("http_status_"))
    else:
        url_reason = "official_host"

    if fetch_usable:
        fetch_reason = "usable"
    elif rejection_reasons:
        fetch_reason = rejection_reasons[0]
    else:
        fetch_reason = "unusable"

    return {
        "host": host,
        "url_allowed": url_allowed,
        "url_reason": url_reason,
        "fetch_usable": fetch_usable,
        "fetch_reason": fetch_reason,
        "trust_for_extraction": trust_for_extraction,
        "binding_allowed": binding_allowed,
        "rejection_reasons": sorted(set(rejection_reasons)),
        "http_status": http_status,
        "refresh_state": refresh_state or None,
        "freshness_status": freshness_status or None,
        "failure_count": failure_count,
        "looks_guessed": looks_guessed,
    }


def _utcnow() -> datetime:
    return datetime.utcnow()


def _loads_dict(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _dumps(value) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return "{}"


def _norm_state(value):
    return (value or "MI").strip().upper()


def _norm_lower(value):
    if value is None:
        return None
    out = value.strip().lower()
    return out or None


def _norm_text(value):
    if value is None:
        return None
    out = value.strip()
    return out or None


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _citation_quality_from_assertion(assertion: PolicyAssertion) -> float:
    citation_json = _loads_dict(getattr(assertion, "citation_json", None))
    explicit = str(getattr(assertion, "source_citation", "") or "").strip()
    score = 0.0
    if explicit:
        score += 0.45
    if citation_json.get("url"):
        score += 0.25
    if citation_json.get("title"):
        score += 0.15
    if citation_json.get("publisher"):
        score += 0.10
    if citation_json.get("raw_excerpt"):
        score += 0.05
    explicit_quality = _safe_float(citation_json.get("citation_quality"), -1.0)
    if explicit_quality >= 0:
        return max(round(min(explicit_quality, 1.0), 6), round(min(score, 1.0), 6))
    return round(min(score, 1.0), 6)




def _authority_policy_for_source(source: PolicySource | None) -> dict[str, object]:
    if source is None:
        return {"use_type": "weak", "binding_sufficient": False, "usable": False}
    summary = _loads_dict(getattr(source, "authority_policy_json", None))
    if summary:
        return summary
    tier = str(getattr(source, "authority_tier", None) or "derived_or_inferred")
    rank = int(getattr(source, "authority_rank", 0) or 0)
    use_type = str(getattr(source, "authority_use_type", None) or "weak")
    return {
        "authority_tier": tier,
        "authority_rank": rank,
        "use_type": use_type,
        "binding_sufficient": rank >= AUTHORITY_TIER_RANKS["authoritative_official"],
        "supporting_only": use_type == "supporting",
        "usable": use_type != "weak",
    }


def _assertion_scope(assertion: PolicyAssertion) -> dict[str, object]:
    return {
        "state": _norm_state(getattr(assertion, "state", None)),
        "county": _norm_lower(getattr(assertion, "county", None)),
        "city": _norm_lower(getattr(assertion, "city", None)),
        "pha_name": _norm_text(getattr(assertion, "pha_name", None)),
        "include_section8": bool(getattr(assertion, "pha_name", None) or getattr(assertion, "program_type", None) == "section8"),
    }


def _category_authority_requirement(assertion: PolicyAssertion) -> dict[str, object]:
    category = normalize_category(getattr(assertion, "normalized_category", None) or getattr(assertion, "rule_category", None))
    scope = _assertion_scope(assertion)
    universe = expected_rule_universe_for_scope(**scope)
    expectations = dict(getattr(universe, "authority_expectations", {}) or {})
    expected = str(expectations.get(category) or "") if category else ""
    legally_binding = set(getattr(universe, "legally_binding_categories", []) or [])
    critical_binding_required = bool(category in legally_binding or category in CRITICAL_BINDING_CATEGORIES)
    return {
        "category": category,
        "required_tier": expected or None,
        "critical_binding_required": critical_binding_required,
        "legally_binding": category in legally_binding if category else False,
    }


def validate_assertion(*, assertion: PolicyAssertion, source: PolicySource | None) -> dict[str, object]:
    confidence = _safe_float(getattr(assertion, "confidence", 0.0))
    extraction_confidence = _safe_float(getattr(assertion, "extraction_confidence", confidence))
    citation_quality = _citation_quality_from_assertion(assertion)
    citation_json = _loads_dict(getattr(assertion, "citation_json", None))
    provenance_json = _loads_dict(getattr(assertion, "rule_provenance_json", None))
    conflict_hints: list[str] = []
    for maybe in (citation_json.get("conflict_hints"), provenance_json.get("conflict_hints")):
        if isinstance(maybe, list):
            conflict_hints.extend(str(item).strip() for item in maybe if str(item).strip())
    explicit_excerpt = str(getattr(assertion, "raw_excerpt", "") or "").strip()
    evidence_state = str(getattr(assertion, "confidence_basis", "") or "").strip().lower()
    blocking = bool(getattr(assertion, "blocking", False))
    authoritative = bool(getattr(source, "is_authoritative", False)) if source is not None else False
    authority_score = _safe_float(getattr(source, "authority_score", 0.0), 0.0) if source is not None else _safe_float(getattr(assertion, "authority_score", 0.0), 0.0)
    support = _validation_support_summary(assertion, source)
    authority_policy = dict(support.get("authority_policy") or {})
    url_validation = dict(support.get("url_validation") or {})
    category_requirement = dict(support.get("category_requirement") or {})
    required_tier = str(category_requirement.get("required_tier") or "")
    critical_binding_required = bool(support.get("critical_binding_required"))
    binding_sufficient = bool(support.get("binding_sufficient"))
    supporting_only = bool(support.get("supporting_only"))
    unusable = bool(support.get("unusable"))
    source_requires_revalidation = bool(support.get("requires_revalidation"))
    source_rejection_reasons = list(url_validation.get("rejection_reasons") or [])

    min_citation = CRITICAL_VALIDATION_MIN_CITATION if (blocking or critical_binding_required) else GENERAL_VALIDATION_MIN_CITATION
    min_confidence = CRITICAL_VALIDATION_MIN_CONFIDENCE if (blocking or critical_binding_required) else GENERAL_VALIDATION_MIN_CONFIDENCE

    if source_requires_revalidation:
        validation_state = VALIDATION_STATE_AMBIGUOUS
        validation_quality = 0.25
        reason = "authoritative_content_changed_requires_revalidation"
    elif conflict_hints or evidence_state == "conflicting":
        validation_state = VALIDATION_STATE_CONFLICTING
        validation_quality = 0.15
        reason = "conflicting_citation_or_interpretation"
    elif unusable:
        validation_state = VALIDATION_STATE_UNSUPPORTED
        validation_quality = 0.10
        if source_rejection_reasons:
            reason = f"source_authority_unusable_for_policy:{source_rejection_reasons[0]}"
        else:
            reason = "source_authority_unusable_for_policy"
    elif critical_binding_required and not binding_sufficient:
        validation_state = VALIDATION_STATE_UNSUPPORTED if supporting_only else VALIDATION_STATE_AMBIGUOUS
        validation_quality = 0.15 if supporting_only else 0.30
        reason = "critical_category_missing_binding_authority"
    elif required_tier and authority_score < 0.60 and not authoritative:
        validation_state = VALIDATION_STATE_WEAK
        validation_quality = 0.35
        reason = "authority_below_required_tier"
    elif not explicit_excerpt or citation_quality < min_citation:
        validation_state = VALIDATION_STATE_UNSUPPORTED if (blocking or critical_binding_required) else VALIDATION_STATE_WEAK
        validation_quality = 0.20 if (blocking or critical_binding_required) else 0.40
        reason = "missing_or_weak_citation_support"
    elif confidence < min_confidence:
        validation_state = VALIDATION_STATE_WEAK if not (blocking or critical_binding_required) else VALIDATION_STATE_UNSUPPORTED
        validation_quality = 0.30 if (blocking or critical_binding_required) else 0.45
        reason = "confidence_below_required_threshold"
    elif authoritative and confidence >= 0.80 and citation_quality >= 0.70:
        validation_state = VALIDATION_STATE_VALIDATED
        validation_quality = 0.95
        reason = "authoritative_citation_backed"
    elif confidence >= 0.70 and citation_quality >= 0.60 and authority_score >= 0.60:
        validation_state = VALIDATION_STATE_VALIDATED
        validation_quality = 0.85
        reason = "supported_and_consistent"
    elif confidence >= 0.55 and citation_quality >= 0.45:
        validation_state = VALIDATION_STATE_WEAK
        validation_quality = 0.60
        reason = "moderate_support_needs_review"
    else:
        validation_state = VALIDATION_STATE_AMBIGUOUS
        validation_quality = 0.40
        reason = "ambiguous_or_low_confidence"

    if validation_state == VALIDATION_STATE_VALIDATED:
        trust_state = TRUST_STATE_TRUSTED if authoritative and binding_sufficient else TRUST_STATE_VALIDATED
    elif validation_state in {VALIDATION_STATE_AMBIGUOUS, VALIDATION_STATE_CONFLICTING, VALIDATION_STATE_WEAK}:
        trust_state = TRUST_STATE_NEEDS_REVIEW
    else:
        trust_state = TRUST_STATE_DOWNGRADED

    if blocking and validation_state != VALIDATION_STATE_VALIDATED:
        reason = f"critical_rule_{reason}"

    return {
        "validation_state": validation_state,
        "validation_quality": round(validation_quality, 6),
        "validation_reason": reason,
        "citation_quality": round(citation_quality, 6),
        "extraction_confidence": round(extraction_confidence, 6),
        "trust_state": trust_state,
        "validated_at": _utcnow(),
        "blocking_issue": bool((blocking or critical_binding_required) and validation_state != VALIDATION_STATE_VALIDATED),
        "authority_policy": authority_policy,
        "category_requirement": category_requirement,
        "binding_authority_missing": bool(critical_binding_required and not binding_sufficient),
        "supporting_only": bool(supporting_only),
        "requires_revalidation": source_requires_revalidation,
        "url_validation": url_validation,
        "source_rejection_reasons": source_rejection_reasons,
    }


def _apply_validation_state_to_source(
    db: Session,
    *,
    source: PolicySource,
    assertions: list[PolicyAssertion],
    counts: dict[str, int],
    blocking_issue_count: int,
) -> dict[str, object]:
    now = _utcnow()
    state_payload = determine_validation_refresh_state(
        validated_count=counts.get("validated", 0),
        weak_support_count=counts.get("weak_support", 0),
        ambiguous_count=counts.get("ambiguous", 0),
        conflicting_count=counts.get("conflicting", 0),
        unsupported_count=counts.get("unsupported", 0),
        blocking_issue_count=blocking_issue_count,
    )
    source.refresh_state = str(state_payload.get("refresh_state") or getattr(source, "refresh_state", None) or "pending")
    source.refresh_status_reason = str(state_payload.get("status_reason") or "validation_complete")
    source.revalidation_required = False
    source.validation_due_at = None
    source.last_verified_at = now if counts.get("validated", 0) > 0 else getattr(source, "last_verified_at", None)
    source.last_state_transition_at = now
    source.last_refresh_completed_at = now
    if source.refresh_state in {"blocked", "degraded"}:
        source.registry_status = "warning"
        source.refresh_retry_count = int(getattr(source, "refresh_retry_count", 0) or 0) + 1
        if source.refresh_state == "blocked":
            source.refresh_blocked_reason = source.refresh_status_reason
        source.next_refresh_due_at = compute_next_retry_due(
            retry_count=int(getattr(source, "refresh_retry_count", 0) or 0),
            base_dt=now,
            min_hours=12,
            max_days=14,
        )
    else:
        source.registry_status = "active"
        source.refresh_blocked_reason = None
        source.refresh_retry_count = 0

    url_validation = _source_url_validation_summary(source)
    summary = {
        "validated_count": counts.get("validated", 0),
        "weak_support_count": counts.get("weak_support", 0),
        "binding_failure_count": counts.get("binding_failures", 0),
        "ambiguous_count": counts.get("ambiguous", 0),
        "conflicting_count": counts.get("conflicting", 0),
        "unsupported_count": counts.get("unsupported", 0),
        "blocking_issue_count": blocking_issue_count,
        "binding_failures": counts.get("binding_failures", 0),
        "source_id": int(source.id),
        "assertion_ids": [int(a.id) for a in assertions],
        "validation_finished_at": now.isoformat(),
        "refresh_state": source.refresh_state,
        "status_reason": source.refresh_status_reason,
        "next_step": state_payload.get("next_step"),
        "url_validation": url_validation,
    }
    source.last_refresh_outcome_json = _dumps({
        **_loads_dict(getattr(source, "last_refresh_outcome_json", None)),
        "validation_summary": summary,
    })
    source.last_change_summary_json = _dumps({
        **_loads_dict(getattr(source, "last_change_summary_json", None)),
        "validation_summary": summary,
    })
    db.add(source)

    inventory_rows = list(
        db.scalars(
            select(PolicySourceInventory).where(PolicySourceInventory.policy_source_id == int(source.id))
        ).all()
    )
    for row in inventory_rows:
        row.refresh_state = source.refresh_state
        row.refresh_status_reason = source.refresh_status_reason
        row.next_refresh_step = str(state_payload.get("next_step") or row.next_refresh_step or "monitor")
        row.revalidation_required = False
        row.validation_due_at = None
        row.last_state_transition_at = now
        row.last_refresh_outcome_json = _dumps({
            **_loads_dict(getattr(row, "last_refresh_outcome_json", None)),
            "validation_summary": summary,
        })
        meta = _loads_dict(getattr(row, "inventory_metadata_json", None))
        meta["last_validation_summary"] = summary
        row.inventory_metadata_json = _dumps(meta)
        if source.refresh_state in {"blocked", "degraded"}:
            row.lifecycle_state = "failed" if source.refresh_state == "blocked" else row.lifecycle_state
            row.crawl_status = "validation_failed"
            row.last_failure_at = now
            row.failure_count = int(getattr(row, "failure_count", 0) or 0) + 1
            row.next_search_retry_due_at = compute_next_retry_due(
                retry_count=int(getattr(row, "failure_count", 0) or 0),
                base_dt=now,
                min_hours=12,
                max_days=14,
            )
            row.next_crawl_due_at = row.next_search_retry_due_at
        else:
            row.crawl_status = "validated"
            row.lifecycle_state = "active"
            row.last_success_at = now
        db.add(row)
    return summary


def validate_market_assertions(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    source_id: int | None = None,
) -> dict[str, object]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    stmt = select(PolicyAssertion).where(PolicyAssertion.state == st)
    if hasattr(PolicyAssertion, "org_id"):
        if org_id is None:
            stmt = stmt.where(PolicyAssertion.org_id.is_(None))
        else:
            stmt = stmt.where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))
    if cnty is None:
        stmt = stmt.where(PolicyAssertion.county.is_(None))
    else:
        stmt = stmt.where(PolicyAssertion.county == cnty)
    if cty is None:
        stmt = stmt.where(PolicyAssertion.city.is_(None))
    else:
        stmt = stmt.where(PolicyAssertion.city == cty)
    if hasattr(PolicyAssertion, "pha_name"):
        if pha is None:
            stmt = stmt.where(or_(PolicyAssertion.pha_name.is_(None), PolicyAssertion.pha_name == ""))
        else:
            stmt = stmt.where(PolicyAssertion.pha_name == pha)
    if source_id is not None:
        stmt = stmt.where(PolicyAssertion.source_id == int(source_id))

    rows = list(db.scalars(stmt).all())
    counts = {"validated": 0, "weak_support": 0, "ambiguous": 0, "conflicting": 0, "unsupported": 0, "binding_failures": 0}
    updated_ids: list[int] = []
    blocking_issue_count = 0
    by_source: dict[int, list[PolicyAssertion]] = {}

    for row in rows:
        source = db.get(PolicySource, int(row.source_id)) if getattr(row, "source_id", None) is not None else None
        payload = validate_assertion(assertion=row, source=source)
        row.validation_state = payload["validation_state"]
        row.validation_score = payload["validation_quality"]
        row.validation_reason = payload["validation_reason"]
        row.validated_at = payload["validated_at"]
        row.trust_state = payload["trust_state"]
        row.extraction_confidence = payload["extraction_confidence"]
        row.conflict_count = len(_loads_dict(getattr(row, "citation_json", None)).get("conflict_hints") or [])
        row.change_summary = _dumps({
            **_loads_dict(getattr(row, "change_summary", None)),
            "validation": {
                "validation_state": payload["validation_state"],
                "validation_score": payload["validation_quality"],
                "validation_reason": payload["validation_reason"],
                "trust_state": payload["trust_state"],
                "requires_revalidation": bool(payload.get("requires_revalidation")),
                "authority_policy": payload.get("authority_policy") or {},
                "category_requirement": payload.get("category_requirement") or {},
                "url_validation": payload.get("url_validation") or {},
            },
        })
        if row.validation_state == VALIDATION_STATE_UNSUPPORTED:
            row.coverage_status = "unsupported"
        elif row.validation_state == VALIDATION_STATE_CONFLICTING:
            row.coverage_status = "conflicting"
        elif row.validation_state == VALIDATION_STATE_VALIDATED and (row.coverage_status or "candidate") in {"candidate", "partial", "approved"}:
            row.coverage_status = "verified"
        counts[row.validation_state] = counts.get(row.validation_state, 0) + 1
        if payload.get("binding_authority_missing"):
            counts["binding_failures"] = counts.get("binding_failures", 0) + 1
        if payload.get("blocking_issue"):
            blocking_issue_count += 1
        updated_ids.append(int(row.id))
        if getattr(row, "source_id", None) is not None:
            by_source.setdefault(int(row.source_id), []).append(row)

    source_summaries: list[dict[str, object]] = []
    for sid, source_rows in by_source.items():
        source = db.get(PolicySource, sid)
        if source is None:
            continue
        local_counts = {"validated": 0, "weak_support": 0, "ambiguous": 0, "conflicting": 0, "unsupported": 0, "binding_failures": 0}
        local_blocking = 0
        for row in source_rows:
            local_counts[str(getattr(row, "validation_state", "unsupported"))] = local_counts.get(str(getattr(row, "validation_state", "unsupported")), 0) + 1
            payload_url_validation = _loads_dict(getattr(row, "change_summary", None)).get("validation", {}).get("url_validation") if getattr(row, "change_summary", None) else {}
            if bool(getattr(row, "blocking", False)) and str(getattr(row, "validation_state", "")) != VALIDATION_STATE_VALIDATED:
                local_blocking += 1
            if isinstance(payload_url_validation, dict) and not bool(payload_url_validation.get("trust_for_extraction", True)):
                local_blocking += 1
        source_summaries.append(_apply_validation_state_to_source(db, source=source, assertions=source_rows, counts=local_counts, blocking_issue_count=local_blocking))

    db.commit()
    return {
        "validated_count": counts.get("validated", 0),
        "weak_support_count": counts.get("weak_support", 0),
        "binding_failure_count": counts.get("binding_failures", 0),
        "ambiguous_count": counts.get("ambiguous", 0),
        "conflicting_count": counts.get("conflicting", 0),
        "unsupported_count": counts.get("unsupported", 0),
        "blocking_issue_count": blocking_issue_count,
        "binding_failures": counts.get("binding_failures", 0),
        "updated_ids": updated_ids,
        "source_validation_summaries": source_summaries,
        "rejected_source_count": sum(1 for item in source_summaries if str(item.get("refresh_state") or "").lower() == "blocked"),
    }


# --- Final official-source validation softening override ---
def _source_is_curated_official_fetched(source: PolicySource | None) -> bool:
    if source is None:
        return False
    url = str(getattr(source, "url", "") or "").strip()
    if not _is_official_host(url):
        return False

    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
    if authority_tier != "authoritative_official":
        return False

    http_status = _source_http_status(source)
    if http_status in SOURCE_HTTP_DEAD_STATUSES:
        return False

    freshness_status = str(getattr(source, "freshness_status", "") or "").strip().lower()
    if freshness_status in {"not_found"}:
        return False

    notes = str(getattr(source, "notes", "") or "").lower()
    return (
        ("[curated]" in notes)
        or bool(getattr(source, "is_authoritative", False))
        or bool(getattr(source, "domain_name", None))
    )


_BASE_SOURCE_URL_VALIDATION_SUMMARY = _source_url_validation_summary
_BASE_APPLY_VALIDATION_STATE_TO_SOURCE = _apply_validation_state_to_source
_BASE_VALIDATE_ASSERTION = validate_assertion
_BASE_VALIDATE_MARKET_ASSERTIONS = validate_market_assertions


def _source_url_validation_summary(source: PolicySource | None) -> dict[str, object]:
    summary = dict(_BASE_SOURCE_URL_VALIDATION_SUMMARY(source))
    if source is None:
        return summary
    if not _source_is_curated_official_fetched(source):
        return summary

    http_status = _source_http_status(source)
    refresh_reason = str(
        getattr(source, "refresh_status_reason", "") or ""
    ).strip().lower()
    rejection_reasons = list(summary.get("rejection_reasons") or [])

    dead = http_status in SOURCE_HTTP_DEAD_STATUSES
    blocked = (
        http_status in SOURCE_HTTP_BLOCK_STATUSES
        or "anti-bot" in refresh_reason
        or "antibot" in refresh_reason
        or "captcha" in refresh_reason
    )

    if dead:
        return summary

    summary["url_allowed"] = True
    summary["url_reason"] = "official_host"
    summary["trust_for_extraction"] = True
    summary["binding_allowed"] = True

    if blocked:
        summary["fetch_usable"] = True
        summary["fetch_reason"] = "blocked_but_official"
        rejection_reasons = [
            r for r in rejection_reasons
            if r not in {"blocked_or_antibot", "refresh_blocked", "repeated_fetch_failed"}
            and not str(r).startswith("http_status_")
        ]
        rejection_reasons.append("blocked_but_official")
    else:
        summary["fetch_usable"] = True
        summary["fetch_reason"] = "official_fetched"
        rejection_reasons = [
            r for r in rejection_reasons
            if r not in {"refresh_blocked", "repeated_fetch_failed"}
            and not str(r).startswith("http_status_")
        ]

    summary["rejection_reasons"] = sorted(set(rejection_reasons))
    return summary


def _apply_validation_softening(
    *,
    payload: dict[str, object],
    assertion: PolicyAssertion,
    source: PolicySource | None,
) -> dict[str, object]:
    out = dict(payload)
    if source is None:
        return out

    validation_support = dict(out.get("validation_support") or {})
    url_validation = dict(
        validation_support.get("url_validation") or _source_url_validation_summary(source)
    )
    validation_support["url_validation"] = url_validation
    out["validation_support"] = validation_support
    out["url_validation"] = url_validation

    category = normalize_category(
        getattr(assertion, "normalized_category", None)
        or getattr(assertion, "rule_category", None)
    )
    critical_binding_required = bool(category in CRITICAL_BINDING_CATEGORIES)
    review_status = str(getattr(assertion, "review_status", "") or "").strip().lower()

    if _source_is_curated_official_fetched(source) and bool(url_validation.get("trust_for_extraction")):
        if out.get("validation_state") in {
            VALIDATION_STATE_UNSUPPORTED,
            VALIDATION_STATE_CONFLICTING,
            VALIDATION_STATE_AMBIGUOUS,
        }:
            verified = review_status == "verified"
            out["validation_state"] = (
                VALIDATION_STATE_VALIDATED if verified else VALIDATION_STATE_WEAK
            )
            out["validation_quality"] = max(
                float(out.get("validation_quality") or 0.0),
                0.72 if verified else 0.58,
            )
            out["validation_reason"] = "curated_official_source_requires_review_not_block"
            out["validated"] = bool(verified)
            out["trust_state"] = TRUST_STATE_TRUSTED if verified else TRUST_STATE_VALIDATED
            out["blocking_issue"] = False if not critical_binding_required else False

    return out


def _apply_validation_state_to_source(
    db: Session,
    *,
    source: PolicySource,
    assertions: list[PolicyAssertion],
    counts: dict[str, int],
    blocking_issue_count: int,
) -> dict[str, object]:
    summary = dict(
        _BASE_APPLY_VALIDATION_STATE_TO_SOURCE(
            db,
            source=source,
            assertions=assertions,
            counts=counts,
            blocking_issue_count=blocking_issue_count,
        )
    )

    if _source_is_curated_official_fetched(source):
        source.authority_use_type = "binding"
        current_state = str(getattr(source, "refresh_state", "") or "").strip().lower()
        if current_state in {"blocked", "degraded"}:
            source.refresh_state = "review_required"
            source.refresh_status_reason = "validation_conflict_needs_review"
            source.registry_status = "active"
            source.refresh_blocked_reason = None
            source.revalidation_required = True
            db.add(source)
            db.flush()

        summary["refresh_state"] = getattr(source, "refresh_state", None)
        summary["status_reason"] = getattr(source, "refresh_status_reason", None)
        summary["next_step"] = "manual_review"
        summary["revalidation_required"] = True
        summary["authority_use_type"] = "binding"
    return summary


# --- Step 4 additive normalization + verification metadata ---
def _loads_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _source_last_verified_iso(source: PolicySource | None) -> str | None:
    if source is None:
        return None
    dt = (
        getattr(source, "last_verified_at", None)
        or getattr(source, "freshness_checked_at", None)
        or getattr(source, "retrieved_at", None)
    )
    return dt.isoformat() if dt else None


def _source_currentness_summary(source: PolicySource | None) -> dict[str, object]:
    if source is None:
        return {
            "is_current": False,
            "is_verified": False,
            "is_real_source": False,
            "current_reason": "missing_source",
            "last_verified_at": None,
        }

    freshness_status = str(getattr(source, "freshness_status", "") or "").strip().lower()
    refresh_state = str(getattr(source, "refresh_state", "") or "").strip().lower()
    http_status = _source_http_status(source)
    url_validation = _source_url_validation_summary(source)

    is_real_source = bool(url_validation.get("url_allowed")) and bool(
        str(getattr(source, "url", "") or "").strip()
    )
    is_verified = bool(getattr(source, "is_authoritative", False)) or (
        str(getattr(source, "authority_tier", "") or "").strip().lower()
        in {"authoritative_official", "approved_official_supporting"}
    )
    is_current = (
        bool(url_validation.get("fetch_usable"))
        and freshness_status not in {"stale", "not_found", "blocked", "fetch_failed", "error"}
        and refresh_state not in {"blocked", "failed"}
        and (http_status is None or 200 <= int(http_status) < 400)
    )

    if not is_real_source:
        reason = "not_real_source"
    elif not is_current:
        reason = freshness_status or refresh_state or "not_current"
    else:
        reason = "current"

    return {
        "is_current": is_current,
        "is_verified": is_verified,
        "is_real_source": is_real_source,
        "current_reason": reason,
        "last_verified_at": _source_last_verified_iso(source),
    }


def _assertion_normalization_payload_base(
    assertion: PolicyAssertion,
    source: PolicySource | None,
) -> dict[str, object]:
    citation_json = _loads_dict(getattr(assertion, "citation_json", None))
    url_validation = _source_url_validation_summary(source)
    currentness = _source_currentness_summary(source)
    normalized_category = normalize_category(
        getattr(assertion, "normalized_category", None)
        or getattr(assertion, "rule_category", None)
    )

    return {
        "rule_key": getattr(assertion, "rule_key", None),
        "normalized_category": normalized_category,
        "display_category": normalized_category,
        "value_text": (
            str(
                getattr(assertion, "raw_excerpt", None)
                or citation_json.get("raw_excerpt")
                or getattr(assertion, "value_text", None)
                or ""
            ).strip()
            or None
        ),
        "citation": {
            "text": str(getattr(assertion, "source_citation", None) or "").strip() or None,
            "url": citation_json.get("url") or getattr(source, "url", None),
            "title": citation_json.get("title") or getattr(source, "title", None),
            "publisher": citation_json.get("publisher") or getattr(source, "publisher", None),
            "raw_excerpt": citation_json.get("raw_excerpt")
            or str(getattr(assertion, "raw_excerpt", None) or "").strip()
            or None,
        },
        "confidence": round(_safe_float(getattr(assertion, "confidence", 0.0)), 6),
        "citation_quality": round(_citation_quality_from_assertion(assertion), 6),
        "authority_level": str(
            getattr(source, "authority_tier", None)
            or getattr(assertion, "authority_tier", None)
            or "derived_or_inferred"
        ),
        "authority_rank": int(
            getattr(source, "authority_rank", 0)
            or getattr(assertion, "authority_rank", 0)
            or 0
        ),
        "authority_score": round(
            _safe_float(
                getattr(source, "authority_score", 0.0)
                or getattr(assertion, "authority_score", 0.0)
            ),
            6,
        ),
        "is_real": bool(currentness.get("is_real_source"))
        and bool(url_validation.get("trust_for_extraction")),
        "is_verified": bool(currentness.get("is_verified"))
        and str(getattr(assertion, "validation_state", "") or "").strip().lower() == "validated",
        "is_current": bool(currentness.get("is_current")),
        "current_reason": currentness.get("current_reason"),
        "last_verified_at": currentness.get("last_verified_at"),
        "source_id": int(getattr(source, "id", 0) or 0) if source is not None else None,
    }


# --- Step 4 additive patch v2: stronger citation/currentness normalization with PDF-aware context ---
def _citation_pages_from_payload(value):
    payload = _loads_dict(value)
    pages = payload.get("pages")
    if isinstance(pages, list):
        out = []
        for page in pages:
            try:
                out.append(int(page))
            except Exception:
                continue
        return out
    page = payload.get("page")
    try:
        return [int(page)] if page is not None else []
    except Exception:
        return []


def _citation_locator_text(payload: dict) -> str | None:
    raw = str(
        payload.get("locator")
        or payload.get("section")
        or payload.get("heading")
        or payload.get("anchor")
        or ""
    ).strip()
    return raw or None


def _citation_source_kind(source: PolicySource | None, citation_payload: dict) -> str:
    publication_type = str(
        getattr(source, "publication_type", None)
        or citation_payload.get("publication_type")
        or ""
    ).strip().lower()
    url = str(
        getattr(source, "url", None) or citation_payload.get("url") or ""
    ).strip().lower()

    if publication_type == "pdf" or url.endswith(".pdf"):
        return "pdf"
    if publication_type in {"api", "json", "json_api"}:
        return "api"
    return "html"


def _normalized_citation_with_pdf_context(
    assertion: PolicyAssertion,
    source: PolicySource | None,
) -> dict[str, object]:
    citation = _loads_dict(getattr(assertion, "citation_json", None))
    raw_excerpt = str(
        getattr(assertion, "raw_excerpt", None) or citation.get("raw_excerpt") or ""
    ).strip() or None
    pages = _citation_pages_from_payload(citation)
    source_kind = _citation_source_kind(source, citation)
    locator = _citation_locator_text(citation)
    url = str(citation.get("url") or getattr(source, "url", None) or "").strip() or None
    title = str(citation.get("title") or getattr(source, "title", None) or "").strip() or None
    publisher = (
        str(citation.get("publisher") or getattr(source, "publisher", None) or "").strip()
        or None
    )
    quality = _citation_quality_from_assertion(assertion)
    has_pinpoint = bool(locator or pages)

    return {
        "url": url,
        "title": title,
        "publisher": publisher,
        "publication_type": str(
            getattr(source, "publication_type", None)
            or citation.get("publication_type")
            or source_kind
        ),
        "source_kind": source_kind,
        "raw_excerpt": raw_excerpt,
        "pages": pages,
        "page_count": len(pages),
        "locator": locator,
        "pinpoint_citation": has_pinpoint,
        "citation_quality": round(float(quality), 6),
        "is_pdf_backed": source_kind == "pdf",
        "is_api_backed": source_kind == "api",
        "is_html_backed": source_kind == "html",
    }


def _confidence_with_currentness(
    assertion: PolicyAssertion,
    source: PolicySource | None,
) -> dict[str, object]:
    currentness = _source_currentness_summary(source)
    citation = _normalized_citation_with_pdf_context(assertion, source)
    confidence = _safe_float(getattr(assertion, "confidence", 0.0))
    validation_state = str(getattr(assertion, "validation_state", None) or "").strip().lower()

    effective = confidence
    if currentness.get("is_current"):
        effective += 0.05
    if citation.get("pinpoint_citation"):
        effective += 0.05
    if citation.get("is_pdf_backed") and citation.get("page_count"):
        effective += 0.03
    if validation_state == "validated":
        effective += 0.07

    return {
        "base_confidence": round(confidence, 6),
        "effective_confidence": round(min(effective, 1.0), 6),
        "is_current": bool(currentness.get("is_current")),
        "pinpoint_citation": bool(citation.get("pinpoint_citation")),
        "is_pdf_backed": bool(citation.get("is_pdf_backed")),
    }


# --- Step 4 additive patch v3: use uploaded NSPIRE PDF zip catalog as real evidence context ---
from pathlib import Path
import zipfile
import re as _step4_re

_STEP4_DEFAULT_ZIP_PATHS = [
    Path(os.getenv("NSPIRE_PDF_ZIP_PATH", "")).expanduser()
    if os.getenv("NSPIRE_PDF_ZIP_PATH")
    else None,
    Path("/mnt/data/pdfs(1).zip"),
]
_STEP4_DEFAULT_PDF_DIRS = [
    Path(os.getenv("NSPIRE_PDF_ROOT", "")).expanduser()
    if os.getenv("NSPIRE_PDF_ROOT")
    else None,
    Path("backend/data/pdfs"),
    Path("/app/backend/data/pdfs"),
    Path("/mnt/data/step4_pdf_catalog/pdfs"),
]


def _step4_iter_pdf_catalog_names() -> list[str]:
    names: list[str] = []
    seen: set[str] = set()

    for maybe_dir in _STEP4_DEFAULT_PDF_DIRS:
        if not maybe_dir:
            continue
        try:
            path = maybe_dir.resolve()
        except Exception:
            path = maybe_dir
        if path.exists() and path.is_dir():
            for pdf in path.rglob("*.pdf"):
                key = pdf.name.lower()
                if key in seen:
                    continue
                seen.add(key)
                names.append(pdf.name)

    if names:
        return sorted(names)

    for maybe_zip in _STEP4_DEFAULT_ZIP_PATHS:
        if not maybe_zip:
            continue
        if maybe_zip.exists() and maybe_zip.is_file():
            try:
                with zipfile.ZipFile(maybe_zip) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".pdf"):
                            base = Path(name).name
                            key = base.lower()
                            if key in seen:
                                continue
                            seen.add(key)
                            names.append(base)
            except Exception:
                continue

    return sorted(names)


def _step4_pdf_catalog_summary() -> dict[str, object]:
    names = _step4_iter_pdf_catalog_names()
    return {
        "pdf_count": len(names),
        "pdf_names": names,
    }


def _step4_tokenize(value: object) -> list[str]:
    text = str(value or "").strip().lower()
    text = text.replace("&", " and ")
    tokens = [
        tok
        for tok in _step4_re.split(r"[^a-z0-9]+", text)
        if tok and tok not in {"nspire", "standard", "pdf", "and", "the", "of"}
    ]
    return tokens


def _step4_match_pdf_catalog(
    *,
    assertion: PolicyAssertion,
    source: PolicySource | None,
    citation: dict[str, object] | None = None,
) -> dict[str, object]:
    catalog = _step4_iter_pdf_catalog_names()
    if not catalog:
        return {
            "matched": False,
            "matched_pdf_name": None,
            "matched_pdf_path": None,
            "catalog_size": 0,
            "match_score": 0.0,
        }

    fields = [
        getattr(assertion, "rule_key", None),
        getattr(assertion, "normalized_category", None),
        getattr(assertion, "rule_category", None),
        getattr(assertion, "source_citation", None),
        getattr(assertion, "raw_excerpt", None),
        getattr(source, "title", None) if source is not None else None,
        getattr(source, "publisher", None) if source is not None else None,
        (citation or {}).get("title") if isinstance(citation, dict) else None,
        (citation or {}).get("locator") if isinstance(citation, dict) else None,
    ]

    wanted = []
    for field in fields:
        wanted.extend(_step4_tokenize(field))
    wanted = sorted(set(tok for tok in wanted if len(tok) >= 3))

    best_name = None
    best_score = 0.0
    for name in catalog:
        low = name.lower()
        score = 0.0
        for tok in wanted:
            if tok in low:
                score += 1.0
        if score > best_score:
            best_score = score
            best_name = name

    matched = best_name is not None and best_score >= 1.0
    matched_path = None
    if matched:
        for maybe_dir in _STEP4_DEFAULT_PDF_DIRS:
            if not maybe_dir:
                continue
            try:
                path = maybe_dir.resolve()
            except Exception:
                path = maybe_dir
            candidate = path / str(best_name)
            if candidate.exists():
                matched_path = str(candidate)
                break

    return {
        "matched": matched,
        "matched_pdf_name": best_name,
        "matched_pdf_path": matched_path,
        "catalog_size": len(catalog),
        "match_score": round(float(best_score), 6),
    }


def _assertion_normalization_payload(
    assertion: PolicyAssertion,
    source: PolicySource | None,
) -> dict[str, object]:
    payload = dict(_assertion_normalization_payload_base(assertion, source))

    citation = _normalized_citation_with_pdf_context(assertion, source)
    confidence_summary = _confidence_with_currentness(assertion, source)
    pdf_match = _step4_match_pdf_catalog(assertion=assertion, source=source, citation=citation)

    payload["citation"] = citation
    payload["citation_pages"] = citation.get("pages")
    payload["citation_locator"] = citation.get("locator")
    payload["citation_has_pinpoint"] = citation.get("pinpoint_citation")
    payload["source_kind"] = citation.get("source_kind")
    payload["publication_type"] = citation.get("publication_type")
    payload["is_pdf_backed"] = bool(citation.get("is_pdf_backed") or pdf_match.get("matched"))
    payload["is_api_backed"] = citation.get("is_api_backed")
    payload["is_html_backed"] = citation.get("is_html_backed")
    payload["effective_confidence"] = confidence_summary.get("effective_confidence")
    payload["base_confidence"] = confidence_summary.get("base_confidence")

    payload["evidence_strength"] = (
        "strong"
        if (
            payload.get("is_verified")
            and payload.get("is_current")
            and citation.get("pinpoint_citation")
        )
        else "moderate"
        if (payload.get("is_real") and citation.get("citation_quality", 0.0) >= 0.6)
        else "weak"
    )

    payload["pdf_catalog_match"] = pdf_match
    payload["matched_pdf_name"] = pdf_match.get("matched_pdf_name")
    payload["matched_pdf_path"] = pdf_match.get("matched_pdf_path")
    payload["pdf_catalog_match_score"] = pdf_match.get("match_score")
    payload["pdf_catalog_size"] = pdf_match.get("catalog_size")

    citation["matched_pdf_name"] = pdf_match.get("matched_pdf_name")
    citation["matched_pdf_path"] = pdf_match.get("matched_pdf_path")
    citation["pdf_catalog_match_score"] = pdf_match.get("match_score")
    citation["pdf_catalog_size"] = pdf_match.get("catalog_size")
    citation["is_pdf_backed"] = bool(citation.get("is_pdf_backed") or pdf_match.get("matched"))
    if pdf_match.get("matched") and not citation.get("pages"):
        citation["catalog_backed"] = True

    payload["citation"] = citation

    if pdf_match.get("matched") and payload.get("evidence_strength") == "weak" and payload.get("is_real"):
        payload["evidence_strength"] = "moderate"

    return payload


def validate_assertion(*, assertion: PolicyAssertion, source: PolicySource | None) -> dict[str, object]:
    payload = dict(_BASE_VALIDATE_ASSERTION(assertion=assertion, source=source))
    payload = _apply_validation_softening(payload=payload, assertion=assertion, source=source)

    normalized = _assertion_normalization_payload(assertion, source)
    payload["normalized_rule"] = normalized
    payload["citation_payload"] = normalized.get("citation")
    payload["authority_level"] = normalized.get("authority_level")
    payload["authority_rank"] = normalized.get("authority_rank")
    payload["authority_score"] = normalized.get("authority_score")
    payload["is_real"] = normalized.get("is_real")
    payload["is_verified"] = normalized.get("is_verified")
    payload["is_current"] = normalized.get("is_current")
    payload["current_reason"] = normalized.get("current_reason")
    payload["last_verified_at"] = normalized.get("last_verified_at")
    payload["citation_pages"] = normalized.get("citation_pages")
    payload["citation_locator"] = normalized.get("citation_locator")
    payload["citation_has_pinpoint"] = normalized.get("citation_has_pinpoint")
    payload["source_kind"] = normalized.get("source_kind")
    payload["publication_type"] = normalized.get("publication_type")
    payload["is_pdf_backed"] = normalized.get("is_pdf_backed")
    payload["effective_confidence"] = normalized.get("effective_confidence")
    payload["evidence_strength"] = normalized.get("evidence_strength")
    return payload


def validate_market_assertions(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    source_id: int | None = None,
) -> dict[str, object]:
    result = dict(
        _BASE_VALIDATE_MARKET_ASSERTIONS(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            source_id=source_id,
        )
    )

    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    stmt = select(PolicyAssertion).where(PolicyAssertion.state == st)
    if hasattr(PolicyAssertion, "org_id"):
        if org_id is None:
            stmt = stmt.where(PolicyAssertion.org_id.is_(None))
        else:
            stmt = stmt.where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))
    if cnty is None:
        stmt = stmt.where(PolicyAssertion.county.is_(None))
    else:
        stmt = stmt.where(PolicyAssertion.county == cnty)
    if cty is None:
        stmt = stmt.where(PolicyAssertion.city.is_(None))
    else:
        stmt = stmt.where(PolicyAssertion.city == cty)
    if hasattr(PolicyAssertion, "pha_name"):
        if pha is None:
            stmt = stmt.where(or_(PolicyAssertion.pha_name.is_(None), PolicyAssertion.pha_name == ""))
        else:
            stmt = stmt.where(PolicyAssertion.pha_name == pha)
    if source_id is not None:
        stmt = stmt.where(PolicyAssertion.source_id == int(source_id))

    rows = list(db.scalars(stmt).all())

    normalized_rules = []
    real_count = 0
    verified_count = 0
    current_count = 0

    for row in rows:
        source = db.get(PolicySource, int(row.source_id)) if getattr(row, "source_id", None) is not None else None
        normalized = _assertion_normalization_payload(row, source)
        normalized["validation_state"] = getattr(row, "validation_state", None)
        normalized["trust_state"] = getattr(row, "trust_state", None)
        normalized_rules.append(normalized)
        real_count += 1 if normalized.get("is_real") else 0
        verified_count += 1 if normalized.get("is_verified") else 0
        current_count += 1 if normalized.get("is_current") else 0

    result["normalized_rules"] = normalized_rules
    result["real_rule_count"] = real_count
    result["verified_current_rule_count"] = sum(
        1 for row in normalized_rules if row.get("is_verified") and row.get("is_current")
    )
    result["verified_rule_count_step4"] = verified_count
    result["current_rule_count"] = current_count
    result["pdf_backed_rule_count"] = sum(1 for row in normalized_rules if row.get("is_pdf_backed"))
    result["pinpoint_citation_rule_count"] = sum(
        1 for row in normalized_rules if row.get("citation_has_pinpoint")
    )
    result["strong_evidence_rule_count"] = sum(
        1 for row in normalized_rules if row.get("evidence_strength") == "strong"
    )
    result["normalization_summary"] = {
        "real": int(result.get("real_rule_count") or 0),
        "verified": int(result.get("verified_rule_count_step4") or 0),
        "current": int(result.get("current_rule_count") or 0),
        "verified_current": int(result.get("verified_current_rule_count") or 0),
        "pdf_backed": int(result.get("pdf_backed_rule_count") or 0),
        "pinpoint_citation": int(result.get("pinpoint_citation_rule_count") or 0),
        "strong_evidence": int(result.get("strong_evidence_rule_count") or 0),
    }

    matched_names = sorted(
        {str(row.get("matched_pdf_name")) for row in normalized_rules if row.get("matched_pdf_name")}
    )
    result["pdf_catalog_summary"] = {
        **_step4_pdf_catalog_summary(),
        "matched_pdf_count": len(matched_names),
        "matched_pdf_names": matched_names,
    }
    result["pdf_catalog_matched_rule_count"] = sum(
        1 for row in normalized_rules if row.get("matched_pdf_name")
    )

    return result


# --- tier-two evidence-first final overrides ---


def _tier2_evidence_family_for_source(source: PolicySource | None) -> dict[str, object]:
    if source is None:
        return {
            "family": "unknown",
            "is_primary_evidence": False,
            "freshness_role": "unknown",
            "truth_role": "unknown",
        }

    source_type = str(getattr(source, "source_type", "") or "").strip().lower()
    publication_type = str(getattr(source, "publication_type", "") or "").strip().lower()
    authority_use_type = str(getattr(source, "authority_use_type", "") or "").strip().lower()
    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()

    family = "crawl"
    if source_type in {"dataset", "artifact", "manual", "catalog", "program", "feed", "registry", "repo_artifact", "api"}:
        family = source_type
    elif publication_type in {"pdf", "json", "json_api", "dataset"}:
        family = publication_type
    elif authority_use_type in {"binding", "supporting"} and authority_tier in {"authoritative_official", "approved_official_supporting"}:
        family = "official_publication"

    primary = family in {
        "dataset",
        "artifact",
        "manual",
        "catalog",
        "program",
        "feed",
        "registry",
        "repo_artifact",
        "api",
        "pdf",
        "json",
        "json_api",
        "official_publication",
    }
    freshness_role = "support_only" if family == "crawl" else "primary_and_refreshable"
    truth_role = "primary_evidence" if primary else "supporting_signal"

    return {
        "family": family,
        "is_primary_evidence": primary,
        "freshness_role": freshness_role,
        "truth_role": truth_role,
        "authority_use_type": authority_use_type or None,
        "authority_tier": authority_tier or None,
    }


_tier2_original_validate_assertion = validate_assertion
_tier2_original_validate_market_assertions = validate_market_assertions


def validate_assertion(*, assertion: PolicyAssertion, source: PolicySource | None) -> dict[str, object]:
    result = dict(_tier2_original_validate_assertion(assertion=assertion, source=source))
    evidence_family = _tier2_evidence_family_for_source(source)

    validation_state = str(result.get("validation_state") or "").strip().lower()
    source_support = dict(result.get("source_support") or {})
    url_validation = dict(source_support.get("url_validation") or {})
    rejection_reasons = list(url_validation.get("rejection_reasons") or [])

    freshness_only_failure = (
        bool(evidence_family.get("is_primary_evidence"))
        and any(str(reason).strip() in {"fetch_failed", "error", "blocked", "refresh_blocked", "blocked_or_antibot"} or str(reason).startswith("http_status_")
                for reason in rejection_reasons)
    )
    blocking_evidence_gap = bool(
        result.get("critical_binding_required")
        and not bool(result.get("binding_sufficient"))
        and validation_state in {
            VALIDATION_STATE_UNSUPPORTED,
            VALIDATION_STATE_AMBIGUOUS,
            VALIDATION_STATE_CONFLICTING,
        }
    )
    degraded_review_required = bool(
        freshness_only_failure
        or validation_state in {
            VALIDATION_STATE_WEAK,
            VALIDATION_STATE_AMBIGUOUS,
            VALIDATION_STATE_CONFLICTING,
        }
    )

    result["evidence_family"] = evidence_family
    result["freshness_is_support_only"] = bool(evidence_family.get("is_primary_evidence"))
    result["freshness_only_failure"] = freshness_only_failure
    result["blocking_evidence_gap"] = blocking_evidence_gap
    result["degraded_review_required"] = degraded_review_required
    result["truth_model"] = {
        "mode": "evidence_first",
        "freshness_role": "support_only" if bool(evidence_family.get("is_primary_evidence")) else "mixed",
        "crawler_role": "discovery_and_refresh_only",
    }

    if freshness_only_failure and not blocking_evidence_gap:
        result["reliance_state"] = TRUST_STATE_NEEDS_REVIEW
        result["reliance_reason"] = "primary_evidence_present_but_live_refresh_failed"
    elif blocking_evidence_gap:
        result["reliance_state"] = TRUST_STATE_DOWNGRADED
        result["reliance_reason"] = "binding_or_authority_gap"
    elif degraded_review_required:
        result["reliance_state"] = TRUST_STATE_NEEDS_REVIEW
        result["reliance_reason"] = "review_required_before_reliance"
    else:
        result["reliance_state"] = TRUST_STATE_TRUSTED
        result["reliance_reason"] = "validated_evidence_ready"

    return result


def validate_market_assertions(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    source_id: int | None = None,
) -> dict[str, object]:
    payload = dict(
        _tier2_original_validate_market_assertions(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            source_id=source_id,
        )
    )

    validations = list(payload.get("validations") or payload.get("rows") or [])
    blocking_categories: set[str] = set()
    degraded_categories: set[str] = set()
    freshness_only_categories: set[str] = set()
    evidence_families: dict[str, int] = {}

    for row in validations:
        if not isinstance(row, dict):
            continue
        category = str(row.get("normalized_category") or row.get("rule_category") or "").strip().lower()
        family_payload = dict(row.get("evidence_family") or {})
        family = str(family_payload.get("family") or "unknown").strip().lower()
        evidence_families[family] = int(evidence_families.get(family, 0)) + 1
        if row.get("blocking_evidence_gap") and category:
            blocking_categories.add(category)
        if row.get("degraded_review_required") and category:
            degraded_categories.add(category)
        if row.get("freshness_only_failure") and category:
            freshness_only_categories.add(category)

    payload["truth_model"] = {
        "mode": "evidence_first",
        "crawler_role": "discovery_and_refresh_only",
        "freshness_role": "support_only",
    }
    payload["evidence_family_counts"] = dict(sorted(evidence_families.items()))
    payload["blocking_categories"] = sorted(blocking_categories)
    payload["degraded_categories"] = sorted(degraded_categories)
    payload["freshness_signal_only_categories"] = sorted(freshness_only_categories)
    payload["review_required"] = bool(payload.get("review_required")) or bool(degraded_categories)
    payload["safe_to_rely_on"] = bool(payload.get("safe_to_rely_on", True)) and not bool(blocking_categories)
    return payload
