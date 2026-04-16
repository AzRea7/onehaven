from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import expected_rule_universe_for_scope, normalize_category
from app.policy_models import PolicyAssertion, PolicySource, PolicySourceInventory
from app.services.policy_change_detection_service import compute_next_retry_due, determine_validation_refresh_state


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


def _source_requires_revalidation(source: PolicySource | None) -> bool:
    return bool(getattr(source, "revalidation_required", False)) if source is not None else False


def _validation_support_summary(assertion: PolicyAssertion, source: PolicySource | None) -> dict[str, object]:
    authority_policy = _authority_policy_for_source(source)
    category_requirement = _category_authority_requirement(assertion)
    return {
        "authority_policy": authority_policy,
        "category_requirement": category_requirement,
        "critical_binding_required": bool(category_requirement.get("critical_binding_required")),
        "binding_sufficient": bool(authority_policy.get("binding_sufficient")),
        "supporting_only": bool(authority_policy.get("supporting_only")),
        "unusable": not bool(authority_policy.get("usable", True)),
        "requires_revalidation": _source_requires_revalidation(source),
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
    category_requirement = dict(support.get("category_requirement") or {})
    required_tier = str(category_requirement.get("required_tier") or "")
    critical_binding_required = bool(support.get("critical_binding_required"))
    binding_sufficient = bool(support.get("binding_sufficient"))
    supporting_only = bool(support.get("supporting_only"))
    unusable = bool(support.get("unusable"))
    source_requires_revalidation = bool(support.get("requires_revalidation"))

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
            if bool(getattr(row, "blocking", False)) and str(getattr(row, "validation_state", "")) != VALIDATION_STATE_VALIDATED:
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
    }
