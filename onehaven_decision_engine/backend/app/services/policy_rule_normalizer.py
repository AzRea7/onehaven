# backend/app/services/policy_rule_normalizer.py
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any, Optional

from app.domain.jurisdiction_categories import normalize_category


@dataclass(frozen=True)
class NormalizedRuleCandidate:
    rule_key: str
    rule_family: str
    rule_category: str | None
    source_level: str
    property_type: str | None
    required: bool
    blocking: bool
    confidence: float
    governance_state: str
    rule_status: str
    normalized_version: str
    version_group: str
    value_json: dict[str, Any]
    source_citation: str | None
    raw_excerpt: str | None
    evidence_state: str
    fingerprint: str
    citation_quality: float
    category_mapping_source: str
    normalized_rule_key: str
    rule_key_confidence: float
    conflict_hints: list[str]
    validation_hint: str | None


def _clean(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


def _clean_excerpt(text: Optional[str]) -> str | None:
    value = str(text or "").strip()
    if not value:
        return None
    return re.sub(r"\s+", " ", value)[:4000]


def _dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return str(value)


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _first_url(text: str) -> str | None:
    match = re.search(r"https?://[^\s)>\]]+", text or "")
    return match.group(0) if match else None


def _bounded_confidence(value: Any, default: float = 0.35) -> float:
    try:
        f = float(value)
    except Exception:
        f = default
    if f < 0:
        return 0.0
    if f > 1:
        return 1.0
    return f


def _norm_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = _clean(str(value or ""))
    if text in {"true", "1", "yes", "y", "required", "must"}:
        return True
    if text in {"false", "0", "no", "n", "optional", "not_required"}:
        return False
    return default


def _norm_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _norm_lower(value: Any) -> str | None:
    text = _norm_text(value)
    return text.lower() if text else None


def _category_for(rule_key: str) -> str | None:
    mapping = {
        "federal_hcv_regulations_anchor": "section8",
        "federal_nspire_anchor": "inspection",
        "federal_notice_anchor": "section8",
        "mi_statute_anchor": "safety",
        "mshda_program_anchor": "section8",
        "pha_admin_plan_anchor": "section8",
        "pha_administrator_changed": "section8",
        "pha_landlord_packet_required": "section8",
        "hap_contract_and_tenancy_addendum_required": "section8",
        "landlord_payment_timing_reference": "section8",
        "rental_registration_required": "registration",
        "inspection_program_exists": "inspection",
        "certificate_required_before_occupancy": "occupancy",
        "local_agent_required": "registration",
        "owner_po_box_allowed": "registration",
        "all_fees_must_be_paid": "fees",
        "property_maintenance_enforcement_anchor": "safety",
        "building_safety_division_anchor": "safety",
        "building_division_anchor": "permits",
        "lead_paint_affidavit_required": "lead",
        "lead_clearance_required": "lead",
        "lead_inspection_required": "lead",
        "rental_license_required": "registration",
        "certificate_of_compliance_required": "occupancy",
        "certificate_of_occupancy_required": "occupancy",
        "fire_safety_inspection_required": "inspection",
        "utilities_required_before_inspection": "inspection",
        "smoke_detector_required": "inspection",
        "carbon_monoxide_detector_required": "inspection",
    }
    return mapping.get(rule_key)


def _family_for(rule_key: str) -> str:
    mapping = {
        "rental_registration_required": "registration",
        "rental_license_required": "registration",
        "inspection_program_exists": "inspection",
        "fire_safety_inspection_required": "inspection",
        "certificate_required_before_occupancy": "occupancy",
        "certificate_of_occupancy_required": "occupancy",
        "certificate_of_compliance_required": "occupancy",
        "lead_paint_affidavit_required": "lead",
        "lead_clearance_required": "lead",
        "lead_inspection_required": "lead",
    }
    return mapping.get(rule_key, rule_key)


def _rule_key_from_text(text: str, hint: Optional[str] = None) -> tuple[str | None, float]:
    whole = f"{_clean(hint)} {_clean(text)}".strip()

    patterns: list[tuple[str, str, float]] = [
        (r"(rental|property).*(registration|register)", "rental_registration_required", 0.92),
        (r"(rental|property).*(license|licence)", "rental_license_required", 0.92),
        (r"(inspection|inspected).*(required|must)|inspection program", "inspection_program_exists", 0.90),
        (r"(fire).*(inspection|required)", "fire_safety_inspection_required", 0.85),
        (r"certificate.*occupancy|occupancy permit", "certificate_of_occupancy_required", 0.90),
        (r"certificate.*compliance", "certificate_of_compliance_required", 0.90),
        (r"certificate.*before occupancy", "certificate_required_before_occupancy", 0.96),
        (r"local agent|local representative|responsible agent", "local_agent_required", 0.88),
        (r"po box.*not allowed|physical address required", "owner_po_box_allowed", 0.80),
        (r"all fees.*paid|fees.*must be paid", "all_fees_must_be_paid", 0.84),
        (r"lead.*affidavit", "lead_paint_affidavit_required", 0.86),
        (r"lead.*clearance", "lead_clearance_required", 0.90),
        (r"lead.*inspection", "lead_inspection_required", 0.84),
        (r"smoke detector", "smoke_detector_required", 0.72),
        (r"carbon monoxide", "carbon_monoxide_detector_required", 0.72),
        (r"utility|utilities.*before inspection", "utilities_required_before_inspection", 0.70),
        (r"nspire", "federal_nspire_anchor", 0.94),
        (r"(housing choice voucher|hcv|24 cfr 982|voucher regulations)", "federal_hcv_regulations_anchor", 0.95),
        (r"\bnotice\b", "federal_notice_anchor", 0.55),
        (r"mshda", "mshda_program_anchor", 0.90),
        (r"admin(istrative)? plan", "pha_admin_plan_anchor", 0.92),
        (r"landlord packet", "pha_landlord_packet_required", 0.88),
        (r"hap contract|tenancy addendum", "hap_contract_and_tenancy_addendum_required", 0.90),
        (r"payment timing|landlord payment", "landlord_payment_timing_reference", 0.82),
        (r"building safety", "building_safety_division_anchor", 0.84),
        (r"property maintenance", "property_maintenance_enforcement_anchor", 0.84),
        (r"building division", "building_division_anchor", 0.84),
        (r"mcl|michigan legislature|public act|compiled laws", "mi_statute_anchor", 0.94),
    ]
    for pattern, key, confidence in patterns:
        if re.search(pattern, whole):
            return key, confidence
    return None, 0.0


def _source_level_from_candidate(candidate: dict[str, Any]) -> str:
    explicit = _norm_lower(candidate.get("source_level"))
    if explicit in {"federal", "state", "county", "city", "program", "property", "local"}:
        return explicit
    source_type = _norm_lower(candidate.get("source_type"))
    if source_type in {"federal", "state", "county", "city", "program"}:
        return source_type
    if candidate.get("pha_name"):
        return "program"
    if candidate.get("city"):
        return "city"
    if candidate.get("county"):
        return "county"
    if _norm_lower(candidate.get("state")):
        return "state"
    return "local"


def _property_type_from_candidate(candidate: dict[str, Any]) -> str | None:
    value = _norm_lower(candidate.get("property_type"))
    if not value:
        return None
    aliases = {
        "single family": "single_family",
        "single-family": "single_family",
        "single_family": "single_family",
        "multifamily": "multi_family",
        "multi family": "multi_family",
        "multi-family": "multi_family",
        "multi_family": "multi_family",
        "duplex": "multi_family",
    }
    return aliases.get(value, value.replace(" ", "_"))


def _required_for(rule_key: str, candidate: dict[str, Any]) -> bool:
    if "required" in candidate:
        return _norm_bool(candidate.get("required"), default=True)
    if "value" in candidate and isinstance(candidate.get("value"), str):
        return _norm_bool(candidate.get("value"), default=True)
    return True


def _blocking_for(rule_key: str, candidate: dict[str, Any]) -> bool:
    if "blocking" in candidate:
        return _norm_bool(candidate.get("blocking"), default=False)
    blocking_defaults = {
        "rental_registration_required",
        "rental_license_required",
        "inspection_program_exists",
        "fire_safety_inspection_required",
        "certificate_required_before_occupancy",
        "certificate_of_occupancy_required",
        "certificate_of_compliance_required",
        "lead_clearance_required",
        "pha_landlord_packet_required",
        "hap_contract_and_tenancy_addendum_required",
    }
    return rule_key in blocking_defaults


def _governance_state_for(candidate: dict[str, Any], confidence: float) -> str:
    state = _norm_lower(candidate.get("governance_state"))
    if state in {"draft", "approved", "active", "replaced"}:
        return state
    if confidence >= 0.9 and _norm_bool(candidate.get("auto_activate"), default=False):
        return "active"
    if confidence >= 0.75 and _norm_bool(candidate.get("auto_approve"), default=False):
        return "approved"
    return "draft"


def _rule_status_for(candidate: dict[str, Any], governance_state: str, confidence: float) -> str:
    rule_status = _norm_lower(candidate.get("rule_status"))
    if rule_status in {
        "candidate",
        "draft",
        "approved",
        "active",
        "replaced",
        "superseded",
        "verified",
        "stale",
        "conflicting",
        "partial",
        "inferred",
    }:
        return rule_status
    if governance_state == "active":
        return "active"
    if governance_state == "approved":
        return "approved"
    if confidence >= 0.85:
        return "verified"
    if confidence >= 0.45:
        return "inferred"
    return "candidate"


def _citation_quality(candidate: dict[str, Any], raw_excerpt: str | None) -> float:
    explicit_citation = _norm_text(candidate.get("source_citation"))
    url = _norm_text(candidate.get("url")) or _first_url(raw_excerpt or "")
    title = _norm_text(candidate.get("title"))
    publisher = _norm_text(candidate.get("publisher"))
    score = 0.0
    if explicit_citation:
        score += 0.45
    if url:
        score += 0.25
    if title:
        score += 0.15
    if publisher:
        score += 0.10
    if raw_excerpt and len(raw_excerpt) >= 25:
        score += 0.05
    return round(min(score, 1.0), 6)


def _category_mapping_source(candidate: dict[str, Any], rule_key: str, normalized_category: str | None) -> str:
    if normalized_category is None:
        return "unmapped"
    if _norm_text(candidate.get("rule_category")) or _norm_text(candidate.get("category")):
        return "explicit_candidate_category"
    if _category_for(rule_key):
        return "rule_key_map"
    return "heuristic"


def _conflict_hints(candidate: dict[str, Any], body: str, normalized_category: str | None) -> list[str]:
    hints: list[str] = []
    combined = _clean(body)
    if re.search(r"\b(may not|not required|optional)\b", combined) and re.search(r"\b(must|required|shall)\b", combined):
        hints.append("mixed_requirement_language")
    if re.search(r"\b(unless|except|however|subject to)\b", combined):
        hints.append("conditional_language")
    if normalized_category is None:
        hints.append("category_unmapped")
    if not (_norm_text(candidate.get("source_citation")) or _norm_text(candidate.get("url"))):
        hints.append("missing_direct_citation")
    return hints


def _evidence_state_for(candidate: dict[str, Any], confidence: float, citation_quality: float, conflict_hints: list[str]) -> str:
    explicit = _norm_lower(candidate.get("evidence_state"))
    if explicit in {"confirmed", "inferred", "unknown", "stale", "conflicting"}:
        return explicit
    if "mixed_requirement_language" in conflict_hints:
        return "conflicting"
    if confidence >= 0.85 and citation_quality >= 0.70:
        return "confirmed"
    if confidence >= 0.45 and citation_quality >= 0.30:
        return "inferred"
    return "unknown"


def _normalized_version_for(candidate: dict[str, Any]) -> str:
    value = _norm_text(candidate.get("normalized_version"))
    return value or "v2"


def _citation_text(candidate: dict[str, Any], raw_excerpt: str | None = None) -> str | None:
    explicit = _norm_text(candidate.get("source_citation"))
    if explicit:
        return explicit
    title = _norm_text(candidate.get("title"))
    publisher = _norm_text(candidate.get("publisher"))
    url = _norm_text(candidate.get("url")) or _first_url(raw_excerpt or "")
    pieces = [p for p in [publisher, title, url] if p]
    return " | ".join(pieces) if pieces else None


def _base_value_json(candidate: dict[str, Any], *, rule_key: str, source_level: str) -> dict[str, Any]:
    payload = _json_dict(candidate.get("value_json"))
    if not payload:
        payload = {}

    for key in [
        "value",
        "operator",
        "amount",
        "fee",
        "frequency",
        "deadline_days",
        "form_name",
        "authority_name",
        "program_type",
        "property_type",
        "county",
        "city",
        "state",
        "effective_date",
        "expires_at",
        "effective_at",
        "notes",
        "status",
        "condition",
        "summary",
    ]:
        if candidate.get(key) is not None and key not in payload:
            payload[key] = candidate.get(key)

    payload.setdefault("rule_key", rule_key)
    payload.setdefault("source_level", source_level)
    return payload


def candidate_provenance_payload(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_id": candidate.get("source_id"),
        "source_version_id": candidate.get("source_version_id"),
        "source_name": candidate.get("source_name"),
        "source_type": candidate.get("source_type"),
        "publisher": candidate.get("publisher"),
        "title": candidate.get("title"),
        "url": candidate.get("url"),
        "jurisdiction_slug": candidate.get("jurisdiction_slug"),
        "state": candidate.get("state"),
        "county": candidate.get("county"),
        "city": candidate.get("city"),
        "pha_name": candidate.get("pha_name"),
        "program_type": candidate.get("program_type"),
        "effective_date": candidate.get("effective_date") or candidate.get("effective_at"),
        "expires_at": candidate.get("expires_at"),
    }


def candidate_citation_payload(
    candidate: dict[str, Any],
    normalized: NormalizedRuleCandidate | None = None,
) -> dict[str, Any]:
    raw_excerpt = normalized.raw_excerpt if normalized else _clean_excerpt(candidate.get("raw_excerpt"))
    return {
        "citation_text": normalized.source_citation if normalized else _citation_text(candidate, raw_excerpt=raw_excerpt),
        "url": candidate.get("url"),
        "publisher": candidate.get("publisher"),
        "title": candidate.get("title"),
        "raw_excerpt": raw_excerpt,
        "source_id": candidate.get("source_id"),
        "source_version_id": candidate.get("source_version_id"),
        "citation_quality": normalized.citation_quality if normalized else _citation_quality(candidate, raw_excerpt),
        "category_mapping_source": normalized.category_mapping_source if normalized else None,
        "conflict_hints": list(normalized.conflict_hints) if normalized else [],
        "normalized_rule_key": normalized.normalized_rule_key if normalized else _norm_text(candidate.get("rule_key")),
        "rule_key_confidence": normalized.rule_key_confidence if normalized else None,
    }


def normalize_rule_candidate(candidate: dict[str, Any]) -> NormalizedRuleCandidate | None:
    if not isinstance(candidate, dict):
        return None

    raw_excerpt = _clean_excerpt(candidate.get("raw_excerpt") or candidate.get("excerpt") or candidate.get("text"))
    hint = _norm_text(candidate.get("rule_key")) or _norm_text(candidate.get("hint")) or _norm_text(candidate.get("title"))
    body = " ".join(
        p
        for p in [
            _norm_text(candidate.get("title")),
            _norm_text(candidate.get("description")),
            _norm_text(candidate.get("text")),
            raw_excerpt,
        ]
        if p
    )

    explicit_rule_key = _norm_text(candidate.get("rule_key"))
    if explicit_rule_key:
        rule_key = explicit_rule_key.strip().lower()
        rule_key_confidence = 1.0
    else:
        rule_key, rule_key_confidence = _rule_key_from_text(body, hint=hint)

    if not rule_key:
        return None

    source_level = _source_level_from_candidate(candidate)
    property_type = _property_type_from_candidate(candidate)

    base_confidence = _bounded_confidence(candidate.get("confidence"), default=0.35)
    if not explicit_rule_key:
        base_confidence = max(base_confidence, rule_key_confidence)

    raw_category_guess = (
        _norm_text(candidate.get("rule_category"))
        or _norm_text(candidate.get("category"))
        or _category_for(rule_key)
    )
    normalized_category = normalize_category(raw_category_guess) if raw_category_guess else None
    category_mapping_source = _category_mapping_source(candidate, rule_key, normalized_category)

    citation_quality = _citation_quality(candidate, raw_excerpt)
    conflict_hints = _conflict_hints(candidate, body, normalized_category)

    confidence = base_confidence
    confidence = (confidence * 0.65) + (citation_quality * 0.20) + (rule_key_confidence * 0.15)
    if "missing_direct_citation" in conflict_hints:
        confidence *= 0.80
    if "category_unmapped" in conflict_hints:
        confidence *= 0.75
    if "mixed_requirement_language" in conflict_hints:
        confidence *= 0.70
    confidence = round(max(0.0, min(confidence, 1.0)), 6)

    required = _required_for(rule_key, candidate)
    blocking = _blocking_for(rule_key, candidate)
    governance_state = _governance_state_for(candidate, confidence)
    rule_status = _rule_status_for(candidate, governance_state, confidence)
    evidence_state = _evidence_state_for(candidate, confidence, citation_quality, conflict_hints)
    normalized_version = _normalized_version_for(candidate)
    rule_family = _family_for(rule_key)
    version_group = (
        _norm_text(candidate.get("version_group"))
        or _norm_text(candidate.get("jurisdiction_slug"))
        or _norm_text(candidate.get("pha_name"))
        or "global"
    )
    version_group = f"{version_group}:{rule_key}:{source_level}"

    value_json = _base_value_json(candidate, rule_key=rule_key, source_level=source_level)
    value_json["normalized_rule_key"] = rule_key
    value_json["normalized_category"] = normalized_category
    value_json["citation_quality"] = citation_quality
    value_json["category_mapping_source"] = category_mapping_source
    value_json["conflict_hints"] = list(conflict_hints)

    source_citation = _citation_text(candidate, raw_excerpt=raw_excerpt)

    payload = {
        "rule_key": rule_key,
        "rule_family": rule_family,
        "rule_category": normalized_category,
        "source_level": source_level,
        "property_type": property_type,
        "required": required,
        "blocking": blocking,
        "confidence": confidence,
        "governance_state": governance_state,
        "rule_status": rule_status,
        "normalized_version": normalized_version,
        "version_group": version_group,
        "value_json": value_json,
        "source_citation": source_citation,
        "raw_excerpt": raw_excerpt,
        "evidence_state": evidence_state,
        "citation_quality": citation_quality,
        "category_mapping_source": category_mapping_source,
        "normalized_rule_key": rule_key,
        "rule_key_confidence": round(rule_key_confidence, 6),
        "conflict_hints": list(conflict_hints),
        "validation_hint": "strong" if confidence >= 0.80 and citation_quality >= 0.70 else ("moderate" if confidence >= 0.55 and citation_quality >= 0.45 else "weak"),
    }
    fingerprint = sha256(_dumps(payload).encode("utf-8")).hexdigest()

    return NormalizedRuleCandidate(
        rule_key=rule_key,
        rule_family=rule_family,
        rule_category=normalized_category,
        source_level=source_level,
        property_type=property_type,
        required=required,
        blocking=blocking,
        confidence=confidence,
        governance_state=governance_state,
        rule_status=rule_status,
        normalized_version=normalized_version,
        version_group=version_group,
        value_json=value_json,
        source_citation=source_citation,
        raw_excerpt=raw_excerpt,
        evidence_state=evidence_state,
        fingerprint=fingerprint,
        citation_quality=citation_quality,
        category_mapping_source=category_mapping_source,
        normalized_rule_key=rule_key,
        rule_key_confidence=round(rule_key_confidence, 6),
        conflict_hints=list(conflict_hints),
        validation_hint=("strong" if confidence >= 0.80 and citation_quality >= 0.70 else ("moderate" if confidence >= 0.55 and citation_quality >= 0.45 else "weak")),
    )


def candidate_to_update_dict(candidate: NormalizedRuleCandidate, raw_candidate: dict[str, Any] | None = None) -> dict[str, Any]:
    raw_candidate = raw_candidate or {}
    citation_payload = candidate_citation_payload(raw_candidate, normalized=candidate)
    provenance_payload = candidate_provenance_payload(raw_candidate)

    coverage_status = "candidate"
    if candidate.evidence_state == "conflicting":
        coverage_status = "conflicting"
    elif candidate.evidence_state == "confirmed" and candidate.citation_quality >= 0.70 and candidate.confidence >= 0.80:
        coverage_status = "covered"
    elif candidate.evidence_state == "inferred":
        coverage_status = "inferred"
    elif candidate.confidence >= 0.45:
        coverage_status = "partial"

    return {
        "rule_key": candidate.rule_key,
        "rule_family": candidate.rule_family,
        "rule_category": candidate.rule_category,
        "normalized_category": candidate.rule_category,
        "source_level": candidate.source_level,
        "property_type": candidate.property_type,
        "required": candidate.required,
        "blocking": candidate.blocking,
        "confidence": candidate.confidence,
        "governance_state": candidate.governance_state,
        "rule_status": candidate.rule_status,
        "normalized_version": candidate.normalized_version,
        "version_group": candidate.version_group,
        "value_json": _dumps(candidate.value_json),
        "value_hash": candidate.fingerprint,
        "source_citation": candidate.source_citation,
        "raw_excerpt": candidate.raw_excerpt,
        "citation_json": _dumps(citation_payload),
        "rule_provenance_json": _dumps(provenance_payload),
        "confidence_basis": f"{candidate.evidence_state}:{candidate.citation_quality:.2f}",
        "change_summary": None if not candidate.conflict_hints else f"conflict_hints={','.join(candidate.conflict_hints)}",
        "coverage_status": coverage_status,
    }


def assertion_fingerprint(assertion: Any) -> str:
    payload = {
        "rule_key": getattr(assertion, "rule_key", None),
        "rule_family": getattr(assertion, "rule_family", None),
        "rule_category": getattr(assertion, "rule_category", None) or getattr(assertion, "normalized_category", None),
        "source_level": getattr(assertion, "source_level", None),
        "property_type": getattr(assertion, "property_type", None),
        "required": bool(getattr(assertion, "required", False)),
        "blocking": bool(getattr(assertion, "blocking", False)),
        "confidence": float(getattr(assertion, "confidence", 0.0) or 0.0),
        "governance_state": getattr(assertion, "governance_state", None),
        "rule_status": getattr(assertion, "rule_status", None),
        "normalized_version": getattr(assertion, "normalized_version", None),
        "version_group": getattr(assertion, "version_group", None),
        "value_json": _json_dict(getattr(assertion, "value_json", None)),
        "source_citation": getattr(assertion, "source_citation", None),
        "raw_excerpt": _clean_excerpt(getattr(assertion, "raw_excerpt", None)),
        "confidence_basis": getattr(assertion, "confidence_basis", None),
    }
    return sha256(_dumps(payload).encode("utf-8")).hexdigest()


def candidate_matches_assertion(candidate: NormalizedRuleCandidate, assertion: Any) -> bool:
    return candidate.fingerprint == assertion_fingerprint(assertion)


def diff_reason(candidate: NormalizedRuleCandidate, assertion: Any) -> str:
    differences: list[str] = []

    comparable_fields = [
        ("rule_category", candidate.rule_category, getattr(assertion, "rule_category", None) or getattr(assertion, "normalized_category", None)),
        ("source_level", candidate.source_level, getattr(assertion, "source_level", None)),
        ("property_type", candidate.property_type, getattr(assertion, "property_type", None)),
        ("required", candidate.required, bool(getattr(assertion, "required", False))),
        ("blocking", candidate.blocking, bool(getattr(assertion, "blocking", False))),
        ("confidence", round(candidate.confidence, 6), round(float(getattr(assertion, "confidence", 0.0) or 0.0), 6)),
        ("governance_state", candidate.governance_state, getattr(assertion, "governance_state", None)),
        ("rule_status", candidate.rule_status, getattr(assertion, "rule_status", None)),
        ("normalized_version", candidate.normalized_version, getattr(assertion, "normalized_version", None)),
        ("source_citation", candidate.source_citation, getattr(assertion, "source_citation", None)),
    ]
    for field_name, new_value, old_value in comparable_fields:
        if new_value != old_value:
            differences.append(field_name)

    old_value_json = _json_dict(getattr(assertion, "value_json", None))
    if candidate.value_json != old_value_json:
        differences.append("value_json")

    old_excerpt = _clean_excerpt(getattr(assertion, "raw_excerpt", None))
    if candidate.raw_excerpt != old_excerpt:
        differences.append("raw_excerpt")

    if not differences:
        return "unchanged"
    return ", ".join(differences)


def normalized_candidate_payload(candidate: NormalizedRuleCandidate) -> dict[str, Any]:
    return asdict(candidate)