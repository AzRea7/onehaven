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


def _first_url(text: str) -> str | None:
    match = re.search(r"https?://[^\s)>\]]+", text or "")
    return match.group(0) if match else None


def _has_any(text: str, phrases: list[str]) -> bool:
    return any(p in text for p in phrases)


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
        "all_fees_must_be_paid": "registration",
        "city_debts_block_license": "registration",
        "source_of_income_discrimination_prohibited": "safety",
        "rental_license_term_years": "registration",
        "renewal_days_before_expiration": "registration",
        "property_maintenance_enforcement_anchor": "safety",
        "building_safety_division_anchor": "safety",
        "building_division_anchor": "permits",
    }
    return normalize_category(mapping.get(rule_key))


def _rule_family_for(rule_key: str) -> str:
    mapping = {
        "federal_hcv_regulations_anchor": "federal_hcv",
        "federal_nspire_anchor": "federal_nspire",
        "federal_notice_anchor": "federal_notice",
        "mi_statute_anchor": "mi_landlord_tenant",
        "mshda_program_anchor": "mshda_program",
        "pha_admin_plan_anchor": "pha_admin_plan",
        "pha_administrator_changed": "pha_admin_transfer",
        "pha_landlord_packet_required": "pha_landlord_workflow",
        "hap_contract_and_tenancy_addendum_required": "voucher_lease_packet",
        "landlord_payment_timing_reference": "landlord_payment_timing",
        "rental_registration_required": "rental_registration",
        "inspection_program_exists": "inspection_program",
        "certificate_required_before_occupancy": "certificate_before_occupancy",
        "local_agent_required": "local_agent",
        "owner_po_box_allowed": "owner_contact",
        "all_fees_must_be_paid": "fees",
        "city_debts_block_license": "debts",
        "source_of_income_discrimination_prohibited": "source_of_income",
        "rental_license_term_years": "license_term",
        "renewal_days_before_expiration": "license_renewal",
        "property_maintenance_enforcement_anchor": "property_maintenance",
        "building_safety_division_anchor": "building_safety",
        "building_division_anchor": "building_division",
    }
    return mapping.get(rule_key, rule_key)


def _source_level_for(rule_key: str, whole: str) -> str:
    if rule_key.startswith("federal_"):
        return "federal"
    if rule_key.startswith("mi_") or rule_key.startswith("mshda_"):
        return "state"
    if rule_key.startswith("pha_") or "housing authority" in whole or "voucher" in whole:
        return "program"
    if "county" in whole:
        return "county"
    return "city"


def _blocking_for(rule_key: str, whole: str) -> bool:
    if rule_key in {
        "rental_registration_required",
        "inspection_program_exists",
        "certificate_required_before_occupancy",
        "city_debts_block_license",
        "hap_contract_and_tenancy_addendum_required",
        "pha_landlord_packet_required",
    }:
        return True
    if _has_any(
        whole,
        [
            "before occupancy",
            "may not occupy",
            "cannot occupy",
            "shall not occupy",
            "license required before",
            "must register before",
            "inspection required before",
            "required prior to occupancy",
        ],
    ):
        return True
    return False


def _required_for(rule_key: str, whole: str) -> bool:
    if rule_key.endswith("_required"):
        return True
    if rule_key.endswith("_anchor"):
        return True
    if rule_key == "inspection_program_exists":
        return True
    if _has_any(
        whole,
        [
            "required",
            "must",
            "shall",
            "need to",
            "is necessary",
            "must be submitted",
            "must be obtained",
        ],
    ):
        return True
    return False


def _confidence_for(rule_key: str, whole: str) -> float:
    if rule_key in {"federal_hcv_regulations_anchor", "federal_nspire_anchor"}:
        return 0.97
    if rule_key in {"mi_statute_anchor", "mshda_program_anchor"}:
        return 0.95
    if rule_key in {
        "rental_registration_required",
        "inspection_program_exists",
        "certificate_required_before_occupancy",
        "pha_landlord_packet_required",
        "hap_contract_and_tenancy_addendum_required",
    }:
        return 0.90
    if ".gov" in whole or "hud.gov" in whole or "legislature.mi.gov" in whole:
        return 0.88
    return 0.76


def normalize_rule_key(raw_text: str, *, hint: Optional[str] = None) -> Optional[str]:
    whole = f"{_clean(hint)} {_clean(raw_text)}".strip()

    patterns = [
        (r"(federal hcv regulations|24 cfr|housing choice voucher program regulations)", "federal_hcv_regulations_anchor"),
        (r"(nspire|national standards for the physical inspection of real estate)", "federal_nspire_anchor"),
        (r"(federal register notice|hud notice|pih notice)", "federal_notice_anchor"),
        (r"(michigan legislature|mcl |compiled laws|landlord tenant statute)", "mi_statute_anchor"),
        (r"(mshda|michigan state housing development authority)", "mshda_program_anchor"),
        (r"(admin plan|administrative plan)", "pha_admin_plan_anchor"),
        (r"(housing authority changed|administrator changed|administered by .* housing commission)", "pha_administrator_changed"),
        (r"(landlord packet|required forms packet)", "pha_landlord_packet_required"),
        (r"(hap contract|tenancy addendum)", "hap_contract_and_tenancy_addendum_required"),
        (r"(landlord payment timing|payment standards schedule|payment timing)", "landlord_payment_timing_reference"),
        (r"(rental|property).*(registration|register)|registration.*rental", "rental_registration_required"),
        (r"(inspection|inspected).*(required|must)|inspection program|inspection required", "inspection_program_exists"),
        (r"certificate.*occupancy|occupancy permit|certificate of compliance", "certificate_required_before_occupancy"),
        (r"local agent|local representative|responsible agent", "local_agent_required"),
        (r"po box.*not allowed|physical address required", "owner_po_box_allowed"),
        (r"fees.*paid|all fees.*paid", "all_fees_must_be_paid"),
        (r"debt.*block|delinquent.*tax|city debts", "city_debts_block_license"),
        (r"source of income|voucher.*discrimination", "source_of_income_discrimination_prohibited"),
        (r"license term|renewal.*year", "rental_license_term_years"),
        (r"renew.*before expiration|renewal.*days", "renewal_days_before_expiration"),
        (r"property maintenance code|maintenance enforcement", "property_maintenance_enforcement_anchor"),
        (r"building safety division", "building_safety_division_anchor"),
        (r"building division", "building_division_anchor"),
    ]

    for pattern, key in patterns:
        if re.search(pattern, whole):
            return key
    return None


def normalize_value(rule_key: str, raw_text: str) -> dict[str, Any]:
    text = _clean(raw_text)

    if rule_key == "owner_po_box_allowed":
        return {"allowed": not ("not allowed" in text or "cannot" in text or "physical address required" in text)}

    if rule_key.endswith("_required"):
        return {"required": True}

    if rule_key in {
        "inspection_program_exists",
        "federal_hcv_regulations_anchor",
        "federal_nspire_anchor",
        "federal_notice_anchor",
        "mi_statute_anchor",
        "mshda_program_anchor",
        "pha_admin_plan_anchor",
        "pha_administrator_changed",
        "property_maintenance_enforcement_anchor",
        "building_safety_division_anchor",
        "building_division_anchor",
    }:
        return {"present": True}

    if rule_key == "source_of_income_discrimination_prohibited":
        return {"prohibited": True}

    miles_match = re.search(r"(\d{1,3})\s*miles?", text)
    if rule_key == "local_agent_required" and miles_match:
        return {"required": True, "max_radius_miles": int(miles_match.group(1))}

    days_match = re.search(r"(\d{1,3})\s*days?", text)
    if rule_key == "renewal_days_before_expiration" and days_match:
        return {"days": int(days_match.group(1))}

    years_match = re.search(r"(\d{1,2})\s*years?", text)
    if rule_key == "rental_license_term_years" and years_match:
        return {"years": int(years_match.group(1))}

    return {"text": str(raw_text or "").strip()}


def normalize_rule_candidate(
    raw_text: str,
    *,
    hint: Optional[str] = None,
    source_url: Optional[str] = None,
    property_type: Optional[str] = None,
    normalized_version: str = "v2",
) -> NormalizedRuleCandidate | None:
    rule_key = normalize_rule_key(raw_text, hint=hint)
    if not rule_key:
        return None

    whole = f"{_clean(hint)} {_clean(raw_text)} {_clean(source_url)}".strip()
    value_json = normalize_value(rule_key, raw_text)

    source_citation = source_url or _first_url(str(hint or "")) or _first_url(str(raw_text or ""))
    raw_excerpt = _clean_excerpt(raw_text)
    rule_category = _category_for(rule_key)
    source_level = _source_level_for(rule_key, whole)
    required = _required_for(rule_key, whole)
    blocking = _blocking_for(rule_key, whole)
    confidence = _confidence_for(rule_key, whole)
    governance_state = "draft"
    rule_status = "candidate"
    evidence_state = "inferred"
    version_group = f"{source_level}:{rule_key}:{property_type or 'all'}"
    payload = {
        "rule_key": rule_key,
        "rule_family": _rule_family_for(rule_key),
        "rule_category": rule_category,
        "source_level": source_level,
        "property_type": property_type,
        "required": required,
        "blocking": blocking,
        "value_json": value_json,
        "source_citation": source_citation,
        "raw_excerpt": raw_excerpt,
        "normalized_version": normalized_version,
    }
    fingerprint = sha256(_dumps(payload).encode("utf-8")).hexdigest()

    return NormalizedRuleCandidate(
        rule_key=rule_key,
        rule_family=_rule_family_for(rule_key),
        rule_category=rule_category,
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
    )


def candidate_to_update_dict(candidate: NormalizedRuleCandidate) -> dict[str, Any]:
    payload = asdict(candidate)
    payload.pop("fingerprint", None)
    return payload


def assertion_fingerprint(assertion: Any) -> str:
    payload = {
        "rule_key": getattr(assertion, "rule_key", None),
        "rule_family": getattr(assertion, "rule_family", None),
        "rule_category": getattr(assertion, "rule_category", None),
        "source_level": getattr(assertion, "source_level", None),
        "property_type": getattr(assertion, "property_type", None),
        "required": bool(getattr(assertion, "required", True)),
        "blocking": bool(getattr(assertion, "blocking", False)),
        "value_json": getattr(assertion, "value_json", None),
        "source_citation": getattr(assertion, "source_citation", None),
        "raw_excerpt": getattr(assertion, "raw_excerpt", None),
        "normalized_version": getattr(assertion, "normalized_version", None),
    }
    return sha256(_dumps(payload).encode("utf-8")).hexdigest()