from __future__ import annotations

import re
from typing import Any, Optional


def _clean(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


def _contains_any(text: str, terms: list[str]) -> bool:
    return any(term in text for term in terms)


def normalize_rule_key(raw_text: str, *, hint: Optional[str] = None) -> Optional[str]:
    whole = f"{_clean(hint)} {_clean(raw_text)}".strip()
    if not whole:
        return None

    patterns: list[tuple[str, str]] = [
        (r"(rental|property).*(registration|register|registered)", "rental_registration_required"),
        (r"(rental|property).*(license|licence|licensed)", "rental_license_required"),
        (r"(inspection|inspected).*(required|must)|inspection program|inspection required", "inspection_program_exists"),
        (r"certificate.*occupancy|occupancy permit|certificate of compliance|compliance certificate", "certificate_required_before_occupancy"),
        (r"local agent|local representative|responsible agent|registered agent", "local_agent_required"),
        (r"po box.*not allowed|p\.?o\.? box.*not allowed|physical address required", "owner_po_box_allowed"),
        (r"fees.*paid|all fees.*paid|fee delinquency", "all_fees_must_be_paid"),
        (r"debt.*block|delinquent.*tax|city debts|unpaid.*utility|unpaid.*assessment", "city_debts_block_license"),
        (r"source of income|voucher.*discrimination|income discrimination", "source_of_income_discrimination_prohibited"),
        (r"license term|license valid|renewal.*year|term.*years", "rental_license_term_years"),
        (r"renew.*before expiration|renewal.*days|days before expiration", "renewal_days_before_expiration"),
        (r"landlord packet|owner packet|lessor packet", "pha_landlord_packet_required"),
        (r"hap contract|tenancy addendum|housing assistance payments contract", "hap_contract_and_tenancy_addendum_required"),
        (r"hcv regulations|24 cfr 982|federal voucher regulations", "federal_hcv_regulations_anchor"),
        (r"nspire|hud inspection standards|federal inspection standards", "federal_nspire_anchor"),
        (r"mcl|michigan landlord|michigan statute", "mi_statute_anchor"),
        (r"mshda|michigan state housing", "mshda_program_anchor"),
        (r"admin plan|administrative plan", "pha_admin_plan_anchor"),
        (r"administrator changed|program administrator changed", "pha_administrator_changed"),
        (r"lead-based paint|lead paint|lead hazard", "lead_based_paint_requirements"),
        (r"smoke detector|carbon monoxide|co detector", "life_safety_detector_requirement"),
        (r"utility.*required|utilities.*responsibility", "utility_documentation_requirement"),
        (r"zoning|use permit|nonconforming use", "zoning_or_use_requirement"),
        (r"permit.*required|rehab permit|building permit", "permit_requirement"),
        (r"fair housing", "fair_housing_anchor"),
        (r"document|application form|packet|checklist|affidavit", "document_reference"),
    ]

    for pattern, key in patterns:
        if re.search(pattern, whole):
            return key

    if _contains_any(whole, ["section 8", "voucher", "housing choice voucher", "hcv"]):
        return "pha_workflow_reference"

    return None


def normalize_value(rule_key: str, raw_text: str) -> dict[str, Any]:
    text = _clean(raw_text)

    if rule_key == "owner_po_box_allowed":
        return {"allowed": not ("not allowed" in text or "cannot" in text or "no p.o. box" in text)}

    if rule_key in {
        "rental_registration_required",
        "rental_license_required",
        "inspection_program_exists",
        "local_agent_required",
        "pha_landlord_packet_required",
        "hap_contract_and_tenancy_addendum_required",
        "federal_hcv_regulations_anchor",
        "federal_nspire_anchor",
        "mi_statute_anchor",
        "mshda_program_anchor",
        "pha_admin_plan_anchor",
        "permit_requirement",
        "utility_documentation_requirement",
        "zoning_or_use_requirement",
        "fair_housing_anchor",
    }:
        return {"required": True}

    if rule_key == "source_of_income_discrimination_prohibited":
        return {"prohibited": True}

    if rule_key == "certificate_required_before_occupancy":
        status = "yes"
        if "conditional" in text or "if applicable" in text or "when required" in text:
            status = "conditional"
        elif "not required" in text or "no certificate" in text:
            status = "no"
        return {"status": status}

    m = re.search(r"(\d{1,3})\s*miles?", text)
    if rule_key == "local_agent_required" and m:
        return {"required": True, "max_radius_miles": int(m.group(1))}

    m = re.search(r"(\d{1,3})\s*days?", text)
    if rule_key == "renewal_days_before_expiration" and m:
        return {"days": int(m.group(1))}

    m = re.search(r"(\d{1,2})\s*years?", text)
    if rule_key == "rental_license_term_years" and m:
        return {"years": int(m.group(1))}

    if rule_key == "lead_based_paint_requirements":
        return {
            "required": True,
            "hazard_focus": True,
            "applies_pre_1978": ("1978" in text or "pre-1978" in text or "before 1978" in text),
        }

    if rule_key == "life_safety_detector_requirement":
        return {
            "required": True,
            "smoke_detector": "smoke" in text,
            "co_detector": "carbon monoxide" in text or "co detector" in text,
        }

    return {"text": raw_text.strip()}