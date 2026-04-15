# backend/app/domain/jurisdiction_categories.py
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping


CATEGORY_RENTAL_LICENSE = "rental_license"
CATEGORY_INSPECTION = "inspection"
CATEGORY_SECTION8 = "section8"
CATEGORY_ZONING = "zoning"
CATEGORY_TAX = "tax"
CATEGORY_UTILITIES = "utilities"
CATEGORY_SAFETY = "safety"
CATEGORY_REGISTRATION = "registration"
CATEGORY_LEAD = "lead"
CATEGORY_OCCUPANCY = "occupancy"
CATEGORY_PERMITS = "permits"
CATEGORY_PROGRAM_OVERLAY = "program_overlay"
CATEGORY_DOCUMENTS = "documents"
CATEGORY_FEES = "fees"
CATEGORY_CONTACTS = "contacts"
CATEGORY_SOURCE_OF_INCOME = "source_of_income"
CATEGORY_UNCATEGORIZED = "uncategorized"

JURISDICTION_TYPE_STATE = "state"
JURISDICTION_TYPE_COUNTY = "county"
JURISDICTION_TYPE_CITY = "city"
JURISDICTION_TYPE_SECTION8_OVERLAY = "section8_overlay"
JURISDICTION_TYPE_PHA_PROGRAM_OVERLAY = JURISDICTION_TYPE_SECTION8_OVERLAY

BINDING_TYPE_LEGAL = "legally_binding"
BINDING_TYPE_OPERATIONAL = "operational_heuristic"

CANONICAL_JURISDICTION_CATEGORIES: tuple[str, ...] = (
    CATEGORY_RENTAL_LICENSE,
    CATEGORY_INSPECTION,
    CATEGORY_SECTION8,
    CATEGORY_ZONING,
    CATEGORY_TAX,
    CATEGORY_UTILITIES,
    CATEGORY_SAFETY,
    CATEGORY_REGISTRATION,
    CATEGORY_LEAD,
    CATEGORY_OCCUPANCY,
    CATEGORY_PERMITS,
    CATEGORY_PROGRAM_OVERLAY,
    CATEGORY_DOCUMENTS,
    CATEGORY_FEES,
    CATEGORY_CONTACTS,
    CATEGORY_SOURCE_OF_INCOME,
)

CATEGORY_DISPLAY_NAMES: dict[str, str] = {
    CATEGORY_RENTAL_LICENSE: "Rental license",
    CATEGORY_INSPECTION: "Inspection",
    CATEGORY_SECTION8: "Section 8 / HCV",
    CATEGORY_ZONING: "Zoning",
    CATEGORY_TAX: "Tax",
    CATEGORY_UTILITIES: "Utilities",
    CATEGORY_SAFETY: "Safety / housing code",
    CATEGORY_REGISTRATION: "Rental registration",
    CATEGORY_LEAD: "Lead / environmental health",
    CATEGORY_OCCUPANCY: "Occupancy / certificate",
    CATEGORY_PERMITS: "Permits",
    CATEGORY_PROGRAM_OVERLAY: "Program overlay",
    CATEGORY_DOCUMENTS: "Documents / packets",
    CATEGORY_FEES: "Fees",
    CATEGORY_CONTACTS: "Contacts / authorities",
    CATEGORY_SOURCE_OF_INCOME: "Source of income",
}

_CATEGORY_ALIASES: dict[str, str] = {
    "license": CATEGORY_RENTAL_LICENSE,
    "licenses": CATEGORY_RENTAL_LICENSE,
    "rental": CATEGORY_RENTAL_LICENSE,
    "rental_license": CATEGORY_RENTAL_LICENSE,
    "rental_licensing": CATEGORY_RENTAL_LICENSE,
    "licensing": CATEGORY_RENTAL_LICENSE,
    "registration": CATEGORY_REGISTRATION,
    "rental_registration": CATEGORY_REGISTRATION,
    "registry": CATEGORY_REGISTRATION,
    "inspection": CATEGORY_INSPECTION,
    "inspections": CATEGORY_INSPECTION,
    "inspection_frequency": CATEGORY_INSPECTION,
    "section8": CATEGORY_SECTION8,
    "section_8": CATEGORY_SECTION8,
    "hqs": CATEGORY_SECTION8,
    "nspire": CATEGORY_SECTION8,
    "voucher": CATEGORY_SECTION8,
    "housing_choice_voucher": CATEGORY_SECTION8,
    "pha": CATEGORY_PROGRAM_OVERLAY,
    "housing_authority": CATEGORY_PROGRAM_OVERLAY,
    "overlay": CATEGORY_PROGRAM_OVERLAY,
    "program_overlay": CATEGORY_PROGRAM_OVERLAY,
    "pha_program_overlay": CATEGORY_PROGRAM_OVERLAY,
    "documents": CATEGORY_DOCUMENTS,
    "document": CATEGORY_DOCUMENTS,
    "paperwork": CATEGORY_DOCUMENTS,
    "fees": CATEGORY_FEES,
    "fee": CATEGORY_FEES,
    "contacts": CATEGORY_CONTACTS,
    "contact": CATEGORY_CONTACTS,
    "local_contact": CATEGORY_CONTACTS,
    "zoning": CATEGORY_ZONING,
    "land_use": CATEGORY_ZONING,
    "tax": CATEGORY_TAX,
    "taxes": CATEGORY_TAX,
    "property_tax": CATEGORY_TAX,
    "utilities": CATEGORY_UTILITIES,
    "utility": CATEGORY_UTILITIES,
    "water": CATEGORY_UTILITIES,
    "sewer": CATEGORY_UTILITIES,
    "safety": CATEGORY_SAFETY,
    "code_safety": CATEGORY_SAFETY,
    "fire_safety": CATEGORY_SAFETY,
    "lead": CATEGORY_LEAD,
    "lead_paint": CATEGORY_LEAD,
    "occupancy": CATEGORY_OCCUPANCY,
    "occupancy_limit": CATEGORY_OCCUPANCY,
    "certificate_of_occupancy": CATEGORY_OCCUPANCY,
    "certificate": CATEGORY_OCCUPANCY,
    "permits": CATEGORY_PERMITS,
    "permit": CATEGORY_PERMITS,
    "building_permit": CATEGORY_PERMITS,
    "source_of_income": CATEGORY_SOURCE_OF_INCOME,
    "soi": CATEGORY_SOURCE_OF_INCOME,
}

RULE_KEY_CATEGORY_MAP: dict[str, str] = {
    "rental_registration_required": CATEGORY_REGISTRATION,
    "registration_required": CATEGORY_REGISTRATION,
    "inspection_program_exists": CATEGORY_INSPECTION,
    "inspection_required": CATEGORY_INSPECTION,
    "certificate_required_before_occupancy": CATEGORY_OCCUPANCY,
    "occupancy_certificate_required": CATEGORY_OCCUPANCY,
    "lead_disclosure_required": CATEGORY_LEAD,
    "lead_clearance_required": CATEGORY_LEAD,
    "permit_required_for_rehab": CATEGORY_PERMITS,
    "rental_license_required": CATEGORY_RENTAL_LICENSE,
    "source_of_income_protected": CATEGORY_SOURCE_OF_INCOME,
    "pha_landlord_packet_required": CATEGORY_SECTION8,
    "hap_contract_and_tenancy_addendum_required": CATEGORY_SECTION8,
    "federal_hcv_regulations_anchor": CATEGORY_SECTION8,
    "federal_nspire_anchor": CATEGORY_SECTION8,
    "mi_statute_anchor": CATEGORY_PROGRAM_OVERLAY,
    "mshda_program_anchor": CATEGORY_PROGRAM_OVERLAY,
    "pha_admin_plan_anchor": CATEGORY_PROGRAM_OVERLAY,
    "pha_administrator_changed": CATEGORY_CONTACTS,
}

CATEGORY_STATUS_SCORES: dict[str, float] = {
    "covered": 1.0,
    "verified": 1.0,
    "fresh": 1.0,
    "partial": 0.6,
    "conditional": 0.55,
    "stale": 0.45,
    "inferred": 0.35,
    "conflicting": 0.15,
    "missing": 0.0,
    "unknown": 0.0,
}

_TIER_SATISFIED_CATEGORY_STATUSES = {"covered", "verified", "fresh"}
_TIER_INFERRED_CATEGORY_STATUSES = {"inferred", "partial", "conditional"}
_TIER_BLOCKING_CATEGORY_STATUSES = {"missing", "stale", "conflicting", "inferred", "partial", "conditional"}

_SECTION8_MARKET_CITIES = {
    "detroit",
    "pontiac",
    "inkster",
    "warren",
    "southfield",
    "dearborn",
    "royal oak",
    "hamtramck",
    "highland park",
}
_SECTION8_MARKET_COUNTIES = {"wayne", "oakland", "macomb"}
_LEAD_FOCUSED_CITIES = {
    "detroit",
    "pontiac",
    "inkster",
    "hamtramck",
    "highland park",
    "river rouge",
    "ecorse",
}

_EXPECTED_CATEGORY_BUNDLES: dict[str, dict[str, tuple[str, ...]]] = {
    JURISDICTION_TYPE_STATE: {
        "required": (CATEGORY_SAFETY, CATEGORY_LEAD, CATEGORY_SOURCE_OF_INCOME, CATEGORY_PERMITS),
        "critical": (CATEGORY_SAFETY, CATEGORY_LEAD),
        "optional": (CATEGORY_ZONING, CATEGORY_TAX, CATEGORY_UTILITIES, CATEGORY_DOCUMENTS),
    },
    JURISDICTION_TYPE_COUNTY: {
        "required": (CATEGORY_REGISTRATION, CATEGORY_INSPECTION, CATEGORY_SAFETY, CATEGORY_PERMITS, CATEGORY_DOCUMENTS, CATEGORY_CONTACTS),
        "critical": (CATEGORY_INSPECTION, CATEGORY_SAFETY),
        "optional": (CATEGORY_OCCUPANCY, CATEGORY_FEES, CATEGORY_TAX, CATEGORY_UTILITIES, CATEGORY_ZONING),
    },
    JURISDICTION_TYPE_CITY: {
        "required": (CATEGORY_RENTAL_LICENSE, CATEGORY_REGISTRATION, CATEGORY_INSPECTION, CATEGORY_OCCUPANCY, CATEGORY_SAFETY, CATEGORY_PERMITS, CATEGORY_DOCUMENTS, CATEGORY_FEES, CATEGORY_CONTACTS),
        "critical": (CATEGORY_RENTAL_LICENSE, CATEGORY_REGISTRATION, CATEGORY_INSPECTION, CATEGORY_OCCUPANCY, CATEGORY_SAFETY),
        "optional": (CATEGORY_LEAD, CATEGORY_ZONING, CATEGORY_TAX, CATEGORY_UTILITIES, CATEGORY_SOURCE_OF_INCOME),
    },
    JURISDICTION_TYPE_SECTION8_OVERLAY: {
        "required": (CATEGORY_SECTION8, CATEGORY_PROGRAM_OVERLAY, CATEGORY_CONTACTS, CATEGORY_DOCUMENTS, CATEGORY_INSPECTION),
        "critical": (CATEGORY_SECTION8, CATEGORY_PROGRAM_OVERLAY, CATEGORY_CONTACTS),
        "optional": (CATEGORY_OCCUPANCY, CATEGORY_SOURCE_OF_INCOME, CATEGORY_SAFETY),
    },
}


@dataclass(frozen=True)
class JurisdictionCategoryCoverage:
    required_categories: list[str]
    covered_categories: list[str]
    missing_categories: list[str]
    completeness_score: float
    completeness_status: str
    coverage_confidence: str
    stale_categories: list[str] | None = None
    inferred_categories: list[str] | None = None
    conflicting_categories: list[str] | None = None
    category_statuses: dict[str, str] | None = None


@dataclass(frozen=True)
class JurisdictionRuleFamily:
    category: str
    family_key: str
    display_name: str
    binding_type: str
    authority_expectation: str
    property_proof_expectation: str
    default_jurisdiction_types: list[str]
    description: str
    typical_rule_keys: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class JurisdictionExpectedRuleUniverse:
    jurisdiction_types: list[str]
    required_categories: list[str]
    critical_categories: list[str]
    optional_categories: list[str]
    category_bundles: dict[str, dict[str, list[str]]]
    tier_order: list[str] | None = None
    rule_family_inventory: dict[str, dict[str, Any]] | None = None
    legally_binding_categories: list[str] | None = None
    operational_heuristic_categories: list[str] | None = None
    authority_expectations: dict[str, str] | None = None
    property_proof_required_categories: list[str] | None = None
    family_bundles: dict[str, list[str]] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["tier_order"] = list(self.tier_order or self.jurisdiction_types)
        payload.setdefault("rule_family_inventory", {})
        payload.setdefault("legally_binding_categories", [])
        payload.setdefault("operational_heuristic_categories", [])
        payload.setdefault("authority_expectations", {})
        payload.setdefault("property_proof_required_categories", [])
        payload.setdefault("family_bundles", {})
        return payload


@dataclass(frozen=True)
class JurisdictionTierCoverage:
    jurisdiction_type: str
    required_categories: list[str]
    critical_categories: list[str]
    optional_categories: list[str]
    covered_required_categories: list[str]
    missing_required_categories: list[str]
    missing_critical_categories: list[str]
    completeness_ratio: float
    complete: bool
    required_category_statuses: dict[str, str] | None = None
    critical_category_statuses: dict[str, str] | None = None
    inferred_required_categories: list[str] | None = None
    stale_required_categories: list[str] | None = None
    conflicting_required_categories: list[str] | None = None
    satisfied_required_categories: list[str] | None = None
    unsatisfied_required_categories: list[str] | None = None
    optional_category_statuses: dict[str, str] | None = None
    status_counts: dict[str, int] | None = None
    blocking_statuses: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_category(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if not raw:
        return None
    raw = raw.replace("-", "_").replace(" ", "_").replace("/", "_")
    while "__" in raw:
        raw = raw.replace("__", "_")
    if raw in CANONICAL_JURISDICTION_CATEGORIES:
        return raw
    return _CATEGORY_ALIASES.get(raw)


def normalize_categories(values: Iterable[Any] | None) -> list[str]:
    if not values:
        return []
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        normalized = normalize_category(value)
        if normalized and normalized not in seen:
            seen.add(normalized)
            ordered.append(normalized)
    return ordered


def normalize_rule_category(rule_key: Any) -> str:
    if rule_key is None:
        return CATEGORY_UNCATEGORIZED
    raw = str(rule_key).strip().lower()
    if not raw:
        return CATEGORY_UNCATEGORIZED
    raw = raw.replace("-", "_").replace(" ", "_").replace("/", "_")
    while "__" in raw:
        raw = raw.replace("__", "_")
    return RULE_KEY_CATEGORY_MAP.get(raw, CATEGORY_UNCATEGORIZED)


def category_score_for_status(status: Any) -> float:
    key = str(status or "unknown").strip().lower() or "unknown"
    return float(CATEGORY_STATUS_SCORES.get(key, CATEGORY_STATUS_SCORES["unknown"]))


def completeness_confidence_label(score: float) -> str:
    if score >= 0.8:
        return "high"
    if score >= 0.5:
        return "medium"
    return "low"


def is_known_category(value: Any) -> bool:
    return normalize_category(value) is not None


def category_label(category: Any) -> str:
    normalized = normalize_category(category)
    if normalized is None:
        raw = str(category or "").strip()
        return raw.replace("_", " ").title() if raw else "Unknown"
    return CATEGORY_DISPLAY_NAMES.get(normalized, normalized.replace("_", " ").title())


_RULE_FAMILY_INVENTORY: dict[str, JurisdictionRuleFamily] = {
    CATEGORY_RENTAL_LICENSE: JurisdictionRuleFamily(
        category=CATEGORY_RENTAL_LICENSE,
        family_key="licensing_and_operator_eligibility",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_RENTAL_LICENSE],
        binding_type=BINDING_TYPE_LEGAL,
        authority_expectation="authoritative_official",
        property_proof_expectation="license number or issuance evidence usually property-specific",
        default_jurisdiction_types=[JURISDICTION_TYPE_CITY],
        description="Whether the operator must hold a rental or landlord license for the property.",
        typical_rule_keys=["rental_license_required"],
    ),
    CATEGORY_REGISTRATION: JurisdictionRuleFamily(
        category=CATEGORY_REGISTRATION,
        family_key="registration_and_registry_filing",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_REGISTRATION],
        binding_type=BINDING_TYPE_LEGAL,
        authority_expectation="authoritative_official",
        property_proof_expectation="registration confirmation, certificate, or registry lookup",
        default_jurisdiction_types=[JURISDICTION_TYPE_COUNTY, JURISDICTION_TYPE_CITY],
        description="Whether the rental must be registered with a city, county, or other local authority.",
        typical_rule_keys=["rental_registration_required", "registration_required"],
    ),
    CATEGORY_INSPECTION: JurisdictionRuleFamily(
        category=CATEGORY_INSPECTION,
        family_key="inspection_program_and_compliance_cycle",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_INSPECTION],
        binding_type=BINDING_TYPE_LEGAL,
        authority_expectation="authoritative_official",
        property_proof_expectation="inspection pass, scheduled inspection, or cycle evidence",
        default_jurisdiction_types=[JURISDICTION_TYPE_COUNTY, JURISDICTION_TYPE_CITY, JURISDICTION_TYPE_SECTION8_OVERLAY],
        description="Inspection authority, cadence, triggers, and pass conditions for rental operation.",
        typical_rule_keys=["inspection_program_exists", "inspection_required"],
    ),
    CATEGORY_OCCUPANCY: JurisdictionRuleFamily(
        category=CATEGORY_OCCUPANCY,
        family_key="occupancy_and_pre_occupancy_clearance",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_OCCUPANCY],
        binding_type=BINDING_TYPE_LEGAL,
        authority_expectation="authoritative_official",
        property_proof_expectation="certificate of occupancy or equivalent local clearance",
        default_jurisdiction_types=[JURISDICTION_TYPE_CITY],
        description="Certificate of occupancy and similar occupancy-clearance requirements.",
        typical_rule_keys=["certificate_required_before_occupancy", "occupancy_certificate_required"],
    ),
    CATEGORY_SAFETY: JurisdictionRuleFamily(
        category=CATEGORY_SAFETY,
        family_key="habitability_and_housing_code_baseline",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_SAFETY],
        binding_type=BINDING_TYPE_LEGAL,
        authority_expectation="authoritative_official",
        property_proof_expectation="inspection evidence, violations history, or remediation records",
        default_jurisdiction_types=[JURISDICTION_TYPE_STATE, JURISDICTION_TYPE_COUNTY, JURISDICTION_TYPE_CITY],
        description="General housing, health, and safety standards that must be met for lawful occupancy.",
        typical_rule_keys=[],
    ),
    CATEGORY_LEAD: JurisdictionRuleFamily(
        category=CATEGORY_LEAD,
        family_key="lead_and_environmental_health_controls",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_LEAD],
        binding_type=BINDING_TYPE_LEGAL,
        authority_expectation="authoritative_official",
        property_proof_expectation="lead disclosure, clearance, abatement, or risk assessment evidence",
        default_jurisdiction_types=[JURISDICTION_TYPE_STATE, JURISDICTION_TYPE_CITY],
        description="Lead disclosure, clearance, and related health protections.",
        typical_rule_keys=["lead_disclosure_required", "lead_clearance_required"],
    ),
    CATEGORY_PERMITS: JurisdictionRuleFamily(
        category=CATEGORY_PERMITS,
        family_key="rehab_and_trade_permitting",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_PERMITS],
        binding_type=BINDING_TYPE_LEGAL,
        authority_expectation="authoritative_official",
        property_proof_expectation="permit number, permit status, or final sign-off",
        default_jurisdiction_types=[JURISDICTION_TYPE_STATE, JURISDICTION_TYPE_COUNTY, JURISDICTION_TYPE_CITY],
        description="Permit requirements affecting rehab, repair, or rental readiness work.",
        typical_rule_keys=["permit_required_for_rehab"],
    ),
    CATEGORY_SOURCE_OF_INCOME: JurisdictionRuleFamily(
        category=CATEGORY_SOURCE_OF_INCOME,
        family_key="tenant_screening_and_soi_protection",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_SOURCE_OF_INCOME],
        binding_type=BINDING_TYPE_LEGAL,
        authority_expectation="authoritative_official",
        property_proof_expectation="generally not property-specific; applied operationally in screening",
        default_jurisdiction_types=[JURISDICTION_TYPE_STATE, JURISDICTION_TYPE_CITY, JURISDICTION_TYPE_SECTION8_OVERLAY],
        description="Rules protecting voucher holders or other income sources during screening.",
        typical_rule_keys=["source_of_income_protected"],
    ),
    CATEGORY_SECTION8: JurisdictionRuleFamily(
        category=CATEGORY_SECTION8,
        family_key="voucher_program_participation_rules",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_SECTION8],
        binding_type=BINDING_TYPE_LEGAL,
        authority_expectation="approved_official_supporting",
        property_proof_expectation="packet completion, HAP execution, or inspection approval",
        default_jurisdiction_types=[JURISDICTION_TYPE_SECTION8_OVERLAY],
        description="Voucher-program rules specific to landlord participation and approval.",
        typical_rule_keys=["pha_landlord_packet_required", "hap_contract_and_tenancy_addendum_required", "federal_hcv_regulations_anchor", "federal_nspire_anchor"],
    ),
    CATEGORY_PROGRAM_OVERLAY: JurisdictionRuleFamily(
        category=CATEGORY_PROGRAM_OVERLAY,
        family_key="program_overlay_and_admin_plan_anchors",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_PROGRAM_OVERLAY],
        binding_type=BINDING_TYPE_LEGAL,
        authority_expectation="approved_official_supporting",
        property_proof_expectation="generally program evidence rather than parcel evidence",
        default_jurisdiction_types=[JURISDICTION_TYPE_SECTION8_OVERLAY, JURISDICTION_TYPE_STATE],
        description="PHA, MSHDA, and other administered program overlays that constrain operations.",
        typical_rule_keys=["mi_statute_anchor", "mshda_program_anchor", "pha_admin_plan_anchor"],
    ),
    CATEGORY_DOCUMENTS: JurisdictionRuleFamily(
        category=CATEGORY_DOCUMENTS,
        family_key="forms_packets_and_required_documents",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_DOCUMENTS],
        binding_type=BINDING_TYPE_OPERATIONAL,
        authority_expectation="approved_official_supporting",
        property_proof_expectation="forms, packets, checklists, and filing receipts",
        default_jurisdiction_types=[JURISDICTION_TYPE_COUNTY, JURISDICTION_TYPE_CITY, JURISDICTION_TYPE_SECTION8_OVERLAY],
        description="Operational document package expectations needed to complete a compliant filing path.",
        typical_rule_keys=[],
    ),
    CATEGORY_FEES: JurisdictionRuleFamily(
        category=CATEGORY_FEES,
        family_key="fees_payments_and_admin_costs",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_FEES],
        binding_type=BINDING_TYPE_OPERATIONAL,
        authority_expectation="approved_official_supporting",
        property_proof_expectation="fee schedule, receipt, invoice, or payment confirmation",
        default_jurisdiction_types=[JURISDICTION_TYPE_CITY, JURISDICTION_TYPE_COUNTY],
        description="Administrative fee expectations that affect readiness but are often operational details.",
        typical_rule_keys=[],
    ),
    CATEGORY_CONTACTS: JurisdictionRuleFamily(
        category=CATEGORY_CONTACTS,
        family_key="responsible_contacts_and_escalation_paths",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_CONTACTS],
        binding_type=BINDING_TYPE_OPERATIONAL,
        authority_expectation="approved_official_supporting",
        property_proof_expectation="contact roster or named authority reference",
        default_jurisdiction_types=[JURISDICTION_TYPE_COUNTY, JURISDICTION_TYPE_CITY, JURISDICTION_TYPE_SECTION8_OVERLAY],
        description="Who administers the program, receives filings, and answers compliance questions.",
        typical_rule_keys=["pha_administrator_changed"],
    ),
    CATEGORY_ZONING: JurisdictionRuleFamily(
        category=CATEGORY_ZONING,
        family_key="land_use_and_zoning_constraints",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_ZONING],
        binding_type=BINDING_TYPE_LEGAL,
        authority_expectation="authoritative_official",
        property_proof_expectation="zoning map, zoning district, or use clearance",
        default_jurisdiction_types=[JURISDICTION_TYPE_STATE, JURISDICTION_TYPE_COUNTY, JURISDICTION_TYPE_CITY],
        description="Land-use restrictions relevant to rental operation and rehab strategy.",
        typical_rule_keys=[],
    ),
    CATEGORY_TAX: JurisdictionRuleFamily(
        category=CATEGORY_TAX,
        family_key="tax_registration_and_charge_expectations",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_TAX],
        binding_type=BINDING_TYPE_OPERATIONAL,
        authority_expectation="semi_authoritative_operational",
        property_proof_expectation="tax bill, assessor record, or city billing program evidence",
        default_jurisdiction_types=[JURISDICTION_TYPE_STATE, JURISDICTION_TYPE_COUNTY, JURISDICTION_TYPE_CITY],
        description="Tax-related supporting expectations that matter operationally but may not block licensure.",
        typical_rule_keys=[],
    ),
    CATEGORY_UTILITIES: JurisdictionRuleFamily(
        category=CATEGORY_UTILITIES,
        family_key="utility_account_and_service_expectations",
        display_name=CATEGORY_DISPLAY_NAMES[CATEGORY_UTILITIES],
        binding_type=BINDING_TYPE_OPERATIONAL,
        authority_expectation="semi_authoritative_operational",
        property_proof_expectation="utility account proof or service transfer evidence",
        default_jurisdiction_types=[JURISDICTION_TYPE_STATE, JURISDICTION_TYPE_COUNTY, JURISDICTION_TYPE_CITY],
        description="Utility setup or transfer requirements used as operational readiness signals.",
        typical_rule_keys=[],
    ),
}


def rule_family_inventory() -> dict[str, JurisdictionRuleFamily]:
    return {k: v for k, v in _RULE_FAMILY_INVENTORY.items()}


def rule_family_inventory_dict() -> dict[str, dict[str, Any]]:
    return {k: v.to_dict() for k, v in _RULE_FAMILY_INVENTORY.items()}


def legally_binding_categories(categories: Iterable[Any] | None = None) -> list[str]:
    selected = normalize_categories(categories) if categories is not None else list(CANONICAL_JURISDICTION_CATEGORIES)
    return [c for c in selected if _RULE_FAMILY_INVENTORY.get(c) and _RULE_FAMILY_INVENTORY[c].binding_type == BINDING_TYPE_LEGAL]


def operational_heuristic_categories(categories: Iterable[Any] | None = None) -> list[str]:
    selected = normalize_categories(categories) if categories is not None else list(CANONICAL_JURISDICTION_CATEGORIES)
    return [c for c in selected if _RULE_FAMILY_INVENTORY.get(c) and _RULE_FAMILY_INVENTORY[c].binding_type == BINDING_TYPE_OPERATIONAL]


def authority_expectations_for_categories(categories: Iterable[Any] | None = None) -> dict[str, str]:
    selected = normalize_categories(categories) if categories is not None else list(CANONICAL_JURISDICTION_CATEGORIES)
    return {c: _RULE_FAMILY_INVENTORY[c].authority_expectation for c in selected if c in _RULE_FAMILY_INVENTORY}


def property_proof_required_categories(categories: Iterable[Any] | None = None) -> list[str]:
    selected = normalize_categories(categories) if categories is not None else list(CANONICAL_JURISDICTION_CATEGORIES)
    out: list[str] = []
    for c in selected:
        family = _RULE_FAMILY_INVENTORY.get(c)
        if family and "property" in family.property_proof_expectation.lower():
            out.append(c)
    return out


def family_bundles_for_jurisdiction_types(jurisdiction_types: Iterable[str]) -> dict[str, list[str]]:
    bundle_map: dict[str, list[str]] = {}
    active_types = {str(v).strip().lower() for v in (jurisdiction_types or []) if str(v).strip()}
    for category, family in _RULE_FAMILY_INVENTORY.items():
        if set(family.default_jurisdiction_types).intersection(active_types):
            for jt in family.default_jurisdiction_types:
                if jt not in active_types:
                    continue
                bucket = bundle_map.setdefault(jt, [])
                if category not in bucket:
                    bucket.append(category)
    return bundle_map


def expected_categories_for_jurisdiction_type(jurisdiction_type: str) -> dict[str, list[str]]:
    key = str(jurisdiction_type or "").strip().lower()
    raw = _EXPECTED_CATEGORY_BUNDLES.get(key, {})
    return {
        "required": normalize_categories(raw.get("required", ())),
        "critical": normalize_categories(raw.get("critical", ())),
        "optional": normalize_categories(raw.get("optional", ())),
    }


def _merge_bundle_categories(
    bundle: dict[str, list[str]],
    *,
    required: Iterable[Any] | None = None,
    critical: Iterable[Any] | None = None,
    optional: Iterable[Any] | None = None,
) -> dict[str, list[str]]:
    next_required = normalize_categories(list(bundle.get("required", [])) + list(required or []))
    next_critical = normalize_categories(list(bundle.get("critical", [])) + list(critical or []))
    next_optional = [
        category
        for category in normalize_categories(list(bundle.get("optional", [])) + list(optional or []))
        if category not in set(next_required)
    ]
    return {
        "required": next_required,
        "critical": next_critical,
        "optional": next_optional,
    }


def _scope_should_include_section8(
    *,
    city: str | None,
    county: str | None,
    pha_name: str | None,
    include_section8: bool,
    tenant_waitlist_depth: str | None = None,
) -> bool:
    if pha_name and str(pha_name).strip():
        return True
    if not include_section8:
        return False
    normalized_city = (city or "").strip().lower()
    normalized_county = (county or "").strip().lower()
    normalized_waitlist = (tenant_waitlist_depth or "").strip().lower()
    return (
        normalized_waitlist in {"medium", "high", "very_high"}
        or normalized_city in _SECTION8_MARKET_CITIES
        or normalized_county in _SECTION8_MARKET_COUNTIES
    )


def infer_jurisdiction_types(
    *,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    include_section8: bool = True,
    tenant_waitlist_depth: str | None = None,
) -> list[str]:
    types: list[str] = []
    if state:
        types.append(JURISDICTION_TYPE_STATE)
    if county:
        types.append(JURISDICTION_TYPE_COUNTY)
    if city:
        types.append(JURISDICTION_TYPE_CITY)
    if _scope_should_include_section8(
        city=city,
        county=county,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    ):
        types.append(JURISDICTION_TYPE_SECTION8_OVERLAY)
    return types


def expected_rule_universe_for_scope(
    *,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    include_section8: bool = True,
    tenant_waitlist_depth: str | None = None,
) -> JurisdictionExpectedRuleUniverse:
    jurisdiction_types = infer_jurisdiction_types(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    )

    category_bundles: dict[str, dict[str, list[str]]] = {}
    for jurisdiction_type in jurisdiction_types:
        category_bundles[jurisdiction_type] = expected_categories_for_jurisdiction_type(jurisdiction_type)

    normalized_city = (city or "").strip().lower()
    if normalized_city in _LEAD_FOCUSED_CITIES and JURISDICTION_TYPE_CITY in category_bundles:
        category_bundles[JURISDICTION_TYPE_CITY] = _merge_bundle_categories(
            category_bundles[JURISDICTION_TYPE_CITY],
            required=[CATEGORY_LEAD, CATEGORY_PERMITS],
            critical=[CATEGORY_LEAD],
        )

    if city and JURISDICTION_TYPE_CITY in category_bundles:
        category_bundles[JURISDICTION_TYPE_CITY] = _merge_bundle_categories(
            category_bundles[JURISDICTION_TYPE_CITY],
            required=[CATEGORY_OCCUPANCY],
        )

    if county and JURISDICTION_TYPE_COUNTY in category_bundles:
        category_bundles[JURISDICTION_TYPE_COUNTY] = _merge_bundle_categories(
            category_bundles[JURISDICTION_TYPE_COUNTY],
            optional=[CATEGORY_TAX],
        )

    if pha_name and JURISDICTION_TYPE_SECTION8_OVERLAY in category_bundles:
        category_bundles[JURISDICTION_TYPE_SECTION8_OVERLAY] = _merge_bundle_categories(
            category_bundles[JURISDICTION_TYPE_SECTION8_OVERLAY],
            required=[CATEGORY_SECTION8, CATEGORY_PROGRAM_OVERLAY, CATEGORY_CONTACTS],
            critical=[CATEGORY_SECTION8, CATEGORY_PROGRAM_OVERLAY],
        )

    required: list[str] = []
    critical: list[str] = []
    optional: list[str] = []
    for jurisdiction_type in jurisdiction_types:
        bundle = category_bundles.get(jurisdiction_type, {})
        required.extend(bundle.get("required", []))
        critical.extend(bundle.get("critical", []))
        optional.extend(bundle.get("optional", []))

    required_norm = normalize_categories(required)
    critical_norm = normalize_categories(critical)
    optional_norm = [category for category in normalize_categories(optional) if category not in set(required_norm)]
    inventory = {category: _RULE_FAMILY_INVENTORY[category].to_dict() for category in required_norm + optional_norm if category in _RULE_FAMILY_INVENTORY}

    normalized_bundles = {
        key: {
            "required": normalize_categories(value.get("required", [])),
            "critical": normalize_categories(value.get("critical", [])),
            "optional": normalize_categories(value.get("optional", [])),
        }
        for key, value in category_bundles.items()
    }

    return JurisdictionExpectedRuleUniverse(
        jurisdiction_types=jurisdiction_types,
        required_categories=required_norm,
        critical_categories=critical_norm,
        optional_categories=optional_norm,
        category_bundles=normalized_bundles,
        tier_order=list(jurisdiction_types),
        rule_family_inventory=inventory,
        legally_binding_categories=legally_binding_categories(required_norm + optional_norm),
        operational_heuristic_categories=operational_heuristic_categories(required_norm + optional_norm),
        authority_expectations=authority_expectations_for_categories(required_norm + optional_norm),
        property_proof_required_categories=property_proof_required_categories(required_norm + optional_norm),
        family_bundles=family_bundles_for_jurisdiction_types(jurisdiction_types),
    )


def get_required_categories(
    *,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    rental_license_required: bool | None = None,
    inspection_authority: str | None = None,
    inspection_frequency: str | None = None,
    tenant_waitlist_depth: str | None = None,
    include_section8: bool = True,
    include_documents: bool = True,
    include_fees: bool = True,
) -> list[str]:
    universe = expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    )
    required: list[str] = list(universe.required_categories)

    normalized_freq = (inspection_frequency or "").strip().lower()
    if bool(rental_license_required):
        required.extend([CATEGORY_RENTAL_LICENSE, CATEGORY_REGISTRATION])
    if inspection_authority or normalized_freq in {"annual", "biennial", "periodic", "complaint"}:
        required.append(CATEGORY_INSPECTION)
    if include_documents:
        required.append(CATEGORY_DOCUMENTS)
    if include_fees:
        required.append(CATEGORY_FEES)
    return normalize_categories(required)


def get_critical_categories(
    *,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    include_section8: bool = True,
    tenant_waitlist_depth: str | None = None,
) -> list[str]:
    return expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    ).critical_categories


def get_optional_categories(
    *,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    include_section8: bool = True,
    tenant_waitlist_depth: str | None = None,
) -> list[str]:
    return expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    ).optional_categories


def get_category_bundle_for_scope(
    *,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    include_section8: bool = True,
    tenant_waitlist_depth: str | None = None,
) -> dict[str, dict[str, list[str]]]:
    return expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    ).category_bundles


def required_categories_for_market(
    *,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    include_section8: bool = True,
    tenant_waitlist_depth: str | None = None,
) -> list[str]:
    return expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    ).required_categories


def required_categories_for_city(
    city: str | None,
    *,
    state: str | None = None,
    county: str | None = None,
    pha_name: str | None = None,
    include_section8: bool = True,
    tenant_waitlist_depth: str | None = None,
) -> list[str]:
    return required_categories_for_market(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    )


def compute_confidence_from_missing(required_categories: Iterable[Any] | None, covered_categories: Iterable[Any] | None) -> str:
    required = normalize_categories(required_categories)
    covered = normalize_categories(covered_categories)
    if not required:
        return "high"
    matched = len(set(required).intersection(set(covered)))
    ratio = matched / float(max(1, len(required)))
    if ratio >= 0.8:
        return "high"
    if ratio >= 0.5:
        return "medium"
    return "low"


def compute_completeness_score(required_categories: Iterable[Any] | None, covered_categories: Iterable[Any] | None) -> JurisdictionCategoryCoverage:
    required = normalize_categories(required_categories)
    covered = normalize_categories(covered_categories)
    required_set = set(required)
    covered_set = set(covered)
    missing = [category for category in required if category not in covered_set]
    category_statuses = {category: ("covered" if category in covered_set else "missing") for category in required}

    if not required:
        score = 1.0
        status = "complete"
    else:
        matched = len(required_set.intersection(covered_set))
        score = matched / float(len(required_set))
        if score >= 0.999:
            status = "complete"
        elif score > 0.0:
            status = "partial"
        else:
            status = "missing"

    confidence = compute_confidence_from_missing(required, covered)
    return JurisdictionCategoryCoverage(
        required_categories=required,
        covered_categories=[category for category in required if category in covered_set],
        missing_categories=missing,
        completeness_score=float(round(score, 6)),
        completeness_status=status,
        coverage_confidence=confidence,
        stale_categories=[],
        inferred_categories=[],
        conflicting_categories=[],
        category_statuses=category_statuses,
    )


def compute_category_score_from_statuses(*, required_categories: Iterable[Any] | None, category_statuses: Mapping[str, Any] | None) -> JurisdictionCategoryCoverage:
    required = normalize_categories(required_categories)
    status_map = category_statuses or {}
    normalized_statuses: dict[str, str] = {}
    covered: list[str] = []
    missing: list[str] = []
    stale: list[str] = []
    inferred: list[str] = []
    conflicting: list[str] = []

    if not required:
        return JurisdictionCategoryCoverage(
            required_categories=[],
            covered_categories=[],
            missing_categories=[],
            completeness_score=1.0,
            completeness_status="complete",
            coverage_confidence="high",
            stale_categories=[],
            inferred_categories=[],
            conflicting_categories=[],
            category_statuses={},
        )

    total = 0.0
    for category in required:
        raw_status = status_map.get(category, "missing")
        normalized = str(raw_status or "missing").strip().lower() or "missing"
        normalized_statuses[category] = normalized
        total += category_score_for_status(normalized)

        if normalized in {"covered", "verified", "fresh"}:
            covered.append(category)
        elif normalized == "stale":
            stale.append(category)
        elif normalized in {"inferred", "partial", "conditional"}:
            inferred.append(category)
        elif normalized == "conflicting":
            conflicting.append(category)
        else:
            missing.append(category)

    score = total / float(len(required))
    if not missing and not stale and not inferred and not conflicting:
        status = "complete"
    elif score <= 0.0:
        status = "missing"
    elif conflicting:
        status = "conflicting"
    elif stale:
        status = "stale"
    else:
        status = "partial"

    return JurisdictionCategoryCoverage(
        required_categories=required,
        covered_categories=covered,
        missing_categories=missing,
        completeness_score=float(round(score, 6)),
        completeness_status=status,
        coverage_confidence=completeness_confidence_label(score),
        stale_categories=stale,
        inferred_categories=inferred,
        conflicting_categories=conflicting,
        category_statuses=normalized_statuses,
    )


def compute_tier_coverage(
    *,
    covered_categories: Iterable[Any] | None = None,
    category_statuses: Mapping[str, Any] | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    include_section8: bool = True,
    tenant_waitlist_depth: str | None = None,
) -> list[JurisdictionTierCoverage]:
    covered = set(normalize_categories(covered_categories))
    raw_statuses = category_statuses or {}
    normalized_statuses: dict[str, str] = {}
    for key, value in raw_statuses.items():
        normalized_key = normalize_category(key)
        if not normalized_key:
            continue
        normalized_statuses[normalized_key] = str(value or "missing").strip().lower() or "missing"

    if not normalized_statuses:
        normalized_statuses = {category: ("covered" if category in covered else "missing") for category in normalize_categories(covered_categories)}

    universe = expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    )

    tier_rows: list[JurisdictionTierCoverage] = []
    for jurisdiction_type in universe.jurisdiction_types:
        bundle = universe.category_bundles.get(jurisdiction_type, {})
        required = normalize_categories(bundle.get("required", []))
        critical = normalize_categories(bundle.get("critical", []))
        optional = normalize_categories(bundle.get("optional", []))
        required_statuses = {category: normalized_statuses.get(category, "missing") for category in required}
        critical_statuses = {category: required_statuses.get(category, normalized_statuses.get(category, "missing")) for category in critical}
        optional_statuses = {category: normalized_statuses.get(category, "missing") for category in optional}

        satisfied_required = [category for category, status in required_statuses.items() if status in _TIER_SATISFIED_CATEGORY_STATUSES]
        missing_required = [category for category, status in required_statuses.items() if status == "missing"]
        stale_required = [category for category, status in required_statuses.items() if status == "stale"]
        conflicting_required = [category for category, status in required_statuses.items() if status == "conflicting"]
        inferred_required = [category for category, status in required_statuses.items() if status in _TIER_INFERRED_CATEGORY_STATUSES]
        unsatisfied_required = [category for category, status in required_statuses.items() if status in _TIER_BLOCKING_CATEGORY_STATUSES]
        missing_critical = [category for category, status in critical_statuses.items() if status in _TIER_BLOCKING_CATEGORY_STATUSES]
        ratio = len(satisfied_required) / float(len(required) or 1)

        status_counts: dict[str, int] = {}
        for status in required_statuses.values():
            status_counts[status] = status_counts.get(status, 0) + 1

        blocking_statuses = sorted({status for status in required_statuses.values() if status in _TIER_BLOCKING_CATEGORY_STATUSES})
        tier_rows.append(
            JurisdictionTierCoverage(
                jurisdiction_type=jurisdiction_type,
                required_categories=required,
                critical_categories=critical,
                optional_categories=optional,
                covered_required_categories=satisfied_required,
                missing_required_categories=missing_required,
                missing_critical_categories=missing_critical,
                completeness_ratio=float(round(ratio, 6)),
                complete=not bool(unsatisfied_required),
                required_category_statuses=required_statuses,
                critical_category_statuses=critical_statuses,
                inferred_required_categories=inferred_required,
                stale_required_categories=stale_required,
                conflicting_required_categories=conflicting_required,
                satisfied_required_categories=satisfied_required,
                unsatisfied_required_categories=unsatisfied_required,
                optional_category_statuses=optional_statuses,
                status_counts=status_counts,
                blocking_statuses=blocking_statuses,
            )
        )
    return tier_rows


def category_coverage_from_rule_keys(*, verified_rule_keys: Iterable[Any] | None, conditional_rule_keys: Iterable[Any] | None = None, required_categories: Iterable[Any] | None = None) -> dict[str, str]:
    verified = {normalize_rule_category(value) for value in (verified_rule_keys or [])}
    conditional = {normalize_rule_category(value) for value in (conditional_rule_keys or [])}
    verified.discard(CATEGORY_UNCATEGORIZED)
    conditional.discard(CATEGORY_UNCATEGORIZED)

    output: dict[str, str] = {}
    for category in normalize_categories(required_categories):
        if category in verified:
            output[category] = "verified"
        elif category in conditional:
            output[category] = "conditional"
        else:
            output[category] = "missing"
    return output
