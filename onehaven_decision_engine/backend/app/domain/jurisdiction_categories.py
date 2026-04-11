# backend/app/domain/jurisdiction_categories.py
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping


# Canonical normalized categories used by completeness + coverage services.
# Keep these stable because they become persisted values in DB rows.
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
    CATEGORY_SECTION8: "Section 8",
    CATEGORY_ZONING: "Zoning",
    CATEGORY_TAX: "Tax",
    CATEGORY_UTILITIES: "Utilities",
    CATEGORY_SAFETY: "Safety",
    CATEGORY_REGISTRATION: "Registration",
    CATEGORY_LEAD: "Lead",
    CATEGORY_OCCUPANCY: "Occupancy / certificate",
    CATEGORY_PERMITS: "Permits",
    CATEGORY_PROGRAM_OVERLAY: "Program overlay",
    CATEGORY_DOCUMENTS: "Documents",
    CATEGORY_FEES: "Fees",
    CATEGORY_CONTACTS: "Contacts",
    CATEGORY_SOURCE_OF_INCOME: "Source of income",
}

# Loose aliases so policy extraction / legacy defaults can map into one stable set.
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
    "voucher": CATEGORY_SECTION8,
    "housing_choice_voucher": CATEGORY_SECTION8,
    "pha": CATEGORY_PROGRAM_OVERLAY,
    "housing_authority": CATEGORY_PROGRAM_OVERLAY,
    "overlay": CATEGORY_PROGRAM_OVERLAY,
    "program_overlay": CATEGORY_PROGRAM_OVERLAY,
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
        "required": (
            CATEGORY_SAFETY,
            CATEGORY_LEAD,
            CATEGORY_SOURCE_OF_INCOME,
            CATEGORY_PERMITS,
        ),
        "critical": (
            CATEGORY_SAFETY,
            CATEGORY_LEAD,
        ),
        "optional": (
            CATEGORY_ZONING,
            CATEGORY_TAX,
            CATEGORY_UTILITIES,
            CATEGORY_DOCUMENTS,
        ),
    },
    JURISDICTION_TYPE_COUNTY: {
        "required": (
            CATEGORY_REGISTRATION,
            CATEGORY_INSPECTION,
            CATEGORY_SAFETY,
            CATEGORY_PERMITS,
            CATEGORY_DOCUMENTS,
            CATEGORY_CONTACTS,
        ),
        "critical": (
            CATEGORY_INSPECTION,
            CATEGORY_SAFETY,
        ),
        "optional": (
            CATEGORY_OCCUPANCY,
            CATEGORY_FEES,
            CATEGORY_TAX,
            CATEGORY_UTILITIES,
            CATEGORY_ZONING,
        ),
    },
    JURISDICTION_TYPE_CITY: {
        "required": (
            CATEGORY_RENTAL_LICENSE,
            CATEGORY_REGISTRATION,
            CATEGORY_INSPECTION,
            CATEGORY_OCCUPANCY,
            CATEGORY_SAFETY,
            CATEGORY_PERMITS,
            CATEGORY_DOCUMENTS,
            CATEGORY_FEES,
            CATEGORY_CONTACTS,
        ),
        "critical": (
            CATEGORY_RENTAL_LICENSE,
            CATEGORY_REGISTRATION,
            CATEGORY_INSPECTION,
            CATEGORY_OCCUPANCY,
            CATEGORY_SAFETY,
        ),
        "optional": (
            CATEGORY_LEAD,
            CATEGORY_ZONING,
            CATEGORY_TAX,
            CATEGORY_UTILITIES,
            CATEGORY_SOURCE_OF_INCOME,
        ),
    },
    JURISDICTION_TYPE_SECTION8_OVERLAY: {
        "required": (
            CATEGORY_SECTION8,
            CATEGORY_PROGRAM_OVERLAY,
            CATEGORY_CONTACTS,
            CATEGORY_DOCUMENTS,
            CATEGORY_INSPECTION,
        ),
        "critical": (
            CATEGORY_SECTION8,
            CATEGORY_PROGRAM_OVERLAY,
            CATEGORY_CONTACTS,
        ),
        "optional": (
            CATEGORY_OCCUPANCY,
            CATEGORY_SOURCE_OF_INCOME,
            CATEGORY_SAFETY,
        ),
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
class JurisdictionExpectedRuleUniverse:
    jurisdiction_types: list[str]
    required_categories: list[str]
    critical_categories: list[str]
    optional_categories: list[str]
    category_bundles: dict[str, dict[str, list[str]]]

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


def expected_categories_for_jurisdiction_type(jurisdiction_type: str) -> dict[str, list[str]]:
    key = str(jurisdiction_type or "").strip().lower()
    raw = _EXPECTED_CATEGORY_BUNDLES.get(key, {})
    return {
        "required": normalize_categories(raw.get("required", ())),
        "critical": normalize_categories(raw.get("critical", ())),
        "optional": normalize_categories(raw.get("optional", ())),
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
    types: list[str] = [JURISDICTION_TYPE_STATE]
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
    required: list[str] = []
    critical: list[str] = []
    optional: list[str] = []

    for jurisdiction_type in jurisdiction_types:
        bundle = expected_categories_for_jurisdiction_type(jurisdiction_type)
        category_bundles[jurisdiction_type] = bundle
        required.extend(bundle["required"])
        critical.extend(bundle["critical"])
        optional.extend(bundle["optional"])

    normalized_city = (city or "").strip().lower()
    if normalized_city in _LEAD_FOCUSED_CITIES:
        required.extend([CATEGORY_LEAD, CATEGORY_PERMITS])
        critical.append(CATEGORY_LEAD)

    if city:
        required.append(CATEGORY_OCCUPANCY)

    if county:
        optional.append(CATEGORY_TAX)

    if pha_name:
        required.extend([CATEGORY_SECTION8, CATEGORY_PROGRAM_OVERLAY, CATEGORY_CONTACTS])
        critical.extend([CATEGORY_SECTION8, CATEGORY_PROGRAM_OVERLAY])

    optional = [category for category in normalize_categories(optional) if category not in set(normalize_categories(required))]

    return JurisdictionExpectedRuleUniverse(
        jurisdiction_types=jurisdiction_types,
        required_categories=normalize_categories(required),
        critical_categories=normalize_categories(critical),
        optional_categories=optional,
        category_bundles={
            key: {
                "required": normalize_categories(value.get("required", [])),
                "critical": normalize_categories(value.get("critical", [])),
                "optional": normalize_categories(value.get("optional", [])),
            }
            for key, value in category_bundles.items()
        },
    )


def get_required_categories(
    *,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
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


def compute_confidence_from_missing(
    required_categories: Iterable[Any] | None,
    covered_categories: Iterable[Any] | None,
) -> str:
    required = normalize_categories(required_categories)
    covered = normalize_categories(covered_categories)

    if not required:
        return "high"

    matched = len(set(required).intersection(set(covered)))
    ratio = matched / float(len(set(required)))

    if ratio >= 0.9:
        return "high"
    if ratio >= 0.5:
        return "medium"
    return "low"


def compute_completeness_score(
    required_categories: Iterable[Any] | None,
    covered_categories: Iterable[Any] | None,
) -> JurisdictionCategoryCoverage:
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


def compute_category_score_from_statuses(
    *,
    required_categories: Iterable[Any] | None,
    category_statuses: Mapping[str, Any] | None,
) -> JurisdictionCategoryCoverage:
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


def category_coverage_from_rule_keys(
    *,
    verified_rule_keys: Iterable[Any] | None,
    conditional_rule_keys: Iterable[Any] | None = None,
    required_categories: Iterable[Any] | None = None,
) -> dict[str, str]:
    verified = {normalize_rule_category(value) for value in (verified_rule_keys or [])}
    conditional = {normalize_rule_category(value) for value in (conditional_rule_keys or [])}
    verified.discard(CATEGORY_UNCATEGORIZED)
    conditional.discard(CATEGORY_UNCATEGORIZED)

    output: dict[str, str] = {}
    for raw_category in list(required_categories or []):
        original = str(raw_category).strip()
        normalized = normalize_category(raw_category)
        if normalized is None:
            output[original or str(raw_category)] = "missing"
            continue
        if normalized in verified:
            output[original or normalized] = "verified"
        elif normalized in conditional:
            output[original or normalized] = "conditional"
        else:
            output[original or normalized] = "missing"
    return output