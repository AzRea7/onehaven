from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable

from onehaven_platform.backend.src.domain.policy.categories import (
    CATEGORY_CONTACTS,
    CATEGORY_DOCUMENTS,
    CATEGORY_FEES,
    CATEGORY_INSPECTION,
    CATEGORY_LEAD,
    CATEGORY_OCCUPANCY,
    CATEGORY_PERMITS,
    CATEGORY_PROGRAM_OVERLAY,
    CATEGORY_REGISTRATION,
    CATEGORY_RENTAL_LICENSE,
    CATEGORY_SAFETY,
    CATEGORY_SECTION8,
    CATEGORY_SOURCE_OF_INCOME,
    CATEGORY_TAX,
    CATEGORY_UTILITIES,
    CATEGORY_ZONING,
    JURISDICTION_TYPE_CITY,
    JURISDICTION_TYPE_COUNTY,
    JURISDICTION_TYPE_SECTION8_OVERLAY,
    JURISDICTION_TYPE_STATE,
    authority_expectations_for_categories,
    authority_scope_for_categories,
    legally_binding_categories,
    normalize_categories,
    operational_heuristic_categories,
    property_proof_required_categories,
    required_source_families_for_categories,
    rule_family_inventory_dict,
)


@dataclass(frozen=True)
class ExpectedRuleFamily:
    family: str
    category: str
    tier: str
    source_scope: str
    authority_expectation: str | None
    critical: bool
    description: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PolicyExpectedUniverse:
    state: str | None
    county: str | None
    city: str | None
    pha_name: str | None
    include_section8: bool
    jurisdiction_types: list[str]
    tier_order: list[str]
    required_categories: list[str]
    critical_categories: list[str]
    optional_categories: list[str]
    required_categories_by_tier: dict[str, list[str]]
    expected_rules_by_category: dict[str, dict[str, Any]]
    expected_rule_families_by_tier: dict[str, list[dict[str, Any]]]
    expected_rule_families: list[dict[str, Any]]
    rule_family_inventory: dict[str, dict[str, Any]]
    legally_binding_categories: list[str]
    operational_heuristic_categories: list[str]
    property_proof_required_categories: list[str]
    authority_expectations: dict[str, str]
    authority_scope_by_category: dict[str, str]
    required_source_families_by_category: dict[str, list[str]]
    critical_source_families: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_BASE_FAMILIES_BY_CATEGORY: dict[str, tuple[str, ...]] = {
    CATEGORY_RENTAL_LICENSE: ("rental_license_required",),
    CATEGORY_REGISTRATION: ("rental_registration_required",),
    CATEGORY_INSPECTION: ("inspection_required", "inspection_program_exists"),
    CATEGORY_OCCUPANCY: ("certificate_required_before_occupancy", "occupancy_certificate_required"),
    CATEGORY_LEAD: ("lead_disclosure_required", "lead_clearance_required"),
    CATEGORY_PERMITS: ("permit_required_for_rehab",),
    CATEGORY_SOURCE_OF_INCOME: ("source_of_income_protected",),
    CATEGORY_SECTION8: (
        "federal_hcv_regulations_anchor",
        "federal_nspire_anchor",
        "pha_landlord_packet_required",
        "hap_contract_and_tenancy_addendum_required",
    ),
    CATEGORY_PROGRAM_OVERLAY: (
        "mi_statute_anchor",
        "mshda_program_anchor",
        "pha_admin_plan_anchor",
    ),
    CATEGORY_DOCUMENTS: ("document_reference",),
    CATEGORY_FEES: ("fee_schedule_reference",),
    CATEGORY_CONTACTS: ("pha_administrator_changed",),
    CATEGORY_SAFETY: ("inspection_required", "federal_nspire_anchor"),
    CATEGORY_ZONING: tuple(),
    CATEGORY_TAX: tuple(),
    CATEGORY_UTILITIES: tuple(),
}

_TIER_CATEGORY_BUNDLES: dict[str, dict[str, tuple[str, ...]]] = {
    JURISDICTION_TYPE_STATE: {
        "required": (CATEGORY_SAFETY, CATEGORY_LEAD, CATEGORY_SOURCE_OF_INCOME, CATEGORY_PERMITS),
        "critical": (CATEGORY_SAFETY, CATEGORY_LEAD, CATEGORY_SOURCE_OF_INCOME),
        "optional": (CATEGORY_TAX,),
    },
    JURISDICTION_TYPE_COUNTY: {
        "required": (CATEGORY_DOCUMENTS, CATEGORY_CONTACTS),
        "critical": tuple(),
        "optional": (CATEGORY_TAX,),
    },
    JURISDICTION_TYPE_CITY: {
        "required": (
            CATEGORY_RENTAL_LICENSE,
            CATEGORY_REGISTRATION,
            CATEGORY_INSPECTION,
            CATEGORY_OCCUPANCY,
            CATEGORY_SAFETY,
            CATEGORY_FEES,
            CATEGORY_CONTACTS,
        ),
        "critical": (
            CATEGORY_RENTAL_LICENSE,
            CATEGORY_INSPECTION,
            CATEGORY_OCCUPANCY,
            CATEGORY_SAFETY,
        ),
        "optional": (CATEGORY_PERMITS, CATEGORY_ZONING, CATEGORY_UTILITIES, CATEGORY_DOCUMENTS),
    },
    JURISDICTION_TYPE_SECTION8_OVERLAY: {
        "required": (CATEGORY_SECTION8, CATEGORY_PROGRAM_OVERLAY, CATEGORY_DOCUMENTS, CATEGORY_CONTACTS),
        "critical": (CATEGORY_SECTION8,),
        "optional": (CATEGORY_FEES,),
    },
}


def _norm_state(value: str | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().upper()
    return raw or None


def _norm_lower(value: str | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    return raw or None


def _norm_text(value: str | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    return raw or None


def _has_section8_overlay(*, county: str | None, city: str | None, pha_name: str | None, include_section8: bool) -> bool:
    if not include_section8:
        return False
    if _norm_text(pha_name):
        return True
    city_norm = _norm_lower(city)
    county_norm = _norm_lower(county)
    return city_norm in {
        "detroit", "pontiac", "inkster", "warren", "southfield", "dearborn",
        "royal oak", "hamtramck", "highland park",
    } or county_norm in {"wayne", "oakland", "macomb"}


def required_categories_by_tier_for_expected_universe(
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    include_section8: bool = True,
    rental_license_required: bool | None = None,
) -> dict[str, list[str]]:
    _ = state
    out: dict[str, list[str]] = {
        JURISDICTION_TYPE_STATE: list(_TIER_CATEGORY_BUNDLES[JURISDICTION_TYPE_STATE]["required"]),
    }
    if county:
        out[JURISDICTION_TYPE_COUNTY] = list(_TIER_CATEGORY_BUNDLES[JURISDICTION_TYPE_COUNTY]["required"])
    if city:
        city_required = list(_TIER_CATEGORY_BUNDLES[JURISDICTION_TYPE_CITY]["required"])
        if rental_license_required is False and CATEGORY_RENTAL_LICENSE in city_required:
            city_required.remove(CATEGORY_RENTAL_LICENSE)
        out[JURISDICTION_TYPE_CITY] = city_required
    if _has_section8_overlay(county=county, city=city, pha_name=pha_name, include_section8=include_section8):
        out[JURISDICTION_TYPE_SECTION8_OVERLAY] = list(_TIER_CATEGORY_BUNDLES[JURISDICTION_TYPE_SECTION8_OVERLAY]["required"])
    return out


def expected_rule_families_by_tier(
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    include_section8: bool = True,
    rental_license_required: bool | None = None,
) -> dict[str, list[ExpectedRuleFamily]]:
    required_by_tier = required_categories_by_tier_for_expected_universe(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        rental_license_required=rental_license_required,
    )
    all_required = [c for cats in required_by_tier.values() for c in cats]
    authority_expectations = authority_expectations_for_categories(all_required)
    authority_scopes = authority_scope_for_categories(all_required)
    critical = set()
    for tier, bundle in _TIER_CATEGORY_BUNDLES.items():
        if tier in required_by_tier:
            critical.update(bundle["critical"])

    output: dict[str, list[ExpectedRuleFamily]] = {}
    for tier, categories in required_by_tier.items():
        families: list[ExpectedRuleFamily] = []
        for category in categories:
            base_families = _BASE_FAMILIES_BY_CATEGORY.get(category, tuple())
            if not base_families:
                base_families = (f"{category}_anchor",)
            for family in base_families:
                families.append(
                    ExpectedRuleFamily(
                        family=family,
                        category=category,
                        tier=tier,
                        source_scope=authority_scopes.get(category, "local"),
                        authority_expectation=authority_expectations.get(category),
                        critical=category in critical,
                        description=f"Expected truth family for {category.replace('_', ' ')} in {tier}.",
                    )
                )
        output[tier] = families
    return output


def build_policy_expected_universe(
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    include_section8: bool = True,
    rental_license_required: bool | None = None,
) -> PolicyExpectedUniverse:
    state_norm = _norm_state(state)
    county_norm = _norm_lower(county)
    city_norm = _norm_lower(city)
    pha_norm = _norm_text(pha_name)

    required_by_tier = required_categories_by_tier_for_expected_universe(
        state=state_norm,
        county=county_norm,
        city=city_norm,
        pha_name=pha_norm,
        include_section8=include_section8,
        rental_license_required=rental_license_required,
    )
    tier_order = list(required_by_tier.keys())
    required_categories = normalize_categories([c for cats in required_by_tier.values() for c in cats])

    critical_categories: list[str] = []
    optional_categories: list[str] = []
    seen_critical: set[str] = set()
    seen_optional: set[str] = set()
    for tier in tier_order:
        bundle = _TIER_CATEGORY_BUNDLES[tier]
        for category in bundle["critical"]:
            if category in required_categories and category not in seen_critical:
                seen_critical.add(category)
                critical_categories.append(category)
        for category in bundle["optional"]:
            if category not in seen_optional:
                seen_optional.add(category)
                optional_categories.append(category)

    authority_expectations = authority_expectations_for_categories(required_categories + optional_categories)
    authority_scopes = authority_scope_for_categories(required_categories + optional_categories)
    required_source_families = required_source_families_for_categories(required_categories + optional_categories)
    families_by_tier = expected_rule_families_by_tier(
        state=state_norm,
        county=county_norm,
        city=city_norm,
        pha_name=pha_norm,
        include_section8=include_section8,
        rental_license_required=rental_license_required,
    )
    flattened = [item.to_dict() for tier in tier_order for item in families_by_tier.get(tier, [])]

    expected_rules_by_category: dict[str, dict[str, Any]] = {}
    for category in required_categories:
        expected_rules_by_category[category] = {
            "category": category,
            "authority_expectation": authority_expectations.get(category),
            "authority_scope": authority_scopes.get(category),
            "required_source_families": list(required_source_families.get(category, [])),
            "expected_rule_families": [row["family"] for row in flattened if row["category"] == category],
            "critical": category in critical_categories,
        }

    critical_source_families = sorted(
        {
            fam
            for category in critical_categories
            for fam in required_source_families.get(category, [])
        }
    )

    return PolicyExpectedUniverse(
        state=state_norm,
        county=county_norm,
        city=city_norm,
        pha_name=pha_norm,
        include_section8=bool(include_section8),
        jurisdiction_types=tier_order,
        tier_order=tier_order,
        required_categories=required_categories,
        critical_categories=critical_categories,
        optional_categories=optional_categories,
        required_categories_by_tier=required_by_tier,
        expected_rules_by_category=expected_rules_by_category,
        expected_rule_families_by_tier={k: [row.to_dict() for row in v] for k, v in families_by_tier.items()},
        expected_rule_families=flattened,
        rule_family_inventory=rule_family_inventory_dict(),
        legally_binding_categories=legally_binding_categories(),
        operational_heuristic_categories=operational_heuristic_categories(),
        property_proof_required_categories=property_proof_required_categories(),
        authority_expectations=authority_expectations,
        authority_scope_by_category=authority_scopes,
        required_source_families_by_category=required_source_families,
        critical_source_families=critical_source_families,
    )
