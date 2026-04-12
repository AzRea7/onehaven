# backend/app/domain/jurisdiction_defaults.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from .jurisdiction_categories import (
    expected_rule_universe_for_scope,
    get_required_categories,
)


DEFAULT_COMPLETENESS_WEIGHTS: dict[str, float] = {
    "coverage": 0.35,
    "freshness": 0.20,
    "authority": 0.15,
    "extraction": 0.15,
    "governance": 0.15,
}

DEFAULT_COMPLETENESS_THRESHOLDS: dict[str, float] = {
    "authoritative_source": 0.65,
    "extraction_confidence": 0.65,
    "citation_quality": 0.55,
    "governance_quality": 0.70,
    "freshness": 0.60,
    "conflict_block": 0.50,
}

DEFAULT_CATEGORY_STATUS_WEIGHTS: dict[str, float] = {
    "covered": 1.0,
    "partial": 0.60,
    "stale": 0.45,
    "inferred": 0.35,
    "conflicting": 0.15,
    "missing": 0.0,
}

DEFAULT_STALE_DAYS = 90

DEFAULT_DISCOVERY_MAX_RETRIES = 3
DEFAULT_DISCOVERY_RETRY_BACKOFF_HOURS = 24
DEFAULT_TRUST_MIN_COMPLETENESS_SCORE = 0.80
DEFAULT_TRUST_LOW_CONFIDENCE_THRESHOLD = 0.60
DEFAULT_TRUST_READY_CONFIDENCE_THRESHOLD = 0.85


@dataclass(frozen=True)
class JurisdictionDefault:
    city: str
    state: str = "MI"

    # Operational baseline fields
    rental_license_required: bool = False
    inspection_authority: str | None = None
    inspection_frequency: str | None = None
    typical_fail_points: list[str] | None = None
    registration_fee: float | None = None
    processing_days: int | None = None
    tenant_waitlist_depth: str | None = None
    notes: str | None = None

    # Coverage / trust fields
    county: str | None = None
    housing_authority: str | None = None
    coverage_confidence: str = "medium"
    source_evidence: list[dict[str, Any]] | None = None
    missing_local_rule_areas: list[str] | None = None
    stale_warning: bool = False
    default_layer: str = "statewide_baseline"

    # New bootstrap defaults
    discovery_search_hints: dict[str, Any] | None = None
    trust_defaults: dict[str, Any] | None = None
    freshness_defaults: dict[str, Any] | None = None

    def to_row_kwargs(self) -> Dict[str, Any]:
        import json

        return {
            "city": self.city,
            "state": self.state,
            "rental_license_required": bool(self.rental_license_required),
            "inspection_authority": self.inspection_authority,
            "inspection_frequency": self.inspection_frequency,
            "typical_fail_points_json": json.dumps(self.typical_fail_points or [], sort_keys=True),
            "registration_fee": self.registration_fee,
            "processing_days": self.processing_days,
            "tenant_waitlist_depth": self.tenant_waitlist_depth,
            "notes": self.notes,
        }

    def expected_rule_universe(self, *, include_section8: bool = True) -> dict[str, Any]:
        return expected_rule_universe_for_scope(
            state=self.state,
            county=self.county,
            city=self.city,
            pha_name=self.housing_authority,
            include_section8=include_section8,
            tenant_waitlist_depth=self.tenant_waitlist_depth,
        ).to_dict()

    def required_categories(self, *, include_section8: bool = True) -> list[str]:
        return get_required_categories(
            state=self.state,
            county=self.county,
            city=self.city,
            rental_license_required=self.rental_license_required,
            inspection_authority=self.inspection_authority,
            inspection_frequency=self.inspection_frequency,
            tenant_waitlist_depth=self.tenant_waitlist_depth,
            include_section8=include_section8,
        )

    def default_discovery_search_hints(self) -> dict[str, Any]:
        if isinstance(self.discovery_search_hints, dict) and self.discovery_search_hints:
            return dict(self.discovery_search_hints)

        scope_terms = [term for term in [self.city, self.county, self.state] if term]
        base_terms = [
            "rental license",
            "rental registration",
            "inspection",
            "certificate of occupancy",
            "housing code",
        ]
        if self.rental_license_required:
            base_terms.append("landlord license")
        if self.housing_authority:
            base_terms.extend(
                [
                    self.housing_authority,
                    "section 8",
                    "housing choice voucher",
                    "payment standards",
                ]
            )
        if self.tenant_waitlist_depth in {"medium", "high"}:
            base_terms.append("voucher program")

        return {
            "scope_terms": scope_terms,
            "base_terms": base_terms,
            "preferred_source_kinds": [
                "municipal_code",
                "city_program_page",
                "housing_authority",
                "county_program_page",
                "state_program_page",
                "state_statute",
            ],
            "preferred_publishers": [
                self.city,
                f"{self.county.title()} County" if self.county else None,
                self.housing_authority,
                "State of Michigan",
                "Michigan Legislature",
            ],
            "seed_urls": [
                item.get("url")
                for item in (self.source_evidence or [])
                if isinstance(item, dict) and item.get("url")
            ],
        }

    def default_trust_defaults(self) -> dict[str, Any]:
        if isinstance(self.trust_defaults, dict) and self.trust_defaults:
            return dict(self.trust_defaults)

        return {
            "min_completeness_score_for_trust": DEFAULT_TRUST_MIN_COMPLETENESS_SCORE,
            "ready_confidence_threshold": DEFAULT_TRUST_READY_CONFIDENCE_THRESHOLD,
            "low_confidence_threshold": DEFAULT_TRUST_LOW_CONFIDENCE_THRESHOLD,
            "default_coverage_confidence": self.coverage_confidence,
            "requires_authoritative_sources_for_critical_categories": True,
            "block_on_critical_missing": True,
            "warn_on_critical_stale": True,
            "warn_on_inferred_critical_categories": True,
        }

    def default_freshness_defaults(self) -> dict[str, Any]:
        if isinstance(self.freshness_defaults, dict) and self.freshness_defaults:
            return dict(self.freshness_defaults)

        return {
            "stale_days": DEFAULT_STALE_DAYS,
            "max_discovery_retries": DEFAULT_DISCOVERY_MAX_RETRIES,
            "retry_backoff_hours": DEFAULT_DISCOVERY_RETRY_BACKOFF_HOURS,
            "require_last_verified_at_for_authoritative_sources": True,
            "freshness_warning_enabled": True,
        }

    def to_profile_policy(self) -> Dict[str, Any]:
        universe = self.expected_rule_universe(include_section8=True)
        discovery_hints = self.default_discovery_search_hints()
        trust_defaults = self.default_trust_defaults()
        freshness_defaults = self.default_freshness_defaults()
        return {
            "summary": f"{self.city}, {self.state} jurisdiction default baseline",
            "resolved_from": {
                "layer": self.default_layer,
                "state": self.state,
                "county": self.county,
                "city": self.city,
                "housing_authority": self.housing_authority,
            },
            "coverage": {
                "coverage_confidence": self.coverage_confidence,
                "missing_local_rule_areas": list(self.missing_local_rule_areas or []),
                "stale_warning": bool(self.stale_warning),
                "scoring_weights": completeness_score_weights(),
                "thresholds": completeness_scoring_thresholds(),
                "expected_rule_universe": universe,
                "required_categories": list(universe.get("required_categories", [])),
                "critical_categories": list(universe.get("critical_categories", [])),
                "optional_categories": list(universe.get("optional_categories", [])),
                "jurisdiction_types": list(universe.get("jurisdiction_types", [])),
            },
            "compliance": {
                "rental_license_required": "yes" if self.rental_license_required else "no",
                "inspection_required": "yes" if self.inspection_authority or self.inspection_frequency else "unknown",
                "inspection_authority": self.inspection_authority,
                "inspection_frequency": self.inspection_frequency,
            },
            "operations": {
                "processing_days": self.processing_days,
                "tenant_waitlist_depth": self.tenant_waitlist_depth,
                "registration_fee": self.registration_fee,
                "typical_fail_points": list(self.typical_fail_points or []),
            },
            "discovery": {
                "search_hints": discovery_hints,
                "bootstrap_enabled": True,
                "max_retries": freshness_defaults.get("max_discovery_retries"),
                "retry_backoff_hours": freshness_defaults.get("retry_backoff_hours"),
            },
            "trust": {
                "projection": trust_defaults,
                "freshness": freshness_defaults,
            },
            "freshness": {
                "policy_sources": freshness_defaults,
            },
            "source_evidence": list(self.source_evidence or []),
            "expected_rule_universe": universe,
            "notes": self.notes,
        }


def completeness_score_weights() -> dict[str, float]:
    return dict(DEFAULT_COMPLETENESS_WEIGHTS)


def completeness_scoring_thresholds() -> dict[str, float]:
    return dict(DEFAULT_COMPLETENESS_THRESHOLDS)


def completeness_status_weights() -> dict[str, float]:
    return dict(DEFAULT_CATEGORY_STATUS_WEIGHTS)


def completeness_scoring_defaults() -> dict[str, Any]:
    return {
        "weights": completeness_score_weights(),
        "thresholds": completeness_scoring_thresholds(),
        "category_status_weights": completeness_status_weights(),
        "stale_days": DEFAULT_STALE_DAYS,
    }


def expected_rule_universe_defaults_for_scope(
    *,
    state: str = "MI",
    county: str | None = None,
    city: str | None = None,
    housing_authority: str | None = None,
    include_section8: bool = True,
    tenant_waitlist_depth: str | None = None,
) -> dict[str, Any]:
    return expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=housing_authority,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    ).to_dict()


def michigan_global_defaults() -> List[JurisdictionDefault]:
    return [
        JurisdictionDefault(
            city="Detroit",
            county="wayne",
            rental_license_required=True,
            inspection_authority="City of Detroit",
            inspection_frequency="annual",
            typical_fail_points=[
                "GFCI missing",
                "handrails",
                "peeling paint",
                "smoke/CO detectors",
                "broken windows",
            ],
            processing_days=21,
            tenant_waitlist_depth="high",
            coverage_confidence="medium",
            missing_local_rule_areas=["program_overlay", "contacts"],
            source_evidence=[
                {
                    "layer": "statewide_baseline",
                    "label": "Michigan baseline operational default",
                    "strength": "baseline",
                }
            ],
            discovery_search_hints={
                "scope_terms": ["Detroit", "Wayne County", "Michigan"],
                "base_terms": [
                    "rental registration",
                    "rental inspection",
                    "certificate of compliance",
                    "landlord",
                    "housing code",
                ],
                "preferred_source_kinds": [
                    "municipal_code",
                    "city_program_page",
                    "city_form",
                    "housing_authority",
                    "state_statute",
                ],
                "preferred_publishers": [
                    "City of Detroit",
                    "Detroit Housing Commission",
                    "State of Michigan",
                ],
            },
            notes="Baseline default. Override per neighborhood, county, city, or housing authority if needed.",
        ),
        JurisdictionDefault(
            city="Pontiac",
            county="oakland",
            rental_license_required=True,
            inspection_authority="City of Pontiac",
            inspection_frequency="annual",
            typical_fail_points=[
                "GFCI missing",
                "peeling paint",
                "egress issues",
                "utilities not secured",
            ],
            processing_days=14,
            tenant_waitlist_depth="medium",
            coverage_confidence="medium",
            missing_local_rule_areas=["contacts"],
            source_evidence=[
                {
                    "layer": "statewide_baseline",
                    "label": "Michigan baseline operational default",
                    "strength": "baseline",
                }
            ],
            discovery_search_hints={
                "scope_terms": ["Pontiac", "Oakland County", "Michigan"],
                "base_terms": [
                    "rental registration",
                    "rental inspection",
                    "certificate of occupancy",
                    "landlord",
                ],
                "preferred_source_kinds": [
                    "municipal_code",
                    "city_program_page",
                    "city_form",
                    "state_statute",
                ],
                "preferred_publishers": [
                    "City of Pontiac",
                    "State of Michigan",
                ],
            },
            notes="Baseline default. Confirm local registration, fees, and housing authority overlays.",
        ),
        JurisdictionDefault(
            city="Southfield",
            county="oakland",
            rental_license_required=True,
            inspection_authority="City of Southfield",
            inspection_frequency="periodic",
            typical_fail_points=[
                "GFCI missing",
                "smoke/CO detectors",
                "handrails",
                "trip hazards",
            ],
            processing_days=14,
            tenant_waitlist_depth="medium",
            coverage_confidence="medium",
            missing_local_rule_areas=["program_overlay"],
            source_evidence=[
                {
                    "layer": "statewide_baseline",
                    "label": "Michigan baseline operational default",
                    "strength": "baseline",
                }
            ],
            discovery_search_hints={
                "scope_terms": ["Southfield", "Oakland County", "Michigan"],
                "base_terms": [
                    "rental inspection",
                    "rental certificate",
                    "landlord",
                    "property maintenance code",
                ],
                "preferred_source_kinds": [
                    "municipal_code",
                    "city_program_page",
                    "city_form",
                    "state_statute",
                ],
                "preferred_publishers": [
                    "City of Southfield",
                    "State of Michigan",
                ],
            },
            notes="Baseline default. Verify rental certification steps and any local overlay workflow.",
        ),
        JurisdictionDefault(
            city="Inkster",
            county="wayne",
            rental_license_required=True,
            inspection_authority="City of Inkster",
            inspection_frequency="annual",
            typical_fail_points=[
                "peeling paint",
                "broken windows",
                "missing detectors",
                "handrails",
                "GFCI missing",
            ],
            processing_days=21,
            tenant_waitlist_depth="high",
            coverage_confidence="low",
            missing_local_rule_areas=["fees", "contacts", "program_overlay"],
            source_evidence=[
                {
                    "layer": "statewide_baseline",
                    "label": "Michigan baseline operational default",
                    "strength": "baseline",
                }
            ],
            discovery_search_hints={
                "scope_terms": ["Inkster", "Wayne County", "Michigan"],
                "base_terms": [
                    "rental registration",
                    "rental inspection",
                    "certificate",
                    "housing code",
                ],
                "preferred_source_kinds": [
                    "municipal_code",
                    "city_program_page",
                    "county_program_page",
                    "state_statute",
                ],
                "preferred_publishers": [
                    "City of Inkster",
                    "State of Michigan",
                ],
            },
            trust_defaults={
                "min_completeness_score_for_trust": 0.85,
                "ready_confidence_threshold": 0.90,
                "low_confidence_threshold": 0.60,
                "default_coverage_confidence": "low",
                "requires_authoritative_sources_for_critical_categories": True,
                "block_on_critical_missing": True,
                "warn_on_critical_stale": True,
                "warn_on_inferred_critical_categories": True,
            },
            notes="Baseline default. Many older housing stock issues. Needs stronger local rule coverage.",
        ),
        JurisdictionDefault(
            city="Dearborn",
            county="wayne",
            rental_license_required=True,
            inspection_authority="City of Dearborn",
            inspection_frequency="periodic",
            typical_fail_points=[
                "handrails",
                "GFCI missing",
                "egress",
                "detectors",
            ],
            processing_days=10,
            tenant_waitlist_depth="medium",
            coverage_confidence="medium",
            missing_local_rule_areas=["program_overlay"],
            source_evidence=[
                {
                    "layer": "statewide_baseline",
                    "label": "Michigan baseline operational default",
                    "strength": "baseline",
                }
            ],
            discovery_search_hints={
                "scope_terms": ["Dearborn", "Wayne County", "Michigan"],
                "base_terms": [
                    "rental inspection",
                    "rental certificate",
                    "certificate of occupancy",
                    "housing code",
                ],
                "preferred_source_kinds": [
                    "municipal_code",
                    "city_program_page",
                    "city_form",
                    "state_statute",
                ],
                "preferred_publishers": [
                    "City of Dearborn",
                    "State of Michigan",
                ],
            },
            notes="Baseline default. Verify frequency by license type.",
        ),
        JurisdictionDefault(
            city="Warren",
            county="macomb",
            rental_license_required=True,
            inspection_authority="City of Warren",
            inspection_frequency="periodic",
            typical_fail_points=[
                "GFCI missing",
                "detectors",
                "handrails",
                "egress",
            ],
            processing_days=10,
            tenant_waitlist_depth="medium",
            coverage_confidence="high",
            missing_local_rule_areas=[],
            source_evidence=[
                {
                    "layer": "statewide_baseline",
                    "label": "Michigan baseline operational default",
                    "strength": "baseline",
                },
                {
                    "layer": "city",
                    "label": "Warren operational profile support",
                    "strength": "strong",
                },
            ],
            discovery_search_hints={
                "scope_terms": ["Warren", "Macomb County", "Michigan"],
                "base_terms": [
                    "rental inspections division",
                    "rental application paperwork",
                    "rental license",
                    "inspection",
                ],
                "preferred_source_kinds": [
                    "city_program_page",
                    "city_form",
                    "municipal_code",
                    "state_statute",
                ],
                "preferred_publishers": [
                    "City of Warren",
                    "State of Michigan",
                ],
            },
            trust_defaults={
                "min_completeness_score_for_trust": 0.75,
                "ready_confidence_threshold": 0.85,
                "low_confidence_threshold": 0.60,
                "default_coverage_confidence": "high",
                "requires_authoritative_sources_for_critical_categories": True,
                "block_on_critical_missing": True,
                "warn_on_critical_stale": True,
                "warn_on_inferred_critical_categories": True,
            },
            notes="Baseline default. Warren should typically resolve with higher confidence because the current codebase already models Warren more deeply.",
        ),
        JurisdictionDefault(
            city="Royal Oak",
            county="oakland",
            rental_license_required=True,
            inspection_authority="City of Royal Oak",
            inspection_frequency="periodic",
            typical_fail_points=[
                "handrails",
                "GFCI missing",
                "smoke/CO detectors",
                "egress",
            ],
            processing_days=10,
            tenant_waitlist_depth="medium",
            coverage_confidence="medium",
            missing_local_rule_areas=["program_overlay", "contacts"],
            source_evidence=[
                {
                    "layer": "statewide_baseline",
                    "label": "Michigan baseline operational default",
                    "strength": "baseline",
                }
            ],
            discovery_search_hints={
                "scope_terms": ["Royal Oak", "Oakland County", "Michigan"],
                "base_terms": [
                    "rental license",
                    "rental inspection",
                    "certificate",
                    "housing code",
                ],
                "preferred_source_kinds": [
                    "municipal_code",
                    "city_program_page",
                    "city_form",
                    "state_statute",
                ],
                "preferred_publishers": [
                    "City of Royal Oak",
                    "State of Michigan",
                ],
            },
            notes="Baseline default. Verify frequency by rental license type.",
        ),
    ]


def jurisdiction_default_map() -> dict[tuple[str, str], JurisdictionDefault]:
    out: dict[tuple[str, str], JurisdictionDefault] = {}
    for item in michigan_global_defaults():
        out[(item.city.strip().lower(), item.state.strip().upper())] = item
    return out


def required_categories_for_city(
    city: str | None,
    state: str = "MI",
    *,
    include_section8: bool = True,
) -> list[str]:
    key = ((city or "").strip().lower(), (state or "MI").strip().upper())
    default = jurisdiction_default_map().get(key)

    if default is not None:
        return default.required_categories(include_section8=include_section8)

    return get_required_categories(
        state=state,
        city=city,
        include_section8=include_section8,
    )


def default_policy_for_scope(
    *,
    state: str = "MI",
    county: str | None = None,
    city: str | None = None,
    housing_authority: str | None = None,
    include_section8: bool = True,
) -> dict[str, Any]:
    key = ((city or "").strip().lower(), (state or "MI").strip().upper())
    default = jurisdiction_default_map().get(key) if city else None
    if default is not None:
        return default.to_profile_policy()

    universe = expected_rule_universe_defaults_for_scope(
        state=state,
        county=county,
        city=city,
        housing_authority=housing_authority,
        include_section8=include_section8,
    )
    missing_local_rule_areas = list(universe.get("required_categories", []))
    discovery_hints = {
        "scope_terms": [term for term in [city, county, state] if term],
        "base_terms": [
            "rental license",
            "rental registration",
            "inspection",
            "certificate of occupancy",
            "housing code",
        ],
        "preferred_source_kinds": [
            "municipal_code",
            "city_program_page",
            "county_program_page",
            "housing_authority",
            "state_statute",
        ],
        "preferred_publishers": [
            city,
            f"{county.title()} County" if county else None,
            housing_authority,
            "State of Michigan",
            "Michigan Legislature",
        ],
        "seed_urls": [],
    }
    trust_defaults = {
        "min_completeness_score_for_trust": DEFAULT_TRUST_MIN_COMPLETENESS_SCORE,
        "ready_confidence_threshold": DEFAULT_TRUST_READY_CONFIDENCE_THRESHOLD,
        "low_confidence_threshold": DEFAULT_TRUST_LOW_CONFIDENCE_THRESHOLD,
        "default_coverage_confidence": "low",
        "requires_authoritative_sources_for_critical_categories": True,
        "block_on_critical_missing": True,
        "warn_on_critical_stale": True,
        "warn_on_inferred_critical_categories": True,
    }
    freshness_defaults = {
        "stale_days": DEFAULT_STALE_DAYS,
        "max_discovery_retries": DEFAULT_DISCOVERY_MAX_RETRIES,
        "retry_backoff_hours": DEFAULT_DISCOVERY_RETRY_BACKOFF_HOURS,
        "require_last_verified_at_for_authoritative_sources": True,
        "freshness_warning_enabled": True,
    }
    return {
        "summary": f"{city or county or state} jurisdiction default baseline",
        "resolved_from": {
            "layer": "statewide_baseline",
            "state": state,
            "county": county,
            "city": city,
            "housing_authority": housing_authority,
        },
        "coverage": {
            "coverage_confidence": "low",
            "missing_local_rule_areas": missing_local_rule_areas,
            "stale_warning": False,
            "scoring_weights": completeness_score_weights(),
            "thresholds": completeness_scoring_thresholds(),
            "expected_rule_universe": universe,
            "required_categories": list(universe.get("required_categories", [])),
            "critical_categories": list(universe.get("critical_categories", [])),
            "optional_categories": list(universe.get("optional_categories", [])),
            "jurisdiction_types": list(universe.get("jurisdiction_types", [])),
        },
        "compliance": {
            "rental_license_required": "unknown",
            "inspection_required": "unknown",
            "inspection_authority": None,
            "inspection_frequency": None,
        },
        "operations": {},
        "discovery": {
            "search_hints": discovery_hints,
            "bootstrap_enabled": True,
            "max_retries": freshness_defaults.get("max_discovery_retries"),
            "retry_backoff_hours": freshness_defaults.get("retry_backoff_hours"),
        },
        "trust": {
            "projection": trust_defaults,
            "freshness": freshness_defaults,
        },
        "freshness": {
            "policy_sources": freshness_defaults,
        },
        "source_evidence": [],
        "expected_rule_universe": universe,
        "notes": "No explicit jurisdiction default exists yet.",
    }


def default_policy_for_city(
    city: str | None,
    state: str = "MI",
) -> dict[str, Any]:
    return default_policy_for_scope(state=state, city=city)


def defaults_for_michigan() -> List[JurisdictionDefault]:
    return michigan_global_defaults()