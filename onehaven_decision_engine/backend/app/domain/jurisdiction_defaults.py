from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from .jurisdiction_categories import get_required_categories


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

    # Chunk 5 governance / coverage fields
    county: str | None = None
    housing_authority: str | None = None
    coverage_confidence: str = "medium"
    source_evidence: list[dict[str, Any]] | None = None
    missing_local_rule_areas: list[str] | None = None
    stale_warning: bool = False
    default_layer: str = "statewide_baseline"

    def to_row_kwargs(self) -> Dict[str, Any]:
        """
        Keep this method stable because other seeders/fixtures may rely on it.
        Only include model-safe fields that are likely to exist on JurisdictionRule.
        """
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

    def to_profile_policy(self) -> Dict[str, Any]:
        """
        Rich operational payload used by the jurisdiction profile / coverage layer.
        This is not written directly to legacy JurisdictionRule rows.
        """
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
            "source_evidence": list(self.source_evidence or []),
            "notes": self.notes,
        }

    def required_categories(self, *, include_section8: bool = True) -> list[str]:
        """
        Operational category baseline for completeness scoring.

        This does not change existing seed behavior; it only exposes a stable
        domain helper that later services can use while deriving
        jurisdiction-profile completeness.
        """
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


def michigan_global_defaults() -> List[JurisdictionDefault]:
    """
    Boring + deterministic seed rules for Michigan.

    IMPORTANT:
    These are NOT "legal truth". They're an operational baseline that should be
    overridden by JurisdictionProfile governance data (policy_models) as you mature.
    """
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
            notes="Baseline default. Verify frequency by rental license type.",
        ),
    ]


def jurisdiction_default_map() -> dict[tuple[str, str], JurisdictionDefault]:
    """
    Useful for services that need deterministic city/state lookup without
    re-looping through the defaults list every time.
    """
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
    """
    Convenience helper for completeness services.

    Falls back to generic required-category logic when a city does not have an
    explicit operational default entry yet.
    """
    key = ((city or "").strip().lower(), (state or "MI").strip().upper())
    default = jurisdiction_default_map().get(key)

    if default is not None:
        return default.required_categories(include_section8=include_section8)

    return get_required_categories(
        state=state,
        city=city,
        include_section8=include_section8,
    )


def default_policy_for_city(
    city: str | None,
    state: str = "MI",
) -> dict[str, Any]:
    """
    Returns the richer baseline policy payload for a city if it exists.
    """
    key = ((city or "").strip().lower(), (state or "MI").strip().upper())
    default = jurisdiction_default_map().get(key)
    if default is None:
        return {
            "summary": f"{city or state} jurisdiction default baseline",
            "resolved_from": {
                "layer": "statewide_baseline",
                "state": state,
                "city": city,
            },
            "coverage": {
                "coverage_confidence": "low",
                "missing_local_rule_areas": ["inspection", "registration", "program_overlay"],
                "stale_warning": False,
            },
            "compliance": {
                "rental_license_required": "unknown",
                "inspection_required": "unknown",
                "inspection_authority": None,
                "inspection_frequency": None,
            },
            "operations": {},
            "source_evidence": [],
            "notes": "No explicit city default exists yet.",
        }
    return default.to_profile_policy()


# ------------------------------
# Backwards-compatible export
# ------------------------------
def defaults_for_michigan() -> List[JurisdictionDefault]:
    """
    Compatibility shim.

    Older/newer code paths import `defaults_for_michigan()`.
    Your service layer currently expects this exact name.
    """
    return michigan_global_defaults()