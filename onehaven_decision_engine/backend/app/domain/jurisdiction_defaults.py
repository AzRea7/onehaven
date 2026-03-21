from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from .jurisdiction_categories import get_required_categories


@dataclass(frozen=True)
class JurisdictionDefault:
    city: str
    state: str = "MI"
    rental_license_required: bool = False
    inspection_authority: str | None = None
    inspection_frequency: str | None = None
    typical_fail_points: list[str] | None = None
    registration_fee: float | None = None
    processing_days: int | None = None
    tenant_waitlist_depth: str | None = None
    notes: str | None = None

    def to_row_kwargs(self) -> Dict[str, Any]:
        # Keep this method stable because other seeders/fixtures may rely on it.
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

    def required_categories(self, *, include_section8: bool = True) -> list[str]:
        """
        Operational category baseline for completeness scoring.

        This does not change existing seed behavior; it only exposes a stable
        domain helper that later services can use while deriving
        jurisdiction-profile completeness.
        """
        return get_required_categories(
            state=self.state,
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
            notes="Baseline default. Override per neighborhood/authority if needed.",
        ),
        JurisdictionDefault(
            city="Pontiac",
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
            notes="Baseline default. Confirm local registration/fees.",
        ),
        JurisdictionDefault(
            city="Southfield",
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
            notes="Baseline default. Verify rental certification steps.",
        ),
        JurisdictionDefault(
            city="Inkster",
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
            notes="Baseline default. Many older housing stock issues.",
        ),
        JurisdictionDefault(
            city="Dearborn",
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
            notes="Baseline default. Verify frequency by license type.",
        ),
        JurisdictionDefault(
            city="Warren",
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
            notes="Baseline default.",
        ),
        JurisdictionDefault(
            city="Royal Oak",
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