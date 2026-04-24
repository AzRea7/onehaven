from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

from onehaven_platform.backend.src.domain.policy.expected_universe import (
    PolicyExpectedUniverse,
    build_policy_expected_universe,
)


@dataclass(frozen=True)
class JurisdictionExpectedUniverseResolution:
    state: str | None
    county: str | None
    city: str | None
    pha_name: str | None
    include_section8: bool
    rental_license_required: bool | None
    program_type: str | None
    property_type: str | None
    expected_universe: PolicyExpectedUniverse

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["expected_universe"] = self.expected_universe.to_dict()
        return payload


def resolve_expected_universe_for_market(
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    include_section8: bool = True,
    rental_license_required: bool | None = None,
    program_type: str | None = None,
    property_type: str | None = None,
) -> JurisdictionExpectedUniverseResolution:
    universe = build_policy_expected_universe(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        rental_license_required=rental_license_required,
    )
    return JurisdictionExpectedUniverseResolution(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=bool(include_section8),
        rental_license_required=rental_license_required,
        program_type=program_type,
        property_type=property_type,
        expected_universe=universe,
    )


def expected_universe_for_source_scope(
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    program_type: str | None = None,
) -> PolicyExpectedUniverse:
    include_section8 = bool(pha_name or str(program_type or "").strip().lower() in {"section8", "hcv", "voucher"})
    return resolve_expected_universe_for_market(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        program_type=program_type,
    ).expected_universe
