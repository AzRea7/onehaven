# backend/app/domain/jurisdiction_defaults.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any


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
        return {
            "city": self.city,
            "state": self.state,
            "rental_license_required": bool(self.rental_license_required),
            "inspection_authority": self.inspection_authority,
            "inspection_frequency": self.inspection_frequency,
            "typical_fail_points_json": (
                __import__("json").dumps(self.typical_fail_points or [], sort_keys=True)
            ),
            "registration_fee": self.registration_fee,
            "processing_days": self.processing_days,
            "tenant_waitlist_depth": self.tenant_waitlist_depth,
            "notes": self.notes,
        }


def michigan_global_defaults() -> List[JurisdictionDefault]:
    # Keep this list boring + deterministic. The “truth” can evolve, but changes should be explicit.
    return [
        JurisdictionDefault(
            city="Detroit",
            rental_license_required=True,
            inspection_authority="City of Detroit",
            inspection_frequency="annual",
            typical_fail_points=["GFCI missing", "handrails", "peeling paint", "smoke/CO detectors", "broken windows"],
            processing_days=21,
            tenant_waitlist_depth="high",
            notes="Baseline default. Override per neighborhood/authority if needed.",
        ),
        JurisdictionDefault(
            city="Pontiac",
            rental_license_required=True,
            inspection_authority="City of Pontiac",
            inspection_frequency="annual",
            typical_fail_points=["GFCI missing", "peeling paint", "egress issues", "utilities not secured"],
            processing_days=14,
            tenant_waitlist_depth="medium",
            notes="Baseline default. Confirm local registration/fees.",
        ),
        JurisdictionDefault(
            city="Southfield",
            rental_license_required=True,
            inspection_authority="City of Southfield",
            inspection_frequency="periodic",
            typical_fail_points=["GFCI missing", "smoke/CO detectors", "handrails", "trip hazards"],
            processing_days=14,
            tenant_waitlist_depth="medium",
            notes="Baseline default. Verify rental certification steps.",
        ),
        JurisdictionDefault(
            city="Inkster",
            rental_license_required=True,
            inspection_authority="City of Inkster",
            inspection_frequency="annual",
            typical_fail_points=["peeling paint", "broken windows", "missing detectors", "handrails", "GFCI missing"],
            processing_days=21,
            tenant_waitlist_depth="high",
            notes="Baseline default. Many older housing stock issues.",
        ),
        JurisdictionDefault(
            city="Dearborn",
            rental_license_required=True,
            inspection_authority="City of Dearborn",
            inspection_frequency="periodic",
            typical_fail_points=["handrails", "GFCI missing", "egress", "detectors"],
            processing_days=10,
            tenant_waitlist_depth="medium",
            notes="Baseline default. Verify frequency by license type.",
        ),
        JurisdictionDefault(
            city="Warren",
            rental_license_required=True,
            inspection_authority="City of Warren",
            inspection_frequency="periodic",
            typical_fail_points=["GFCI missing", "detectors", "handrails", "egress"],
            processing_days=10,
            tenant_waitlist_depth="medium",
            notes="Baseline default.",
        ),
        JurisdictionDefault(
            city="Royal Oak",
            rental_license_required=True,
            inspection_authority="City of Royal Oak",
            inspection_frequency="periodic",
            typical_fail_points=["handrails", "GFCI missing", "smoke/CO detectors", "egress"],
            processing_days=10,
            tenant_waitlist_depth="medium",
            notes="Baseline default. Verify frequency by rental license type.",
        ),
    ]