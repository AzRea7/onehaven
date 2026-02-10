from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import json

from ..models import JurisdictionRule


@dataclass(frozen=True)
class JurisdictionFriction:
    multiplier: float
    reasons: list[str]


def compute_friction(jr: Optional[JurisdictionRule]) -> JurisdictionFriction:
    if jr is None:
        return JurisdictionFriction(multiplier=1.0, reasons=["No jurisdiction rules found (neutral)."])

    mult = 1.0
    reasons: list[str] = []

    if jr.rental_license_required:
        mult -= 0.05
        reasons.append("Rental license required (admin friction).")

    freq = (getattr(jr, "inspection_frequency", None) or "").strip().lower()
    if freq == "annual":
        mult -= 0.05
        reasons.append("Annual inspection cadence (higher compliance friction).")
    elif freq == "biennial":
        mult -= 0.02
        reasons.append("Biennial inspection cadence (moderate compliance friction).")
    elif freq == "complaint":
        reasons.append("Complaint-based inspections (lower recurring friction).")

    if jr.processing_days is not None:
        if jr.processing_days >= 45:
            mult -= 0.10
            reasons.append(f"Long processing (~{jr.processing_days} days).")
        elif jr.processing_days >= 21:
            mult -= 0.05
            reasons.append(f"Moderate processing (~{jr.processing_days} days).")

    try:
        fps = json.loads(jr.typical_fail_points_json) if jr.typical_fail_points_json else []
        if isinstance(fps, list) and len(fps) >= 6:
            mult -= 0.05
            reasons.append("Many typical fail points (reinspect likelihood).")
    except Exception:
        reasons.append("Typical fail points not parseable (unknown).")

    w = (jr.tenant_waitlist_depth or "").strip().lower()
    if any(tok in w for tok in ["closed", "not accepting", "stopped"]):
        mult -= 0.10
        reasons.append("Tenant waitlist closed/restricted (rent-flow delay risk).")
    elif any(tok in w for tok in ["deep", "long", "months", "year"]):
        mult -= 0.05
        reasons.append("Tenant waitlist deep/slow (rent-flow delay risk).")

    # clamp
    if mult < 0.70:
        mult = 0.70
    if mult > 1.05:
        mult = 1.05

    return JurisdictionFriction(multiplier=mult, reasons=reasons or ["Computed jurisdiction friction."])
