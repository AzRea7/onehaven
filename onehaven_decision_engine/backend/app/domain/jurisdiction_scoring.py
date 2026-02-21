# onehaven_decision_engine/backend/app/domain/jurisdiction_scoring.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
import json

from ..models import JurisdictionRule


@dataclass(frozen=True)
class JurisdictionFrictionReason:
    rule_field: str
    input_value: Any
    weight: float
    delta: float
    text: str


@dataclass(frozen=True)
class JurisdictionFriction:
    multiplier: float
    reasons: list[str]                 # back-compat (human strings)
    reasons_trace: list[dict[str, Any]]  # new (structured)


def _push(
    trace: list[JurisdictionFrictionReason],
    *,
    rule_field: str,
    input_value: Any,
    weight: float,
    delta: float,
    text: str,
) -> None:
    trace.append(
        JurisdictionFrictionReason(
            rule_field=rule_field,
            input_value=input_value,
            weight=float(weight),
            delta=float(delta),
            text=text,
        )
    )


def compute_friction(jr: Optional[JurisdictionRule]) -> JurisdictionFriction:
    """
    Deterministic jurisdiction friction.

    Returns:
      - multiplier: float (<=1.0 generally)
      - reasons: list[str] (legacy)
      - reasons_trace: list[dict] (new)
    """
    trace: list[JurisdictionFrictionReason] = []

    if jr is None:
        _push(
            trace,
            rule_field="missing_rule",
            input_value=None,
            weight=1.0,
            delta=-0.05,
            text="No jurisdiction data for city/state → REVIEW bias (unknown compliance friction).",
        )
        mult = 0.95
        return JurisdictionFriction(
            multiplier=mult,
            reasons=[t.text for t in trace],
            reasons_trace=[t.__dict__ for t in trace],
        )

    mult = 1.0

    # License requirement
    if bool(jr.rental_license_required):
        delta = -0.05
        mult += delta
        _push(
            trace,
            rule_field="rental_license_required",
            input_value=True,
            weight=1.0,
            delta=delta,
            text="Rental license required (admin friction).",
        )

    # Inspection frequency
    freq = (getattr(jr, "inspection_frequency", None) or "").strip().lower()
    if freq == "annual":
        delta = -0.05
        mult += delta
        _push(
            trace,
            rule_field="inspection_frequency",
            input_value=freq,
            weight=1.0,
            delta=delta,
            text="Annual inspection cadence (higher compliance friction).",
        )
    elif freq == "biennial":
        delta = -0.02
        mult += delta
        _push(
            trace,
            rule_field="inspection_frequency",
            input_value=freq,
            weight=1.0,
            delta=delta,
            text="Biennial inspection cadence (moderate compliance friction).",
        )
    elif freq == "complaint":
        _push(
            trace,
            rule_field="inspection_frequency",
            input_value=freq,
            weight=1.0,
            delta=0.0,
            text="Complaint-based inspections (lower recurring friction).",
        )

    # Processing days
    pd = getattr(jr, "processing_days", None)
    if pd is not None:
        pdv = int(pd)
        if pdv >= 45:
            delta = -0.10
            mult += delta
            _push(
                trace,
                rule_field="processing_days",
                input_value=pdv,
                weight=1.0,
                delta=delta,
                text=f"Long processing (~{pdv} days).",
            )
        elif pdv >= 21:
            delta = -0.05
            mult += delta
            _push(
                trace,
                rule_field="processing_days",
                input_value=pdv,
                weight=1.0,
                delta=delta,
                text=f"Moderate processing (~{pdv} days).",
            )

    # Typical fail points count heuristic
    try:
        fps = json.loads(jr.typical_fail_points_json) if jr.typical_fail_points_json else []
        if isinstance(fps, list) and len(fps) >= 6:
            delta = -0.05
            mult += delta
            _push(
                trace,
                rule_field="typical_fail_points_json",
                input_value=len(fps),
                weight=1.0,
                delta=delta,
                text="Many typical fail points (reinspect likelihood).",
            )
    except Exception:
        _push(
            trace,
            rule_field="typical_fail_points_json",
            input_value="unparseable",
            weight=1.0,
            delta=0.0,
            text="Typical fail points not parseable (unknown).",
        )

    # Clamp (never negative / insane)
    mult = max(0.50, min(1.05, float(mult)))

    return JurisdictionFriction(
        multiplier=float(mult),
        reasons=[t.text for t in trace],
        reasons_trace=[t.__dict__ for t in trace],
    )