from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional
import json

from ..models import JurisdictionRule
from ..policy_models import JurisdictionProfile
from .jurisdiction_categories import compute_completeness_score, normalize_categories


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
    reasons: list[str]  # back-compat (human strings)
    reasons_trace: list[dict[str, Any]]  # new (structured)


@dataclass(frozen=True)
class JurisdictionCompleteness:
    completeness_score: float
    completeness_status: str
    required_categories: list[str]
    covered_categories: list[str]
    missing_categories: list[str]


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


def _safe_json_list(value: Any) -> list[Any]:
    if value is None:
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, tuple):
        return list(value)

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return parsed
            return []
        except Exception:
            return []

    return []


def compute_category_completeness(
    *,
    required_categories: Iterable[Any] | None,
    covered_categories: Iterable[Any] | None,
) -> JurisdictionCompleteness:
    coverage = compute_completeness_score(required_categories, covered_categories)
    return JurisdictionCompleteness(
        completeness_score=coverage.completeness_score,
        completeness_status=coverage.completeness_status,
        required_categories=coverage.required_categories,
        covered_categories=coverage.covered_categories,
        missing_categories=coverage.missing_categories,
    )


def compute_profile_completeness(profile: Optional[JurisdictionProfile]) -> JurisdictionCompleteness:
    if profile is None:
        return compute_category_completeness(required_categories=[], covered_categories=[])

    required = _safe_json_list(getattr(profile, "required_categories_json", None))
    covered = _safe_json_list(getattr(profile, "covered_categories_json", None))

    return compute_category_completeness(
        required_categories=required,
        covered_categories=covered,
    )


def compute_coverage_status_score(
    *,
    required_categories_json: str | None = None,
    covered_categories_json: str | None = None,
) -> JurisdictionCompleteness:
    return compute_category_completeness(
        required_categories=_safe_json_list(required_categories_json),
        covered_categories=_safe_json_list(covered_categories_json),
    )


def derive_categories_from_rule(jr: Optional[JurisdictionRule]) -> list[str]:
    """
    Best-effort category derivation from legacy JurisdictionRule rows.

    This lets older rule records participate in the new completeness layer.
    """
    if jr is None:
        return []

    categories: list[str] = []

    if bool(getattr(jr, "rental_license_required", False)):
        categories.extend(["rental_license", "registration"])

    if getattr(jr, "inspection_authority", None) or getattr(jr, "inspection_frequency", None):
        categories.append("inspection")

    if getattr(jr, "tenant_waitlist_depth", None):
        categories.append("section8")

    try:
        fps = json.loads(jr.typical_fail_points_json) if jr.typical_fail_points_json else []
        if isinstance(fps, list) and fps:
            categories.append("safety")
            # crude but deterministic signal for older SE Michigan housing stock
            if any("paint" in str(x).lower() for x in fps):
                categories.append("lead")
    except Exception:
        pass

    return normalize_categories(categories)


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
    elif freq == "periodic":
        delta = -0.03
        mult += delta
        _push(
            trace,
            rule_field="inspection_frequency",
            input_value=freq,
            weight=1.0,
            delta=delta,
            text="Periodic inspection cadence (moderate compliance friction).",
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
        elif pdv >= 10:
            delta = -0.02
            mult += delta
            _push(
                trace,
                rule_field="processing_days",
                input_value=pdv,
                weight=1.0,
                delta=delta,
                text=f"Operational processing (~{pdv} days).",
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
        elif isinstance(fps, list) and len(fps) >= 4:
            delta = -0.02
            mult += delta
            _push(
                trace,
                rule_field="typical_fail_points_json",
                input_value=len(fps),
                weight=1.0,
                delta=delta,
                text="Several typical fail points (moderate reinspection risk).",
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
