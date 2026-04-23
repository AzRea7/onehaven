from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable


RISK_LOW = "low"
RISK_MEDIUM = "medium"
RISK_HIGH = "high"


@dataclass(frozen=True)
class InspectionRiskFinding:
    code: str
    label: str
    severity: str
    blocking: bool
    quantity: int
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except Exception:
        return default


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _severity_bucket(item: Any) -> str:
    raw = str(getattr(item, "severity_label", None) or getattr(item, "severity", None) or "").strip().lower()
    if raw in {"life_threatening", "critical", "urgent", "high"}:
        return "critical"
    if raw in {"severe", "medium_high", "moderate"}:
        return "major"
    if raw in {"low", "minor"}:
        return "minor"
    numeric = _safe_int(getattr(item, "severity", None), 0)
    if numeric >= 4:
        return "critical"
    if numeric >= 2:
        return "major"
    return "minor"


def _timeline_risk_from_days(days: int | None) -> str:
    if days is None:
        return "unknown"
    if days <= 3:
        return "immediate"
    if days <= 14:
        return "near_term"
    return "standard"


def _inspection_reason_code(item: Any) -> str:
    return str(
        getattr(item, "code", None)
        or getattr(item, "hqs_code", None)
        or getattr(item, "category", None)
        or "inspection_issue"
    ).strip().lower()


def _inspection_reason_label(item: Any) -> str:
    return str(
        getattr(item, "description", None)
        or getattr(item, "title", None)
        or getattr(item, "category", None)
        or getattr(item, "code", None)
        or "Inspection issue"
    ).strip()


def _iter_failed_items(inspection_items: Iterable[Any] | None) -> list[Any]:
    rows: list[Any] = []
    for item in inspection_items or []:
        failed = bool(getattr(item, "failed", False))
        status = str(getattr(item, "status", "") or "").strip().lower()
        outcome = str(getattr(item, "outcome", "") or "").strip().lower()
        if failed or status in {"failed", "deficient"} or outcome in {"fail", "failed"}:
            rows.append(item)
    return rows


def build_inspection_risk_summary(
    *,
    inspection_items: Iterable[Any] | None,
    unresolved_requirements: Iterable[str] | None = None,
    unresolved_conflicts: Iterable[str] | None = None,
    stale_authoritative_categories: Iterable[str] | None = None,
) -> dict[str, Any]:
    failed_items = _iter_failed_items(inspection_items)
    critical = [item for item in failed_items if _severity_bucket(item) == "critical"]
    major = [item for item in failed_items if _severity_bucket(item) == "major"]
    minor = [item for item in failed_items if _severity_bucket(item) == "minor"]

    unresolved_requirements = [str(x).strip() for x in (unresolved_requirements or []) if str(x).strip()]
    unresolved_conflicts = [str(x).strip() for x in (unresolved_conflicts or []) if str(x).strip()]
    stale_authoritative_categories = [str(x).strip() for x in (stale_authoritative_categories or []) if str(x).strip()]

    score = 0.0
    score += min(45.0, len(critical) * 18.0)
    score += min(30.0, len(major) * 8.0)
    score += min(10.0, len(minor) * 2.0)
    score += min(10.0, len(unresolved_requirements) * 2.5)
    score += min(8.0, len(unresolved_conflicts) * 4.0)
    score += min(7.0, len(stale_authoritative_categories) * 2.0)
    score = max(0.0, min(100.0, round(score, 2)))

    level = RISK_LOW
    if score >= 70:
        level = RISK_HIGH
    elif score >= 40:
        level = RISK_MEDIUM

    findings: list[InspectionRiskFinding] = []
    if critical:
        findings.append(
            InspectionRiskFinding(
                code="critical_failed_items",
                label="Critical failed inspection items",
                severity="critical",
                blocking=True,
                quantity=len(critical),
                reason="Property has critical inspection deficiencies that can block operation or payment.",
            )
        )
    if major:
        findings.append(
            InspectionRiskFinding(
                code="major_failed_items",
                label="Major failed inspection items",
                severity="major",
                blocking=bool(critical),
                quantity=len(major),
                reason="Property has major unresolved inspection deficiencies.",
            )
        )
    if unresolved_requirements:
        findings.append(
            InspectionRiskFinding(
                code="unresolved_requirements",
                label="Unresolved compliance requirements",
                severity="major",
                blocking=True,
                quantity=len(unresolved_requirements),
                reason="Required compliance obligations remain unresolved.",
            )
        )
    if unresolved_conflicts:
        findings.append(
            InspectionRiskFinding(
                code="unresolved_conflicts",
                label="Unresolved compliance conflicts",
                severity="critical",
                blocking=True,
                quantity=len(unresolved_conflicts),
                reason="Conflicting compliance evidence or rules require review.",
            )
        )
    if stale_authoritative_categories:
        findings.append(
            InspectionRiskFinding(
                code="stale_authoritative_categories",
                label="Stale authoritative categories",
                severity="major",
                blocking=False,
                quantity=len(stale_authoritative_categories),
                reason="Authoritative coverage exists but is stale and may affect inspection readiness.",
            )
        )

    earliest_days: int | None = None
    for item in critical + major:
        for attr in ("correction_days", "days_to_correct", "deadline_days"):
            value = getattr(item, attr, None)
            if value is None:
                continue
            days = _safe_int(value, 0)
            if earliest_days is None or days < earliest_days:
                earliest_days = days
    inspection_timeline_risk = _timeline_risk_from_days(earliest_days)

    next_deadline = None
    if earliest_days is not None:
        next_deadline = (datetime.utcnow() + timedelta(days=max(0, earliest_days))).isoformat()

    return {
        "inspection_risk_score": score,
        "inspection_risk_level": level,
        "inspection_timeline_risk": inspection_timeline_risk,
        "inspection_timeline_days": earliest_days,
        "next_deadline_estimate": next_deadline,
        "failed_item_count": len(failed_items),
        "critical_failed_item_count": len(critical),
        "major_failed_item_count": len(major),
        "minor_failed_item_count": len(minor),
        "reason_codes": [item.code for item in findings],
        "findings": [item.to_dict() for item in findings],
    }
