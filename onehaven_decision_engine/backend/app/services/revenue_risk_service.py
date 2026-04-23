from __future__ import annotations

from typing import Any, Iterable


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except Exception:
        return default


def build_revenue_risk_summary(
    *,
    monthly_rent: float | None = None,
    section8_monthly_rent: float | None = None,
    lockout_active: bool = False,
    blocking_categories: Iterable[str] | None = None,
    inspection_risk_level: str | None = None,
    failed_item_count: int = 0,
    critical_failed_item_count: int = 0,
    stale_authoritative_categories: Iterable[str] | None = None,
) -> dict[str, Any]:
    blocking_categories = [str(x).strip().lower() for x in (blocking_categories or []) if str(x).strip()]
    stale_authoritative_categories = [str(x).strip().lower() for x in (stale_authoritative_categories or []) if str(x).strip()]

    base_rent = max(_safe_float(section8_monthly_rent, 0.0), _safe_float(monthly_rent, 0.0))
    if base_rent <= 0:
        base_rent = max(_safe_float(monthly_rent, 0.0), 1200.0)

    payment_disruption_risk = 0.0
    vacancy_delay_risk = 0.0

    if lockout_active:
        payment_disruption_risk += 0.55 * base_rent
        vacancy_delay_risk += 0.35 * base_rent

    if "section8" in blocking_categories or "program_overlay" in blocking_categories:
        payment_disruption_risk += 0.35 * base_rent

    if inspection_risk_level == "high":
        vacancy_delay_risk += 0.30 * base_rent
    elif inspection_risk_level == "medium":
        vacancy_delay_risk += 0.15 * base_rent

    payment_disruption_risk += min(base_rent * 0.20, critical_failed_item_count * 120.0)
    vacancy_delay_risk += min(base_rent * 0.20, failed_item_count * 35.0)
    vacancy_delay_risk += min(base_rent * 0.10, len(stale_authoritative_categories) * 40.0)

    money_at_risk_monthly = round(payment_disruption_risk + vacancy_delay_risk, 2)

    risk_level = "low"
    if money_at_risk_monthly >= base_rent * 0.60:
        risk_level = "high"
    elif money_at_risk_monthly >= base_rent * 0.25:
        risk_level = "medium"

    return {
        "rent_payment_risk": round(payment_disruption_risk, 2),
        "section8_payment_disruption_risk": round(payment_disruption_risk if ("section8" in blocking_categories or "program_overlay" in blocking_categories) else 0.0, 2),
        "vacancy_or_delay_risk": round(vacancy_delay_risk, 2),
        "money_at_risk_monthly": money_at_risk_monthly,
        "revenue_risk_level": risk_level,
        "baseline_monthly_rent": round(base_rent, 2),
    }
