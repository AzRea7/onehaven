from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, List

from ..config import settings


@dataclass(frozen=True)
class DealContext:
    asking_price: float
    bedrooms: int
    has_garage: bool

    rent_market: Optional[float]
    rent_ceiling: Optional[float]
    inventory_count: Optional[int]
    starbucks_minutes: Optional[int]


@dataclass(frozen=True)
class Decision:
    decision: str
    score: int
    reasons: List[str]


def _rent_used(rent_market: Optional[float], rent_ceiling: Optional[float]) -> Optional[float]:
    if rent_market is None and rent_ceiling is None:
        return None
    if rent_market is None:
        return rent_ceiling
    if rent_ceiling is None:
        return rent_market
    return min(rent_market, rent_ceiling)


def evaluate_deal_rules(ctx: DealContext) -> Decision:
    """
    Deterministic rule engine for deal triage (PASS/REVIEW/REJECT).

    This is intentionally not the full underwriting engine (DSCR, CoC, etc.).
    It's your 'Deal Intake & Scoring' module: reject bad deals early.
    """
    reasons: list[str] = []
    score = 50

    if ctx.asking_price > settings.max_price:
        return Decision("REJECT", 0, [f"Price {ctx.asking_price:.0f} exceeds max ${settings.max_price}"])

    if ctx.bedrooms < settings.min_bedrooms:
        return Decision("REJECT", 0, [f"Bedrooms {ctx.bedrooms} below minimum {settings.min_bedrooms}"])

    if ctx.has_garage:
        reasons.append("Garage present (rehab/maintenance risk flag)")
        score -= 5

    rent = _rent_used(ctx.rent_market, ctx.rent_ceiling)
    if rent is None:
        reasons.append("Missing rent inputs (need market rent and/or FMR/ceiling)")
        score -= 20
    else:
        min_rent = ctx.asking_price * settings.rent_rule_min_pct
        target_rent = ctx.asking_price * settings.rent_rule_target_pct

        if rent < min_rent:
            return Decision("REJECT", 0, [f"Fails 1.3% rule: rent {rent:.0f} < {min_rent:.0f}"])

        if rent >= target_rent:
            score += 15
            reasons.append("Meets 1.5% target rent rule")
        else:
            score += 5
            reasons.append("Meets 1.3% minimum rent rule")

    if ctx.rent_ceiling is not None and ctx.rent_market is not None and ctx.rent_market > ctx.rent_ceiling:
        reasons.append("Market rent exceeds Section 8 ceiling (rent will be capped)")
        score -= 5

    if ctx.inventory_count is None:
        reasons.append("Missing inventory count proxy")
        score -= 5
    else:
        if ctx.inventory_count < settings.min_inventory:
            reasons.append(f"Inventory proxy low ({ctx.inventory_count} < {settings.min_inventory})")
            score -= 15
        else:
            reasons.append("Inventory proxy healthy")
            score += 10

    if ctx.starbucks_minutes is not None:
        if ctx.starbucks_minutes <= 10:
            reasons.append("Starbucks proxy good (<= 10 minutes)")
            score += 10
        else:
            reasons.append("Starbucks proxy weak (> 10 minutes)")
            score -= 5

    score = max(0, min(100, score))

    if score >= 75:
        decision = "PASS"
    elif score >= 55:
        decision = "REVIEW"
    else:
        decision = "REJECT"

    return Decision(decision, score, reasons)


# ✅ Compatibility wrapper for routers that import score_and_decide
def score_and_decide(ctx: DealContext) -> Decision:
    """
    Backwards-compatible entrypoint used by routers/evaluate.py.
    Keeps the router stable while we evolve internals.
    """
    return evaluate_deal_rules(ctx)


def reasons_to_json(reasons: list[str]) -> str:
    return json.dumps(reasons, ensure_ascii=False)


def reasons_from_json(s: str) -> list[str]:
    try:
        v = json.loads(s)
        if isinstance(v, list):
            return [str(x) for x in v]
    except Exception:
        pass
    return []
