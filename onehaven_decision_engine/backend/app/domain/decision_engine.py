# backend/app/domain/decision_engine.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, List, Any

from ..config import settings


@dataclass(frozen=True)
class DealContext:
    asking_price: float
    bedrooms: int
    has_garage: bool

    rent_market: Optional[float]
    rent_ceiling: Optional[float]   # should be your *safe* cap (approved_rent_ceiling preferred)
    inventory_count: Optional[int]  # kept for analytics only (NOT a deal killer)
    starbucks_minutes: Optional[int]


@dataclass(frozen=True)
class Decision:
    decision: str
    score: int
    reasons: List[str]


def _rent_used(rent_market: Optional[float], rent_ceiling: Optional[float]) -> Optional[float]:
    """
    Rent used for rule checks:
      - if we have both market and a ceiling, use min(market, ceiling)
      - else fall back to whichever exists
    """
    if rent_market is None and rent_ceiling is None:
        return None
    if rent_market is None:
        return rent_ceiling
    if rent_ceiling is None:
        return rent_market
    return min(rent_market, rent_ceiling)


def evaluate_deal_rules(ctx: DealContext) -> Decision:
    """
    Deterministic, explainable scoring.
    Key updates:
      - Inventory is NOT a reject/penalty gate anymore.
      - Market rent can exceed HUD FMR; we use rent_ceiling (approved_rent_ceiling) to cap.
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
        reasons.append("Missing rent inputs (need market rent and/or safe ceiling)")
        score -= 20
    else:
        min_rent = ctx.asking_price * settings.rent_rule_min_pct
        target_rent = ctx.asking_price * settings.rent_rule_target_pct

        # Hard gate (you can relax later if you want REVIEW instead)
        if rent < min_rent:
            return Decision("REJECT", 0, [f"Fails rent rule: rent_used {rent:.0f} < min {min_rent:.0f}"])

        if rent >= target_rent:
            score += 15
            reasons.append("Meets target rent rule")
        else:
            score += 5
            reasons.append("Meets minimum rent rule")

    # Explain capping (small penalty only)
    if ctx.rent_ceiling is not None and ctx.rent_market is not None and ctx.rent_market > ctx.rent_ceiling:
        reasons.append("Market rent above conservative Section 8 ceiling (we cap rent_used)")
        score -= 3

    # Inventory is NO LONGER used for scoring gates.
    if ctx.inventory_count is not None:
        reasons.append(f"Inventory proxy recorded (snapshot count): {ctx.inventory_count}")

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


def score_and_decide(*args: Any, **kwargs: Any):
    """
    Supports BOTH call patterns:
      A) score_and_decide(ctx: DealContext) -> Decision
      B) score_and_decide(property=..., deal=..., rent_assumption=..., underwriting=...) -> (decision, score, reasons)
    """

    # --- Mode A
    if len(args) == 1 and isinstance(args[0], DealContext):
        return evaluate_deal_rules(args[0])

    # --- Mode B
    prop = kwargs.get("property")
    deal = kwargs.get("deal")
    ra = kwargs.get("rent_assumption")
    uw = kwargs.get("underwriting")

    asking_price = float(getattr(deal, "asking_price", 0.0) or 0.0)
    bedrooms = int(getattr(prop, "bedrooms", 0) or 0)
    has_garage = bool(getattr(prop, "has_garage", False) or False)

    rent_market = getattr(ra, "market_rent_estimate", None)

    # Prefer approved_rent_ceiling first (this is the “safe cap” we compute in enrichment)
    ceiling_candidates = []
    for field in ("approved_rent_ceiling", "rent_reasonableness_comp", "section8_fmr"):
        v = getattr(ra, field, None)
        if v is not None:
            try:
                fv = float(v)
                if fv > 0:
                    ceiling_candidates.append(fv)
            except Exception:
                pass
    rent_ceiling = min(ceiling_candidates) if ceiling_candidates else None

    inventory_count = getattr(ra, "inventory_count", None)
    starbucks_minutes = getattr(ra, "starbucks_minutes", None)

    ctx = DealContext(
        asking_price=asking_price,
        bedrooms=bedrooms,
        has_garage=has_garage,
        rent_market=float(rent_market) if rent_market is not None else None,
        rent_ceiling=float(rent_ceiling) if rent_ceiling is not None else None,
        inventory_count=int(inventory_count) if inventory_count is not None else None,
        starbucks_minutes=int(starbucks_minutes) if starbucks_minutes is not None else None,
    )

    dec = evaluate_deal_rules(ctx)
    decision = dec.decision
    score = dec.score
    reasons = list(dec.reasons)

    # Optional underwriting gates
    min_dscr = float(getattr(settings, "min_dscr", 1.10))
    min_cashflow = float(getattr(settings, "min_cashflow", 400.0))

    if uw is not None:
        try:
            dscr = float(getattr(uw, "dscr", 999))
            cash_flow = float(getattr(uw, "cash_flow", 999999))
            if dscr < min_dscr:
                return ("REJECT", 0, [f"DSCR {dscr:.2f} below minimum {min_dscr:.2f}"])
            if cash_flow < min_cashflow:
                return ("REJECT", 0, [f"Cash flow ${cash_flow:.0f} below minimum ${min_cashflow:.0f}"])
        except Exception:
            reasons.append("Could not validate DSCR/cashflow gates (underwriting parse failed)")
            score = min(score, 55)
            decision = "REVIEW"

    return (decision, score, reasons)
