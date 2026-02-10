from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, List, Any

from ..config import settings


# ---------------------------- Data Structures ----------------------------

@dataclass(frozen=True)
class DealContext:
    asking_price: float
    bedrooms: int
    has_garage: bool

    # Strategy changes rent logic:
    #   - section8 => cap by ceiling if present
    #   - market   => use market rent only
    strategy: str = "section8"

    # Rent inputs (already in your RentAssumption)
    rent_market: Optional[float] = None
    rent_ceiling: Optional[float] = None   # prefer approved_rent_ceiling

    # Soft signals (analytics / scoring only)
    inventory_count: Optional[int] = None
    starbucks_minutes: Optional[int] = None


@dataclass(frozen=True)
class Decision:
    decision: str
    score: int
    reasons: List[str]


# ---------------------------- Rent Logic ----------------------------

def _rent_used(strategy: str, rent_market: Optional[float], rent_ceiling: Optional[float]) -> Optional[float]:
    """
    Rent used for rule checks, depending on strategy.

    Market strategy:
      - use market rent (no cap)

    Section 8 strategy:
      - if we have both market + ceiling => min(market, ceiling)
      - else fallback to whichever exists
    """
    strategy = (strategy or "section8").strip().lower()

    if strategy == "market":
        return rent_market

    # section8 default
    if rent_market is None and rent_ceiling is None:
        return None
    if rent_market is None:
        return rent_ceiling
    if rent_ceiling is None:
        return rent_market
    return min(rent_market, rent_ceiling)


# ---------------------------- Scoring Rules ----------------------------

def evaluate_deal_rules(ctx: DealContext) -> Decision:
    """
    Deterministic, explainable scoring.

    Key behavior:
      - Price and bedrooms are hard filters (still).
      - 1.3% rule is NOT an auto-reject anymore (soft scoring).
      - Section 8 rent is capped by conservative ceiling (approved_rent_ceiling preferred).
      - Inventory count is recorded but not a gate.
      - Starbucks is a soft signal.
    """
    reasons: list[str] = []
    score = 50

    # --- Hard gates: basic feasibility ---
    if ctx.asking_price > float(getattr(settings, "max_price", 150_000)):
        return Decision("REJECT", 0, [f"Price {ctx.asking_price:.0f} exceeds max ${getattr(settings, 'max_price', 150_000)}"])

    if ctx.bedrooms < int(getattr(settings, "min_bedrooms", 2)):
        return Decision("REJECT", 0, [f"Bedrooms {ctx.bedrooms} below minimum {getattr(settings, 'min_bedrooms', 2)}"])

    # --- Garage: soft risk flag, not a killer ---
    if ctx.has_garage:
        reasons.append("Garage present (possible rehab/maintenance complexity)")
        score -= 5

    # --- Rent evaluation (strategy-aware) ---
    rent = _rent_used(ctx.strategy, ctx.rent_market, ctx.rent_ceiling)

    if rent is None:
        reasons.append("Missing rent inputs (need market rent and/or conservative ceiling)")
        score -= 20
    else:
        min_pct = float(getattr(settings, "rent_rule_min_pct", 0.013))     # 1.3%
        target_pct = float(getattr(settings, "rent_rule_target_pct", 0.015))  # 1.5%

        min_rent = ctx.asking_price * min_pct
        target_rent = ctx.asking_price * target_pct

        # Soft scoring (not hard reject)
        if rent < min_rent:
            reasons.append(f"Below minimum rent heuristic: rent_used {rent:.0f} < min {min_rent:.0f}")
            score -= 25
        elif rent >= target_rent:
            reasons.append("Meets target rent heuristic")
            score += 15
        else:
            reasons.append("Meets minimum rent heuristic")
            score += 5

    # --- Explain capping for Section 8 ---
    strat = (ctx.strategy or "section8").lower()
    if strat != "market":
        if ctx.rent_ceiling is not None and ctx.rent_market is not None and ctx.rent_market > ctx.rent_ceiling:
            reasons.append("Market rent above conservative Section 8 ceiling (rent_used is capped)")
            score -= 3

    # --- Inventory: analytics only ---
    if ctx.inventory_count is not None:
        reasons.append(f"Inventory proxy recorded: {ctx.inventory_count}")

    # --- Starbucks proxy: soft signal ---
    if ctx.starbucks_minutes is not None:
        if ctx.starbucks_minutes <= 10:
            reasons.append("Starbucks proxy strong (<= 10 minutes)")
            score += 10
        else:
            reasons.append("Starbucks proxy weak (> 10 minutes)")
            score -= 5

    # clamp
    score = max(0, min(100, score))

    # decision thresholds (tunable)
    if score >= 75:
        decision = "PASS"
    elif score >= 55:
        decision = "REVIEW"
    else:
        decision = "REJECT"

    return Decision(decision, score, reasons)


# ---------------------------- JSON helpers ----------------------------

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


# ---------------------------- Compatibility wrapper ----------------------------

def score_and_decide(*args: Any, **kwargs: Any):
    """
    Supports BOTH call patterns:
      A) score_and_decide(ctx: DealContext) -> Decision
      B) score_and_decide(property=..., deal=..., rent_assumption=..., underwriting=..., strategy=...) -> (decision, score, reasons)
    """

    # --- Mode A
    if len(args) == 1 and isinstance(args[0], DealContext):
        return evaluate_deal_rules(args[0])

    # --- Mode B
    prop = kwargs.get("property")
    deal = kwargs.get("deal")
    ra = kwargs.get("rent_assumption")
    uw = kwargs.get("underwriting")

    strategy = (kwargs.get("strategy") or "section8").strip().lower()

    asking_price = float(getattr(deal, "asking_price", 0.0) or 0.0)
    bedrooms = int(getattr(prop, "bedrooms", 0) or 0)
    has_garage = bool(getattr(prop, "has_garage", False) or False)

    # Rent inputs
    rent_market = getattr(ra, "market_rent_estimate", None)

    # IMPORTANT: prefer approved_rent_ceiling if present.
    approved = getattr(ra, "approved_rent_ceiling", None)

    # If approved ceiling missing, fall back to something defensible:
    # min(FMR, reasonableness_comp) when both exist; else whichever exists.
    rent_reasonableness = getattr(ra, "rent_reasonableness_comp", None)
    fmr = getattr(ra, "section8_fmr", None)

    rent_ceiling: Optional[float] = None
    try:
        if approved is not None and float(approved) > 0:
            rent_ceiling = float(approved)
        else:
            candidates: list[float] = []
            if rent_reasonableness is not None and float(rent_reasonableness) > 0:
                candidates.append(float(rent_reasonableness))
            if fmr is not None and float(fmr) > 0:
                candidates.append(float(fmr))
            rent_ceiling = min(candidates) if candidates else None
    except Exception:
        rent_ceiling = None

    inventory_count = getattr(ra, "inventory_count", None)
    starbucks_minutes = getattr(ra, "starbucks_minutes", None)

    ctx = DealContext(
        asking_price=asking_price,
        bedrooms=bedrooms,
        has_garage=has_garage,
        strategy=strategy,
        rent_market=float(rent_market) if rent_market is not None else None,
        rent_ceiling=float(rent_ceiling) if rent_ceiling is not None else None,
        inventory_count=int(inventory_count) if inventory_count is not None else None,
        starbucks_minutes=int(starbucks_minutes) if starbucks_minutes is not None else None,
    )

    dec = evaluate_deal_rules(ctx)
    decision = dec.decision
    score = dec.score
    reasons = list(dec.reasons)

    # --- Underwriting hard gates (these should be the real killers) ---
    dscr_min = float(getattr(settings, "dscr_min", 1.10))
    cashflow_min = float(getattr(settings, "target_monthly_cashflow", 400.0))

    if uw is not None:
        try:
            dscr = float(getattr(uw, "dscr", 999))
            cash_flow = float(getattr(uw, "cash_flow", 999999))

            if dscr < dscr_min:
                return ("REJECT", 0, [f"DSCR {dscr:.2f} below minimum {dscr_min:.2f}"])
            if cash_flow < cashflow_min:
                return ("REJECT", 0, [f"Cash flow ${cash_flow:.0f} below minimum ${cashflow_min:.0f}"])
        except Exception:
            reasons.append("Could not validate DSCR/cashflow gates (underwriting parse failed)")
            score = min(score, 55)
            decision = "REVIEW"

    return (decision, score, reasons)
