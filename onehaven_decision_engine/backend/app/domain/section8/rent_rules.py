# backend/app/domain/section8/rent_rules.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


def _to_pos_float(x: object) -> Optional[float]:
    try:
        v = float(x)  # type: ignore[arg-type]
        return v if v > 0 else None
    except Exception:
        return None


def _normalize_payment_standard_pct(x: object, default: float = 110.0) -> float:
    """
    Accept either:
      - 110 (meaning 110%)
      - 1.10 (meaning 110% as a ratio)
    Returns a percent number, e.g. 110.0.
    """
    v = _to_pos_float(x)
    if v is None:
        return float(default)

    # If someone stores 1.10 in config, treat it as a ratio.
    if 0 < v <= 3.0:
        return float(v * 100.0)

    return float(v)


@dataclass(frozen=True)
class CeilingCandidate:
    type: str
    value: float


@dataclass(frozen=True)
class RentDecision:
    strategy: str
    market_rent_estimate: Optional[float]
    approved_rent_ceiling: Optional[float]
    rent_used: Optional[float]
    cap_reason: str  # "none" | "capped" | "uncapped"
    explanation: str
    candidates: list[CeilingCandidate]


def compute_approved_ceiling(
    *,
    section8_fmr: Optional[float],
    payment_standard_pct: float,
    rent_reasonableness_comp: Optional[float],
    manual_override: Optional[float],
) -> tuple[Optional[float], list[CeilingCandidate]]:
    """
    Returns (approved_ceiling, candidates).

    Rule (S8):
      - candidate #1: FMR adjusted by payment standard pct (e.g., 110% of FMR)
      - candidate #2: rent reasonableness comp cap (if present)
      - approved ceiling = manual override if present else min(candidates)
    """
    candidates: list[CeilingCandidate] = []
    caps: list[float] = []

    fmr = _to_pos_float(section8_fmr)
    rr = _to_pos_float(rent_reasonableness_comp)

    pct = _normalize_payment_standard_pct(payment_standard_pct, default=110.0)

    # Candidate: adjusted FMR
    if fmr is not None:
        adjusted = float(fmr) * (pct / 100.0)
        if adjusted > 0:
            caps.append(adjusted)
            candidates.append(CeilingCandidate(type="fmr_adjusted", value=adjusted))

    # Candidate: rent reasonableness comp
    if rr is not None:
        caps.append(float(rr))
        candidates.append(CeilingCandidate(type="rent_reasonableness", value=float(rr)))

    computed = min(caps) if caps else None

    manual = _to_pos_float(manual_override)
    approved = manual if manual is not None else computed

    return approved, candidates


def compute_rent_used(
    *,
    strategy: str,
    market: Optional[float],
    approved: Optional[float],
    candidates: Optional[list[CeilingCandidate]] = None,
) -> RentDecision:
    """
    Strategy:
      - market: rent_used = market (no ceiling)
      - section8 (default): rent_used = min(market, approved) when both exist
    """
    s = (strategy or "section8").strip().lower()
    m = _to_pos_float(market)
    a = _to_pos_float(approved)
    cands = candidates or []

    if s == "market":
        if m is None:
            return RentDecision(
                strategy=s,
                market_rent_estimate=m,
                approved_rent_ceiling=a,
                rent_used=None,
                cap_reason="none",
                explanation="Market strategy: market_rent_estimate is missing, so rent_used cannot be computed.",
                candidates=cands,
            )
        return RentDecision(
            strategy=s,
            market_rent_estimate=m,
            approved_rent_ceiling=a,
            rent_used=float(m),
            cap_reason="none",
            explanation="Market strategy uses market_rent_estimate (no Section 8 ceiling cap applied).",
            candidates=cands,
        )

    # Default: section8
    if m is None and a is None:
        return RentDecision(
            strategy="section8",
            market_rent_estimate=m,
            approved_rent_ceiling=a,
            rent_used=None,
            cap_reason="none",
            explanation="Section 8 strategy: both market_rent_estimate and ceiling inputs are missing; cannot compute rent_used.",
            candidates=cands,
        )
    if m is None:
        return RentDecision(
            strategy="section8",
            market_rent_estimate=m,
            approved_rent_ceiling=a,
            rent_used=float(a) if a is not None else None,
            cap_reason="none",
            explanation="Section 8 strategy: market estimate missing; using approved ceiling as rent_used.",
            candidates=cands,
        )
    if a is None:
        return RentDecision(
            strategy="section8",
            market_rent_estimate=m,
            approved_rent_ceiling=a,
            rent_used=float(m),
            cap_reason="none",
            explanation="Section 8 strategy: ceiling missing; using market_rent_estimate as rent_used.",
            candidates=cands,
        )

    rent_used = float(min(float(m), float(a)))
    cap_reason = "capped" if float(m) > float(a) else "uncapped"
    return RentDecision(
        strategy="section8",
        market_rent_estimate=m,
        approved_rent_ceiling=a,
        rent_used=rent_used,
        cap_reason=cap_reason,
        explanation="Section 8 strategy caps rent by the strictest limit (approved ceiling vs market estimate).",
        candidates=cands,
    )
