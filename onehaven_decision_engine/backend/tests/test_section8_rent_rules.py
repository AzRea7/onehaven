# backend/tests/test_section8_rent_rules.py
from __future__ import annotations

from app.domain.section8.rent_rules import compute_approved_ceiling, compute_rent_used


def test_ceiling_candidates_min_logic():
    # FMR 1500 with 110% payment standard => 1650
    # RR comp 1600 => stricter cap should be 1600
    approved, cands = compute_approved_ceiling(
        section8_fmr=1500,
        payment_standard_pct=110,
        rent_reasonableness_comp=1600,
        manual_override=None,
    )
    assert approved == 1600
    assert len(cands) == 2
    assert any(c.type == "fmr_adjusted" and abs(c.value - 1650) < 1e-6 for c in cands)
    assert any(c.type == "rent_reasonableness" and abs(c.value - 1600) < 1e-6 for c in cands)


def test_manual_override_wins():
    approved, _cands = compute_approved_ceiling(
        section8_fmr=1500,
        payment_standard_pct=110,
        rent_reasonableness_comp=1200,
        manual_override=2000,
    )
    assert approved == 2000


def test_section8_rent_used_is_min_of_market_and_ceiling():
    approved, cands = compute_approved_ceiling(
        section8_fmr=1500,
        payment_standard_pct=110,   # 1650
        rent_reasonableness_comp=1600,
        manual_override=None,
    )
    d = compute_rent_used(strategy="section8", market=1750, approved=approved, candidates=cands)
    assert d.rent_used == 1600
    assert d.cap_reason == "capped"


def test_section8_uncapped_when_market_below_ceiling():
    approved, cands = compute_approved_ceiling(
        section8_fmr=1500,
        payment_standard_pct=110,   # 1650
        rent_reasonableness_comp=None,
        manual_override=None,
    )
    d = compute_rent_used(strategy="section8", market=1400, approved=approved, candidates=cands)
    assert d.rent_used == 1400
    assert d.cap_reason == "uncapped"


def test_market_strategy_ignores_ceiling():
    approved, cands = compute_approved_ceiling(
        section8_fmr=1500,
        payment_standard_pct=110,
        rent_reasonableness_comp=1200,
        manual_override=None,
    )
    d = compute_rent_used(strategy="market", market=1750, approved=approved, candidates=cands)
    assert d.rent_used == 1750
    assert d.cap_reason == "none"