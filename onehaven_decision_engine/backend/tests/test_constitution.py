# backend/tests/test_constitution.py
from __future__ import annotations

from app.domain.decision_engine import DealContext, evaluate_deal_rules
from app.config import settings


def test_missing_rent_penalty():
    ctx = DealContext(
        asking_price=120_000,
        bedrooms=3,
        has_garage=False,
        strategy="section8",
        rent_market=None,
        rent_ceiling=None,
    )
    dec = evaluate_deal_rules(ctx)
    assert dec.decision in {"REVIEW", "PASS", "REJECT"}
    assert "Missing rent inputs" in " ".join(dec.reasons)
    assert dec.score <= 30  # base 50 - 20 penalty (and maybe other adjustments)


def test_over_max_price_reject(monkeypatch):
    monkeypatch.setattr(settings, "max_price", 150_000, raising=False)
    ctx = DealContext(
        asking_price=200_000,
        bedrooms=3,
        has_garage=False,
        strategy="section8",
        rent_market=2000,
        rent_ceiling=1800,
    )
    dec = evaluate_deal_rules(ctx)
    assert dec.decision == "REJECT"
    assert dec.score == 0
    assert "exceeds max" in " ".join(dec.reasons).lower()


def test_under_min_bedrooms_reject(monkeypatch):
    monkeypatch.setattr(settings, "min_bedrooms", 2, raising=False)
    ctx = DealContext(
        asking_price=120_000,
        bedrooms=1,
        has_garage=False,
        strategy="section8",
        rent_market=2000,
        rent_ceiling=1800,
    )
    dec = evaluate_deal_rules(ctx)
    assert dec.decision == "REJECT"
    assert dec.score == 0
    assert "below minimum" in " ".join(dec.reasons).lower()
