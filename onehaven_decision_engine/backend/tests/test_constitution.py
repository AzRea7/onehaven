# backend/app/tests/test_constitution.py
from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.domain.operating_truth_enforcement import DealIntakeFacts, enforce_constitution_for_deal_intake


def test_reject_over_max_price(monkeypatch):
    # hardwire max_price for deterministic test
    from app import config
    monkeypatch.setattr(config.settings, "max_price", 150_000, raising=False)

    facts = DealIntakeFacts(
        address="1 Main St",
        city="Detroit",
        state="MI",
        zip="48201",
        bedrooms=3,
        bathrooms=1.0,
        asking_price=200_000,
    )
    with pytest.raises(HTTPException) as e:
        enforce_constitution_for_deal_intake(facts)
    assert "exceeds max_price" in str(e.value.detail)


def test_reject_under_min_bedrooms(monkeypatch):
    from app import config
    monkeypatch.setattr(config.settings, "min_bedrooms", 2, raising=False)

    facts = DealIntakeFacts(
        address="1 Main St",
        city="Detroit",
        state="MI",
        zip="48201",
        bedrooms=1,
        bathrooms=1.0,
        asking_price=100_000,
    )
    with pytest.raises(HTTPException) as e:
        enforce_constitution_for_deal_intake(facts)
    assert "below min_bedrooms" in str(e.value.detail)


def test_accept_valid_deal(monkeypatch):
    from app import config
    monkeypatch.setattr(config.settings, "max_price", 150_000, raising=False)
    monkeypatch.setattr(config.settings, "min_bedrooms", 2, raising=False)

    facts = DealIntakeFacts(
        address="55 Logic Ave",
        city="Royal Oak",
        state="MI",
        zip="48067",
        bedrooms=3,
        bathrooms=1.5,
        asking_price=149_999,
    )
    # should not raise
    enforce_constitution_for_deal_intake(facts)
