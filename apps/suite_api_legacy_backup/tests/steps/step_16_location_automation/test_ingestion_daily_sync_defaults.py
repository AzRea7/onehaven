from __future__ import annotations

from products.acquire.backend.src.services.ingestion_scheduler_service import (
    build_runtime_payload,
    list_default_daily_markets,
)


def test_list_default_daily_markets_has_expected_shape() -> None:
    markets = list_default_daily_markets()

    assert isinstance(markets, list)
    assert len(markets) >= 1
    assert all(isinstance(x, dict) for x in markets)
    assert all("state" in x for x in markets)
    assert all("county" in x for x in markets)
    assert all("city" in x for x in markets)


def test_build_runtime_payload_for_daily_market_refresh() -> None:
    payload = build_runtime_payload(
        state="MI",
        county="wayne",
        city="detroit",
    )

    assert payload["trigger_type"] == "daily_refresh"
    assert payload["state"] == "MI"
    assert payload["county"] == "wayne"
    assert payload["city"] == "detroit"
    assert payload["limit"] == 250
    