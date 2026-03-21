from __future__ import annotations

from app.services import ingestion_scheduler_service as scheduler


def test_daily_sync_idempotency_context_is_stable_for_same_org_source_market_day():
    class DummySource:
        id = 91
        provider = "rentcast"
        slug = "rentcast-sale-listings"

    market = {"state": "MI", "county": "wayne", "city": "detroit"}
    day_key = "2026-03-21"

    dispatch_key_1 = "daily_sync_dispatch:7:2026-03-21:rentcast:rentcast-sale-listings:mi:wayne:detroit"
    dispatch_key_2 = "daily_sync_dispatch:7:2026-03-21:rentcast:rentcast-sale-listings:mi:wayne:detroit"

    ctx1 = scheduler.build_scheduler_idempotency_context(
        org_id=7,
        source=DummySource(),
        market=market,
        day_key=day_key,
        dispatch_key=dispatch_key_1,
    )
    ctx2 = scheduler.build_scheduler_idempotency_context(
        org_id=7,
        source=DummySource(),
        market=market,
        day_key=day_key,
        dispatch_key=dispatch_key_2,
    )

    assert ctx1 == ctx2
    assert ctx1["org_id"] == 7
    assert ctx1["schedule_day"] == "2026-03-21"
    assert ctx1["scope"] == "daily_sync"


def test_daily_sync_idempotency_context_is_org_scoped():
    class DummySource:
        id = 91
        provider = "rentcast"
        slug = "rentcast-sale-listings"

    market = {"state": "MI", "county": "wayne", "city": "detroit"}
    day_key = "2026-03-21"

    ctx1 = scheduler.build_scheduler_idempotency_context(
        org_id=7,
        source=DummySource(),
        market=market,
        day_key=day_key,
        dispatch_key="daily_sync_dispatch:7:2026-03-21:rentcast:rentcast-sale-listings:mi:wayne:detroit",
    )
    ctx2 = scheduler.build_scheduler_idempotency_context(
        org_id=8,
        source=DummySource(),
        market=market,
        day_key=day_key,
        dispatch_key="daily_sync_dispatch:8:2026-03-21:rentcast:rentcast-sale-listings:mi:wayne:detroit",
    )

    assert ctx1["org_id"] != ctx2["org_id"]
    assert ctx1["dispatch_key"] != ctx2["dispatch_key"]


def test_same_runtime_payload_twice_is_duplicate_safe_shape():
    payload1 = scheduler.build_runtime_payload(state="MI", county="wayne", city="detroit")
    payload2 = scheduler.build_runtime_payload(state="MI", county="wayne", city="detroit")

    assert payload1 == payload2
    assert payload1["trigger_type"] == "daily_refresh"
    assert payload1["county"] == "wayne"
    assert payload1["city"] == "detroit"