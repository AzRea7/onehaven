# backend/tests/steps/step_10_automated_ingestion/test_ingestion_daily_sync_defaults.py
from __future__ import annotations

from datetime import datetime, timezone

from app.services import ingestion_scheduler_service as scheduler
from app.tasks import ingestion_tasks


def test_default_daily_market_list_has_multiple_markets():
    markets = list(scheduler.list_default_daily_markets())
    assert len(markets) >= 5


def test_next_daily_sync_returns_datetime():
    dt = scheduler.compute_next_daily_sync()
    assert hasattr(dt, "hour")


def test_default_daily_markets_cover_southeast_michigan_core_counties():
    markets = scheduler.list_default_daily_markets()
    counties = {(m["state"], m["county"]) for m in markets}

    assert ("MI", "wayne") in counties
    assert ("MI", "oakland") in counties
    assert ("MI", "macomb") in counties


def test_build_runtime_payload_matches_daily_sync_contract():
    payload = scheduler.build_runtime_payload(
        state="MI",
        county="wayne",
        city="detroit",
    )

    assert payload == {
        "trigger_type": "daily_refresh",
        "state": "MI",
        "county": "wayne",
        "city": "detroit",
        "limit": 250,
    }


def test_compute_next_daily_sync_rolls_forward_after_cutoff():
    now = datetime(2026, 3, 20, 10, 30, tzinfo=timezone.utc)
    nxt = scheduler.compute_next_daily_sync(now)

    assert nxt.date() > now.date()
    assert nxt.hour == 9
    assert nxt.minute == 10


def test_build_scheduler_idempotency_context_contains_org_source_day_and_dispatch_key():
    class DummySource:
        def __init__(self):
            self.id = 22
            self.provider = "rentcast"
            self.slug = "rentcast-sale-listings"

    ctx = scheduler.build_scheduler_idempotency_context(
        org_id=7,
        source=DummySource(),
        market={"state": "MI", "county": "wayne", "city": "detroit"},
        day_key="2026-03-21",
        dispatch_key="daily_sync_dispatch:7:2026-03-21:rentcast:rentcast-sale-listings:mi:wayne:detroit",
    )

    assert ctx["mode"] == "scheduler"
    assert ctx["scope"] == "daily_sync"
    assert ctx["org_id"] == 7
    assert ctx["source_id"] == 22
    assert ctx["source_key"] == "rentcast:rentcast-sale-listings"
    assert ctx["schedule_day"] == "2026-03-21"
    assert ctx["dispatch_key"].startswith("daily_sync_dispatch:")


def test_daily_market_refresh_task_uses_same_runtime_builder_for_all_sources(monkeypatch):
    queued = []

    class DummySource:
        def __init__(self, source_id: int, is_enabled: bool = True):
            self.id = source_id
            self.is_enabled = is_enabled
            self.provider = "rentcast"
            self.slug = f"rentcast-source-{source_id}"

    monkeypatch.setattr(ingestion_tasks, "SessionLocal", lambda: type("DB", (), {"close": lambda self: None})())
    monkeypatch.setattr(ingestion_tasks, "ensure_default_manual_sources", lambda db, org_id: None)
    monkeypatch.setattr(
        ingestion_tasks,
        "list_default_daily_markets",
        lambda: [
            {"state": "MI", "county": "wayne", "city": "detroit"},
            {"state": "MI", "county": "macomb", "city": "warren"},
        ],
    )
    monkeypatch.setattr(
        ingestion_tasks,
        "list_sources",
        lambda db, org_id: [DummySource(10), DummySource(20)],
    )

    def fake_build_runtime_payload(*, state, county, city):
        return {
            "trigger_type": "daily_refresh",
            "state": state,
            "county": county,
            "city": city,
            "limit": 250,
        }

    monkeypatch.setattr(ingestion_tasks, "build_runtime_payload", fake_build_runtime_payload)
    monkeypatch.setattr(
        ingestion_tasks.sync_source_task,
        "delay",
        lambda org_id, source_id, trigger_type, runtime_config: queued.append(
            {
                "org_id": org_id,
                "source_id": source_id,
                "trigger_type": trigger_type,
                "runtime_config": dict(runtime_config),
            }
        ),
    )

    result = ingestion_tasks.daily_market_refresh_task()

    assert result["ok"] is True
    assert result["queued"] == 4

    assert len(queued) == 4
    assert [q["source_id"] for q in queued] == [10, 20, 10, 20]

    for item in queued:
        assert item["org_id"] == 1
        assert item["trigger_type"] == "daily_refresh"
        assert item["runtime_config"]["trigger_type"] == "daily_refresh"
        assert item["runtime_config"]["state"] == "MI"
        assert item["runtime_config"]["limit"] == 250

    detroit = queued[0]["runtime_config"]
    assert detroit["county"] == "wayne"
    assert detroit["city"] == "detroit"

    warren = queued[2]["runtime_config"]
    assert warren["county"] == "macomb"
    assert warren["city"] == "warren"

    # scheduler-triggered syncs should now carry an idempotency context
    contexts = [q["runtime_config"].get("idempotency_context") for q in queued if q["runtime_config"].get("idempotency_context")]
    if contexts:
        assert len(contexts) == 4
        for ctx in contexts:
            assert ctx["mode"] == "scheduler"
            assert ctx["scope"] == "daily_sync"
            assert ctx["org_id"] == 1
            assert ctx["schedule_day"]
            assert ctx["dispatch_key"]