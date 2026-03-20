# backend/tests/steps/step_10_automated_ingestion/test_ingestion_run_execute.py
from __future__ import annotations

from types import SimpleNamespace

from app.services import ingestion_run_execute as svc
from app.services.ingestion_dedupe_service import build_property_fingerprint


def test_property_fingerprint_uses_normalized_address():
    a = build_property_fingerprint(
        address="123 Main Street",
        city="Detroit",
        state="MI",
        zip_code="48201",
    )
    b = build_property_fingerprint(
        address="123 main st.",
        city="Detroit",
        state="MI",
        zip_code="48201-1234",
    )
    assert a == b


def test_execute_source_sync_runs_property_first_pipeline_and_writes_summary(monkeypatch):
    started = []
    finished = []

    class FakeRun:
        def __init__(self):
            self.id = 999
            self.status = "running"
            self.summary_json = {}

    run = FakeRun()

    source = SimpleNamespace(
        id=77,
        provider="rentcast",
        cursor_json={},
    )

    prop_created = SimpleNamespace(id=501, org_id=1)
    prop_updated = SimpleNamespace(id=502, org_id=1)

    pipeline_calls = []

    monkeypatch.setattr(
        svc,
        "start_run",
        lambda db, org_id, source_id, trigger_type: started.append(
            {
                "org_id": org_id,
                "source_id": source_id,
                "trigger_type": trigger_type,
            }
        )
        or run,
    )

    monkeypatch.setattr(
        svc,
        "finish_run",
        lambda db, row, status, summary, error_summary=None, error_json=None: finished.append(
            {
                "status": status,
                "summary": dict(summary),
                "error_summary": error_summary,
                "error_json": error_json,
            }
        )
        or SimpleNamespace(
            id=row.id,
            status=status,
            summary_json=dict(summary),
        ),
    )

    monkeypatch.setattr(
        svc,
        "_collect_matching_rows",
        lambda source, trigger_type, runtime_config: (
            [
                {
                    "listingId": "ext-1",
                    "formattedAddress": "123 Main St",
                    "city": "Detroit",
                    "county": "wayne",
                    "state": "MI",
                    "zipCode": "48201",
                    "bedrooms": 3,
                    "bathrooms": 1.0,
                    "squareFootage": 1200,
                    "yearBuilt": 1950,
                    "propertyType": "single_family",
                    "price": 85000,
                    "photos": ["https://img/1.jpg"],
                },
                {
                    "listingId": "ext-2",
                    "formattedAddress": "456 Oak Ave",
                    "city": "Detroit",
                    "county": "wayne",
                    "state": "MI",
                    "zipCode": "48202",
                    "bedrooms": 4,
                    "bathrooms": 1.5,
                    "squareFootage": 1400,
                    "yearBuilt": 1948,
                    "propertyType": "single_family",
                    "price": 92000,
                    "photos": ["https://img/2.jpg"],
                },
            ],
            {"page": 2},
            {
                "records_seen": 2,
                "invalid_rows": 0,
                "filtered_out": 0,
                "duplicates_skipped": 0,
                "filter_reason_counts": {},
                "provider_pages_scanned": 1,
                "provider_fetch_limit": 250,
            },
        ),
    )

    monkeypatch.setattr(svc, "find_existing_by_external_id", lambda *args, **kwargs: None)

    upserted_props = [prop_created, prop_updated]

    monkeypatch.setattr(
        svc,
        "_upsert_property",
        lambda db, org_id, payload: (
            upserted_props.pop(0),
            payload["external_record_id"] == "ext-1",
        ),
    )
    monkeypatch.setattr(
        svc,
        "_upsert_deal",
        lambda db, org_id, property_id, payload: (
            SimpleNamespace(id=property_id + 1000),
            property_id == 501,
        ),
    )
    monkeypatch.setattr(
        svc,
        "_upsert_rent_assumption",
        lambda db, org_id, property_id, payload: (
            SimpleNamespace(id=property_id + 2000),
            True,
        ),
    )
    monkeypatch.setattr(
        svc,
        "_upsert_photos",
        lambda db, org_id, property_id, provider, photos: len(photos),
    )
    monkeypatch.setattr(
        svc,
        "upsert_record_link",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        svc,
        "execute_post_ingestion_pipeline",
        lambda db, org_id, property_id, actor_user_id=None, emit_events=False: pipeline_calls.append(property_id)
        or {
            "geo_ok": True,
            "risk_ok": True,
            "rent_ok": True,
            "evaluate_ok": True,
            "state_ok": True,
            "workflow_ok": True,
            "next_actions_ok": True,
            "partial": False,
            "errors": [],
        },
    )

    class FakeDB:
        def add(self, obj):
            return None

        def flush(self):
            return None

        def commit(self):
            return None

        def refresh(self, obj):
            return None

        def rollback(self):
            return None

        def get(self, model, pk):
            return None

        def scalar(self, *args, **kwargs):
            return None

    result = svc.execute_source_sync(
        FakeDB(),
        org_id=1,
        source=source,
        trigger_type="manual",
        runtime_config={
            "state": "MI",
            "county": "wayne",
            "city": "Detroit",
            "limit": 50,
        },
    )

    assert started == [{"org_id": 1, "source_id": 77, "trigger_type": "manual"}]
    assert pipeline_calls == [501, 502]

    assert result.status == "success"
    summary = result.summary_json

    assert summary["records_seen"] == 2
    assert summary["records_imported"] == 2
    assert summary["properties_created"] == 1
    assert summary["properties_updated"] == 1
    assert summary["deals_created"] == 1
    assert summary["deals_updated"] == 1
    assert summary["rent_rows_upserted"] == 2
    assert summary["photos_upserted"] == 2
    assert summary["geo_enriched"] == 2
    assert summary["risk_scored"] == 2
    assert summary["rent_refreshed"] == 2
    assert summary["evaluated"] == 2
    assert summary["state_synced"] == 2
    assert summary["workflow_synced"] == 2
    assert summary["next_actions_seeded"] == 2
    assert summary["post_import_failures"] == 0
    assert summary["post_import_partials"] == 0
    assert summary["filter_reason_counts"] == {}


def test_execute_source_sync_updates_cursor_for_scheduled_runs(monkeypatch):
    class FakeRun:
        def __init__(self):
            self.id = 321
            self.status = "running"
            self.summary_json = {}

    run = FakeRun()

    source = SimpleNamespace(
        id=88,
        provider="rentcast",
        cursor_json={"page": 1},
    )

    monkeypatch.setattr(svc, "start_run", lambda db, org_id, source_id, trigger_type: run)
    monkeypatch.setattr(
        svc,
        "finish_run",
        lambda db, row, status, summary, error_summary=None, error_json=None: SimpleNamespace(
            id=row.id,
            status=status,
            summary_json=dict(summary),
        ),
    )
    monkeypatch.setattr(
        svc,
        "_collect_matching_rows",
        lambda source, trigger_type, runtime_config: (
            [],
            {"page": 3},
            {
                "records_seen": 0,
                "invalid_rows": 0,
                "filtered_out": 0,
                "duplicates_skipped": 0,
                "filter_reason_counts": {"city": 2},
                "provider_pages_scanned": 2,
                "provider_fetch_limit": 250,
            },
        ),
    )

    class FakeDB:
        def __init__(self):
            self.commits = 0

        def add(self, obj):
            return None

        def commit(self):
            self.commits += 1

        def refresh(self, obj):
            return None

        def rollback(self):
            return None

    db = FakeDB()

    result = svc.execute_source_sync(
        db,
        org_id=1,
        source=source,
        trigger_type="daily_refresh",
        runtime_config={"city": "Detroit", "limit": 250},
    )

    assert result.status == "success"
    assert source.cursor_json == {"page": 3}
    assert db.commits >= 1
    assert result.summary_json["filter_reason_counts"] == {"city": 2}
    