from __future__ import annotations

from datetime import datetime, timedelta

from app.models import Property
from app.tasks.ingestion_tasks import (
    refresh_property_location_task,
    refresh_stale_locations_task,
)


def test_refresh_property_location_task_returns_result(monkeypatch, db_session) -> None:
    prop = Property(
        org_id=1,
        address="123 Main St",
        city="Detroit",
        state="MI",
        zip="48226",
        bedrooms=3,
        bathrooms=1.0,
    )
    db_session.add(prop)
    db_session.commit()
    db_session.refresh(prop)

    def fake_enrich_property_geo(db, *, org_id: int, property_id: int, force: bool = False, google_api_key=None):
        row = db.get(Property, property_id)
        row.normalized_address = "123 Main St, Detroit, MI 48226"
        row.lat = 42.3314
        row.lng = -83.0458
        row.geocode_source = "google"
        row.geocode_confidence = 0.99
        db.add(row)
        db.commit()
        return {"ok": True, "property_id": property_id, "lat": row.lat, "lng": row.lng}

    monkeypatch.setattr(
        "app.tasks.ingestion_tasks.enrich_property_geo",
        fake_enrich_property_geo,
    )

    result = refresh_property_location_task.run(org_id=1, property_id=prop.id, force=True)

    assert result["ok"] is True
    assert result["property_id"] == prop.id
    assert result["force"] is True
    assert result["result"]["ok"] is True


def test_refresh_stale_locations_task_queues_matching_properties(monkeypatch, db_session) -> None:
    stale = Property(
        org_id=1,
        address="123 Main St",
        city="Detroit",
        state="MI",
        zip="48226",
        bedrooms=3,
        bathrooms=1.0,
        lat=42.33,
        lng=-83.04,
        normalized_address="123 Main St, Detroit, MI 48226",
        geocode_last_refreshed=datetime.utcnow() - timedelta(days=30),
    )
    missing = Property(
        org_id=1,
        address="456 Oak St",
        city="Detroit",
        state="MI",
        zip="48227",
        bedrooms=3,
        bathrooms=1.0,
        lat=None,
        lng=None,
    )
    fresh = Property(
        org_id=1,
        address="789 Pine St",
        city="Detroit",
        state="MI",
        zip="48228",
        bedrooms=3,
        bathrooms=1.0,
        lat=42.34,
        lng=-83.05,
        normalized_address="789 Pine St, Detroit, MI 48228",
        geocode_last_refreshed=datetime.utcnow(),
    )

    db_session.add_all([stale, missing, fresh])
    db_session.commit()

    queued_calls: list[tuple[int, int, bool]] = []

    def fake_delay(org_id: int, property_id: int, force: bool):
        queued_calls.append((org_id, property_id, force))
        return {"ok": True}

    monkeypatch.setattr(
        "app.tasks.ingestion_tasks.refresh_property_location_task.delay",
        fake_delay,
    )

    class DummySessionLocal:
        def __call__(self):
            return db_session

    monkeypatch.setattr("app.tasks.ingestion_tasks.SessionLocal", DummySessionLocal())

    result = refresh_stale_locations_task.run(org_id=1, force=False, batch_size=10)

    assert result["ok"] is True
    assert result["queued"] == 2

    queued_ids = {item[1] for item in queued_calls}
    assert stale.id in queued_ids
    assert missing.id in queued_ids
    assert fresh.id not in queued_ids