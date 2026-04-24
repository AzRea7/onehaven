from __future__ import annotations

from datetime import datetime, timedelta

from app.models import Property
from products.acquire.backend.src.services.ingestion_scheduler_service import (
    build_location_refresh_payload,
    list_properties_needing_location_refresh,
)


def test_build_location_refresh_payload_defaults() -> None:
    payload = build_location_refresh_payload()

    assert payload["trigger_type"] == "location_refresh"
    assert payload["force"] is False
    assert isinstance(payload["batch_size"], int)
    assert payload["batch_size"] >= 1


def test_build_location_refresh_payload_custom_values() -> None:
    payload = build_location_refresh_payload(force=True, batch_size=25)

    assert payload == {
        "trigger_type": "location_refresh",
        "force": True,
        "batch_size": 25,
    }


def test_list_properties_needing_location_refresh_returns_missing_coordinates(db_session) -> None:
    p1 = Property(
        org_id=1,
        address="123 Main St",
        city="Detroit",
        state="MI",
        zip="48226",
        bedrooms=3,
        bathrooms=1.0,
        lat=None,
        lng=None,
    )
    p2 = Property(
        org_id=1,
        address="456 Oak St",
        city="Detroit",
        state="MI",
        zip="48227",
        bedrooms=3,
        bathrooms=1.0,
        lat=42.3,
        lng=-83.0,
        normalized_address="456 Oak St, Detroit, MI 48227",
        geocode_last_refreshed=datetime.utcnow(),
    )

    db_session.add_all([p1, p2])
    db_session.commit()

    rows = list_properties_needing_location_refresh(db_session, org_id=1, batch_size=50)
    ids = {x.id for x in rows}

    assert p1.id in ids
    assert p2.id not in ids


def test_list_properties_needing_location_refresh_returns_stale_records(db_session) -> None:
    stale = Property(
        org_id=1,
        address="789 Pine St",
        city="Detroit",
        state="MI",
        zip="48228",
        bedrooms=3,
        bathrooms=1.0,
        lat=42.30,
        lng=-83.10,
        normalized_address="789 Pine St, Detroit, MI 48228",
        geocode_last_refreshed=datetime.utcnow() - timedelta(days=30),
    )
    fresh = Property(
        org_id=1,
        address="100 Cedar St",
        city="Detroit",
        state="MI",
        zip="48229",
        bedrooms=3,
        bathrooms=1.0,
        lat=42.31,
        lng=-83.11,
        normalized_address="100 Cedar St, Detroit, MI 48229",
        geocode_last_refreshed=datetime.utcnow(),
    )

    db_session.add_all([stale, fresh])
    db_session.commit()

    rows = list_properties_needing_location_refresh(db_session, org_id=1, batch_size=50)
    ids = {x.id for x in rows}

    assert stale.id in ids
    assert fresh.id not in ids


def test_list_properties_needing_location_refresh_respects_org_and_limit(db_session) -> None:
    rows_to_add = []
    for idx in range(5):
        rows_to_add.append(
            Property(
                org_id=1,
                address=f"{idx} Main St",
                city="Detroit",
                state="MI",
                zip="48226",
                bedrooms=3,
                bathrooms=1.0,
                lat=None,
                lng=None,
            )
        )
    rows_to_add.append(
        Property(
            org_id=2,
            address="999 Other St",
            city="Detroit",
            state="MI",
            zip="48226",
            bedrooms=3,
            bathrooms=1.0,
            lat=None,
            lng=None,
        )
    )

    db_session.add_all(rows_to_add)
    db_session.commit()

    rows = list_properties_needing_location_refresh(db_session, org_id=1, batch_size=3)

    assert len(rows) == 3
    assert all(x.org_id == 1 for x in rows)
    