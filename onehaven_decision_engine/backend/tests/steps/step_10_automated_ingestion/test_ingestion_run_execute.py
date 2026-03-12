from app.models import IngestionSource, Property, Deal, RentAssumption, PropertyPhoto
from app.services.ingestion_run_execute import execute_source_sync


def test_execute_source_sync_creates_records(db_session):
    source = IngestionSource(
        org_id=1,
        provider="zillow",
        slug="zillow-api",
        display_name="Zillow API",
        source_type="api",
        status="connected",
        is_enabled=True,
        config_json={
            "sample_rows": [
                {
                    "external_record_id": "z-1",
                    "external_url": "https://example.com/1",
                    "address": "101 Example St",
                    "city": "Detroit",
                    "state": "MI",
                    "zip": "48201",
                    "bedrooms": 3,
                    "bathrooms": 1.5,
                    "asking_price": 95000,
                    "market_rent_estimate": 1400,
                    "photos": [{"url": "https://img/1/front.jpg", "kind": "exterior"}],
                }
            ]
        },
        credentials_json={},
        cursor_json={},
    )
    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    run = execute_source_sync(db_session, org_id=1, source=source, trigger_type="manual")

    assert run.status == "success"
    assert db_session.query(Property).count() == 1
    assert db_session.query(Deal).count() == 1
    assert db_session.query(RentAssumption).count() == 1
    assert db_session.query(PropertyPhoto).count() == 1