from app.models import IngestionSource
from app.services.ingestion_run_execute import execute_source_sync


def test_execute_source_sync_reports_geo_and_workflow_counts(db_session):
    source = IngestionSource(
        org_id=1,
        provider="rentcast",
        slug="rentcast-sale-listings",
        display_name="RentCast Sale Listings",
        source_type="api",
        status="connected",
        is_enabled=True,
        config_json={
            "sample_rows": [
                {
                    "external_record_id": "rc-post-import-a",
                    "address": "111 Workflow Ave",
                    "city": "Detroit",
                    "state": "MI",
                    "zip": "48201",
                    "bedrooms": 3,
                    "bathrooms": 1,
                    "asking_price": 95000,
                    "photos": [],
                }
            ]
        },
        credentials_json={},
        cursor_json={},
    )
    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    run = execute_source_sync(db_session, org_id=1, source=source, trigger_type="manual", runtime_config={"city": "Detroit", "limit": 10})

    assert run.status == "success"
    summary = run.summary_json or {}
    assert summary.get("workflow_recomputed", 0) >= 1
    assert "geo_enriched" in summary
