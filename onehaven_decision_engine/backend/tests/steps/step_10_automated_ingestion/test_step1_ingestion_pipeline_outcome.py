from __future__ import annotations

from app.routers.ingestion import _pipeline_outcome


def test_pipeline_outcome_maps_summary_fields() -> None:
    summary = {
        "records_seen": 12,
        "records_imported": 5,
        "properties_created": 3,
        "properties_updated": 2,
        "deals_created": 3,
        "deals_updated": 2,
        "rent_rows_upserted": 5,
        "photos_upserted": 7,
        "duplicates_skipped": 4,
        "invalid_rows": 1,
        "filtered_out": 2,
        "location_automation_enabled": True,
        "geo_enriched": 5,
        "risk_scored": 5,
        "rent_refreshed": 5,
        "evaluated": 5,
        "state_synced": 5,
        "workflow_synced": 5,
        "next_actions_seeded": 4,
        "post_import_failures": 1,
        "post_import_partials": 1,
        "post_import_errors": [{"property_id": 12, "errors": ["geo:failed"]}],
        "filter_reason_counts": {"county": 1, "min_price": 1},
        "normal_path": True,
    }

    out = _pipeline_outcome(summary)

    assert out["records_seen"] == 12
    assert out["records_imported"] == 5
    assert out["properties_created"] == 3
    assert out["properties_updated"] == 2
    assert out["deals_created"] == 3
    assert out["deals_updated"] == 2
    assert out["rent_rows_upserted"] == 5
    assert out["photos_upserted"] == 7
    assert out["duplicates_skipped"] == 4
    assert out["invalid_rows"] == 1
    assert out["filtered_out"] == 2
    assert out["location_automation_enabled"] is True

    assert out["enrichments_completed"]["geo"] == 5
    assert out["enrichments_completed"]["risk"] == 5
    assert out["enrichments_completed"]["rent"] == 5
    assert out["evaluations_completed"] == 5

    assert out["workflow"]["state_synced"] == 5
    assert out["workflow"]["workflow_synced"] == 5
    assert out["workflow"]["next_actions_seeded"] == 4

    assert out["failures"] == 1
    assert out["partials"] == 1
    assert out["errors"] == [{"property_id": 12, "errors": ["geo:failed"]}]
    assert out["filter_reason_counts"] == {"county": 1, "min_price": 1}
    assert out["normal_path"] is True
    