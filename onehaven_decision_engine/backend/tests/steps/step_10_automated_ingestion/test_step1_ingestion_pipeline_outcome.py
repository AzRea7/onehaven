from __future__ import annotations

from app.routers.ingestion import _pipeline_outcome


def test_pipeline_outcome_defaults_to_zero_and_empty_containers():
    out = _pipeline_outcome(None)

    assert out["records_seen"] == 0
    assert out["records_imported"] == 0
    assert out["properties_created"] == 0
    assert out["properties_updated"] == 0
    assert out["deals_created"] == 0
    assert out["deals_updated"] == 0
    assert out["rent_rows_upserted"] == 0
    assert out["photos_upserted"] == 0
    assert out["duplicates_skipped"] == 0
    assert out["invalid_rows"] == 0
    assert out["filtered_out"] == 0

    assert out["enrichments_completed"] == {
        "geo": 0,
        "risk": 0,
        "rent": 0,
    }
    assert out["evaluations_completed"] == 0
    assert out["workflow"] == {
        "state_synced": 0,
        "workflow_synced": 0,
        "next_actions_seeded": 0,
    }
    assert out["failures"] == 0
    assert out["partials"] == 0
    assert out["errors"] == []
    assert out["filter_reason_counts"] == {}


def test_pipeline_outcome_maps_property_first_summary_fields():
    summary = {
        "records_seen": 42,
        "records_imported": 18,
        "properties_created": 9,
        "properties_updated": 6,
        "deals_created": 9,
        "deals_updated": 5,
        "rent_rows_upserted": 18,
        "photos_upserted": 31,
        "duplicates_skipped": 7,
        "invalid_rows": 2,
        "filtered_out": 15,
        "geo_enriched": 14,
        "risk_scored": 14,
        "rent_refreshed": 13,
        "evaluated": 12,
        "state_synced": 11,
        "workflow_synced": 11,
        "next_actions_seeded": 10,
        "post_import_failures": 3,
        "post_import_partials": 2,
        "post_import_errors": [
            {"property_id": 1001, "step": "rent", "error": "timeout"},
            {"property_id": 1002, "step": "evaluate", "error": "missing_deal"},
        ],
        "filter_reason_counts": {
            "county_mismatch": 4,
            "price_out_of_range": 6,
        },
    }

    out = _pipeline_outcome(summary)

    assert out["records_seen"] == 42
    assert out["records_imported"] == 18
    assert out["properties_created"] == 9
    assert out["properties_updated"] == 6
    assert out["deals_created"] == 9
    assert out["deals_updated"] == 5
    assert out["rent_rows_upserted"] == 18
    assert out["photos_upserted"] == 31
    assert out["duplicates_skipped"] == 7
    assert out["invalid_rows"] == 2
    assert out["filtered_out"] == 15

    assert out["enrichments_completed"] == {
        "geo": 14,
        "risk": 14,
        "rent": 13,
    }
    assert out["evaluations_completed"] == 12
    assert out["workflow"] == {
        "state_synced": 11,
        "workflow_synced": 11,
        "next_actions_seeded": 10,
    }
    assert out["failures"] == 3
    assert out["partials"] == 2
    assert out["errors"] == [
        {"property_id": 1001, "step": "rent", "error": "timeout"},
        {"property_id": 1002, "step": "evaluate", "error": "missing_deal"},
    ]
    assert out["filter_reason_counts"] == {
        "county_mismatch": 4,
        "price_out_of_range": 6,
    }


def test_pipeline_outcome_tolerates_partial_summary_keys():
    summary = {
        "properties_created": 5,
        "properties_updated": 3,
        "evaluated": 4,
    }

    out = _pipeline_outcome(summary)

    assert out["properties_created"] == 5
    assert out["properties_updated"] == 3
    assert out["evaluations_completed"] == 4

    assert out["enrichments_completed"]["geo"] == 0
    assert out["enrichments_completed"]["risk"] == 0
    assert out["enrichments_completed"]["rent"] == 0
    assert out["workflow"]["state_synced"] == 0
    assert out["workflow"]["workflow_synced"] == 0
    assert out["workflow"]["next_actions_seeded"] == 0