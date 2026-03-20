# backend/tests/steps/step_10_automated_ingestion/test_ingestion_pipeline_post_import.py
from __future__ import annotations

from types import SimpleNamespace

from app.services.ingestion_enrichment_service import (
    apply_pipeline_summary,
    canonical_listing_payload,
)
from app.services.ingestion_scheduler_service import list_default_daily_markets


def test_canonical_listing_payload_keeps_core_fields():
    row = {
        "listingId": "abc123",
        "formattedAddress": "123 Main St",
        "city": "Detroit",
        "state": "MI",
        "zipCode": "48201",
        "price": 120000,
        "rentEstimate": 1450,
    }
    out = canonical_listing_payload(row)
    assert out["external_record_id"] == "abc123"
    assert out["address"] == "123 Main St"
    assert out["asking_price"] == 120000
    assert out["market_rent_estimate"] == 1450


def test_default_daily_markets_are_seeded_for_southeast_michigan():
    markets = list_default_daily_markets()
    city_names = {m["city"].lower() for m in markets}
    assert "detroit" in city_names
    assert "warren" in city_names
    assert "pontiac" in city_names


def test_apply_pipeline_summary_counts_completed_pipeline_steps():
    summary = {}
    pipeline_res = {
        "geo_ok": True,
        "risk_ok": True,
        "rent_ok": True,
        "evaluate_ok": True,
        "state_ok": True,
        "workflow_ok": True,
        "next_actions_ok": True,
        "partial": False,
        "errors": [],
    }

    apply_pipeline_summary(summary, pipeline_res, property_id=101)

    assert summary["post_import_pipeline_attempted"] == 1
    assert summary["geo_enriched"] == 1
    assert summary["risk_scored"] == 1
    assert summary["rent_refreshed"] == 1
    assert summary["evaluated"] == 1
    assert summary["state_synced"] == 1
    assert summary["workflow_synced"] == 1
    assert summary["next_actions_seeded"] == 1
    assert summary.get("post_import_failures", 0) == 0
    assert summary.get("post_import_partials", 0) == 0
    assert summary.get("post_import_errors", []) == []


def test_apply_pipeline_summary_records_partial_failures_with_property_context():
    summary = {}
    pipeline_res = {
        "geo_ok": True,
        "risk_ok": False,
        "rent_ok": True,
        "evaluate_ok": False,
        "state_ok": True,
        "workflow_ok": False,
        "next_actions_ok": False,
        "partial": True,
        "errors": [
            "risk:RuntimeError:boom",
            "evaluate:ValueError:no_deal",
        ],
    }

    apply_pipeline_summary(summary, pipeline_res, property_id=202)

    assert summary["post_import_pipeline_attempted"] == 1
    assert summary["geo_enriched"] == 1
    assert summary["rent_refreshed"] == 1
    assert summary["state_synced"] == 1
    assert summary["post_import_partials"] == 1
    assert summary["post_import_failures"] == 1

    errors = summary["post_import_errors"]
    assert len(errors) == 1
    assert errors[0]["property_id"] == 202
    assert "risk:RuntimeError:boom" in errors[0]["errors"]
    assert "evaluate:ValueError:no_deal" in errors[0]["errors"]
    