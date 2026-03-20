from __future__ import annotations

from app.services.ingestion_enrichment_service import apply_pipeline_summary


def test_apply_pipeline_summary_counts_successes() -> None:
    summary: dict = {}

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


def test_apply_pipeline_summary_counts_partial_and_errors() -> None:
    summary: dict = {}

    pipeline_res = {
        "geo_ok": True,
        "risk_ok": False,
        "rent_ok": True,
        "evaluate_ok": False,
        "state_ok": True,
        "workflow_ok": False,
        "next_actions_ok": False,
        "partial": True,
        "errors": ["risk:RuntimeError:boom", "evaluate:ValueError:bad"],
    }

    apply_pipeline_summary(summary, pipeline_res, property_id=202)

    assert summary["post_import_pipeline_attempted"] == 1
    assert summary["geo_enriched"] == 1
    assert summary["rent_refreshed"] == 1
    assert summary["state_synced"] == 1
    assert summary["post_import_partials"] == 1
    assert summary["post_import_failures"] == 1

    errors = list(summary["post_import_errors"])
    assert len(errors) == 1
    assert errors[0]["property_id"] == 202
    assert errors[0]["errors"] == ["risk:RuntimeError:boom", "evaluate:ValueError:bad"]
    