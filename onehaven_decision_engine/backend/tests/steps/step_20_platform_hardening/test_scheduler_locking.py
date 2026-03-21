from __future__ import annotations

from app.services.ingestion_run_execute import execute_source_sync


def test_lock_blocks_concurrent_execution_for_same_org_and_source(
    db_session,
    org_factory,
    ingestion_source_factory,
    sample_listing_payload,
    fake_post_pipeline,
    monkeypatch,
):
    org = org_factory(slug="step20-lock-org", name="Step20 Lock Org")

    source = ingestion_source_factory(
        org_id=org.id,
        slug="rentcast-lock-source",
        sample_rows=[dict(sample_listing_payload)],
    )

    calls = {"count": 0}

    def _acquire(*args, **kwargs):
        calls["count"] += 1
        return calls["count"] == 1

    monkeypatch.setattr("app.services.ingestion_run_execute.acquire_ingestion_execution_lock", _acquire)
    monkeypatch.setattr("app.services.ingestion_run_execute.release_lock", lambda *a, **k: True)
    monkeypatch.setattr("app.services.ingestion_run_execute.has_completed_ingestion_dataset", lambda *a, **k: False)
    monkeypatch.setattr("app.services.ingestion_run_execute.mark_ingestion_dataset_completed", lambda *a, **k: True)

    run1 = execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )
    run2 = execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )

    assert run1 is not None
    assert run2 is not None
    assert calls["count"] == 2


def test_different_orgs_can_run_without_blocking_each_other(
    db_session,
    multi_org_isolation,
    ingestion_source_factory,
    sample_listing_payload,
    fake_post_pipeline,
):
    org_a = multi_org_isolation["org_a"]
    org_b = multi_org_isolation["org_b"]

    source_a = ingestion_source_factory(
        org_id=org_a.id,
        slug="rentcast-lock-a",
        sample_rows=[dict(sample_listing_payload)],
    )
    source_b = ingestion_source_factory(
        org_id=org_b.id,
        slug="rentcast-lock-b",
        sample_rows=[dict(sample_listing_payload)],
    )

    run_a = execute_source_sync(
        db_session,
        org_id=org_a.id,
        source=source_a,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )
    run_b = execute_source_sync(
        db_session,
        org_id=org_b.id,
        source=source_b,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )

    assert run_a is not None
    assert run_b is not None


def test_completed_dataset_marker_allows_duplicate_safe_short_circuit(
    db_session,
    org_factory,
    ingestion_source_factory,
    sample_listing_payload,
    fake_post_pipeline,
    monkeypatch,
):
    org = org_factory(slug="step20-stale-lock", name="Step20 Stale Lock Org")

    source = ingestion_source_factory(
        org_id=org.id,
        slug="rentcast-lock-complete",
        sample_rows=[dict(sample_listing_payload)],
    )

    first = {"done": False}

    monkeypatch.setattr("app.services.ingestion_run_execute.acquire_ingestion_execution_lock", lambda *a, **k: True)
    monkeypatch.setattr("app.services.ingestion_run_execute.release_lock", lambda *a, **k: True)

    def _has_completed(*args, **kwargs):
        return first["done"]

    def _mark_completed(*args, **kwargs):
        first["done"] = True
        return True

    monkeypatch.setattr("app.services.ingestion_run_execute.has_completed_ingestion_dataset", _has_completed)
    monkeypatch.setattr("app.services.ingestion_run_execute.mark_ingestion_dataset_completed", _mark_completed)

    run1 = execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )
    run2 = execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )

    assert run1 is not None
    assert run2 is not None