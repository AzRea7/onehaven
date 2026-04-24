from __future__ import annotations

from app.services import jurisdiction_refresh_service as refresh_svc
from app.tasks import ingestion_tasks


def test_build_refresh_payload_contains_scope_and_force():
    payload = refresh_svc.build_jurisdiction_refresh_payload(
        state="MI",
        county="macomb",
        city="warren",
        pha_name=None,
        org_id=1,
        force=True,
    )

    assert payload["state"] == "MI"
    assert payload["county"] == "macomb"
    assert payload["city"] == "warren"
    assert payload["org_id"] == 1
    assert payload["force"] is True


def test_daily_jurisdiction_refresh_task_queues_refreshes(monkeypatch):
    queued = []

    monkeypatch.setattr(refresh_svc, "list_markets_needing_refresh", lambda db, org_id=None, limit=None: [
        {"state": "MI", "county": "macomb", "city": "warren", "pha_name": None},
        {"state": "MI", "county": "wayne", "city": "detroit", "pha_name": None},
    ])

    class DummyDelay:
        def delay(self, *args, **kwargs):
            queued.append((args, kwargs))

    monkeypatch.setattr(ingestion_tasks, "refresh_jurisdiction_market_task", DummyDelay())

    result = ingestion_tasks.refresh_due_jurisdictions_task()

    assert result["ok"] is True
    assert result["queued"] == 2
    assert len(queued) == 2