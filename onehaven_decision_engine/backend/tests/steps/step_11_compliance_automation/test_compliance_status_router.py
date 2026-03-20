from __future__ import annotations

from types import SimpleNamespace

from app.routers import compliance as router_mod


class DummyScalarResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def all(self):
        return list(self._rows)


class DummyDB:
    def __init__(self, checklist_rows, inspection_row):
        self.checklist_rows = checklist_rows
        self.inspection_row = inspection_row
        self.scalar_calls = 0

    def scalar(self, *args, **kwargs):
        self.scalar_calls += 1
        if self.scalar_calls == 1:
            return self.inspection_row
        return None

    def scalars(self, *args, **kwargs):
        return DummyScalarResult(self.checklist_rows)


def test_compliance_status_exposes_jurisdiction_counts(monkeypatch):
    fake_prop = SimpleNamespace(
        id=55,
        org_id=7,
        city="Warren",
        county="Macomb",
        state="MI",
    )
    fake_principal = SimpleNamespace(org_id=7)

    checklist_rows = [
        SimpleNamespace(status="done"),
        SimpleNamespace(status="failed"),
        SimpleNamespace(status="todo"),
    ]
    inspection_row = SimpleNamespace(passed=False)

    db = DummyDB(checklist_rows, inspection_row)

    monkeypatch.setattr(router_mod, "_must_get_property", lambda db, org_id, property_id: fake_prop)
    monkeypatch.setattr(router_mod, "require_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        router_mod,
        "build_workflow_summary",
        lambda db, org_id, property_id, recompute=True: {"current_stage": "compliance"},
    )

    monkeypatch.setattr(
        router_mod,
        "build_property_inspection_readiness",
        lambda db, org_id, property_id: {
            "ok": True,
            "property_id": property_id,
            "overall_status": "not_ready",
            "score_pct": 62,
            "readiness": {
                "hqs_ready": False,
                "local_ready": False,
                "voucher_ready": True,
                "lease_up_ready": True,
            },
            "counts": {
                "blocking_count": 1,
                "warning_count": 1,
                "recommended_action_count": 1,
            },
            "blocking_items": [{"code": "NO_PO_BOX"}],
            "warning_items": [{"code": "INSPECTION_NOT_PASSED"}],
            "recommended_actions": [{"code": "CITY_DEBT_CLEARANCE"}],
            "coverage": {
                "profile_id": 501,
                "scope": "org",
                "match_level": "city",
            },
        },
    )

    out = router_mod.compliance_status(property_id=55, db=db, p=fake_principal)

    assert out["property_id"] == 55
    assert out["passed"] is False
    assert out["overall_status"] == "not_ready"
    assert out["score_pct"] == 62
    assert out["readiness"]["hqs_ready"] is False
    assert out["counts"]["blocking_count"] == 1
    assert out["blocking_items"][0]["code"] == "NO_PO_BOX"
    assert out["recommended_actions"][0]["code"] == "CITY_DEBT_CLEARANCE"
    assert out["coverage"]["profile_id"] == 501
    