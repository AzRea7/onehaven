from __future__ import annotations

from types import SimpleNamespace

from app.routers import compliance as router_mod


class DummyScalarResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class DummyDB:
    def __init__(self, checklist_rows, inspection_row):
        self._checklist_rows = checklist_rows
        self._inspection_row = inspection_row

    def scalars(self, stmt):
        return DummyScalarResult(self._checklist_rows)

    def scalar(self, stmt):
        return self._inspection_row


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
        "resolve_operational_policy",
        lambda db, org_id, city, county, state: {
            "profile_id": 501,
            "scope": "org",
            "match_level": "city",
            "blocking_items": [{"code": "NO_PO_BOX"}],
            "required_actions": [{"code": "CITY_DEBT_CLEARANCE"}],
        },
    )
    monkeypatch.setattr(
        router_mod,
        "build_workflow_summary",
        lambda db, org_id, property_id, recompute=True: {"current_stage": "compliance"},
    )

    out = router_mod.compliance_status(property_id=55, db=db, p=fake_principal)

    assert out["property_id"] == 55
    assert out["jurisdiction_profile_id"] == 501
    assert out["jurisdiction_scope"] == "org"
    assert out["jurisdiction_match_level"] == "city"
    assert out["blocking_item_count"] == 1
    assert out["required_action_count"] == 1
    assert out["passed"] is False