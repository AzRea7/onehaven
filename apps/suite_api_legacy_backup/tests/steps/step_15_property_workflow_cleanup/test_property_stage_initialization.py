# backend/tests/steps/step_15_property_workflow_cleanup/test_property_stage_initialization.py
from __future__ import annotations

from types import SimpleNamespace

from app.services import property_state_machine as psm


class FakeDB:
    def __init__(self, state_row=None):
        self.state_row = state_row
        self.added = []

    def scalar(self, query):
        return self.state_row

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        return None


def test_ensure_state_row_initializes_new_property_state():
    db = FakeDB(state_row=None)

    row = psm.ensure_state_row(
        db,
        org_id=1,
        property_id=501,
    )

    assert row.org_id == 1
    assert row.property_id == 501
    assert row.current_stage == "deal"
    assert row in db.added


def test_ensure_state_row_returns_existing_state_without_recreating():
    existing = SimpleNamespace(
        org_id=1,
        property_id=501,
        current_stage="rehab",
    )
    db = FakeDB(state_row=existing)

    row = psm.ensure_state_row(
        db,
        org_id=1,
        property_id=501,
    )

    assert row is existing
    assert db.added == []
    