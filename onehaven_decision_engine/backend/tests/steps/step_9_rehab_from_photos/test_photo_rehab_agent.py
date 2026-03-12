from __future__ import annotations

from types import SimpleNamespace

from app.services import photo_rehab_agent as agent


class FakeScalarResult:
    def __init__(self, items):
        self._items = items

    def all(self):
        return list(self._items)


class FakeDB:
    def __init__(self, prop, photos, existing_task=None):
        self.prop = prop
        self.photos = photos
        self.existing_task = existing_task
        self.added = []
        self.flushed = 0
        self.committed = 0

    def scalar(self, stmt):
        text = str(stmt)
        if "FROM properties" in text:
            return self.prop
        if "FROM rehab_tasks" in text:
            return self.existing_task
        return None

    def scalars(self, stmt):
        return FakeScalarResult(self.photos)

    def add(self, row):
        self.added.append(row)

    def flush(self):
        self.flushed += 1
        # give fake ids
        for idx, row in enumerate(self.added, start=1):
            if getattr(row, "id", None) is None:
                setattr(row, "id", idx)

    def commit(self):
        self.committed += 1


def test_analyze_property_photos_returns_issues():
    db = FakeDB(
        prop=SimpleNamespace(id=10, org_id=1),
        photos=[
            SimpleNamespace(id=1, kind="interior"),
            SimpleNamespace(id=2, kind="exterior"),
        ],
    )

    out = agent.analyze_property_photos(db, org_id=1, property_id=10)

    assert out["ok"] is True
    assert out["photo_count"] == 2
    assert len(out["issues"]) >= 2
    assert out["summary"]["interior"] == 1
    assert out["summary"]["exterior"] == 1


def test_analyze_and_create_rehab_tasks_is_idempotent_when_existing_found():
    db = FakeDB(
        prop=SimpleNamespace(id=10, org_id=1),
        photos=[SimpleNamespace(id=1, kind="interior")],
        existing_task=SimpleNamespace(id=777, title="Kitchen / bath turnover scope"),
    )

    analysis = {
        "ok": True,
        "property_id": 10,
        "photo_count": 1,
        "summary": {"interior": 1, "exterior": 0, "unknown": 0},
        "issues": [
            {
                "title": "Kitchen / bath turnover scope",
                "category": "interior_finish",
                "severity": "medium",
                "estimated_cost": 4200.0,
                "blocker": False,
                "notes": "test",
                "evidence_photo_ids": [1],
            }
        ],
    }

    out = agent.create_rehab_tasks_from_analysis(
        db,
        org_id=1,
        property_id=10,
        analysis=analysis,
    )

    assert out["ok"] is True
    assert out["created"] == 0