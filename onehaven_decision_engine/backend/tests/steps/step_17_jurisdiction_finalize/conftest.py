from __future__ import annotations

import json
from types import SimpleNamespace

import pytest


class FakeQuery:
    def __init__(self, rows):
        self._rows = list(rows)

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def count(self):
        return len(self._rows)


class FakeDB:
    def __init__(self, *, queries: dict[type, list] | None = None):
        self.queries = queries or {}
        self.added = []
        self.committed = 0
        self.refreshed = []

    def query(self, model):
        return FakeQuery(self.queries.get(model, []))

    def add(self, row):
        self.added.append(row)

    def add_all(self, rows):
        self.added.extend(rows)

    def commit(self):
        self.committed += 1

    def refresh(self, row):
        self.refreshed.append(row)


@pytest.fixture
def fake_db():
    return FakeDB()


@pytest.fixture
def sample_profile():
    return SimpleNamespace(
        id=101,
        org_id=None,
        state="MI",
        county="macomb",
        city="warren",
        friction_multiplier=1.25,
        pha_name=None,
        policy_json=json.dumps(
            {
                "coverage": {"coverage_status": "verified_extended"},
                "required_actions": [
                    {
                        "code": "REGISTER_RENTAL",
                        "title": "Register rental property",
                        "category": "rental_registration",
                    },
                    {
                        "code": "SCHEDULE_INSPECTION",
                        "title": "Schedule local inspection",
                        "category": "inspection",
                    },
                ],
                "blocking_items": [
                    {
                        "code": "CERT_BEFORE_OCCUPANCY",
                        "title": "Certificate required before occupancy",
                        "category": "certificate_of_occupancy",
                    }
                ],
            }
        ),
        notes="Projected from verified policy assertions.",
        completeness_status="partial",
        completeness_score=0.67,
        stale_status="fresh",
        stale_reason=None,
        last_verified_at=None,
        source_freshness_json=json.dumps(
            {
                "fresh": 4,
                "stale": 1,
            }
        ),
        required_categories_json=json.dumps(
            [
                "rental_registration",
                "inspection",
                "certificate_of_occupancy",
                "source_of_income",
            ]
        ),
        category_coverage_json=json.dumps(
            {
                "rental_registration": "verified",
                "inspection": "verified",
                "certificate_of_occupancy": "conditional",
                "source_of_income": "missing",
            }
        ),
    )