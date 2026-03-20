from __future__ import annotations

from types import SimpleNamespace

from app.services import dashboard_rollups


class _FakeScalarResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _FakeDB:
    def __init__(self, props):
        self._props = props

    def scalars(self, stmt):
        return _FakeScalarResult(self._props)


def test_compute_rollups_uses_normalized_state_truth(monkeypatch):
    props = [
        SimpleNamespace(
            id=1,
            org_id=1,
            address="1 Good St",
            city="Detroit",
            state="MI",
            county="wayne",
            crime_score=20.0,
            offender_count=0,
            is_red_zone=False,
        ),
        SimpleNamespace(
            id=2,
            org_id=1,
            address="2 Review St",
            city="Detroit",
            state="MI",
            county="wayne",
            crime_score=50.0,
            offender_count=1,
            is_red_zone=False,
        ),
        SimpleNamespace(
            id=3,
            org_id=1,
            address="3 Reject St",
            city="Warren",
            state="MI",
            county="macomb",
            crime_score=90.0,
            offender_count=3,
            is_red_zone=True,
        ),
    ]

    fake_db = _FakeDB(props)

    def _fake_state_payload(db, org_id, property_id, recompute=True):
        mapping = {
            1: {
                "current_stage": "deal",
                "normalized_decision": "GOOD",
                "gate_status": "OPEN",
                "constraints": {"crime_label": "LOW"},
                "stage_completion_summary": {"completed_count": 1, "total_count": 6},
                "next_actions": [],
            },
            2: {
                "current_stage": "rehab",
                "normalized_decision": "REVIEW",
                "gate_status": "BLOCKED",
                "constraints": {"crime_label": "MODERATE"},
                "stage_completion_summary": {"completed_count": 1, "total_count": 6},
                "next_actions": ["Complete rehab tasks"],
            },
            3: {
                "current_stage": "deal",
                "normalized_decision": "REJECT",
                "gate_status": "BLOCKED",
                "constraints": {"crime_label": "HIGH"},
                "stage_completion_summary": {"completed_count": 0, "total_count": 6},
                "next_actions": ["Update assumptions if re-underwriting is desired"],
            },
        }
        return mapping[property_id]

    monkeypatch.setattr(dashboard_rollups, "get_state_payload", _fake_state_payload)
    monkeypatch.setattr(
        dashboard_rollups,
        "_latest_deal",
        lambda db, org_id, property_id: SimpleNamespace(
            asking_price={1: 80000, 2: 70000, 3: 60000}[property_id],
            updated_at=None,
            id=property_id,
        ),
    )
    monkeypatch.setattr(
        dashboard_rollups,
        "_latest_uw",
        lambda db, org_id, property_id: SimpleNamespace(
            cash_flow={1: 500.0, 2: 200.0, 3: -100.0}[property_id],
            dscr={1: 1.35, 2: 1.10, 3: 0.80}[property_id],
            created_at=None,
            id=property_id,
        ),
    )

    payload = dashboard_rollups.compute_rollups(
        fake_db,
        org_id=1,
        state="MI",
        limit=50,
    )

    assert payload["summary"]["property_count"] == 3
    assert payload["summary"]["good_count"] == 1
    assert payload["summary"]["review_count"] == 1
    assert payload["summary"]["reject_count"] == 1

    assert payload["buckets"]["decisions"] == {
        "GOOD": 1,
        "REVIEW": 1,
        "REJECT": 1,
    }
    assert payload["buckets"]["stages"] == {
        "deal": 2,
        "rehab": 1,
    }

    rows = payload["rows"]
    assert len(rows) == 3
    assert rows[0]["normalized_decision"] in {"GOOD", "REVIEW", "REJECT"}
    assert "asking_price" in rows[0]
    assert "projected_monthly_cashflow" in rows[0]
    assert "dscr" in rows[0]
    assert "crime_label" in rows[0]
    assert "stage_completion_summary" in rows[0]
    assert "next_actions" in rows[0]