from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from app.routers import ops as ops_router


def test_decision_health_scores_complete_assets_higher():
    inspection = SimpleNamespace(
        id=11,
        inspection_date=datetime(2026, 3, 12, 10, 0, 0),
        passed=True,
        reinspect_required=False,
        notes=None,
    )

    underwriting = {
        "id": 1,
        "decision": "GOOD",
        "score": 88.0,
        "dscr": 1.35,
        "cash_flow": 425.0,
    }

    good = ops_router._decision_health(
        underwriting=underwriting,
        checklist=ops_router.ChecklistProgress(
            total=3,
            todo=0,
            in_progress=0,
            blocked=0,
            done=3,
        ),
        rehab={
            "total": 3,
            "todo": 0,
            "in_progress": 0,
            "blocked": 0,
            "done": 3,
            "is_complete": True,
        },
        inspection_latest=inspection,
        open_failed_items=0,
        active_lease=SimpleNamespace(id=1),
        cash_n={"income": 2500.0, "expense": 900.0, "capex": 0.0, "net": 1600.0},
        equity={"estimated_value": 150000.0, "estimated_equity": 45000.0},
    )

    weak = ops_router._decision_health(
        underwriting=None,
        checklist=ops_router.ChecklistProgress(
            total=0,
            todo=0,
            in_progress=0,
            blocked=0,
            done=0,
        ),
        rehab={
            "total": 0,
            "todo": 0,
            "in_progress": 0,
            "blocked": 0,
            "done": 0,
            "is_complete": False,
        },
        inspection_latest=None,
        open_failed_items=2,
        active_lease=None,
        cash_n={"income": 0.0, "expense": 0.0, "capex": 0.0, "net": 0.0},
        equity=None,
    )

    assert good["score"] > weak["score"]
    assert good["band"] in {"medium", "high"}
    assert weak["band"] == "low"
    assert "underwriting_positive" in good["flags"]
    assert "missing_underwriting" in weak["warnings"]


def test_txn_bucket_maps_common_types():
    assert ops_router._txn_bucket("income") == "income"
    assert ops_router._txn_bucket("rent") == "income"
    assert ops_router._txn_bucket("expense") == "expense"
    assert ops_router._txn_bucket("capex") == "capex"
    assert ops_router._txn_bucket("weird") == "other"
    