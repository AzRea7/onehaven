from __future__ import annotations

from app.models import InspectionItem, PropertyChecklistItem
from app.services.inspection_template_service import (
    apply_raw_inspection_payload,
    map_raw_inspection_payload,
)


def test_map_raw_inspection_payload_normalizes_form_answers():
    raw_payload = {
        "items": [
            {
                "code": "smoke_detectors",
                "result": "pass",
                "details": "working",
            },
            {
                "code": "GFCI_KITCHEN",
                "result": "fail",
                "details": "missing by sink",
                "location": "kitchen",
            },
            {
                "code": "handrails",
                "result": "blocked",
                "details": "cannot verify until debris removed",
            },
        ]
    }

    rows = map_raw_inspection_payload(raw_payload=raw_payload)

    assert isinstance(rows, list)
    assert len(rows) >= 3

    codes = {row["code"] for row in rows}
    assert "SMOKE_DETECTORS" in codes
    assert "GFCI_KITCHEN" in codes
    assert "HANDRAILS" in codes

    gfci = next(row for row in rows if row["code"] == "GFCI_KITCHEN")
    assert gfci["result_status"] == "fail"
    assert gfci["failed"] is True


def test_apply_raw_inspection_payload_persists_normalized_items(
    real_inspection_seed,
    real_inspection,
    db_session,
):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]
    insp = real_inspection

    raw_payload = {
        "items": [
            {"code": "SMOKE_DETECTORS", "result": "pass"},
            {
                "code": "GFCI_KITCHEN",
                "result": "fail",
                "details": "missing gfci",
                "location": "kitchen",
            },
        ]
    }

    result = apply_raw_inspection_payload(
        db_session,
        org_id=org.id,
        property_id=prop.id,
        inspection_id=insp.id,
        raw_payload=raw_payload,
        sync_checklist=True,
    )

    assert result["ok"] is True
    assert result["inspection_id"] == insp.id
    assert result["mapped_count"] >= 2
    assert result["readiness"]["counts"]["total_items"] >= 1

    rows = db_session.query(InspectionItem).filter(
        InspectionItem.inspection_id == insp.id
    ).all()
    assert len(rows) >= 2

    checklist_rows = db_session.query(PropertyChecklistItem).filter(
        PropertyChecklistItem.org_id == org.id,
        PropertyChecklistItem.property_id == prop.id,
    ).all()
    assert len(checklist_rows) >= 2

    by_code = {row.item_code: row for row in checklist_rows}
    assert by_code["SMOKE_DETECTORS"].status in {"done", "todo", "failed", "blocked"}
    assert by_code["GFCI_KITCHEN"].status == "failed"