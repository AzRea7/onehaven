from __future__ import annotations

from pathlib import Path

from app.services.compliance_document_service import (
    build_property_document_stack,
    create_compliance_document_from_path,
    delete_compliance_document,
    get_compliance_document,
    list_compliance_documents,
)


def test_create_list_and_group_compliance_documents(step19_seed, db_session, monkeypatch, tmp_path):
    monkeypatch.setattr(
        "app.services.compliance_document_service.scan_file",
        lambda path: {
            "infected": False,
            "scan_status": "clean",
            "scan_result": "ok",
        },
    )
    monkeypatch.setenv("COMPLIANCE_DOCUMENT_UPLOAD_DIR", str(tmp_path / "uploads"))

    org = step19_seed["org"]
    prop = step19_seed["property_a"]
    inspection = step19_seed["inspection_a"]

    source_path = tmp_path / "inspection-summary.txt"
    source_path.write_text("Smoke detectors repaired and GFCI installed.", encoding="utf-8")

    created = create_compliance_document_from_path(
        db_session,
        org_id=org.id,
        actor_user_id=19,
        property_id=prop.id,
        inspection_id=inspection.id,
        checklist_item_id=123,
        category="inspection_report",
        absolute_path=source_path,
        original_filename="inspection-summary.txt",
        content_type="text/plain",
        label="Initial inspection report",
        notes="Uploaded by operator",
        parse_document=True,
    )

    fetched = get_compliance_document(db_session, org_id=org.id, document_id=created["id"])
    listed = list_compliance_documents(db_session, org_id=org.id, property_id=prop.id)
    stack = build_property_document_stack(db_session, org_id=org.id, property_id=prop.id)

    assert created["category"] == "inspection_report"
    assert created["parse_status"] == "parsed"
    assert "Smoke detectors repaired" in created["extracted_text_preview"]
    assert fetched["id"] == created["id"]
    assert len(listed) == 1
    assert stack["count"] == 1
    assert "inspection_report" in stack["by_category"]
    assert str(inspection.id) in stack["by_inspection"]
    assert str(123) in stack["by_checklist_item"]


def test_delete_compliance_document_hides_it_from_active_views(step19_seed, db_session, monkeypatch, tmp_path):
    monkeypatch.setattr(
        "app.services.compliance_document_service.scan_file",
        lambda path: {
            "infected": False,
            "scan_status": "clean",
            "scan_result": "ok",
        },
    )
    monkeypatch.setenv("COMPLIANCE_DOCUMENT_UPLOAD_DIR", str(tmp_path / "uploads"))

    org = step19_seed["org"]
    prop = step19_seed["property_a"]

    source_path = tmp_path / "utility-proof.txt"
    source_path.write_text("Utility transfer confirmed.", encoding="utf-8")

    created = create_compliance_document_from_path(
        db_session,
        org_id=org.id,
        actor_user_id=19,
        property_id=prop.id,
        category="utility_confirmation",
        absolute_path=Path(source_path),
        original_filename="utility-proof.txt",
        content_type="text/plain",
        label="Utilities",
        parse_document=True,
    )

    deleted = delete_compliance_document(
        db_session,
        org_id=org.id,
        actor_user_id=19,
        document_id=created["id"],
    )
    listed = list_compliance_documents(db_session, org_id=org.id, property_id=prop.id)

    assert deleted["deleted"] is True
    assert listed == []
