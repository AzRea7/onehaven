from __future__ import annotations

from app.models import PropertyChecklist, PropertyChecklistItem
from app.services.inspection_template_service import (
    build_inspection_template,
    ensure_template_backed_checklist,
)


def test_build_inspection_template_returns_real_template(real_inspection_seed, db_session):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]

    result = build_inspection_template(
        db_session,
        org_id=org.id,
        property_id=prop.id,
    )

    assert result["ok"] is True
    assert result["property_id"] == prop.id
    assert result["template_key"] == "hud_52580a"
    assert "template_version" in result
    assert "items" in result
    assert isinstance(result["items"], list)
    assert len(result["items"]) > 0

    first = result["items"][0]
    assert "code" in first
    assert "description" in first
    assert "category" in first
    assert "severity" in first
    assert "severity_int" in first


def test_ensure_template_backed_checklist_creates_checklist_and_items(real_inspection_seed, db_session):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]

    result = ensure_template_backed_checklist(
        db_session,
        org_id=org.id,
        property_id=prop.id,
    )

    assert result["ok"] is True
    assert result["created_checklist"] is True
    assert result["created_items"] > 0
    assert result["checklist_id"] is not None

    checklist = db_session.query(PropertyChecklist).filter(
        PropertyChecklist.id == result["checklist_id"]
    ).one()

    items = db_session.query(PropertyChecklistItem).filter(
        PropertyChecklistItem.org_id == org.id,
        PropertyChecklistItem.property_id == prop.id,
    ).all()

    assert checklist.property_id == prop.id
    assert len(items) > 0


def test_ensure_template_backed_checklist_is_idempotent(real_inspection_seed, db_session):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]

    first = ensure_template_backed_checklist(
        db_session,
        org_id=org.id,
        property_id=prop.id,
    )
    second = ensure_template_backed_checklist(
        db_session,
        org_id=org.id,
        property_id=prop.id,
    )

    assert first["checklist_id"] == second["checklist_id"]
    assert second["created_checklist"] is False