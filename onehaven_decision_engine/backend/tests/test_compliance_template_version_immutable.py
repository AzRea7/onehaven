# backend/tests/test_compliance_template_version_immutable.py
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.mark.usefixtures("db_session")  # keep consistent with your test harness if present
def test_v1_templates_lock_after_any_inspection_exists():
    """
    Phase 3 governance DoD:
    - Once inspections exist, template version v1 must be immutable.
    - Attempting to PUT /compliance/templates for v1 returns 409.
    """
    c = TestClient(app)

    headers = {
        "X-Org-Slug": "demo",
        "X-User-Email": "austin@demo.local",
        "X-User-Role": "owner",
    }

    # 1) Create an inspection for ANY property (the lock is org-level, version-level)
    # If your inspections router path differs, adjust this.
    # This assumes you already have at least property_id=1 in fixtures/demo seed.
    r = c.post(
        "/inspections",
        headers=headers,
        json={"property_id": 1, "scheduled_for": None, "passed": False, "notes": "test lock"},
    )
    assert r.status_code in (200, 201), r.text

    # 2) Now try to mutate v1 template
    payload = {
        "strategy": "section8",
        "version": "v1",
        "code": "SMOKE_CO_DETECTORS",
        "category": "Safety",
        "description": "SHOULD NOT BE ALLOWED TO CHANGE",
        "severity": 4,
        "common_fail": True,
        "applies_if": None,
    }

    r2 = c.put("/compliance/templates", headers=headers, json=payload)
    assert r2.status_code == 409, r2.text
    assert "locked" in (r2.json().get("detail") or "").lower()