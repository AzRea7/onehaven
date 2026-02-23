# backend/tests/test_hqs_library_has_required_categories.py
from __future__ import annotations

from sqlalchemy.orm import Session

from app.services.policy_seed import ensure_policy_seeded
from app.domain.compliance.hqs_library import load_hqs_items, required_categories_present


def test_hqs_library_has_required_categories(db_session: Session):
    org_id = 1
    ensure_policy_seeded(db_session, org_id=org_id)

    items = load_hqs_items(db_session, org_id=org_id, jurisdiction_profile_id=None)
    assert len(items) >= 8
    assert required_categories_present(items) is True