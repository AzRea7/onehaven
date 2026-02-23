# onehaven_decision_engine/backend/tests/test_jurisdiction_friction.py
from __future__ import annotations

from backend.app.domain.jurisdiction_scoring import compute_friction
from backend.app.models import JurisdictionRule


def test_friction_increases_when_registration_and_inspection_required():
    jr = JurisdictionRule(
        org_id=1,
        city="Detroit",
        state="MI",
        require_rental_registration=True,
        require_city_inspection=True,
        lead_paint_affidavit_required=True,
        criminal_background_policy="moderate",
        typical_days_to_approve=21,
        friction_weight=1.25,
        notes="",
    )

    f = compute_friction(jr)

    # These are deterministic outputs; we only assert the direction + sanity.
    assert f.friction_multiplier >= 1.0
    assert f.expected_delay_days >= 0
    assert f.friction_multiplier > 1.0