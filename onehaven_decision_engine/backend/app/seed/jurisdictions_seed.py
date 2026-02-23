# onehaven_decision_engine/backend/app/seed/jurisdictions_seed.py
from __future__ import annotations

"""
Seed a realistic starter set of jurisdiction rules for Michigan cities.

Run example:
  python -m backend.app.seed.jurisdictions_seed
(or run via your venv with PYTHONPATH set to repo root)
"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..models import JurisdictionRule


SEED = [
    # Detroit (starter assumptions; tune later)
    dict(
        city="Detroit",
        state="MI",
        require_rental_registration=True,
        require_city_inspection=True,
        lead_paint_affidavit_required=True,
        criminal_background_policy="moderate",
        typical_days_to_approve=21,
        friction_weight=1.25,
        notes="Detroit requires registration + inspection; expect delays.",
    ),
    # Pontiac
    dict(
        city="Pontiac",
        state="MI",
        require_rental_registration=True,
        require_city_inspection=True,
        lead_paint_affidavit_required=True,
        criminal_background_policy="moderate",
        typical_days_to_approve=18,
        friction_weight=1.15,
        notes="Pontiac: registration/inspection common; slightly faster than Detroit.",
    ),
    # Southfield
    dict(
        city="Southfield",
        state="MI",
        require_rental_registration=True,
        require_city_inspection=False,
        lead_paint_affidavit_required=True,
        criminal_background_policy="strict",
        typical_days_to_approve=14,
        friction_weight=1.10,
        notes="Southfield often stricter screening; registration present.",
    ),
    # Royal Oak (often fewer municipal hurdles)
    dict(
        city="Royal Oak",
        state="MI",
        require_rental_registration=False,
        require_city_inspection=False,
        lead_paint_affidavit_required=False,
        criminal_background_policy="moderate",
        typical_days_to_approve=10,
        friction_weight=1.00,
        notes="Royal Oak: generally fewer municipal process steps.",
    ),
]


def upsert_rule(db: Session, org_id: int, payload: dict) -> JurisdictionRule:
    city = payload["city"]
    state = payload["state"]

    existing = db.execute(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id == org_id,
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    ).scalar_one_or_none()

    if existing:
        for k, v in payload.items():
            setattr(existing, k, v)
        return existing

    rule = JurisdictionRule(org_id=org_id, **payload)
    db.add(rule)
    return rule


def main():
    # Seed into org_id=1 by default (demo org). Adjust if needed.
    org_id = 1

    db = SessionLocal()
    try:
        for payload in SEED:
            upsert_rule(db, org_id, payload)
        db.commit()
        print(f"Seeded {len(SEED)} jurisdiction rules into org_id={org_id}.")
    finally:
        db.close()


if __name__ == "__main__":
    main()