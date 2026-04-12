# backend/app/seed/jurisdictions_seed.py
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
from ..domain.jurisdiction_defaults import defaults_for_michigan


def _notes_from_default(default) -> str:
    policy = default.to_profile_policy()
    discovery = ((policy.get("discovery") or {}).get("search_hints") or {})
    required_categories = (
        ((policy.get("coverage") or {}).get("required_categories") or [])
    )
    thresholds = ((policy.get("coverage") or {}).get("thresholds") or {})
    trust = ((policy.get("trust") or {}).get("projection") or {})
    freshness = ((policy.get("freshness") or {}).get("policy_sources") or {})

    note_parts = [
        default.notes or "",
        f"Expected categories: {', '.join(required_categories)}." if required_categories else "",
        f"Discovery base terms: {', '.join(discovery.get('base_terms') or [])}." if discovery.get("base_terms") else "",
        f"Primary source hints: {', '.join(discovery.get('preferred_source_kinds') or [])}." if discovery.get("preferred_source_kinds") else "",
        f"Authoritative source threshold: {thresholds.get('authoritative_source')}." if thresholds.get("authoritative_source") is not None else "",
        f"Default stale-days threshold: {freshness.get('stale_days')}." if freshness.get("stale_days") is not None else "",
        f"Projection trust minimum: {trust.get('min_completeness_score_for_trust')}." if trust.get("min_completeness_score_for_trust") is not None else "",
    ]
    return " ".join(part.strip() for part in note_parts if part and str(part).strip())


def _seed_payload_from_default(default) -> dict:
    return dict(
        city=default.city,
        state=default.state,
        require_rental_registration=bool(default.rental_license_required),
        require_city_inspection=bool(default.inspection_authority or default.inspection_frequency),
        lead_paint_affidavit_required=bool(
            "lead" in {str(x).strip().lower() for x in (default.required_categories(include_section8=True) or [])}
            or any("lead" in str(x).lower() for x in (default.typical_fail_points or []))
        ),
        criminal_background_policy="moderate",
        typical_days_to_approve=int(default.processing_days or 14),
        friction_weight=float(
            1.25 if default.coverage_confidence == "low"
            else 1.10 if default.coverage_confidence == "medium"
            else 1.00
        ),
        notes=_notes_from_default(default),
    )


SEED = [_seed_payload_from_default(default) for default in defaults_for_michigan()]


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