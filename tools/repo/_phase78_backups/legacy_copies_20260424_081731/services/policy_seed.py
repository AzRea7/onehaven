# backend/app/services/policy_seed.py
from __future__ import annotations

import json
from datetime import date, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.policy_models import JurisdictionProfile, HqsRule


def _j(v) -> str:
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False)


def ensure_policy_seeded(db: Session, *, org_id: int | None = None) -> None:
    """
    Seeds:
      - baseline HQS rules library
      - GLOBAL jurisdiction profiles for MI (org_id = NULL)

    Org overrides are typically created via UI, but you can pass org_id if you want
    to create an org-scoped default row too.
    """

    # -------------------------
    # 1) HQS baseline
    # -------------------------
    existing = db.scalar(select(HqsRule).limit(1))
    if existing is None:
        rules = [
            (
                "HQS_SMOKE_DETECTOR",
                "safety",
                "fail",
                "Working smoke detector on each level and near sleeping areas",
                ["Photo of installed detector", "Test button video"],
                ["Install/replace detector", "Check battery"],
            ),
            (
                "HQS_CO_DETECTOR",
                "safety",
                "fail",
                "Working CO detector where required/appropriate",
                ["Photo", "Test button video"],
                ["Install/replace CO detector"],
            ),
            (
                "HQS_GFCI_KITCHEN",
                "electrical",
                "fail",
                "GFCI protection present for kitchen countertop receptacles",
                ["Photo of GFCI outlet", "Test/reset works"],
                ["Install GFCI receptacle or breaker"],
            ),
            (
                "HQS_GFCI_BATH",
                "electrical",
                "fail",
                "GFCI protection present for bathroom receptacles",
                ["Photo", "Test/reset works"],
                ["Install GFCI receptacle or breaker"],
            ),
            (
                "HQS_EXPOSED_WIRING",
                "electrical",
                "fail",
                "No exposed live wiring; cover plates installed",
                ["Photo"],
                ["Install cover plates", "Repair/replace damaged wiring"],
            ),
            (
                "HQS_HOT_WATER",
                "plumbing",
                "fail",
                "Hot water available at bathroom/kitchen fixtures",
                ["Video demonstrating hot water"],
                ["Repair water heater", "Repair mixing valve"],
            ),
            (
                "HQS_LEAKS",
                "plumbing",
                "fail",
                "No active leaks; fixtures drain properly",
                ["Photo/video"],
                ["Repair traps/supply lines", "Seal fixtures"],
            ),
            (
                "HQS_EGRESS_WINDOWS",
                "egress",
                "fail",
                "Bedrooms have acceptable emergency egress (where applicable)",
                ["Photo of window size/condition"],
                ["Repair window operation", "Clear obstructions"],
            ),
            (
                "HQS_HANDRAILS",
                "egress",
                "fail",
                "Handrails secure on stairs where required",
                ["Photo"],
                ["Install/secure handrail"],
            ),
            (
                "HQS_HEAT",
                "thermal",
                "fail",
                "Permanent heat source maintains safe indoor temperature",
                ["Thermostat photo", "Heat run video"],
                ["Service furnace", "Repair thermostat"],
            ),
            (
                "HQS_ROOF_LEAKS",
                "structure",
                "fail",
                "No roof leaks or active water intrusion",
                ["Attic photo", "Ceiling stain photo"],
                ["Patch roof", "Repair flashing"],
            ),
            (
                "HQS_STAIRS_SAFE",
                "exterior",
                "fail",
                "Exterior steps/porches structurally sound",
                ["Photo"],
                ["Repair/rebuild steps", "Install guards"],
            ),
            (
                "HQS_FLOOR_TRIP",
                "interior",
                "fail",
                "Floors free of major trip hazards (loose flooring, holes)",
                ["Photo"],
                ["Repair subfloor", "Secure flooring"],
            ),
        ]

        for code, category, severity, desc, evidence, hints in rules:
            db.add(
                HqsRule(
                    code=code,
                    category=category,
                    severity=severity,
                    description=desc,
                    evidence_json=_j(evidence),
                    remediation_hints_json=_j(hints),
                    source_urls_json=_j(["(add HUD/pha source urls here)"]),
                    effective_date=date(2026, 1, 1),
                )
            )
        db.commit()

    # -------------------------
    # 2) GLOBAL jurisdiction profiles (MI)
    # -------------------------
    def upsert_global(
        state: str,
        county: str | None,
        city: str | None,
        friction: float,
        pha: str | None,
        policy: dict,
        notes: str | None,
    ):
        q = (
            select(JurisdictionProfile)
            .where(JurisdictionProfile.org_id.is_(None))
            .where(JurisdictionProfile.state == state)
            .where(
                JurisdictionProfile.county.is_(None)
                if county is None
                else JurisdictionProfile.county == county.lower()
            )
            .where(
                JurisdictionProfile.city.is_(None)
                if city is None
                else JurisdictionProfile.city == city.lower()
            )
        )
        row = db.scalar(q)
        now = datetime.utcnow()

        if row is None:
            row = JurisdictionProfile(
                org_id=None,
                state=state,
                county=county.lower() if county else None,
                city=city.lower() if city else None,
                friction_multiplier=float(friction),
                pha_name=pha,
                policy_json=_j(policy),
                notes=notes,
                updated_at=now,
            )
            db.add(row)
        else:
            row.friction_multiplier = float(friction)
            row.pha_name = pha
            row.policy_json = _j(policy)
            row.notes = notes
            row.updated_at = now

    # MI state default
    upsert_global(
        "MI",
        None,
        None,
        1.0,
        None,
        policy={
            "summary": "Michigan baseline defaults. Override per county/city as you learn real timelines.",
            "licensing": {"typical": "Varies by city. Many suburbs have rental registration/CO programs."},
            "inspections": {"typical": "PHA-driven; expect initial + periodic inspections."},
            "notes": ["Treat as operational starting point, not legal advice."],
        },
        notes="Global baseline. Add verified sources in docs/michigan_jurisdictions.md",
    )

    # Wayne County default (example)
    upsert_global(
        "MI",
        "Wayne",
        None,
        1.15,
        None,
        policy={
            "summary": "Wayne County tends to have higher friction due to city overlays + older housing stock.",
            "ops": {"watchouts": ["Older electrical/plumbing", "Permit/inspection delays in certain cities"]},
        },
        notes="Starter friction multiplier; calibrate from your own inspection pass/fail history.",
    )

    # Detroit (example)
    upsert_global(
        "MI",
        "Wayne",
        "Detroit",
        1.30,
        "Detroit Housing Commission / serving PHA (verify)",
        policy={
            "summary": "Detroit: assume higher process friction + higher HQS fail risk until proven otherwise.",
            "licensing": {"typical": "Verify rental registration and local compliance overlays."},
            "inspections": {"typical": "Expect reinspection cycles; track common fails and repair windows."},
            "common_fail_patterns": ["GFCI", "handrails", "heat", "hot_water", "floor_trip"],
        },
        notes="Starter. Replace pha_name + details after verification.",
    )

    db.commit()

    # Optional: create a starter ORG override row (state default)
    if org_id is not None:
        q = (
            select(JurisdictionProfile)
            .where(JurisdictionProfile.org_id == org_id)
            .where(JurisdictionProfile.state == "MI")
            .where(JurisdictionProfile.county.is_(None))
            .where(JurisdictionProfile.city.is_(None))
        )
        row = db.scalar(q)
        if row is None:
            now = datetime.utcnow()
            db.add(
                JurisdictionProfile(
                    org_id=org_id,
                    state="MI",
                    county=None,
                    city=None,
                    friction_multiplier=1.0,
                    pha_name=None,
                    policy_json=_j({"summary": "Org defaults (override)."}),
                    notes="Org override baseline. Edit in UI.",
                    updated_at=now,
                )
            )
            db.commit()
            