# backend/app/services/policy_seed.py
from __future__ import annotations

import json
from datetime import date, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..policy_models import JurisdictionProfile, HqsRule


def _j(v) -> str:
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False)


def ensure_policy_seeded(db: Session, *, org_id: int) -> None:
    """
    Seeds:
      - a baseline HQS rules library (small but category-complete)
      - a couple jurisdiction profiles for MI (Detroit + Royal Oak)
    This is your "policy graph bootstrapping".
    """

    # -------------------------
    # 1) HQS baseline (small but real categories)
    # -------------------------
    existing = db.scalar(select(HqsRule).limit(1))
    if existing is None:
        rules = [
            # safety
            ("HQS_SMOKE_DETECTOR", "safety", "fail", "Working smoke detector on each level and near sleeping areas",
             ["Photo of installed detector", "Test button video"], ["Install/replace detector", "Check battery"]),
            ("HQS_CO_DETECTOR", "safety", "fail", "Working CO detector where required/appropriate",
             ["Photo", "Test button video"], ["Install/replace CO detector"]),
            # electrical
            ("HQS_GFCI_KITCHEN", "electrical", "fail", "GFCI protection present for kitchen countertop receptacles",
             ["Photo of GFCI outlet", "Test/reset works"], ["Install GFCI receptacle or breaker"]),
            ("HQS_GFCI_BATH", "electrical", "fail", "GFCI protection present for bathroom receptacles",
             ["Photo", "Test/reset works"], ["Install GFCI receptacle or breaker"]),
            ("HQS_EXPOSED_WIRING", "electrical", "fail", "No exposed live wiring; cover plates installed",
             ["Photo"], ["Install cover plates", "Repair/replace damaged wiring"]),
            # plumbing/sanitary
            ("HQS_HOT_WATER", "plumbing", "fail", "Hot water available at bathroom/kitchen fixtures",
             ["Video demonstrating hot water"], ["Repair water heater", "Repair mixing valve"]),
            ("HQS_LEAKS", "plumbing", "fail", "No active leaks; fixtures drain properly",
             ["Photo/video"], ["Repair traps/supply lines", "Seal fixtures"]),
            # egress
            ("HQS_EGRESS_WINDOWS", "egress", "fail", "Bedrooms have acceptable emergency egress (where applicable)",
             ["Photo of window size/condition"], ["Repair window operation", "Clear obstructions"]),
            ("HQS_HANDRAILS", "egress", "fail", "Handrails secure on stairs where required",
             ["Photo"], ["Install/secure handrail"]),
            # thermal/environment
            ("HQS_HEAT", "thermal", "fail", "Permanent heat source maintains safe indoor temperature",
             ["Thermostat photo", "Heat run video"], ["Service furnace", "Repair thermostat"]),
            # structure/exterior
            ("HQS_ROOF_LEAKS", "structure", "fail", "No roof leaks or active water intrusion",
             ["Attic photo", "Ceiling stain photo"], ["Patch roof", "Repair flashing"]),
            ("HQS_STAIRS_SAFE", "exterior", "fail", "Exterior steps/porches structurally sound",
             ["Photo"], ["Repair/rebuild steps", "Install guards"]),
            # interior
            ("HQS_FLOOR_TRIP", "interior", "fail", "Floors free of major trip hazards (loose flooring, holes)",
             ["Photo"], ["Repair subfloor", "Secure flooring"]),
        ]

        now = datetime.utcnow()
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
    # 2) Jurisdiction profiles (starter playbooks)
    # -------------------------
    def upsert_profile(key: str, payload: dict) -> None:
        jp = db.scalar(
            select(JurisdictionProfile)
            .where(JurisdictionProfile.org_id == org_id)
            .where(JurisdictionProfile.key == key)
            .where(JurisdictionProfile.effective_date == payload["effective_date"])
        )
        if jp is None:
            db.add(JurisdictionProfile(org_id=org_id, key=key, **payload))
        # else: leave immutable; add new effective_date to change policy

    # Detroit / Wayne
    upsert_profile(
        "mi_detroit_wayne_hcv",
        dict(
            name="MI — Detroit (Wayne County) — HCV starter playbook",
            state="MI",
            county="Wayne",
            city="Detroit",
            zip_prefix="482",
            pha_name="(set your serving PHA name)",
            pha_code=None,
            program_type="hcv",
            payment_standard_pct=1.10,
            uses_safmr=0,
            inspection_cadence_json=_j(
                {
                    "initial": "Required before HAP begins",
                    "annual": "Annual/periodic per PHA",
                    "special": "As-needed on complaint/repair follow-up",
                    "notes": "Verify exact cadence + fail repair windows per serving PHA policy.",
                }
            ),
            packet_requirements_json=_j(
                {
                    "packet": [
                        {"item": "RFTA (Request for Tenancy Approval)", "who": "landlord+tenant", "required": True},
                        {"item": "W-9", "who": "landlord", "required": True},
                        {"item": "Direct deposit/owner pay setup", "who": "landlord", "required": True},
                        {"item": "Lease (PHA-compliant addendum)", "who": "landlord", "required": True},
                        {"item": "Lead disclosures (if applicable)", "who": "landlord", "required": "conditional"},
                        {"item": "Proof of ownership / management authorization", "who": "landlord", "required": "conditional"},
                    ],
                    "notes": "Exact packet varies. Store links + last_verified_at once you confirm.",
                }
            ),
            local_overlays_json=_j(
                {
                    "registration": ["City rental registration (if required)", "Certificate of compliance (if applicable)"],
                    "lead": ["Lead-based paint disclosures for pre-1978"],
                    "notes": "Local overlays can make or break timelines. Verify per city/county.",
                }
            ),
            utility_allowance_notes="Utility allowance affects tenant portion vs HAP; ensure gross rent comparison uses correct utilities.",
            effective_date=date(2026, 2, 1),
            source_urls_json=_j(["(add sources you verified)"]),
            notes="Starter policy only. Replace with verified PHA + city docs.",
            last_verified_at=None,
            created_at=datetime.utcnow(),
        ),
    )

    # Royal Oak / Oakland
    upsert_profile(
        "mi_royal_oak_oakland_hcv",
        dict(
            name="MI — Royal Oak (Oakland County) — HCV starter playbook",
            state="MI",
            county="Oakland",
            city="Royal Oak",
            zip_prefix="48067",
            pha_name="(set your serving PHA name)",
            pha_code=None,
            program_type="hcv",
            payment_standard_pct=1.10,
            uses_safmr=0,
            inspection_cadence_json=_j(
                {"initial": "Required", "annual": "Periodic", "notes": "Verify local PHA cadence and addenda."}
            ),
            packet_requirements_json=_j(
                {
                    "packet": [
                        {"item": "RFTA", "who": "landlord+tenant", "required": True},
                        {"item": "W-9", "who": "landlord", "required": True},
                        {"item": "Lease + addenda", "who": "landlord", "required": True},
                    ],
                    "notes": "Add local addendum requirements after verification.",
                }
            ),
            local_overlays_json=_j({"notes": "Verify rental registration / inspections if applicable."}),
            utility_allowance_notes="Track which utilities are tenant-paid; affects gross rent reasonableness.",
            effective_date=date(2026, 2, 1),
            source_urls_json=_j(["(add sources you verified)"]),
            notes="Starter policy only. Replace with verified docs.",
            last_verified_at=None,
            created_at=datetime.utcnow(),
        ),
    )

    db.commit()