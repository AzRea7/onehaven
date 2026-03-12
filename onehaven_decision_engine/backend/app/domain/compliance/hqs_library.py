# backend/app/domain/compliance/hqs_library.py
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property
from app.policy_models import HqsAddendum, HqsRule


def _baseline_hqs_items() -> list[dict[str, Any]]:
    return [
        {
            "code": "SMOKE_DETECTORS",
            "description": "Working smoke detectors installed in required locations",
            "category": "safety",
            "severity": "fail",
            "suggested_fix": "Install or replace smoke detectors and verify operation.",
        },
        {
            "code": "CO_DETECTORS",
            "description": "Carbon monoxide detectors installed where required",
            "category": "safety",
            "severity": "fail",
            "suggested_fix": "Install CO detectors near sleeping areas and fuel-burning appliances where required.",
        },
        {
            "code": "EGRESS",
            "description": "Bedrooms and habitable areas have safe legal egress",
            "category": "egress",
            "severity": "fail",
            "suggested_fix": "Repair blocked windows/doors and ensure emergency escape is functional.",
        },
        {
            "code": "WINDOWS_LOCKS",
            "description": "Windows are intact, weather-tight, and lockable",
            "category": "egress",
            "severity": "fail",
            "suggested_fix": "Repair or replace damaged windows, glazing, or locks.",
        },
        {
            "code": "DOORS_SECURE",
            "description": "Exterior doors are secure and operable",
            "category": "security",
            "severity": "fail",
            "suggested_fix": "Repair jambs, locks, weatherstripping, and door hardware.",
        },
        {
            "code": "HANDRAILS",
            "description": "Required stair handrails are secure",
            "category": "safety",
            "severity": "fail",
            "suggested_fix": "Install or repair handrails and verify they are firmly anchored.",
        },
        {
            "code": "HEAT",
            "description": "Permanent heat source is present and operational",
            "category": "hvac",
            "severity": "fail",
            "suggested_fix": "Repair furnace/boiler/electric heat and verify thermostat control.",
        },
        {
            "code": "HOT_WATER",
            "description": "Safe hot water is available",
            "category": "plumbing",
            "severity": "fail",
            "suggested_fix": "Repair water heater, leaks, or venting issues.",
        },
        {
            "code": "PLUMBING_LEAKS",
            "description": "No active plumbing leaks or unsafe moisture conditions",
            "category": "plumbing",
            "severity": "fail",
            "suggested_fix": "Repair leaks and replace damaged materials.",
        },
        {
            "code": "TOILET_SINK_TUB",
            "description": "Required bathroom fixtures are present and operable",
            "category": "plumbing",
            "severity": "fail",
            "suggested_fix": "Repair or replace nonfunctional bathroom fixtures.",
        },
        {
            "code": "KITCHEN_WORKING",
            "description": "Kitchen has operable sink, prep area, and safe utility service",
            "category": "interior",
            "severity": "fail",
            "suggested_fix": "Restore sink, cabinets, counters, and utility connections as needed.",
        },
        {
            "code": "GFCI_KITCHEN",
            "description": "Kitchen counter outlets have GFCI protection where required",
            "category": "electrical",
            "severity": "fail",
            "suggested_fix": "Install GFCI receptacles or breaker protection in kitchen wet areas.",
        },
        {
            "code": "GFCI_BATH",
            "description": "Bathroom outlets have GFCI protection where required",
            "category": "electrical",
            "severity": "fail",
            "suggested_fix": "Install GFCI receptacles or breaker protection in bathrooms.",
        },
        {
            "code": "ELECT_PANEL",
            "description": "Electrical panel and wiring are safe with no exposed live parts",
            "category": "electrical",
            "severity": "fail",
            "suggested_fix": "Install blanks/covers and correct unsafe wiring conditions.",
        },
        {
            "code": "OUTLETS_LIGHTS",
            "description": "Required outlets, switches, and lighting are operable and safe",
            "category": "electrical",
            "severity": "fail",
            "suggested_fix": "Repair dead outlets, switches, fixtures, and unsafe splices.",
        },
        {
            "code": "LEAKS_ROOF",
            "description": "No active roof leaks or significant water intrusion",
            "category": "exterior",
            "severity": "fail",
            "suggested_fix": "Repair roof, flashing, gutters, and replace damaged finishes.",
        },
        {
            "code": "FOUNDATION_STRUCTURAL",
            "description": "No obvious structural instability or dangerous settlement",
            "category": "structure",
            "severity": "fail",
            "suggested_fix": "Repair structural defects and obtain contractor/engineer evaluation if needed.",
        },
        {
            "code": "PEELING_PAINT",
            "description": "No hazardous deteriorated paint where prohibited",
            "category": "lead",
            "severity": "warn",
            "suggested_fix": "Stabilize peeling paint and follow lead-safe work practices.",
        },
        {
            "code": "TRIP_HAZARDS",
            "description": "No dangerous trip/fall hazards in walking paths and stairs",
            "category": "safety",
            "severity": "fail",
            "suggested_fix": "Repair broken flooring, loose treads, and uneven transitions.",
        },
        {
            "code": "PESTS",
            "description": "No severe infestation or unsanitary pest conditions",
            "category": "sanitation",
            "severity": "warn",
            "suggested_fix": "Treat infestation and seal entry points.",
        },
    ]


def _normalize_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "code": str(item.get("code") or "").strip().upper(),
        "description": str(item.get("description") or "").strip(),
        "category": str(item.get("category") or "other").strip().lower(),
        "severity": str(item.get("severity") or "fail").strip().lower(),
        "suggested_fix": (str(item.get("suggested_fix")).strip() if item.get("suggested_fix") else None),
        "source": item.get("source"),
    }


def _load_hqs_rule_rows(db: Session) -> list[Any]:
    try:
        return list(db.scalars(select(HqsRule)).all())
    except Exception:
        return []


def _load_hqs_addendum_rows(db: Session, *, org_id: int | None = None) -> list[Any]:
    try:
        if hasattr(HqsAddendum, "org_id") and org_id is not None:
            return list(
                db.scalars(
                    select(HqsAddendum).where(
                        (HqsAddendum.org_id == org_id) | (HqsAddendum.org_id.is_(None))
                    )
                ).all()
            )
        return list(db.scalars(select(HqsAddendum)).all())
    except Exception:
        return []


def get_effective_hqs_items(db: Session, *, org_id: int, prop: Property) -> dict[str, Any]:
    """
    Effective HQS set:
      1) internal baseline
      2) HqsRule policy table overrides/extensions
      3) HqsAddendum policy table overrides/extensions
      4) inferred local adds from property / jurisdiction context when present
    """
    items: dict[str, dict[str, Any]] = {
        row["code"]: _normalize_item({**row, "source": {"type": "baseline_internal", "name": "OneHaven HQS baseline"}})
        for row in _baseline_hqs_items()
    }

    sources: list[dict[str, Any]] = [
        {"type": "baseline_internal", "name": "OneHaven HQS baseline", "count": len(items)}
    ]

    for row in _load_hqs_rule_rows(db):
        code = str(getattr(row, "code", "") or "").strip().upper()
        if not code:
            continue
        prior = items.get(code, {})
        items[code] = _normalize_item(
            {
                "code": code,
                "description": getattr(row, "description", None) or prior.get("description") or code.replace("_", " ").title(),
                "category": getattr(row, "category", None) or prior.get("category") or "other",
                "severity": getattr(row, "severity", None) or prior.get("severity") or "fail",
                "suggested_fix": getattr(row, "suggested_fix", None) or prior.get("suggested_fix"),
                "source": {"type": "policy_table", "table": "HqsRule"},
            }
        )

    if _load_hqs_rule_rows(db):
        sources.append({"type": "policy_table", "table": "HqsRule"})

    for row in _load_hqs_addendum_rows(db, org_id=org_id):
        code = str(getattr(row, "code", "") or "").strip().upper()
        if not code:
            continue
        prior = items.get(code, {})
        items[code] = _normalize_item(
            {
                "code": code,
                "description": getattr(row, "description", None) or prior.get("description") or code.replace("_", " ").title(),
                "category": getattr(row, "category", None) or prior.get("category") or "other",
                "severity": getattr(row, "severity", None) or prior.get("severity") or "fail",
                "suggested_fix": getattr(row, "suggested_fix", None) or prior.get("suggested_fix"),
                "source": {"type": "policy_table", "table": "HqsAddendum"},
            }
        )

    addenda = _load_hqs_addendum_rows(db, org_id=org_id)
    if addenda:
        sources.append({"type": "policy_table", "table": "HqsAddendum", "count": len(addenda)})

    # Small context-sensitive adds that are safe and generic.
    year_built = getattr(prop, "year_built", None)
    if isinstance(year_built, int) and year_built < 1978:
        items["LEAD_SAFE_SURFACES"] = _normalize_item(
            {
                "code": "LEAD_SAFE_SURFACES",
                "description": "Pre-1978 property should be checked for deteriorated paint / lead-safe compliance",
                "category": "lead",
                "severity": "warn",
                "suggested_fix": "Verify lead-safe workflow, stabilization, and required disclosures/certifications.",
                "source": {"type": "contextual_rule", "reason": "pre_1978"},
            }
        )

    return {
        "items": sorted(items.values(), key=lambda x: (x["category"], x["code"])),
        "sources": sources,
    }