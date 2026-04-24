from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .inspection_rules import normalize_rule_code


@dataclass(frozen=True)
class PhotoRuleMapping:
    trigger: str
    observed_issue: str
    probable_failed_inspection_item: str
    rule_code: str
    standard_label: str
    standard_citation: str | None
    severity: str
    rehab_category: str
    recommended_fix: str
    requires_reinspection: bool
    keywords: tuple[str, ...] = ()


PHOTO_RULE_MAPPINGS: tuple[PhotoRuleMapping, ...] = (
    PhotoRuleMapping(
        trigger="outlet_cover_missing",
        observed_issue="Missing outlet or switch cover",
        probable_failed_inspection_item="Electrical safety fail",
        rule_code="ELECTRICAL_OUTLET_COVER",
        standard_label="Electrical hazards / outlet safety",
        standard_citation="HUD-52580-A electrical safety",
        severity="high",
        rehab_category="electrical",
        recommended_fix="Install properly secured outlet or switch cover plates and verify exposed conductors are not reachable.",
        requires_reinspection=True,
        keywords=("outlet", "switch", "cover", "plate", "missing"),
    ),
    PhotoRuleMapping(
        trigger="chipping_paint",
        observed_issue="Chipping, peeling, or deteriorated paint",
        probable_failed_inspection_item="Paint or surface hazard fail",
        rule_code="LEAD_PAINT_SURFACE_HAZARD",
        standard_label="Interior or exterior surfaces free of hazardous deterioration",
        standard_citation="HUD-52580-A lead or paint hazard readiness",
        severity="high",
        rehab_category="paint",
        recommended_fix="Stabilize deteriorated surfaces, scrape and repair safely, then repaint using lead-safe controls where applicable.",
        requires_reinspection=True,
        keywords=("paint", "peeling", "chipping", "surface", "flaking"),
    ),
    PhotoRuleMapping(
        trigger="handrail_missing",
        observed_issue="Missing or unsafe stair handrail",
        probable_failed_inspection_item="Safety fail",
        rule_code="STAIRS_HANDRAIL_REQUIRED",
        standard_label="Stairs and rails in safe condition",
        standard_citation="HUD-52580-A stairs / rails safety",
        severity="critical",
        rehab_category="safety",
        recommended_fix="Install a secure graspable handrail on all required stair runs and repair loose guard components.",
        requires_reinspection=True,
        keywords=("stairs", "stair", "rail", "handrail", "banister"),
    ),
    PhotoRuleMapping(
        trigger="window_sash_broken",
        observed_issue="Broken window sash, cracked pane, or unsecured window",
        probable_failed_inspection_item="Weatherization or security fail",
        rule_code="WINDOW_WEATHER_TIGHT_SECURE",
        standard_label="Windows must be weather-tight and secure",
        standard_citation="HUD-52580-A windows / security",
        severity="high",
        rehab_category="windows",
        recommended_fix="Repair or replace the damaged sash, glazing, or locks so the window opens, closes, and secures correctly.",
        requires_reinspection=True,
        keywords=("window", "sash", "glass", "broken", "cracked"),
    ),
    PhotoRuleMapping(
        trigger="exposed_wiring",
        observed_issue="Exposed wiring or open electrical splice",
        probable_failed_inspection_item="Critical electrical fail",
        rule_code="ELECTRICAL_EXPOSED_WIRING",
        standard_label="No exposed wiring or unsafe electrical condition",
        standard_citation="HUD-52580-A electrical hazards",
        severity="critical",
        rehab_category="electrical",
        recommended_fix="De-energize if necessary, place wiring in approved boxes or conduit, and complete repair by a qualified electrician.",
        requires_reinspection=True,
        keywords=("wire", "wiring", "exposed", "electrical", "splice"),
    ),
    PhotoRuleMapping(
        trigger="missing_smoke_detector",
        observed_issue="Smoke detector appears missing or disconnected",
        probable_failed_inspection_item="Life safety fail",
        rule_code="SMOKE_DETECTOR_REQUIRED",
        standard_label="Required smoke detectors present and operable",
        standard_citation="HUD-52580-A smoke detector requirement",
        severity="critical",
        rehab_category="safety",
        recommended_fix="Install a working smoke detector in required locations and test operation.",
        requires_reinspection=True,
        keywords=("smoke", "alarm", "detector", "ceiling"),
    ),
)


def photo_rule_mapping_bank() -> list[dict[str, Any]]:
    return [mapping.__dict__.copy() for mapping in PHOTO_RULE_MAPPINGS]


def mapping_for_trigger(trigger: str) -> PhotoRuleMapping | None:
    raw = str(trigger or "").strip().lower()
    for mapping in PHOTO_RULE_MAPPINGS:
        if mapping.trigger == raw:
            return mapping
    return None


def mapping_for_rule_code(code: str) -> PhotoRuleMapping | None:
    normalized = normalize_rule_code(code)
    for mapping in PHOTO_RULE_MAPPINGS:
        if normalize_rule_code(mapping.rule_code) == normalized:
            return mapping
    return None
