# backend/app/domain/compliance/inspection_mapping.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class MappingResult:
    checklist_code: str
    rehab_title: Optional[str] = None
    rehab_category: str = "rehab"
    inspection_relevant: bool = True


INSPECTION_CODE_TO_ACTION: dict[str, MappingResult] = {
    "GFCI_MISSING": MappingResult(
        checklist_code="ELECTRICAL_GFCI",
        rehab_title="Install/replace missing GFCI outlets",
    ),
    "SMOKE_DETECTOR_MISSING": MappingResult(
        checklist_code="SAFETY_SMOKE_DETECTORS",
        rehab_title="Install smoke detectors (all required locations)",
    ),
    "CO_DETECTOR_MISSING": MappingResult(
        checklist_code="SAFETY_CO_DETECTORS",
        rehab_title="Install CO detectors (all required locations)",
    ),
    "HANDRAIL_MISSING": MappingResult(
        checklist_code="SAFETY_HANDRAILS",
        rehab_title="Install/repair handrails",
    ),
    "PEELING_PAINT": MappingResult(
        checklist_code="INTERIOR_PAINT",
        rehab_title="Scrape/prime/paint (lead-safe if needed)",
    ),
    "BROKEN_WINDOW": MappingResult(
        checklist_code="WINDOWS",
        rehab_title="Repair/replace broken windows",
    ),
    "EGRESS_BLOCKED": MappingResult(
        checklist_code="EGRESS",
        rehab_title="Clear/repair egress path/door/window",
    ),
}


def map_inspection_code(code: str) -> Optional[MappingResult]:
    if not code:
        return None
    key = code.strip().upper()
    return INSPECTION_CODE_TO_ACTION.get(key)