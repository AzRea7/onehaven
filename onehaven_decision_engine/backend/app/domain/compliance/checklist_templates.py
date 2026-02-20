# backend/app/domain/compliance/checklist_templates.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class ChecklistTemplateItem:
    code: str
    description: str
    category: str  # safety|electrical|plumbing|egress|exterior|interior
    default_status: str = "todo"


def base_hqs_template() -> list[ChecklistTemplateItem]:
    # “boring but real” HQS-ish starter pack.
    return [
        ChecklistTemplateItem("SMOKE_CO", "Smoke + CO detectors present and functional", "safety"),
        ChecklistTemplateItem("GFCI_KITCHEN", "GFCI present at kitchen counter outlets", "electrical"),
        ChecklistTemplateItem("GFCI_BATH", "GFCI present at bathroom outlets", "electrical"),
        ChecklistTemplateItem("HANDRAILS", "Handrails secure on stairs (where required)", "safety"),
        ChecklistTemplateItem("ELECT_PANEL", "Electrical panel labeled / no exposed conductors", "electrical"),
        ChecklistTemplateItem("WINDOWS", "Windows open/close/lock; no broken panes", "interior"),
        ChecklistTemplateItem("DOORS_LOCKS", "Exterior doors lock and seal properly", "safety"),
        ChecklistTemplateItem("HEAT", "Permanent heat source operational", "safety"),
        ChecklistTemplateItem("HOT_WATER", "Water heater operational; TPR discharge safe", "plumbing"),
        ChecklistTemplateItem("PLUMB_LEAKS", "No active plumbing leaks", "plumbing"),
        ChecklistTemplateItem("EGRESS", "Bedrooms have safe egress", "egress"),
        ChecklistTemplateItem("PEELING_PAINT", "No peeling/chipping paint (esp. pre-1978 risk)", "interior"),
        ChecklistTemplateItem("ROOF_LEAKS", "No visible roof leaks/water intrusion", "exterior"),
        ChecklistTemplateItem("TRIP_HAZARDS", "Floors/steps free of trip hazards", "safety"),
    ]


def normalize_fail_point(text: str) -> str:
    return " ".join((text or "").strip().split())


def fail_points_to_items(fail_points: Iterable[str]) -> list[ChecklistTemplateItem]:
    out: list[ChecklistTemplateItem] = []
    for fp in fail_points:
        t = normalize_fail_point(fp)
        if not t:
            continue
        code = "FP_" + "".join(ch for ch in t.upper() if ch.isalnum())[:20]
        out.append(ChecklistTemplateItem(code=code, description=t, category="safety"))
    return out