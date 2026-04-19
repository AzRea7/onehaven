from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from .inspection_rules import (
    lookup_hud_criterion,
    normalize_inspection_item_status,
    normalize_rule_code,
)


@dataclass(frozen=True)
class MappingResult:
    checklist_code: str
    inspection_rule_code: str
    rehab_title: Optional[str] = None
    rehab_category: str = "rehab"
    inspection_relevant: bool = True
    default_fail_reason: Optional[str] = None


INSPECTION_CODE_TO_ACTION: dict[str, MappingResult] = {
    # 1 / 2 / 3 room systems
    "LIVING_ROOM_ELECTRICITY": MappingResult(
        checklist_code="LIVING_ROOM_ELECTRICITY",
        inspection_rule_code="LIVING_ROOM_ELECTRICITY",
        rehab_title="Repair living room outlets / lighting",
        rehab_category="electrical",
        default_fail_reason="Living room electrical service is insufficient or not functional.",
    ),
    "LIVING_ROOM_ELECTRICAL_HAZARDS": MappingResult(
        checklist_code="LIVING_ROOM_ELECTRICAL_HAZARDS",
        inspection_rule_code="LIVING_ROOM_ELECTRICAL_HAZARDS",
        rehab_title="Correct living room electrical hazards",
        rehab_category="electrical",
        default_fail_reason="Living room contains unsafe electrical hazard.",
    ),
    "LIVING_ROOM_SECURITY": MappingResult(
        checklist_code="LIVING_ROOM_SECURITY",
        inspection_rule_code="LIVING_ROOM_SECURITY",
        rehab_title="Repair living room locks / secure openings",
        rehab_category="security",
        default_fail_reason="Living room accessible openings are not lockable.",
    ),
    "LIVING_ROOM_WINDOW_CONDITION": MappingResult(
        checklist_code="LIVING_ROOM_WINDOW_CONDITION",
        inspection_rule_code="LIVING_ROOM_WINDOW_CONDITION",
        rehab_title="Repair living room windows",
        rehab_category="exterior",
        default_fail_reason="Living room window condition is unsafe or severely deteriorated.",
    ),
    "LIVING_ROOM_CEILING_CONDITION": MappingResult(
        checklist_code="LIVING_ROOM_CEILING_CONDITION",
        inspection_rule_code="LIVING_ROOM_CEILING_CONDITION",
        rehab_title="Repair living room ceiling defects",
        rehab_category="interior",
        default_fail_reason="Living room ceiling is unsafe or unsound.",
    ),
    "LIVING_ROOM_WALL_CONDITION": MappingResult(
        checklist_code="LIVING_ROOM_WALL_CONDITION",
        inspection_rule_code="LIVING_ROOM_WALL_CONDITION",
        rehab_title="Repair living room wall defects",
        rehab_category="interior",
        default_fail_reason="Living room walls are unsafe or unsound.",
    ),
    "LIVING_ROOM_FLOOR_CONDITION": MappingResult(
        checklist_code="LIVING_ROOM_FLOOR_CONDITION",
        inspection_rule_code="LIVING_ROOM_FLOOR_CONDITION",
        rehab_title="Repair living room floor defects",
        rehab_category="interior",
        default_fail_reason="Living room floor is unsafe or structurally unsound.",
    ),
    "LIVING_ROOM_LEAD_BASED_PAINT": MappingResult(
        checklist_code="LIVING_ROOM_LEAD_BASED_PAINT",
        inspection_rule_code="LIVING_ROOM_LEAD_BASED_PAINT",
        rehab_title="Stabilize living room deteriorated paint",
        rehab_category="lead",
        default_fail_reason="Living room deteriorated paint exceeds applicable threshold.",
    ),

    "KITCHEN_ELECTRICITY": MappingResult(
        checklist_code="KITCHEN_ELECTRICITY",
        inspection_rule_code="KITCHEN_ELECTRICITY",
        rehab_title="Repair kitchen power / lighting",
        rehab_category="electrical",
        default_fail_reason="Kitchen lacks required functional outlet/lighting.",
    ),
    "KITCHEN_ELECTRICAL_HAZARDS": MappingResult(
        checklist_code="KITCHEN_ELECTRICAL_HAZARDS",
        inspection_rule_code="KITCHEN_ELECTRICAL_HAZARDS",
        rehab_title="Correct kitchen electrical hazards",
        rehab_category="electrical",
        default_fail_reason="Unsafe kitchen electrical condition present.",
    ),
    "KITCHEN_SECURITY": MappingResult(
        checklist_code="KITCHEN_SECURITY",
        inspection_rule_code="KITCHEN_SECURITY",
        rehab_title="Repair kitchen locks / secure openings",
        rehab_category="security",
        default_fail_reason="Kitchen accessible openings are not lockable.",
    ),
    "KITCHEN_WINDOW_CONDITION": MappingResult(
        checklist_code="KITCHEN_WINDOW_CONDITION",
        inspection_rule_code="KITCHEN_WINDOW_CONDITION",
        rehab_title="Repair kitchen windows",
        rehab_category="exterior",
        default_fail_reason="Kitchen window condition is unsafe or severely deteriorated.",
    ),
    "KITCHEN_CEILING_CONDITION": MappingResult(
        checklist_code="KITCHEN_CEILING_CONDITION",
        inspection_rule_code="KITCHEN_CEILING_CONDITION",
        rehab_title="Repair kitchen ceiling defects",
        rehab_category="interior",
        default_fail_reason="Kitchen ceiling is unsafe or unsound.",
    ),
    "KITCHEN_WALL_CONDITION": MappingResult(
        checklist_code="KITCHEN_WALL_CONDITION",
        inspection_rule_code="KITCHEN_WALL_CONDITION",
        rehab_title="Repair kitchen wall defects",
        rehab_category="interior",
        default_fail_reason="Kitchen walls are unsafe or unsound.",
    ),
    "KITCHEN_FLOOR_CONDITION": MappingResult(
        checklist_code="KITCHEN_FLOOR_CONDITION",
        inspection_rule_code="KITCHEN_FLOOR_CONDITION",
        rehab_title="Repair kitchen floor defects",
        rehab_category="interior",
        default_fail_reason="Kitchen floor is unsafe or structurally unsound.",
    ),
    "KITCHEN_LEAD_BASED_PAINT": MappingResult(
        checklist_code="KITCHEN_LEAD_BASED_PAINT",
        inspection_rule_code="KITCHEN_LEAD_BASED_PAINT",
        rehab_title="Stabilize kitchen deteriorated paint",
        rehab_category="lead",
        default_fail_reason="Kitchen deteriorated paint exceeds applicable threshold.",
    ),
    "KITCHEN_STOVE_OR_RANGE_WITH_OVEN": MappingResult(
        checklist_code="KITCHEN_STOVE_OR_RANGE_WITH_OVEN",
        inspection_rule_code="KITCHEN_STOVE_OR_RANGE_WITH_OVEN",
        rehab_title="Install/repair stove range and oven",
        rehab_category="appliances",
        default_fail_reason="Required cooking equipment is missing, unsafe, or nonfunctional.",
    ),
    "KITCHEN_REFRIGERATOR": MappingResult(
        checklist_code="KITCHEN_REFRIGERATOR",
        inspection_rule_code="KITCHEN_REFRIGERATOR",
        rehab_title="Install/repair refrigerator",
        rehab_category="appliances",
        default_fail_reason="Refrigerator missing or not maintaining safe temperature.",
    ),
    "KITCHEN_SINK": MappingResult(
        checklist_code="KITCHEN_SINK",
        inspection_rule_code="KITCHEN_SINK",
        rehab_title="Repair kitchen sink / water service",
        rehab_category="plumbing",
        default_fail_reason="Kitchen sink missing or not working correctly.",
    ),
    "KITCHEN_SPACE_FOR_STORAGE_PREPARATION_AND_SERVING_OF_FOOD": MappingResult(
        checklist_code="KITCHEN_SPACE_FOR_STORAGE_PREPARATION_AND_SERVING_OF_FOOD",
        inspection_rule_code="KITCHEN_SPACE_FOR_STORAGE_PREPARATION_AND_SERVING_OF_FOOD",
        rehab_title="Add kitchen prep / storage space",
        rehab_category="interior",
        default_fail_reason="Kitchen lacks sufficient storage/prep/serving area.",
    ),

    "BATHROOM_ELECTRICITY": MappingResult(
        checklist_code="BATHROOM_ELECTRICITY",
        inspection_rule_code="BATHROOM_ELECTRICITY",
        rehab_title="Repair bathroom lighting",
        rehab_category="electrical",
        default_fail_reason="Bathroom lacks required permanent lighting.",
    ),
    "BATHROOM_ELECTRICAL_HAZARDS": MappingResult(
        checklist_code="BATHROOM_ELECTRICAL_HAZARDS",
        inspection_rule_code="BATHROOM_ELECTRICAL_HAZARDS",
        rehab_title="Correct bathroom electrical hazards",
        rehab_category="electrical",
        default_fail_reason="Unsafe bathroom electrical condition present.",
    ),
    "BATHROOM_SECURITY": MappingResult(
        checklist_code="BATHROOM_SECURITY",
        inspection_rule_code="BATHROOM_SECURITY",
        rehab_title="Repair bathroom locks / secure openings",
        rehab_category="security",
        default_fail_reason="Bathroom accessible openings are not lockable.",
    ),
    "BATHROOM_WINDOW_CONDITION": MappingResult(
        checklist_code="BATHROOM_WINDOW_CONDITION",
        inspection_rule_code="BATHROOM_WINDOW_CONDITION",
        rehab_title="Repair bathroom windows",
        rehab_category="exterior",
        default_fail_reason="Bathroom window condition is unsafe or severely deteriorated.",
    ),
    "BATHROOM_CEILING_CONDITION": MappingResult(
        checklist_code="BATHROOM_CEILING_CONDITION",
        inspection_rule_code="BATHROOM_CEILING_CONDITION",
        rehab_title="Repair bathroom ceiling defects",
        rehab_category="interior",
        default_fail_reason="Bathroom ceiling is unsafe or unsound.",
    ),
    "BATHROOM_WALL_CONDITION": MappingResult(
        checklist_code="BATHROOM_WALL_CONDITION",
        inspection_rule_code="BATHROOM_WALL_CONDITION",
        rehab_title="Repair bathroom wall defects",
        rehab_category="interior",
        default_fail_reason="Bathroom walls are unsafe or unsound.",
    ),
    "BATHROOM_FLOOR_CONDITION": MappingResult(
        checklist_code="BATHROOM_FLOOR_CONDITION",
        inspection_rule_code="BATHROOM_FLOOR_CONDITION",
        rehab_title="Repair bathroom floor defects",
        rehab_category="interior",
        default_fail_reason="Bathroom floor is unsafe or structurally unsound.",
    ),
    "BATHROOM_LEAD_BASED_PAINT": MappingResult(
        checklist_code="BATHROOM_LEAD_BASED_PAINT",
        inspection_rule_code="BATHROOM_LEAD_BASED_PAINT",
        rehab_title="Stabilize bathroom deteriorated paint",
        rehab_category="lead",
        default_fail_reason="Bathroom deteriorated paint exceeds applicable threshold.",
    ),
    "BATHROOM_FLUSH_TOILET_IN_ENCLOSED_ROOM_IN_UNIT": MappingResult(
        checklist_code="BATHROOM_FLUSH_TOILET_IN_ENCLOSED_ROOM_IN_UNIT",
        inspection_rule_code="BATHROOM_FLUSH_TOILET_IN_ENCLOSED_ROOM_IN_UNIT",
        rehab_title="Repair/install compliant toilet",
        rehab_category="plumbing",
        default_fail_reason="Bathroom toilet is missing, nonfunctional, or not compliant.",
    ),
    "BATHROOM_FIXED_WASH_BASIN_OR_LAVATORY_IN_UNIT": MappingResult(
        checklist_code="BATHROOM_FIXED_WASH_BASIN_OR_LAVATORY_IN_UNIT",
        inspection_rule_code="BATHROOM_FIXED_WASH_BASIN_OR_LAVATORY_IN_UNIT",
        rehab_title="Repair/install bathroom sink basin",
        rehab_category="plumbing",
        default_fail_reason="Bathroom sink basin missing or not working correctly.",
    ),
    "BATHROOM_TUB_OR_SHOWER": MappingResult(
        checklist_code="BATHROOM_TUB_OR_SHOWER",
        inspection_rule_code="BATHROOM_TUB_OR_SHOWER",
        rehab_title="Repair/install tub or shower",
        rehab_category="plumbing",
        default_fail_reason="Tub or shower missing or not working correctly.",
    ),
    "BATHROOM_VENTILATION": MappingResult(
        checklist_code="BATHROOM_VENTILATION",
        inspection_rule_code="BATHROOM_VENTILATION",
        rehab_title="Repair bathroom ventilation",
        rehab_category="hvac",
        default_fail_reason="Bathroom ventilation is missing or nonfunctional.",
    ),

    # Part 4 / 5 / 6 / 7 / 8
    "OTHER_ROOM_ELECTRICITY_OR_ILLUMINATION": MappingResult(
        checklist_code="OTHER_ROOM_ELECTRICITY_OR_ILLUMINATION",
        inspection_rule_code="OTHER_ROOM_ELECTRICITY_OR_ILLUMINATION",
        rehab_title="Repair other-room lighting / power",
        rehab_category="electrical",
        default_fail_reason="Other room lacks required power or illumination.",
    ),
    "OTHER_ROOM_ELECTRICAL_HAZARDS": MappingResult(
        checklist_code="OTHER_ROOM_ELECTRICAL_HAZARDS",
        inspection_rule_code="OTHER_ROOM_ELECTRICAL_HAZARDS",
        rehab_title="Correct other-room electrical hazards",
        rehab_category="electrical",
        default_fail_reason="Unsafe electrical condition present in other room/hall.",
    ),
    "OTHER_ROOM_SECURITY": MappingResult(
        checklist_code="OTHER_ROOM_SECURITY",
        inspection_rule_code="OTHER_ROOM_SECURITY",
        rehab_title="Repair other-room locks / secure openings",
        rehab_category="security",
        default_fail_reason="Other room accessible openings are not lockable.",
    ),
    "OTHER_ROOM_WINDOW_CONDITION": MappingResult(
        checklist_code="OTHER_ROOM_WINDOW_CONDITION",
        inspection_rule_code="OTHER_ROOM_WINDOW_CONDITION",
        rehab_title="Repair other-room windows",
        rehab_category="exterior",
        default_fail_reason="Other room window is missing where required or is unsafe/deteriorated.",
    ),
    "OTHER_ROOM_CEILING_CONDITION": MappingResult(
        checklist_code="OTHER_ROOM_CEILING_CONDITION",
        inspection_rule_code="OTHER_ROOM_CEILING_CONDITION",
        rehab_title="Repair other-room ceiling defects",
        rehab_category="interior",
        default_fail_reason="Other room ceiling is unsafe or unsound.",
    ),
    "OTHER_ROOM_WALL_CONDITION": MappingResult(
        checklist_code="OTHER_ROOM_WALL_CONDITION",
        inspection_rule_code="OTHER_ROOM_WALL_CONDITION",
        rehab_title="Repair other-room wall defects",
        rehab_category="interior",
        default_fail_reason="Other room walls are unsafe or unsound.",
    ),
    "OTHER_ROOM_FLOOR_CONDITION": MappingResult(
        checklist_code="OTHER_ROOM_FLOOR_CONDITION",
        inspection_rule_code="OTHER_ROOM_FLOOR_CONDITION",
        rehab_title="Repair other-room floor defects",
        rehab_category="interior",
        default_fail_reason="Other room floor is unsafe or structurally unsound.",
    ),
    "OTHER_ROOM_LEAD_BASED_PAINT": MappingResult(
        checklist_code="OTHER_ROOM_LEAD_BASED_PAINT",
        inspection_rule_code="OTHER_ROOM_LEAD_BASED_PAINT",
        rehab_title="Stabilize other-room deteriorated paint",
        rehab_category="lead",
        default_fail_reason="Other room deteriorated paint exceeds applicable threshold.",
    ),
    "OTHER_ROOM_SMOKE_DETECTORS": MappingResult(
        checklist_code="OTHER_ROOM_SMOKE_DETECTORS",
        inspection_rule_code="OTHER_ROOM_SMOKE_DETECTORS",
        rehab_title="Install/test smoke detectors on required levels",
        rehab_category="safety",
        default_fail_reason="Required smoke detector coverage does not comply.",
    ),
    "SECONDARY_ROOMS_SECURITY": MappingResult(
        checklist_code="SECONDARY_ROOMS_SECURITY",
        inspection_rule_code="SECONDARY_ROOMS_SECURITY",
        rehab_title="Secure secondary-room openings",
        rehab_category="security",
        default_fail_reason="Secondary-room accessible openings are not lockable.",
    ),
    "SECONDARY_ROOMS_ELECTRICAL_HAZARDS": MappingResult(
        checklist_code="SECONDARY_ROOMS_ELECTRICAL_HAZARDS",
        inspection_rule_code="SECONDARY_ROOMS_ELECTRICAL_HAZARDS",
        rehab_title="Correct secondary-room electrical hazards",
        rehab_category="electrical",
        default_fail_reason="Unsafe electrical hazard present in secondary room.",
    ),
    "SECONDARY_ROOMS_OTHER_POTENTIALLY_HAZARDOUS_FEATURES": MappingResult(
        checklist_code="SECONDARY_ROOMS_OTHER_POTENTIALLY_HAZARDOUS_FEATURES",
        inspection_rule_code="SECONDARY_ROOMS_OTHER_POTENTIALLY_HAZARDOUS_FEATURES",
        rehab_title="Repair hazardous secondary-room condition",
        rehab_category="safety",
        default_fail_reason="Other hazardous condition present in secondary room.",
    ),
    "BUILDING_EXTERIOR_CONDITION_OF_FOUNDATION": MappingResult(
        checklist_code="BUILDING_EXTERIOR_CONDITION_OF_FOUNDATION",
        inspection_rule_code="BUILDING_EXTERIOR_CONDITION_OF_FOUNDATION",
        rehab_title="Repair foundation defects",
        rehab_category="structure",
        default_fail_reason="Foundation is unsound or hazardous.",
    ),
    "BUILDING_EXTERIOR_CONDITION_OF_STAIRS_RAILS_AND_PORCHES": MappingResult(
        checklist_code="BUILDING_EXTERIOR_CONDITION_OF_STAIRS_RAILS_AND_PORCHES",
        inspection_rule_code="BUILDING_EXTERIOR_CONDITION_OF_STAIRS_RAILS_AND_PORCHES",
        rehab_title="Repair exterior stairs, rails, and porches",
        rehab_category="structure",
        default_fail_reason="Exterior stairs, rails, or porches are unsafe.",
    ),
    "BUILDING_EXTERIOR_CONDITION_OF_ROOF_AND_GUTTERS": MappingResult(
        checklist_code="BUILDING_EXTERIOR_CONDITION_OF_ROOF_AND_GUTTERS",
        inspection_rule_code="BUILDING_EXTERIOR_CONDITION_OF_ROOF_AND_GUTTERS",
        rehab_title="Repair roof / gutters / downspouts",
        rehab_category="exterior",
        default_fail_reason="Roof/gutter condition is hazardous or allows major infiltration.",
    ),
    "BUILDING_EXTERIOR_CONDITION_OF_EXTERIOR_SURFACES": MappingResult(
        checklist_code="BUILDING_EXTERIOR_CONDITION_OF_EXTERIOR_SURFACES",
        inspection_rule_code="BUILDING_EXTERIOR_CONDITION_OF_EXTERIOR_SURFACES",
        rehab_title="Repair hazardous exterior surfaces",
        rehab_category="exterior",
        default_fail_reason="Exterior surface condition is unsafe or severely deteriorated.",
    ),
    "BUILDING_EXTERIOR_CONDITION_OF_CHIMNEY": MappingResult(
        checklist_code="BUILDING_EXTERIOR_CONDITION_OF_CHIMNEY",
        inspection_rule_code="BUILDING_EXTERIOR_CONDITION_OF_CHIMNEY",
        rehab_title="Repair chimney defects",
        rehab_category="exterior",
        default_fail_reason="Chimney is unsafe, leaning, or disintegrating.",
    ),
    "BUILDING_EXTERIOR_LEAD_BASED_PAINT_EXTERIOR_SURFACES": MappingResult(
        checklist_code="BUILDING_EXTERIOR_LEAD_BASED_PAINT_EXTERIOR_SURFACES",
        inspection_rule_code="BUILDING_EXTERIOR_LEAD_BASED_PAINT_EXTERIOR_SURFACES",
        rehab_title="Stabilize exterior deteriorated paint",
        rehab_category="lead",
        default_fail_reason="Exterior deteriorated paint exceeds applicable threshold.",
    ),
    "BUILDING_EXTERIOR_MANUFACTURED_HOMES_TIE_DOWNS": MappingResult(
        checklist_code="BUILDING_EXTERIOR_MANUFACTURED_HOMES_TIE_DOWNS",
        inspection_rule_code="BUILDING_EXTERIOR_MANUFACTURED_HOMES_TIE_DOWNS",
        rehab_title="Repair manufactured-home tie-downs",
        rehab_category="structure",
        default_fail_reason="Manufactured-home tie-down/anchoring is unsafe or missing.",
    ),
    "HEATING_AND_PLUMBING_ADEQUACY_OF_HEATING_EQUIPMENT": MappingResult(
        checklist_code="HEATING_AND_PLUMBING_ADEQUACY_OF_HEATING_EQUIPMENT",
        inspection_rule_code="HEATING_AND_PLUMBING_ADEQUACY_OF_HEATING_EQUIPMENT",
        rehab_title="Repair/replace heating equipment",
        rehab_category="hvac",
        default_fail_reason="Heating equipment cannot provide adequate heat to living areas.",
    ),
    "HEATING_AND_PLUMBING_SAFETY_OF_HEATING_EQUIPMENT": MappingResult(
        checklist_code="HEATING_AND_PLUMBING_SAFETY_OF_HEATING_EQUIPMENT",
        inspection_rule_code="HEATING_AND_PLUMBING_SAFETY_OF_HEATING_EQUIPMENT",
        rehab_title="Correct unsafe heating conditions",
        rehab_category="hvac",
        default_fail_reason="Unsafe heating condition is present.",
    ),
    "HEATING_AND_PLUMBING_VENTILATION_AND_ADEQUACY_OF_COOLING": MappingResult(
        checklist_code="HEATING_AND_PLUMBING_VENTILATION_AND_ADEQUACY_OF_COOLING",
        inspection_rule_code="HEATING_AND_PLUMBING_VENTILATION_AND_ADEQUACY_OF_COOLING",
        rehab_title="Repair ventilation / cooling",
        rehab_category="hvac",
        default_fail_reason="Ventilation or cooling is inadequate.",
    ),
    "HEATING_AND_PLUMBING_WATER_HEATER": MappingResult(
        checklist_code="HEATING_AND_PLUMBING_WATER_HEATER",
        inspection_rule_code="HEATING_AND_PLUMBING_WATER_HEATER",
        rehab_title="Correct water-heater safety issues",
        rehab_category="plumbing",
        default_fail_reason="Water heater is installed or equipped unsafely.",
    ),
    "HEATING_AND_PLUMBING_WATER_SUPPLY": MappingResult(
        checklist_code="HEATING_AND_PLUMBING_WATER_SUPPLY",
        inspection_rule_code="HEATING_AND_PLUMBING_WATER_SUPPLY",
        rehab_title="Restore approvable water supply",
        rehab_category="plumbing",
        default_fail_reason="Water supply is not sanitary or approvable.",
    ),
    "HEATING_AND_PLUMBING_PLUMBING": MappingResult(
        checklist_code="HEATING_AND_PLUMBING_PLUMBING",
        inspection_rule_code="HEATING_AND_PLUMBING_PLUMBING",
        rehab_title="Repair major plumbing leaks/corrosion",
        rehab_category="plumbing",
        default_fail_reason="Major leaks or serious corrosion/contamination are present.",
    ),
    "HEATING_AND_PLUMBING_SEWER_CONNECTION": MappingResult(
        checklist_code="HEATING_AND_PLUMBING_SEWER_CONNECTION",
        inspection_rule_code="HEATING_AND_PLUMBING_SEWER_CONNECTION",
        rehab_title="Repair sewer/disposal connection",
        rehab_category="plumbing",
        default_fail_reason="Sewer/disposal connection is not approvable or backup evidence exists.",
    ),
    "GENERAL_HEALTH_AND_SAFETY_ACCESS_TO_UNIT": MappingResult(
        checklist_code="GENERAL_HEALTH_AND_SAFETY_ACCESS_TO_UNIT",
        inspection_rule_code="GENERAL_HEALTH_AND_SAFETY_ACCESS_TO_UNIT",
        rehab_title="Provide compliant direct unit access",
        rehab_category="safety",
        default_fail_reason="Unit access requires passage through another unit.",
    ),
    "GENERAL_HEALTH_AND_SAFETY_EXITS": MappingResult(
        checklist_code="GENERAL_HEALTH_AND_SAFETY_EXITS",
        inspection_rule_code="GENERAL_HEALTH_AND_SAFETY_EXITS",
        rehab_title="Restore compliant emergency exit",
        rehab_category="safety",
        default_fail_reason="Acceptable fire exit missing or blocked.",
    ),
    "GENERAL_HEALTH_AND_SAFETY_EVIDENCE_OF_INFESTATION": MappingResult(
        checklist_code="GENERAL_HEALTH_AND_SAFETY_EVIDENCE_OF_INFESTATION",
        inspection_rule_code="GENERAL_HEALTH_AND_SAFETY_EVIDENCE_OF_INFESTATION",
        rehab_title="Treat infestation and seal entry points",
        rehab_category="sanitation",
        default_fail_reason="Serious infestation evidence present.",
    ),
    "GENERAL_HEALTH_AND_SAFETY_GARBAGE_AND_DEBRIS": MappingResult(
        checklist_code="GENERAL_HEALTH_AND_SAFETY_GARBAGE_AND_DEBRIS",
        inspection_rule_code="GENERAL_HEALTH_AND_SAFETY_GARBAGE_AND_DEBRIS",
        rehab_title="Remove garbage and debris",
        rehab_category="sanitation",
        default_fail_reason="Heavy garbage/debris accumulation present.",
    ),
    "GENERAL_HEALTH_AND_SAFETY_REFUSE_DISPOSAL": MappingResult(
        checklist_code="GENERAL_HEALTH_AND_SAFETY_REFUSE_DISPOSAL",
        inspection_rule_code="GENERAL_HEALTH_AND_SAFETY_REFUSE_DISPOSAL",
        rehab_title="Provide approved covered refuse disposal",
        rehab_category="sanitation",
        default_fail_reason="Adequate covered refuse facilities are not provided.",
    ),
    "GENERAL_HEALTH_AND_SAFETY_INTERIOR_STAIRS_AND_COMMON_HALLS": MappingResult(
        checklist_code="GENERAL_HEALTH_AND_SAFETY_INTERIOR_STAIRS_AND_COMMON_HALLS",
        inspection_rule_code="GENERAL_HEALTH_AND_SAFETY_INTERIOR_STAIRS_AND_COMMON_HALLS",
        rehab_title="Repair interior stairs / halls hazards",
        rehab_category="safety",
        default_fail_reason="Interior stairs/common halls contain a serious hazard.",
    ),
    "GENERAL_HEALTH_AND_SAFETY_OTHER_INTERIOR_HAZARDS": MappingResult(
        checklist_code="GENERAL_HEALTH_AND_SAFETY_OTHER_INTERIOR_HAZARDS",
        inspection_rule_code="GENERAL_HEALTH_AND_SAFETY_OTHER_INTERIOR_HAZARDS",
        rehab_title="Repair other interior hazard",
        rehab_category="safety",
        default_fail_reason="Other interior hazard present.",
    ),
    "GENERAL_HEALTH_AND_SAFETY_ELEVATORS": MappingResult(
        checklist_code="GENERAL_HEALTH_AND_SAFETY_ELEVATORS",
        inspection_rule_code="GENERAL_HEALTH_AND_SAFETY_ELEVATORS",
        rehab_title="Repair/certify elevator safety",
        rehab_category="safety",
        default_fail_reason="Elevator is unsafe or lacks required inspection certificate.",
    ),
    "GENERAL_HEALTH_AND_SAFETY_INTERIOR_AIR_QUALITY": MappingResult(
        checklist_code="GENERAL_HEALTH_AND_SAFETY_INTERIOR_AIR_QUALITY",
        inspection_rule_code="GENERAL_HEALTH_AND_SAFETY_INTERIOR_AIR_QUALITY",
        rehab_title="Correct interior air quality hazard",
        rehab_category="health",
        default_fail_reason="Interior air quality hazard is present.",
    ),
    "GENERAL_HEALTH_AND_SAFETY_SITE_AND_NEIGHBORHOOD_CONDITIONS": MappingResult(
        checklist_code="GENERAL_HEALTH_AND_SAFETY_SITE_AND_NEIGHBORHOOD_CONDITIONS",
        inspection_rule_code="GENERAL_HEALTH_AND_SAFETY_SITE_AND_NEIGHBORHOOD_CONDITIONS",
        rehab_title="Address dangerous site/neighborhood condition",
        rehab_category="health",
        default_fail_reason="Site/neighborhood condition seriously endangers health or safety.",
    ),
    "GENERAL_HEALTH_AND_SAFETY_LEAD_BASED_PAINT_OWNER_CERTIFICATION": MappingResult(
        checklist_code="GENERAL_HEALTH_AND_SAFETY_LEAD_BASED_PAINT_OWNER_CERTIFICATION",
        inspection_rule_code="GENERAL_HEALTH_AND_SAFETY_LEAD_BASED_PAINT_OWNER_CERTIFICATION",
        rehab_title="Obtain lead-based paint owner certification",
        rehab_category="lead",
        default_fail_reason="Required Lead-Based Paint Owner Certification has not been received.",
    ),

    # legacy aliases
    "GFCI_MISSING": MappingResult(
        checklist_code="KITCHEN_ELECTRICAL_HAZARDS",
        inspection_rule_code="KITCHEN_ELECTRICAL_HAZARDS",
        rehab_title="Install/replace missing GFCI outlets",
        rehab_category="electrical",
        default_fail_reason="Required GFCI protection missing.",
    ),
    "SMOKE_DETECTOR_MISSING": MappingResult(
        checklist_code="OTHER_ROOM_SMOKE_DETECTORS",
        inspection_rule_code="OTHER_ROOM_SMOKE_DETECTORS",
        rehab_title="Install smoke detectors on required levels",
        rehab_category="safety",
        default_fail_reason="Required smoke detector missing or not functional.",
    ),
    "HANDRAIL_MISSING": MappingResult(
        checklist_code="BUILDING_EXTERIOR_CONDITION_OF_STAIRS_RAILS_AND_PORCHES",
        inspection_rule_code="BUILDING_EXTERIOR_CONDITION_OF_STAIRS_RAILS_AND_PORCHES",
        rehab_title="Install/repair handrails",
        rehab_category="safety",
        default_fail_reason="Required handrail missing or insecure.",
    ),
    "BROKEN_WINDOW": MappingResult(
        checklist_code="LIVING_ROOM_WINDOW_CONDITION",
        inspection_rule_code="LIVING_ROOM_WINDOW_CONDITION",
        rehab_title="Repair/replace broken windows",
        rehab_category="exterior",
        default_fail_reason="Window broken or severely deteriorated.",
    ),
    "EGRESS_BLOCKED": MappingResult(
        checklist_code="GENERAL_HEALTH_AND_SAFETY_EXITS",
        inspection_rule_code="GENERAL_HEALTH_AND_SAFETY_EXITS",
        rehab_title="Clear/repair egress path/door/window",
        rehab_category="safety",
        default_fail_reason="Emergency exit path is blocked or unusable.",
    ),
    "PEELING_PAINT": MappingResult(
        checklist_code="LIVING_ROOM_LEAD_BASED_PAINT",
        inspection_rule_code="LIVING_ROOM_LEAD_BASED_PAINT",
        rehab_title="Scrape/prime/paint (lead-safe if needed)",
        rehab_category="lead",
        default_fail_reason="Deteriorated paint observed.",
    ),
}


def map_inspection_code(code: str) -> Optional[MappingResult]:
    key = normalize_rule_code(code)
    if not key:
        return None
    return INSPECTION_CODE_TO_ACTION.get(key)


def _extract_first_value(d: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    for key in keys:
        if key in d:
            return d.get(key)
    return default


def normalize_raw_answer_status(answer: Any) -> str:
    if isinstance(answer, bool):
        return "pass" if answer else "fail"

    if isinstance(answer, (int, float)):
        if int(answer) == 1:
            return "pass"
        if int(answer) == 0:
            return "fail"

    if isinstance(answer, dict):
        raw = _extract_first_value(
            answer,
            ["result_status", "status", "answer", "value", "selected", "response"],
        )
        failed = answer.get("failed")
        return normalize_inspection_item_status(raw, failed=failed)

    return normalize_inspection_item_status(answer)



def _nspire_designation_rank(value: str | None) -> int:
    raw = str(value or "").strip().upper()
    if raw == "LT":
        return 4
    if raw == "S":
        return 3
    if raw == "M":
        return 2
    if raw == "L":
        return 1
    return 0


def _severity_from_nspire_designation(value: str | None, fallback: str | None = None) -> str:
    raw = str(value or "").strip().upper()
    if raw == "LT":
        return "critical"
    if raw in {"S", "M"}:
        return "fail"
    if raw == "L":
        return "warn"
    return str(fallback or "fail").strip().lower() or "fail"


def _default_correction_days_for_designation(value: str | None) -> int | None:
    raw = str(value or "").strip().upper()
    if raw == "LT":
        return 1
    if raw in {"S", "M"}:
        return 30
    return None


def _normalized_rule_category(mapped: MappingResult | None, criterion: Any, fallback: str = "other") -> str:
    if mapped is not None and getattr(mapped, "rehab_category", None):
        return str(mapped.rehab_category).strip().lower() or fallback
    if criterion is not None and getattr(criterion, "category", None):
        return str(criterion.category).strip().lower() or fallback
    return fallback


def build_normalized_inspection_result(
    *,
    code: str,
    answer: Any,
    notes: str | None = None,
    location: str | None = None,
    evidence_json: str | None = None,
    photo_references_json: str | None = None,
) -> dict[str, Any]:
    normalized_code = normalize_rule_code(code)
    mapped = map_inspection_code(normalized_code)
    criterion = lookup_hud_criterion(normalized_code)

    if mapped is None and criterion is not None:
        mapped = MappingResult(
            checklist_code=criterion.code,
            inspection_rule_code=criterion.code,
            rehab_title=criterion.remediation_guidance,
            rehab_category=criterion.category,
            inspection_relevant=True,
            default_fail_reason=criterion.fail_reason_hint,
        )

    status = normalize_raw_answer_status(answer)

    fail_reason = None
    remediation_guidance = None
    category = _normalized_rule_category(mapped, criterion)
    severity = "fail"
    standard_label = None
    standard_citation = None
    not_applicable_allowed = False
    nspire_designation = None
    correction_days = None
    affirmative_habitability_requirement = False
    nspire_standard_key = None
    nspire_standard_code = None
    nspire_standard_label = None
    nspire_deficiency_description = None

    if criterion is not None:
        category = getattr(criterion, "category", category)
        severity = getattr(criterion, "severity", severity)
        remediation_guidance = getattr(criterion, "remediation_guidance", remediation_guidance)
        standard_label = getattr(criterion, "standard_label", standard_label)
        standard_citation = getattr(criterion, "standard_citation", standard_citation)
        not_applicable_allowed = bool(getattr(criterion, "not_applicable_allowed", False))
        nspire_standard_key = getattr(criterion, "nspire_standard_key", None)
        nspire_standard_code = getattr(criterion, "nspire_standard_code", None)
        nspire_standard_label = getattr(criterion, "nspire_standard_label", None)
        nspire_deficiency_description = getattr(criterion, "nspire_deficiency_description", None)
        nspire_designation = getattr(criterion, "nspire_designation", None)
        correction_days = getattr(criterion, "correction_days", None)
        affirmative_habitability_requirement = bool(
            getattr(criterion, "affirmative_habitability_requirement", False)
        )

    if isinstance(answer, dict):
        fail_reason = _extract_first_value(answer, ["fail_reason", "reason", "comment", "details"])
        remediation_guidance = _extract_first_value(
            answer,
            ["remediation_guidance", "suggested_fix", "fix", "repair_hint"],
            remediation_guidance,
        )
        location = _extract_first_value(answer, ["location", "room", "area"], location)
        notes = _extract_first_value(answer, ["notes", "note"], notes)
        evidence_json = answer.get("evidence_json", evidence_json)
        photo_references_json = answer.get("photo_references_json", photo_references_json)
        category = str(answer.get("category") or category).strip().lower() or category
        standard_label = answer.get("standard_label", standard_label)
        standard_citation = answer.get("standard_citation", standard_citation)
        nspire_standard_key = answer.get("nspire_standard_key", nspire_standard_key)
        nspire_standard_code = answer.get("nspire_standard_code", nspire_standard_code)
        nspire_standard_label = answer.get("nspire_standard_label", nspire_standard_label)
        nspire_deficiency_description = answer.get(
            "nspire_deficiency_description", nspire_deficiency_description
        )
        nspire_designation = answer.get("nspire_designation", nspire_designation)
        correction_days = answer.get("correction_days", correction_days)
        if "affirmative_habitability_requirement" in answer:
            affirmative_habitability_requirement = bool(answer.get("affirmative_habitability_requirement"))
    else:
        if status == "fail" and mapped is not None:
            fail_reason = mapped.default_fail_reason

    if status == "not_applicable" and not not_applicable_allowed:
        status = "pending"

    if status == "fail" and not fail_reason and mapped is not None:
        fail_reason = mapped.default_fail_reason

    if status == "fail" and not remediation_guidance and mapped is not None:
        remediation_guidance = mapped.rehab_title

    nspire_designation = str(nspire_designation or "").strip().upper() or None
    if correction_days is not None:
        try:
            correction_days = int(correction_days)
        except Exception:
            correction_days = _default_correction_days_for_designation(nspire_designation)
    else:
        correction_days = _default_correction_days_for_designation(nspire_designation)

    severity = _severity_from_nspire_designation(nspire_designation, severity)

    severity_rank = {
        "info": 1,
        "warn": 2,
        "fail": 3,
        "critical": 4,
    }

    severity_int = severity_rank.get(str(severity).lower(), 3)
    readiness_impact = 0.0
    if status == "fail":
        if nspire_designation == "LT":
            readiness_impact = 30.0
        elif nspire_designation in {"S", "M"}:
            readiness_impact = 18.0
        elif nspire_designation == "L":
            readiness_impact = 6.0
        else:
            readiness_impact = 25.0 if severity_int >= 4 else 15.0 if severity_int == 3 else 8.0 if severity_int == 2 else 0.0
    elif status in {"blocked", "inconclusive"}:
        readiness_impact = 12.0 if severity_int >= 3 else 5.0

    return {
        "code": normalized_code,
        "item_code": mapped.checklist_code if mapped else normalized_code,
        "inspection_rule_code": mapped.inspection_rule_code if mapped else normalized_code,
        "inspection_relevant": bool(mapped.inspection_relevant) if mapped else True,
        "result_status": status,
        "failed": status == "fail",
        "severity": severity,
        "severity_int": severity_int,
        "category": category,
        "location": location,
        "details": notes,
        "fail_reason": fail_reason,
        "remediation_guidance": remediation_guidance,
        "evidence_json": evidence_json or "[]",
        "photo_references_json": photo_references_json or "[]",
        "readiness_impact": float(readiness_impact),
        "requires_reinspection": status in {"fail", "blocked", "inconclusive"},
        "standard_label": standard_label,
        "standard_citation": standard_citation,
        "rehab_title": mapped.rehab_title if mapped else remediation_guidance,
        "rehab_category": mapped.rehab_category if mapped else category,
        # Step 5 additive NSPIRE structure
        "nspire_standard_key": nspire_standard_key,
        "nspire_standard_code": nspire_standard_code,
        "nspire_standard_label": nspire_standard_label,
        "nspire_deficiency_description": nspire_deficiency_description,
        "nspire_designation": nspire_designation,
        "correction_days": correction_days,
        "affirmative_habitability_requirement": bool(affirmative_habitability_requirement),
        "structured_deficiency": {
            "rule_id": mapped.inspection_rule_code if mapped else normalized_code,
            "category": category,
            "designation": nspire_designation,
            "correction_days": correction_days,
            "standard_key": nspire_standard_key,
            "standard_code": nspire_standard_code,
            "standard_label": nspire_standard_label,
            "deficiency_description": nspire_deficiency_description,
        },
    }


def map_raw_form_answers(payload: dict[str, Any] | list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if payload is None:
        return []

    rows: list[dict[str, Any]] = []

    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            code = _extract_first_value(item, ["code", "item_code", "inspection_rule_code"])
            if not code:
                continue
            rows.append(
                build_normalized_inspection_result(
                    code=str(code),
                    answer=item,
                )
            )
        return rows

    if not isinstance(payload, dict):
        return []

    items = payload.get("items")
    if isinstance(items, list):
        return map_raw_form_answers(items)

    answers = payload.get("answers")
    if isinstance(answers, dict):
        for code, answer in answers.items():
            rows.append(
                build_normalized_inspection_result(
                    code=str(code),
                    answer=answer,
                )
            )
        return rows

    for code, answer in payload.items():
        if not isinstance(code, str):
            continue
        normalized_code = normalize_rule_code(code)
        if not normalized_code:
            continue
        if lookup_hud_criterion(normalized_code) is None and map_inspection_code(normalized_code) is None:
            continue
        rows.append(
            build_normalized_inspection_result(
                code=normalized_code,
                answer=answer,
            )
        )

    return rows


def build_property_item_outcome(
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None,
    jurisdiction: str | None,
    raw_result: dict[str, Any],
    template_item: dict[str, Any] | None = None,
    inspector_name: str | None = None,
    inspection_date: str | None = None,
) -> dict[str, Any]:
    """
    Domain helper for turning a normalized inspection result into a property-specific
    checklist line outcome. This keeps services thin and preserves per-property
    history across first inspection, reinspections, and ongoing readiness work.
    """
    template_item = dict(template_item or {})
    item_code = str(
        raw_result.get("item_code")
        or template_item.get("code")
        or raw_result.get("code")
        or ""
    ).strip()

    result_status = normalize_inspection_item_status(
        raw_result.get("result_status"),
        failed=raw_result.get("failed"),
    )
    failed = result_status == "fail"
    blocked = result_status == "blocked"

    return {
        "org_id": org_id,
        "property_id": property_id,
        "inspection_id": inspection_id,
        "inspection_date": inspection_date,
        "inspector_name": inspector_name,
        "jurisdiction": (jurisdiction or "").strip() or None,
        "template_key": template_item.get("template_key") or "hud_52580a",
        "template_version": template_item.get("template_version") or "hud_52580a_2019",
        "section": template_item.get("section"),
        "item_number": template_item.get("item_number"),
        "room_scope": template_item.get("room_scope"),
        "item_code": item_code,
        "inspection_rule_code": raw_result.get("inspection_rule_code") or item_code,
        "description": template_item.get("description"),
        "category": raw_result.get("category") or template_item.get("category") or "other",
        "severity": raw_result.get("severity") or template_item.get("severity") or "fail",
        "result_status": result_status,
        "failed": failed,
        "blocked": blocked,
        "not_applicable": result_status == "not_applicable",
        "inconclusive": result_status == "inconclusive",
        "requires_reinspection": bool(raw_result.get("requires_reinspection", failed or blocked)),
        "readiness_impact": float(raw_result.get("readiness_impact", 0.0) or 0.0),
        "fail_reason": raw_result.get("fail_reason"),
        "details": raw_result.get("details"),
        "location": raw_result.get("location"),
        "remediation_guidance": raw_result.get("remediation_guidance") or template_item.get("suggested_fix"),
        "evidence_json": raw_result.get("evidence_json") or "[]",
        "photo_references_json": raw_result.get("photo_references_json") or "[]",
        "rehab_title": raw_result.get("rehab_title"),
        "rehab_category": raw_result.get("rehab_category"),
        "standard_label": raw_result.get("standard_label") or template_item.get("standard_label"),
        "standard_citation": raw_result.get("standard_citation") or template_item.get("standard_citation"),
        "common_fail": bool(template_item.get("common_fail", True)),
        "not_applicable_allowed": bool(template_item.get("not_applicable_allowed", False)),
        "is_resolved": False,
        # Step 5 additive NSPIRE fields
        "nspire_standard_key": raw_result.get("nspire_standard_key") or template_item.get("nspire_standard_key"),
        "nspire_standard_code": raw_result.get("nspire_standard_code") or template_item.get("nspire_standard_code"),
        "nspire_standard_label": raw_result.get("nspire_standard_label") or template_item.get("nspire_standard_label"),
        "nspire_deficiency_description": raw_result.get("nspire_deficiency_description") or template_item.get("nspire_deficiency_description"),
        "nspire_designation": raw_result.get("nspire_designation") or template_item.get("nspire_designation"),
        "correction_days": raw_result.get("correction_days") if raw_result.get("correction_days") is not None else template_item.get("correction_days"),
        "affirmative_habitability_requirement": bool(
            raw_result.get("affirmative_habitability_requirement")
            or template_item.get("affirmative_habitability_requirement")
        ),
    }


def summarize_property_item_outcomes(rows: list[dict[str, Any]] | None) -> dict[str, Any]:
    rows = list(rows or [])
    total = len(rows)
    failed = sum(1 for row in rows if str(row.get("result_status") or "").lower() == "fail")
    blocked = sum(1 for row in rows if str(row.get("result_status") or "").lower() == "blocked")
    passed = sum(1 for row in rows if str(row.get("result_status") or "").lower() == "pass")
    not_applicable = sum(1 for row in rows if str(row.get("result_status") or "").lower() == "not_applicable")
    inconclusive = sum(1 for row in rows if str(row.get("result_status") or "").lower() == "inconclusive")
    pending = sum(
        1
        for row in rows
        if str(row.get("result_status") or "").lower() in {"todo", "pending", "scheduled", ""}
    )
    life_threatening = sum(1 for row in rows if str(row.get("nspire_designation") or "").upper() == "LT")
    severe = sum(1 for row in rows if str(row.get("nspire_designation") or "").upper() == "S")
    moderate = sum(1 for row in rows if str(row.get("nspire_designation") or "").upper() == "M")
    low = sum(1 for row in rows if str(row.get("nspire_designation") or "").upper() == "L")

    unresolved = failed + blocked + inconclusive + pending
    pass_rate = (passed / total) if total else None
    readiness_penalty = sum(float(row.get("readiness_impact", 0.0) or 0.0) for row in rows)
    readiness_score = max(0.0, 100.0 - readiness_penalty)

    return {
        "total_items": total,
        "passed": passed,
        "failed": failed,
        "blocked": blocked,
        "not_applicable": not_applicable,
        "inconclusive": inconclusive,
        "pending": pending,
        "unresolved": unresolved,
        "pass_rate": pass_rate,
        "readiness_score": round(readiness_score, 2),
        "latest_inspection_passed": total > 0 and unresolved == 0,
        "requires_reinspection": failed > 0 or blocked > 0 or inconclusive > 0,
        "life_threatening": life_threatening,
        "severe": severe,
        "moderate": moderate,
        "low": low,
    }
