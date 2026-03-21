from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional


CANONICAL_ITEM_STATUSES = {
    "pending",
    "pass",
    "fail",
    "blocked",
    "not_applicable",
    "not_inspected",
    "inconclusive",
}

SEVERITY_TO_WEIGHT = {
    "info": 0.0,
    "warn": 5.0,
    "fail": 15.0,
    "critical": 30.0,
}


@dataclass(frozen=True)
class InspectionCriterion:
    section: str
    item_number: str
    code: str
    label: str
    category: str
    severity: str = "fail"  # info|warn|fail|critical
    description: str = ""
    fail_reason_hint: Optional[str] = None
    remediation_guidance: Optional[str] = None
    requires_reinspection: bool = True
    common_fail: bool = True
    standard_label: Optional[str] = None
    standard_citation: Optional[str] = None
    template_key: str = "hud_52580a"
    template_version: str = "hud_52580a_2019"
    room_scope: Optional[str] = None
    not_applicable_allowed: bool = False
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class NormalizedInspectionItem:
    code: str
    category: str
    status: str
    severity: str
    readiness_impact: float
    failed: bool
    passed: bool
    blocked: bool
    not_applicable: bool
    requires_reinspection: bool
    fail_reason: Optional[str] = None
    remediation_guidance: Optional[str] = None
    evidence_json: str = "[]"
    photo_references_json: str = "[]"
    standard_label: Optional[str] = None
    standard_citation: Optional[str] = None


@dataclass(frozen=True)
class ReadinessScoreResult:
    total_items: int
    scored_items: int
    passed_items: int
    failed_items: int
    blocked_items: int
    na_items: int
    failed_critical_items: int
    readiness_score: float
    readiness_status: str
    result_status: str


def normalize_rule_code(raw: Optional[str]) -> str:
    text = str(raw or "").strip().upper()
    if not text:
        return ""
    out: list[str] = []
    prev_underscore = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            prev_underscore = False
        else:
            if not prev_underscore:
                out.append("_")
            prev_underscore = True
    return "".join(out).strip("_")


def normalize_severity(raw: Optional[str]) -> str:
    s = str(raw or "").strip().lower()
    if s in {"critical", "life_safety", "life-safety"}:
        return "critical"
    if s in {"warn", "warning"}:
        return "warn"
    if s in {"info", "informational"}:
        return "info"
    return "fail"


def normalize_inspection_item_status(raw: Optional[str], *, failed: Optional[bool] = None) -> str:
    s = str(raw or "").strip().lower()

    if s in {"pass", "passed", "ok", "done", "complete", "completed", "good", "yes"}:
        return "pass"
    if s in {"fail", "failed", "bad", "no"}:
        return "fail"
    if s in {"blocked", "stuck", "needs_access", "cannot_verify"}:
        return "blocked"
    if s in {"na", "n/a", "not_applicable", "not applicable"}:
        return "not_applicable"
    if s in {"inconclusive", "unknown", "verify", "verification_needed"}:
        return "inconclusive"
    if s in {"pending", "todo", "not_started", "not started", ""}:
        if failed is True:
            return "fail"
        if failed is False:
            return "pass"
        return "pending"
    if s in {"not_inspected", "not inspected", "uninspected"}:
        return "not_inspected"

    if failed is True:
        return "fail"
    if failed is False:
        return "pass"
    return "pending"


def is_fail_status(status: str) -> bool:
    return normalize_inspection_item_status(status) == "fail"


def is_pass_status(status: str) -> bool:
    return normalize_inspection_item_status(status) == "pass"


def is_blocked_status(status: str) -> bool:
    return normalize_inspection_item_status(status) == "blocked"


def is_na_status(status: str) -> bool:
    return normalize_inspection_item_status(status) == "not_applicable"


def compute_readiness_impact(*, severity: Optional[str], status: Optional[str]) -> float:
    sev = normalize_severity(severity)
    st = normalize_inspection_item_status(status)
    base = float(SEVERITY_TO_WEIGHT.get(sev, 15.0))

    if st == "fail":
        return base
    if st in {"blocked", "inconclusive"}:
        return max(5.0, round(base * 0.6, 2))
    return 0.0


def normalize_inspection_item(
    *,
    code: Optional[str],
    category: Optional[str],
    status: Optional[str],
    severity: Optional[str],
    failed: Optional[bool] = None,
    fail_reason: Optional[str] = None,
    remediation_guidance: Optional[str] = None,
    evidence_json: Optional[str] = None,
    photo_references_json: Optional[str] = None,
    standard_label: Optional[str] = None,
    standard_citation: Optional[str] = None,
    requires_reinspection: Optional[bool] = None,
) -> NormalizedInspectionItem:
    normalized_code = normalize_rule_code(code)
    normalized_status = normalize_inspection_item_status(status, failed=failed)
    normalized_sev = normalize_severity(severity)
    readiness_impact = compute_readiness_impact(severity=normalized_sev, status=normalized_status)
    default_requires_reinspection = normalized_status in {"fail", "blocked", "inconclusive"}

    return NormalizedInspectionItem(
        code=normalized_code,
        category=str(category or "other").strip().lower() or "other",
        status=normalized_status,
        severity=normalized_sev,
        readiness_impact=readiness_impact,
        failed=normalized_status == "fail",
        passed=normalized_status == "pass",
        blocked=normalized_status in {"blocked", "inconclusive"},
        not_applicable=normalized_status == "not_applicable",
        requires_reinspection=bool(
            default_requires_reinspection if requires_reinspection is None else requires_reinspection
        ),
        fail_reason=(str(fail_reason).strip() if fail_reason else None),
        remediation_guidance=(str(remediation_guidance).strip() if remediation_guidance else None),
        evidence_json=evidence_json or "[]",
        photo_references_json=photo_references_json or "[]",
        standard_label=(str(standard_label).strip() if standard_label else None),
        standard_citation=(str(standard_citation).strip() if standard_citation else None),
    )


def score_readiness(items: Iterable[Any]) -> ReadinessScoreResult:
    total_items = 0
    scored_items = 0
    passed_items = 0
    failed_items = 0
    blocked_items = 0
    na_items = 0
    failed_critical_items = 0
    impact_total = 0.0

    for raw in items or []:
        total_items += 1

        code = raw.get("code") if isinstance(raw, dict) else getattr(raw, "code", None)
        category = raw.get("category") if isinstance(raw, dict) else getattr(raw, "category", None)
        status = raw.get("result_status") if isinstance(raw, dict) else getattr(raw, "result_status", None)
        if not status:
            status = raw.get("status") if isinstance(raw, dict) else getattr(raw, "status", None)
        severity = raw.get("severity") if isinstance(raw, dict) else getattr(raw, "severity", None)
        failed = raw.get("failed") if isinstance(raw, dict) else getattr(raw, "failed", None)

        item = normalize_inspection_item(
            code=code,
            category=category,
            status=status,
            severity=severity,
            failed=failed,
        )

        if item.not_applicable:
            na_items += 1
            continue

        if item.status != "not_inspected":
            scored_items += 1

        if item.passed:
            passed_items += 1
        elif item.failed:
            failed_items += 1
            impact_total += item.readiness_impact
            if item.severity == "critical":
                failed_critical_items += 1
        elif item.blocked:
            blocked_items += 1
            impact_total += item.readiness_impact

    if scored_items <= 0:
        readiness_score = 0.0
    else:
        readiness_score = max(0.0, round(100.0 - impact_total, 2))

    if scored_items <= 0:
        readiness_status = "unknown"
        result_status = "pending"
    elif failed_critical_items > 0:
        readiness_status = "critical"
        result_status = "fail"
    elif failed_items > 0 or blocked_items > 0:
        readiness_status = "needs_work"
        result_status = "fail"
    elif passed_items > 0:
        readiness_status = "ready"
        result_status = "pass"
    else:
        readiness_status = "unknown"
        result_status = "pending"

    return ReadinessScoreResult(
        total_items=int(total_items),
        scored_items=int(scored_items),
        passed_items=int(passed_items),
        failed_items=int(failed_items),
        blocked_items=int(blocked_items),
        na_items=int(na_items),
        failed_critical_items=int(failed_critical_items),
        readiness_score=float(readiness_score),
        readiness_status=readiness_status,
        result_status=result_status,
    )


def rank_common_fail_points(rows: Iterable[dict[str, Any]], *, limit: int = 10) -> list[dict[str, int | str]]:
    counts: dict[str, int] = {}
    for row in rows or []:
        code = normalize_rule_code(row.get("code"))
        if not code:
            continue
        counts[code] = counts.get(code, 0) + int(row.get("count", 1) or 1)

    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    return [{"code": code, "count": count} for code, count in ranked[: max(0, int(limit))]]


def _criterion(
    section: str,
    item_number: str,
    code: str,
    label: str,
    category: str,
    *,
    severity: str = "fail",
    description: str = "",
    fail_reason_hint: str | None = None,
    remediation_guidance: str | None = None,
    requires_reinspection: bool = True,
    common_fail: bool = True,
    standard_label: str | None = None,
    room_scope: str | None = None,
    not_applicable_allowed: bool = False,
    aliases: tuple[str, ...] = (),
) -> InspectionCriterion:
    return InspectionCriterion(
        section=section,
        item_number=item_number,
        code=normalize_rule_code(code),
        label=label,
        category=category,
        severity=normalize_severity(severity),
        description=description,
        fail_reason_hint=fail_reason_hint,
        remediation_guidance=remediation_guidance,
        requires_reinspection=requires_reinspection,
        common_fail=common_fail,
        standard_label=standard_label or label,
        standard_citation=f"HUD-52580-A {item_number}",
        room_scope=room_scope,
        not_applicable_allowed=not_applicable_allowed,
        aliases=tuple(normalize_rule_code(a) for a in aliases if a),
    )


def get_hud_52580a_criteria() -> list[InspectionCriterion]:
    """
    Full inspection decision catalog for the uploaded HUD-52580-A (07/19) form.
    Covers the pass/fail/inconclusive checklist items across Parts 1-8.
    """
    return [
        # ------------------------------------------------------------------
        # 1. Living Room
        # ------------------------------------------------------------------
        _criterion(
            "living_room", "1.1", "LIVING_ROOM_PRESENT", "Living Room Present", "occupancy",
            severity="fail",
            description="Is there a living room?",
            fail_reason_hint="Required living room area not present.",
            remediation_guidance="Provide a compliant living room or verify unit is an efficiency layout where the living area is present.",
            standard_label="Living room presence",
            room_scope="living_room",
            aliases=("ROOM_PRESENT",),
        ),
        _criterion(
            "living_room", "1.2", "LIVING_ROOM_ELECTRICITY", "Electricity", "electrical",
            severity="fail",
            description="At least two working outlets or one working outlet and one working light fixture.",
            fail_reason_hint="Insufficient working power or lighting in living room.",
            remediation_guidance="Repair outlets/fixtures and restore safe electrical service.",
            standard_label="Living room electricity",
            room_scope="living_room",
            aliases=("ELECTRICITY", "OUTLETS_LIGHTS"),
        ),
        _criterion(
            "living_room", "1.3", "LIVING_ROOM_ELECTRICAL_HAZARDS", "Electrical Hazards", "electrical",
            severity="critical",
            description="Room must be free from electrical hazards.",
            fail_reason_hint="Electrical hazard present in living room.",
            remediation_guidance="Repair unsafe wiring, exposed conductors, damaged receptacles, covers, or overloaded circuits.",
            standard_label="Living room electrical safety",
            room_scope="living_room",
            aliases=("ELECTRICAL_HAZARDS",),
        ),
        _criterion(
            "living_room", "1.4", "LIVING_ROOM_SECURITY", "Security", "security",
            severity="fail",
            description="Windows and doors accessible from outside must be lockable.",
            fail_reason_hint="Accessible living room opening is not lockable.",
            remediation_guidance="Repair or replace locks, latches, or secure operability hardware.",
            standard_label="Living room security",
            room_scope="living_room",
            aliases=("SECURITY", "DOORS_SECURE", "WINDOWS_LOCKS"),
        ),
        _criterion(
            "living_room", "1.5", "LIVING_ROOM_WINDOW_CONDITION", "Window Condition", "egress",
            severity="fail",
            description="At least one window and all windows free from severe deterioration or broken panes.",
            fail_reason_hint="Living room window missing, broken, or severely deteriorated.",
            remediation_guidance="Repair or replace damaged glazing, sash, seals, or weather protection.",
            standard_label="Living room window condition",
            room_scope="living_room",
            aliases=("WINDOW_CONDITION", "BROKEN_WINDOW"),
        ),
        _criterion(
            "living_room", "1.6", "LIVING_ROOM_CEILING_CONDITION", "Ceiling Condition", "structure",
            severity="fail",
            description="Ceiling must be sound and free from hazardous defects.",
            fail_reason_hint="Living room ceiling is unsound or hazardous.",
            remediation_guidance="Repair structural ceiling defects, sagging, holes, loose material, or water-damaged surfaces.",
            standard_label="Living room ceiling condition",
            room_scope="living_room",
            aliases=("CEILING_CONDITION",),
        ),
        _criterion(
            "living_room", "1.7", "LIVING_ROOM_WALL_CONDITION", "Wall Condition", "structure",
            severity="fail",
            description="Walls must be sound and free from hazardous defects.",
            fail_reason_hint="Living room walls are unsound or hazardous.",
            remediation_guidance="Repair structural wall defects, large openings, air infiltration, or unstable members.",
            standard_label="Living room wall condition",
            room_scope="living_room",
            aliases=("WALL_CONDITION",),
        ),
        _criterion(
            "living_room", "1.8", "LIVING_ROOM_FLOOR_CONDITION", "Floor Condition", "structure",
            severity="fail",
            description="Floor must be sound and free from hazardous defects.",
            fail_reason_hint="Living room floor is unsafe, unstable, or severely damaged.",
            remediation_guidance="Repair loose, buckled, missing, or structurally unsafe flooring.",
            standard_label="Living room floor condition",
            room_scope="living_room",
            aliases=("FLOOR_CONDITION", "TRIP_HAZARDS"),
        ),
        _criterion(
            "living_room", "1.9", "LIVING_ROOM_LEAD_BASED_PAINT", "Lead-Based Paint", "lead",
            severity="warn",
            description="Painted surfaces must be free of deteriorated paint unless not applicable.",
            fail_reason_hint="Living room deteriorated paint exceeds HUD threshold.",
            remediation_guidance="Stabilize deteriorated paint, repair substrate, apply protective coating, and complete lead-safe workflow where required.",
            standard_label="Living room lead-based paint",
            room_scope="living_room",
            not_applicable_allowed=True,
            aliases=("LEAD_BASED_PAINT", "PEELING_PAINT"),
        ),

        # ------------------------------------------------------------------
        # 2. Kitchen
        # ------------------------------------------------------------------
        _criterion(
            "kitchen", "2.1", "KITCHEN_AREA_PRESENT", "Kitchen Area Present", "occupancy",
            severity="fail",
            description="Is there a kitchen?",
            fail_reason_hint="Kitchen area not present.",
            remediation_guidance="Provide a compliant kitchen area for food preparation.",
            standard_label="Kitchen presence",
            room_scope="kitchen",
            aliases=("KITCHEN_PRESENT",),
        ),
        _criterion(
            "kitchen", "2.2", "KITCHEN_ELECTRICITY", "Electricity", "electrical",
            severity="fail",
            description="At least one working outlet and one permanently installed light fixture.",
            fail_reason_hint="Kitchen power/lighting requirement not met.",
            remediation_guidance="Repair outlet, permanent fixture, wiring, or service.",
            standard_label="Kitchen electricity",
            room_scope="kitchen",
            aliases=("GFCI_KITCHEN", "KITCHEN_OUTLETS_LIGHTS"),
        ),
        _criterion(
            "kitchen", "2.3", "KITCHEN_ELECTRICAL_HAZARDS", "Electrical Hazards", "electrical",
            severity="critical",
            description="Kitchen must be free from electrical hazards.",
            fail_reason_hint="Electrical hazard present in kitchen.",
            remediation_guidance="Correct exposed conductors, damaged receptacles, unsafe wet-area power conditions, or missing covers.",
            standard_label="Kitchen electrical safety",
            room_scope="kitchen",
            aliases=("KITCHEN_HAZARDS",),
        ),
        _criterion(
            "kitchen", "2.4", "KITCHEN_SECURITY", "Security", "security",
            severity="fail",
            description="Accessible windows and doors must be lockable.",
            fail_reason_hint="Accessible kitchen opening is not lockable.",
            remediation_guidance="Repair or replace locks, latches, or secure hardware.",
            standard_label="Kitchen security",
            room_scope="kitchen",
        ),
        _criterion(
            "kitchen", "2.5", "KITCHEN_WINDOW_CONDITION", "Window Condition", "envelope",
            severity="fail",
            description="All windows must be free from deterioration or broken panes. No window does not fail this item.",
            fail_reason_hint="Kitchen window severely deteriorated or broken.",
            remediation_guidance="Repair or replace damaged window components or glazing.",
            standard_label="Kitchen window condition",
            room_scope="kitchen",
        ),
        _criterion(
            "kitchen", "2.6", "KITCHEN_CEILING_CONDITION", "Ceiling Condition", "structure",
            severity="fail",
            description="Ceiling must be sound and free from hazardous defects.",
            fail_reason_hint="Kitchen ceiling is unsafe or unsound.",
            remediation_guidance="Repair hazardous ceiling conditions, holes, sagging, or unstable materials.",
            standard_label="Kitchen ceiling condition",
            room_scope="kitchen",
        ),
        _criterion(
            "kitchen", "2.7", "KITCHEN_WALL_CONDITION", "Wall Condition", "structure",
            severity="fail",
            description="Walls must be sound and free from hazardous defects.",
            fail_reason_hint="Kitchen walls are unsafe or unsound.",
            remediation_guidance="Repair hazardous wall defects, openings, or unstable surfaces.",
            standard_label="Kitchen wall condition",
            room_scope="kitchen",
        ),
        _criterion(
            "kitchen", "2.8", "KITCHEN_FLOOR_CONDITION", "Floor Condition", "structure",
            severity="fail",
            description="Floor must be sound and free from hazardous defects.",
            fail_reason_hint="Kitchen floor is unsafe or structurally unsound.",
            remediation_guidance="Repair buckled, damaged, soft, or unsafe kitchen flooring.",
            standard_label="Kitchen floor condition",
            room_scope="kitchen",
        ),
        _criterion(
            "kitchen", "2.9", "KITCHEN_LEAD_BASED_PAINT", "Lead-Based Paint", "lead",
            severity="warn",
            description="Painted surfaces must be free of deteriorated paint unless not applicable.",
            fail_reason_hint="Kitchen deteriorated paint exceeds HUD threshold.",
            remediation_guidance="Stabilize deteriorated paint and complete lead-safe remediation workflow where required.",
            standard_label="Kitchen lead-based paint",
            room_scope="kitchen",
            not_applicable_allowed=True,
        ),
        _criterion(
            "kitchen", "2.10", "KITCHEN_STOVE_OR_RANGE_WITH_OVEN", "Stove or Range with Oven", "appliances",
            severity="fail",
            description="Working oven and stove/range with working top burners, or approved microwave substitution where allowed.",
            fail_reason_hint="Required cooking equipment is missing, nonfunctional, or unsafe.",
            remediation_guidance="Install/repair approved cooking equipment and correct unsafe gas/electric hookups.",
            standard_label="Kitchen cooking equipment",
            room_scope="kitchen",
            aliases=("STOVE_OR_RANGE", "OVEN_RANGE"),
        ),
        _criterion(
            "kitchen", "2.11", "KITCHEN_REFRIGERATOR", "Refrigerator", "appliances",
            severity="fail",
            description="Working refrigerator that maintains safe food temperature.",
            fail_reason_hint="Refrigerator missing or unable to maintain safe temperature.",
            remediation_guidance="Install/repair refrigerator or restore service needed to verify operation.",
            standard_label="Kitchen refrigerator",
            room_scope="kitchen",
        ),
        _criterion(
            "kitchen", "2.12", "KITCHEN_SINK", "Sink", "plumbing",
            severity="fail",
            description="Working kitchen sink with hot and cold running water and proper drain.",
            fail_reason_hint="Kitchen sink missing or not working properly.",
            remediation_guidance="Install/repair sink, faucets, supply lines, trap, drain, and hot water service.",
            standard_label="Kitchen sink",
            room_scope="kitchen",
            aliases=("SINK",),
        ),
        _criterion(
            "kitchen", "2.13", "KITCHEN_SPACE_FOR_STORAGE_PREPARATION_AND_SERVING_OF_FOOD",
            "Space for Storage, Preparation, and Serving of Food", "interior",
            severity="fail",
            description="Space must be available to store, prepare, and serve food.",
            fail_reason_hint="Kitchen lacks sufficient food storage/preparation/serving space.",
            remediation_guidance="Provide compliant cabinets, counters, shelving, or acceptable substitute layout.",
            standard_label="Kitchen prep/storage space",
            room_scope="kitchen",
            aliases=("FOOD_PREP_SPACE",),
        ),

        # ------------------------------------------------------------------
        # 3. Bathroom
        # ------------------------------------------------------------------
        _criterion(
            "bathroom", "3.1", "BATHROOM_PRESENT", "Bathroom Present", "occupancy",
            severity="fail",
            description="Is there a bathroom?",
            fail_reason_hint="Required bathroom area not present.",
            remediation_guidance="Provide a compliant enclosed bathroom area with required fixtures.",
            standard_label="Bathroom presence",
            room_scope="bathroom",
        ),
        _criterion(
            "bathroom", "3.2", "BATHROOM_ELECTRICITY", "Electricity", "electrical",
            severity="fail",
            description="At least one permanently installed light fixture.",
            fail_reason_hint="Bathroom lighting requirement not met.",
            remediation_guidance="Install/repair permanent lighting and restore safe power service.",
            standard_label="Bathroom electricity",
            room_scope="bathroom",
        ),
        _criterion(
            "bathroom", "3.3", "BATHROOM_ELECTRICAL_HAZARDS", "Electrical Hazards", "electrical",
            severity="critical",
            description="Bathroom must be free from electrical hazards.",
            fail_reason_hint="Bathroom electrical hazard present.",
            remediation_guidance="Repair unsafe power devices, wet-area outlet conditions, exposed wiring, or damaged fixtures.",
            standard_label="Bathroom electrical safety",
            room_scope="bathroom",
            aliases=("GFCI_BATH",),
        ),
        _criterion(
            "bathroom", "3.4", "BATHROOM_SECURITY", "Security", "security",
            severity="fail",
            description="Accessible windows and doors must be lockable.",
            fail_reason_hint="Accessible bathroom opening is not lockable.",
            remediation_guidance="Repair or replace lock/latch hardware or secure window/door operation.",
            standard_label="Bathroom security",
            room_scope="bathroom",
        ),
        _criterion(
            "bathroom", "3.5", "BATHROOM_WINDOW_CONDITION", "Window Condition", "envelope",
            severity="fail",
            description="All windows must be free of deterioration or broken panes. No window does not fail if ventilation otherwise complies.",
            fail_reason_hint="Bathroom window severely deteriorated or broken.",
            remediation_guidance="Repair or replace damaged glazing, frame, or seal conditions.",
            standard_label="Bathroom window condition",
            room_scope="bathroom",
        ),
        _criterion(
            "bathroom", "3.6", "BATHROOM_CEILING_CONDITION", "Ceiling Condition", "structure",
            severity="fail",
            description="Ceiling must be sound and free from hazardous defects.",
            fail_reason_hint="Bathroom ceiling unsafe or unsound.",
            remediation_guidance="Repair structural ceiling failure, sagging, water damage, or loose materials.",
            standard_label="Bathroom ceiling condition",
            room_scope="bathroom",
        ),
        _criterion(
            "bathroom", "3.7", "BATHROOM_WALL_CONDITION", "Wall Condition", "structure",
            severity="fail",
            description="Walls must be sound and free from hazardous defects.",
            fail_reason_hint="Bathroom walls unsafe or unsound.",
            remediation_guidance="Repair unstable walls, tile failure, moisture-damaged substrate, or hazardous openings.",
            standard_label="Bathroom wall condition",
            room_scope="bathroom",
        ),
        _criterion(
            "bathroom", "3.8", "BATHROOM_FLOOR_CONDITION", "Floor Condition", "structure",
            severity="fail",
            description="Floor must be sound and free from hazardous defects.",
            fail_reason_hint="Bathroom floor unsafe or unsound.",
            remediation_guidance="Repair missing, weak, soft, water-damaged, or structurally unsafe floor areas.",
            standard_label="Bathroom floor condition",
            room_scope="bathroom",
        ),
        _criterion(
            "bathroom", "3.9", "BATHROOM_LEAD_BASED_PAINT", "Lead-Based Paint", "lead",
            severity="warn",
            description="Painted surfaces must be free of deteriorated paint unless not applicable.",
            fail_reason_hint="Bathroom deteriorated paint exceeds HUD threshold.",
            remediation_guidance="Stabilize deteriorated paint and complete required lead-safe workflow.",
            standard_label="Bathroom lead-based paint",
            room_scope="bathroom",
            not_applicable_allowed=True,
        ),
        _criterion(
            "bathroom", "3.10", "BATHROOM_FLUSH_TOILET_IN_ENCLOSED_ROOM_IN_UNIT",
            "Flush Toilet in Enclosed Room in Unit", "plumbing",
            severity="critical",
            description="Working toilet in enclosed room in unit for exclusive private use.",
            fail_reason_hint="Toilet missing, nonfunctional, leaking, unclogged, untrapped, or not exclusive/private.",
            remediation_guidance="Install/repair compliant toilet, water supply, sewer connection, venting, trap, and privacy enclosure.",
            standard_label="Bathroom toilet",
            room_scope="bathroom",
            aliases=("TOILET", "FLUSH_TOILET"),
        ),
        _criterion(
            "bathroom", "3.11", "BATHROOM_FIXED_WASH_BASIN_OR_LAVATORY_IN_UNIT",
            "Fixed Wash Basin or Lavatory in Unit", "plumbing",
            severity="fail",
            description="Working permanently installed wash basin with hot and cold water.",
            fail_reason_hint="Bathroom wash basin missing or not working correctly.",
            remediation_guidance="Install/repair permanent basin, supply lines, faucets, trap, drain, and hot water service.",
            standard_label="Bathroom wash basin",
            room_scope="bathroom",
            aliases=("WASH_BASIN", "LAVATORY"),
        ),
        _criterion(
            "bathroom", "3.12", "BATHROOM_TUB_OR_SHOWER", "Tub or Shower", "plumbing",
            severity="fail",
            description="Working tub or shower with hot and cold running water.",
            fail_reason_hint="Tub/shower missing, private-use requirement not met, or not working properly.",
            remediation_guidance="Install/repair tub or shower, water supply, drain, and required privacy/support hardware.",
            standard_label="Bathroom tub or shower",
            room_scope="bathroom",
            aliases=("TUB_OR_SHOWER",),
        ),
        _criterion(
            "bathroom", "3.13", "BATHROOM_VENTILATION", "Ventilation", "hvac",
            severity="fail",
            description="Operable windows or a working vent system vented properly.",
            fail_reason_hint="Bathroom ventilation inadequate or nonfunctional.",
            remediation_guidance="Repair/install window ventilation or a properly vented mechanical exhaust system.",
            standard_label="Bathroom ventilation",
            room_scope="bathroom",
        ),

        # ------------------------------------------------------------------
        # 4. Other Rooms Used for Living and Halls
        # ------------------------------------------------------------------
        _criterion(
            "other_rooms_used_for_living", "4.2", "OTHER_ROOM_ELECTRICITY_OR_ILLUMINATION",
            "Electricity/Illumination", "electrical",
            severity="fail",
            description="Sleeping rooms require 2 working outlets or 1 outlet and 1 working permanent light; other rooms require means of illumination.",
            fail_reason_hint="Other living room lacks required power or illumination.",
            remediation_guidance="Repair/install required outlets, fixtures, or other acceptable illumination.",
            standard_label="Other room electricity/illumination",
            room_scope="other_room",
            aliases=("OTHER_ROOM_LIGHTING",),
        ),
        _criterion(
            "other_rooms_used_for_living", "4.3", "OTHER_ROOM_ELECTRICAL_HAZARDS",
            "Electrical Hazards", "electrical",
            severity="critical",
            description="Room must be free from electrical hazards.",
            fail_reason_hint="Electrical hazard present in other room or hall.",
            remediation_guidance="Repair unsafe wiring, devices, covers, fixtures, or exposed conductors.",
            standard_label="Other room electrical safety",
            room_scope="other_room",
        ),
        _criterion(
            "other_rooms_used_for_living", "4.4", "OTHER_ROOM_SECURITY",
            "Security", "security",
            severity="fail",
            description="Accessible windows and doors must be lockable.",
            fail_reason_hint="Accessible opening in other room/hall not lockable.",
            remediation_guidance="Repair or replace lock/latch hardware and restore secure operation.",
            standard_label="Other room security",
            room_scope="other_room",
        ),
        _criterion(
            "other_rooms_used_for_living", "4.5", "OTHER_ROOM_WINDOW_CONDITION",
            "Window Condition", "egress",
            severity="fail",
            description="Sleeping rooms need at least one window; all windows must be free of severe deterioration or broken panes.",
            fail_reason_hint="Sleeping room lacks required window or window condition is unsafe.",
            remediation_guidance="Repair/replace damaged window components or provide required sleeping-room window.",
            standard_label="Other room window condition",
            room_scope="other_room",
        ),
        _criterion(
            "other_rooms_used_for_living", "4.6", "OTHER_ROOM_CEILING_CONDITION",
            "Ceiling Condition", "structure",
            severity="fail",
            description="Ceiling must be sound and free from hazardous defects.",
            fail_reason_hint="Other room ceiling unsafe or unsound.",
            remediation_guidance="Repair hazardous ceiling conditions, holes, sagging, or loose materials.",
            standard_label="Other room ceiling condition",
            room_scope="other_room",
        ),
        _criterion(
            "other_rooms_used_for_living", "4.7", "OTHER_ROOM_WALL_CONDITION",
            "Wall Condition", "structure",
            severity="fail",
            description="Walls must be sound and free from hazardous defects.",
            fail_reason_hint="Other room walls unsafe or unsound.",
            remediation_guidance="Repair structural wall defects, unstable surfaces, or hazardous openings.",
            standard_label="Other room wall condition",
            room_scope="other_room",
        ),
        _criterion(
            "other_rooms_used_for_living", "4.8", "OTHER_ROOM_FLOOR_CONDITION",
            "Floor Condition", "structure",
            severity="fail",
            description="Floor must be sound and free from hazardous defects.",
            fail_reason_hint="Other room floor unsafe or unsound.",
            remediation_guidance="Repair damaged, weak, missing, or unsafe flooring.",
            standard_label="Other room floor condition",
            room_scope="other_room",
        ),
        _criterion(
            "other_rooms_used_for_living", "4.9", "OTHER_ROOM_LEAD_BASED_PAINT",
            "Lead-Based Paint", "lead",
            severity="warn",
            description="Painted surfaces must be free of deteriorated paint unless not applicable.",
            fail_reason_hint="Other room deteriorated paint exceeds HUD threshold.",
            remediation_guidance="Stabilize deteriorated paint and complete lead-safe workflow where required.",
            standard_label="Other room lead-based paint",
            room_scope="other_room",
            not_applicable_allowed=True,
        ),
        _criterion(
            "other_rooms_used_for_living", "4.10", "OTHER_ROOM_SMOKE_DETECTORS",
            "Smoke Detectors", "safety",
            severity="critical",
            description="Working smoke detector on each level meeting NFPA 74, with hearing-impaired alarm where applicable.",
            fail_reason_hint="Smoke detector coverage/operation does not meet HUD/NFPA requirement.",
            remediation_guidance="Install/test smoke detectors on each required level and provide compliant alert type where required.",
            standard_label="Smoke detector coverage",
            room_scope="other_room",
            aliases=("SMOKE_DETECTORS", "SMOKE_DETECTOR_MISSING"),
        ),

        # ------------------------------------------------------------------
        # 5. All Secondary Rooms (Rooms not used for living)
        # ------------------------------------------------------------------
        _criterion(
            "secondary_rooms", "5.1", "SECONDARY_ROOMS_NONE",
            "None", "metadata",
            severity="info",
            description='Used when there are no secondary rooms; go to Part 6.',
            fail_reason_hint=None,
            remediation_guidance=None,
            requires_reinspection=False,
            common_fail=False,
            standard_label="No secondary rooms",
            room_scope="secondary_rooms",
            not_applicable_allowed=True,
        ),
        _criterion(
            "secondary_rooms", "5.2", "SECONDARY_ROOMS_SECURITY",
            "Security", "security",
            severity="fail",
            description="All windows and doors accessible from outside must be lockable.",
            fail_reason_hint="Secondary room accessible opening not lockable.",
            remediation_guidance="Repair/replace locking hardware or secure the opening.",
            standard_label="Secondary room security",
            room_scope="secondary_rooms",
        ),
        _criterion(
            "secondary_rooms", "5.3", "SECONDARY_ROOMS_ELECTRICAL_HAZARDS",
            "Electrical Hazards", "electrical",
            severity="critical",
            description="Secondary rooms must be free from electrical hazards.",
            fail_reason_hint="Electrical hazard present in secondary room.",
            remediation_guidance="Repair unsafe wiring, fixtures, devices, or exposed conductors.",
            standard_label="Secondary room electrical safety",
            room_scope="secondary_rooms",
        ),
        _criterion(
            "secondary_rooms", "5.4", "SECONDARY_ROOMS_OTHER_POTENTIALLY_HAZARDOUS_FEATURES",
            "Other Potentially Hazardous Features", "safety",
            severity="critical",
            description="Secondary rooms must be free from other potentially hazardous features.",
            fail_reason_hint="Other hazardous feature present in secondary room.",
            remediation_guidance="Document the hazard, control interior access, and repair/remove the dangerous condition.",
            standard_label="Secondary room other hazards",
            room_scope="secondary_rooms",
        ),

        # ------------------------------------------------------------------
        # 6. Building Exterior
        # ------------------------------------------------------------------
        _criterion(
            "building_exterior", "6.1", "BUILDING_EXTERIOR_CONDITION_OF_FOUNDATION",
            "Condition of Foundation", "structure",
            severity="critical",
            description="Foundation must be sound and free from hazards.",
            fail_reason_hint="Foundation is unsound, hazardous, or allows significant water entry.",
            remediation_guidance="Repair structural foundation defects, settlement issues, or major water intrusion problems.",
            standard_label="Foundation condition",
            aliases=("FOUNDATION_STRUCTURAL",),
        ),
        _criterion(
            "building_exterior", "6.2", "BUILDING_EXTERIOR_CONDITION_OF_STAIRS_RAILS_AND_PORCHES",
            "Condition of Stairs, Rails, and Porches", "safety",
            severity="critical",
            description="Exterior stairs, rails, and porches must be sound and free from hazards.",
            fail_reason_hint="Exterior stairs/rails/porches are unsafe or structurally unsound.",
            remediation_guidance="Repair/replace steps, handrails, guardrails, porches, balconies, or decks with unsafe conditions.",
            standard_label="Exterior stairs, rails, and porches",
            aliases=("HANDRAILS",),
        ),
        _criterion(
            "building_exterior", "6.3", "BUILDING_EXTERIOR_CONDITION_OF_ROOF_AND_GUTTERS",
            "Condition of Roof and Gutters", "exterior",
            severity="fail",
            description="Roof, gutters, and downspouts must be sound and free from hazards.",
            fail_reason_hint="Roof/gutter condition allows major water or air infiltration or presents structural hazard.",
            remediation_guidance="Repair roof structure, covering, flashing, soffits, drainage, and related weather-tight components.",
            standard_label="Roof and gutters",
            aliases=("LEAKS_ROOF", "ROOF_LEAKS"),
        ),
        _criterion(
            "building_exterior", "6.4", "BUILDING_EXTERIOR_CONDITION_OF_EXTERIOR_SURFACES",
            "Condition of Exterior Surfaces", "exterior",
            severity="fail",
            description="Exterior surfaces must be sound and free from hazards.",
            fail_reason_hint="Exterior surface condition is unsafe or severely deteriorated.",
            remediation_guidance="Repair hazardous siding, trim, cladding, openings, or exterior envelope failures.",
            standard_label="Exterior surfaces",
        ),
        _criterion(
            "building_exterior", "6.5", "BUILDING_EXTERIOR_CONDITION_OF_CHIMNEY",
            "Condition of Chimney", "exterior",
            severity="fail",
            description="Chimney must be sound and free from hazards.",
            fail_reason_hint="Chimney leaning, disintegrating, or structurally unsafe.",
            remediation_guidance="Repair/rebuild chimney masonry, flue, cap, or stabilization components.",
            standard_label="Chimney condition",
        ),
        _criterion(
            "building_exterior", "6.6", "BUILDING_EXTERIOR_LEAD_BASED_PAINT_EXTERIOR_SURFACES",
            "Lead-Based Paint: Exterior Surfaces", "lead",
            severity="warn",
            description="Exterior painted surfaces must be free of deteriorated paint beyond HUD threshold unless not applicable.",
            fail_reason_hint="Exterior deteriorated paint exceeds HUD threshold.",
            remediation_guidance="Stabilize deteriorated exterior paint, repair substrate, and complete required lead-safe workflow.",
            standard_label="Exterior lead-based paint",
            not_applicable_allowed=True,
        ),
        _criterion(
            "building_exterior", "6.7", "BUILDING_EXTERIOR_MANUFACTURED_HOMES_TIE_DOWNS",
            "Manufactured Homes: Tie Downs", "structure",
            severity="critical",
            description="Manufactured home must be properly placed and tied down unless not applicable.",
            fail_reason_hint="Manufactured home anchoring or placement is unsafe or noncompliant.",
            remediation_guidance="Install/repair approved tie-down and anchoring system and correct unstable placement conditions.",
            standard_label="Manufactured home tie-downs",
            not_applicable_allowed=True,
        ),

        # ------------------------------------------------------------------
        # 7. Heating and Plumbing
        # ------------------------------------------------------------------
        _criterion(
            "heating_and_plumbing", "7.1", "HEATING_AND_PLUMBING_ADEQUACY_OF_HEATING_EQUIPMENT",
            "Adequacy of Heating Equipment", "hvac",
            severity="critical",
            description="Heating equipment must provide adequate heat to all rooms used for living.",
            fail_reason_hint="Heating system cannot provide adequate heat to all living areas.",
            remediation_guidance="Repair/replace heating equipment or distribution so all required living areas receive adequate heat.",
            standard_label="Heating adequacy",
            aliases=("HEAT",),
        ),
        _criterion(
            "heating_and_plumbing", "7.2", "HEATING_AND_PLUMBING_SAFETY_OF_HEATING_EQUIPMENT",
            "Safety of Heating Equipment", "hvac",
            severity="critical",
            description="Unit must be free from unvented fuel-burning space heaters and other unsafe heating conditions.",
            fail_reason_hint="Unsafe heating condition present.",
            remediation_guidance="Remove unvented heaters and repair unsafe flues, venting, combustion, clearances, or damaged equipment.",
            standard_label="Heating safety",
        ),
        _criterion(
            "heating_and_plumbing", "7.3", "HEATING_AND_PLUMBING_VENTILATION_AND_ADEQUACY_OF_COOLING",
            "Ventilation and Adequacy of Cooling", "hvac",
            severity="fail",
            description="Unit must have adequate ventilation and cooling via openable windows or a working cooling system.",
            fail_reason_hint="Ventilation/cooling inadequate for healthy occupancy.",
            remediation_guidance="Restore operable windows or repair/install compliant cooling/ventilation system.",
            standard_label="Ventilation and cooling",
        ),
        _criterion(
            "heating_and_plumbing", "7.4", "HEATING_AND_PLUMBING_WATER_HEATER",
            "Water Heater", "plumbing",
            severity="critical",
            description="Water heater must be located, equipped, and installed safely.",
            fail_reason_hint="Water heater located/equipped/installed unsafely.",
            remediation_guidance="Correct water heater venting, relief valve, piping, clearances, seismic/placement, or combustion safety issues.",
            standard_label="Water heater safety",
            aliases=("HOT_WATER",),
        ),
        _criterion(
            "heating_and_plumbing", "7.5", "HEATING_AND_PLUMBING_WATER_SUPPLY",
            "Water Supply", "plumbing",
            severity="critical",
            description="Unit must be served by an approvable public or private sanitary water supply.",
            fail_reason_hint="Water supply not approvable or not sanitary.",
            remediation_guidance="Repair/restore safe sanitary public or private water service and resolve contamination concerns.",
            standard_label="Water supply",
        ),
        _criterion(
            "heating_and_plumbing", "7.6", "HEATING_AND_PLUMBING_PLUMBING",
            "Plumbing", "plumbing",
            severity="fail",
            description="Plumbing must be free from major leaks or serious persistent corrosion/contamination.",
            fail_reason_hint="Major plumbing leak or serious corrosion/contamination present.",
            remediation_guidance="Repair leaks, replace corroded pipes, restore safe potable water quality, and correct drainage faults.",
            standard_label="Plumbing condition",
            aliases=("PLUMBING_LEAKS",),
        ),
        _criterion(
            "heating_and_plumbing", "7.7", "HEATING_AND_PLUMBING_SEWER_CONNECTION",
            "Sewer Connection", "plumbing",
            severity="critical",
            description="Plumbing must connect to an approvable disposal system and be free from sewer backup.",
            fail_reason_hint="Sewer connection/disposal system not approvable or evidence of sewer backup present.",
            remediation_guidance="Repair sewer connection, septic/disposal system, traps, or drainage backup condition.",
            standard_label="Sewer connection",
        ),

        # ------------------------------------------------------------------
        # 8. General Health and Safety
        # ------------------------------------------------------------------
        _criterion(
            "general_health_and_safety", "8.1", "GENERAL_HEALTH_AND_SAFETY_ACCESS_TO_UNIT",
            "Access to Unit", "safety",
            severity="fail",
            description="Unit must be enterable without going through another unit.",
            fail_reason_hint="Unit access requires passage through another dwelling unit.",
            remediation_guidance="Provide compliant direct access to the unit.",
            standard_label="Access to unit",
        ),
        _criterion(
            "general_health_and_safety", "8.2", "GENERAL_HEALTH_AND_SAFETY_EXITS",
            "Exits", "egress",
            severity="critical",
            description="Acceptable unblocked fire exit must be available.",
            fail_reason_hint="Required fire exit missing, blocked, or not acceptable.",
            remediation_guidance="Provide/clear compliant emergency egress and restore usable exit path.",
            standard_label="Fire exit / egress",
            aliases=("EGRESS", "EGRESS_BLOCKED"),
        ),
        _criterion(
            "general_health_and_safety", "8.3", "GENERAL_HEALTH_AND_SAFETY_EVIDENCE_OF_INFESTATION",
            "Evidence of Infestation", "sanitation",
            severity="fail",
            description="Unit must be free from rats or severe infestation by mice or vermin.",
            fail_reason_hint="Evidence of serious infestation present.",
            remediation_guidance="Treat infestation, clean contaminated areas, and seal entry points.",
            standard_label="Infestation",
            aliases=("PESTS",),
        ),
        _criterion(
            "general_health_and_safety", "8.4", "GENERAL_HEALTH_AND_SAFETY_GARBAGE_AND_DEBRIS",
            "Garbage and Debris", "sanitation",
            severity="fail",
            description="Unit must be free from heavy accumulation of garbage or debris inside and outside.",
            fail_reason_hint="Heavy garbage/debris accumulation presents health/safety hazard.",
            remediation_guidance="Remove debris/garbage and restore sanitary condition.",
            standard_label="Garbage and debris",
        ),
        _criterion(
            "general_health_and_safety", "8.5", "GENERAL_HEALTH_AND_SAFETY_REFUSE_DISPOSAL",
            "Refuse Disposal", "sanitation",
            severity="fail",
            description="Adequate covered temporary storage/disposal facilities for food wastes must be available and approvable.",
            fail_reason_hint="Refuse disposal facilities inadequate or not approvable.",
            remediation_guidance="Provide covered approved refuse storage/disposal facilities.",
            standard_label="Refuse disposal",
        ),
        _criterion(
            "general_health_and_safety", "8.6", "GENERAL_HEALTH_AND_SAFETY_INTERIOR_STAIRS_AND_COMMON_HALLS",
            "Interior Stairs and Common Halls", "safety",
            severity="critical",
            description="Interior stairs and common halls must be free from hazards such as broken steps, missing/insecure rails, inadequate lighting, or other hazards.",
            fail_reason_hint="Interior stairs/common halls contain serious safety hazard.",
            remediation_guidance="Repair stairs, handrails, railings, lighting, wiring, or trip hazards in common/interior circulation areas.",
            standard_label="Interior stairs and common halls",
            aliases=("HANDRAILS", "TRIP_HAZARDS"),
        ),
        _criterion(
            "general_health_and_safety", "8.7", "GENERAL_HEALTH_AND_SAFETY_OTHER_INTERIOR_HAZARDS",
            "Other Interior Hazards", "safety",
            severity="critical",
            description="Interior of unit must be free from other hazards not identified previously.",
            fail_reason_hint="Other interior hazard present.",
            remediation_guidance="Remove or repair unaddressed interior hazard and document corrective action.",
            standard_label="Other interior hazards",
        ),
        _criterion(
            "general_health_and_safety", "8.8", "GENERAL_HEALTH_AND_SAFETY_ELEVATORS",
            "Elevators", "safety",
            severity="critical",
            description="Where required, elevators must have current inspection certificate; otherwise they must be working and safe.",
            fail_reason_hint="Elevator certificate missing where required or elevator unsafe.",
            remediation_guidance="Obtain current elevator inspection/certification and repair unsafe elevator conditions.",
            standard_label="Elevator safety",
            not_applicable_allowed=True,
        ),
        _criterion(
            "general_health_and_safety", "8.9", "GENERAL_HEALTH_AND_SAFETY_INTERIOR_AIR_QUALITY",
            "Interior Air Quality", "health",
            severity="fail",
            description="Unit must be free from abnormally high levels of air pollution from exhaust, sewer gas, fuel gas, dust, or other pollutants.",
            fail_reason_hint="Interior air quality hazard present.",
            remediation_guidance="Eliminate pollutant source and repair ventilation, combustion, sewer gas, or contamination issue.",
            standard_label="Interior air quality",
        ),
        _criterion(
            "general_health_and_safety", "8.10", "GENERAL_HEALTH_AND_SAFETY_SITE_AND_NEIGHBORHOOD_CONDITIONS",
            "Site and Neighborhood Conditions", "health",
            severity="critical",
            description="Site and immediate neighborhood must be free from conditions that seriously and continuously endanger resident health or safety.",
            fail_reason_hint="Serious and continuous external site/neighborhood danger present.",
            remediation_guidance="Document and mitigate dangerous site condition or determine unit is unsuitable until resolved.",
            standard_label="Site and neighborhood conditions",
        ),
        _criterion(
            "general_health_and_safety", "8.11", "GENERAL_HEALTH_AND_SAFETY_LEAD_BASED_PAINT_OWNER_CERTIFICATION",
            "Lead-Based Paint: Owner Certification", "lead",
            severity="warn",
            description="Owner certification is required when lead-based paint hazards were corrected under applicable HUD requirements.",
            fail_reason_hint="Required lead-based paint owner certification not received.",
            remediation_guidance="Obtain completed Lead-Based Paint Owner Certification after hazard correction in compliance with 24 CFR Part 35.",
            standard_label="Lead-based paint owner certification",
            not_applicable_allowed=True,
        ),
    ]


def get_hud_52580a_criteria_map() -> dict[str, InspectionCriterion]:
    items = get_hud_52580a_criteria()
    out: dict[str, InspectionCriterion] = {}
    for item in items:
        out[item.code] = item
        for alias in item.aliases:
            if alias and alias not in out:
                out[alias] = item
    return out


def lookup_hud_criterion(code: str | None) -> InspectionCriterion | None:
    if not code:
        return None
    return get_hud_52580a_criteria_map().get(normalize_rule_code(code))


def criteria_for_section(section: str) -> list[InspectionCriterion]:
    key = str(section or "").strip().lower()
    return [c for c in get_hud_52580a_criteria() if c.section == key]


def criteria_as_dicts() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for c in get_hud_52580a_criteria():
        rows.append(
            {
                "section": c.section,
                "item_number": c.item_number,
                "code": c.code,
                "label": c.label,
                "description": c.description,
                "category": c.category,
                "severity": c.severity,
                "fail_reason_hint": c.fail_reason_hint,
                "suggested_fix": c.remediation_guidance,
                "common_fail": c.common_fail,
                "template_key": c.template_key,
                "template_version": c.template_version,
                "standard_label": c.standard_label,
                "standard_citation": c.standard_citation,
                "room_scope": c.room_scope,
                "not_applicable_allowed": c.not_applicable_allowed,
                "aliases": list(c.aliases),
            }
        )
    return rows
