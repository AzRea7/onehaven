from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable



from .inspection_rules import get_hud_52580a_criteria, normalize_rule_code, normalize_severity


@dataclass(frozen=True)
class ChecklistTemplateItem:
    code: str
    description: str
    category: str
    default_status: str = "todo"
    severity: str = "fail"
    common_fail: bool = True
    inspection_rule_code: str | None = None
    suggested_fix: str | None = None
    template_key: str = "hud_52580a"
    template_version: str = "hud_52580a_2019"
    section: str | None = None
    item_number: str | None = None
    room_scope: str | None = None
    not_applicable_allowed: bool = False
    # Step 5 additive NSPIRE metadata
    nspire_standard_key: str | None = None
    nspire_standard_code: str | None = None
    nspire_standard_label: str | None = None
    nspire_deficiency_description: str | None = None
    nspire_designation: str | None = None
    correction_days: int | None = None
    affirmative_habitability_requirement: bool = False
    source_name: str | None = None
    source_type: str | None = None
    source_pdf_name: str | None = None
    source_pdf_path: str | None = None
    source_citation: str | None = None


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "y", "required"}:
        return True
    if raw in {"0", "false", "no", "n"}:
        return False
    return default


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def base_hqs_template() -> list[ChecklistTemplateItem]:
    out: list[ChecklistTemplateItem] = []
    for c in get_hud_52580a_criteria():
        out.append(
            ChecklistTemplateItem(
                code=c.code,
                description=c.label,
                category=c.category,
                default_status="todo",
                severity=c.severity,
                common_fail=c.common_fail,
                inspection_rule_code=c.code,
                suggested_fix=c.remediation_guidance,
                template_key=c.template_key,
                template_version=c.template_version,
                section=c.section,
                item_number=c.item_number,
                room_scope=c.room_scope,
                not_applicable_allowed=c.not_applicable_allowed,
                nspire_standard_key=getattr(c, "nspire_standard_key", None),
                nspire_standard_code=getattr(c, "nspire_standard_code", None),
                nspire_standard_label=getattr(c, "nspire_standard_label", None),
                nspire_deficiency_description=getattr(c, "nspire_deficiency_description", None),
                nspire_designation=getattr(c, "nspire_designation", None),
                correction_days=_coerce_int(getattr(c, "correction_days", None)),
                affirmative_habitability_requirement=_coerce_bool(
                    getattr(c, "affirmative_habitability_requirement", False)
                ),
                source_name=getattr(c, "source_name", None),
                source_type=getattr(c, "source_type", None),
                source_pdf_name=getattr(c, "source_pdf_name", None),
                source_pdf_path=getattr(c, "source_pdf_path", None),
                source_citation=getattr(c, "source_citation", None),
            )
        )
    return ordered_template_items(out)


def template_items_from_effective_rules(effective_items: Iterable[dict[str, Any]]) -> list[ChecklistTemplateItem]:
    out: list[ChecklistTemplateItem] = []
    for raw in effective_items or []:
        code = normalize_rule_code(raw.get("code") or raw.get("item_code") or raw.get("inspection_rule_code"))
        if not code:
            continue
        out.append(
            ChecklistTemplateItem(
                code=code,
                description=str(raw.get("description") or raw.get("label") or code.replace("_", " ").title()).strip(),
                category=str(raw.get("category") or "other").strip().lower() or "other",
                default_status=str(raw.get("default_status") or "todo"),
                severity=normalize_severity(raw.get("severity") or "fail"),
                common_fail=bool(raw.get("common_fail", True)),
                inspection_rule_code=normalize_rule_code(raw.get("inspection_rule_code") or code),
                suggested_fix=raw.get("suggested_fix"),
                template_key=str(raw.get("template_key") or "hud_52580a"),
                template_version=str(raw.get("template_version") or "hud_52580a_2019"),
                section=raw.get("section"),
                item_number=raw.get("item_number"),
                room_scope=raw.get("room_scope"),
                not_applicable_allowed=bool(raw.get("not_applicable_allowed", False)),
                nspire_standard_key=raw.get("nspire_standard_key"),
                nspire_standard_code=raw.get("nspire_standard_code"),
                nspire_standard_label=raw.get("nspire_standard_label"),
                nspire_deficiency_description=raw.get("nspire_deficiency_description"),
                nspire_designation=raw.get("nspire_designation"),
                correction_days=_coerce_int(raw.get("correction_days")),
                affirmative_habitability_requirement=_coerce_bool(
                    raw.get("affirmative_habitability_requirement", False)
                ),
                source_name=raw.get("source_name")
                or (raw.get("source") or {}).get("name") if isinstance(raw.get("source"), dict) else raw.get("source_name"),
                source_type=(raw.get("source") or {}).get("type") if isinstance(raw.get("source"), dict) else raw.get("source_type"),
                source_pdf_name=raw.get("source_pdf_name"),
                source_pdf_path=raw.get("source_pdf_path"),
                source_citation=raw.get("source_citation") or raw.get("standard_citation"),
            )
        )
    return ordered_template_items(out)


def template_lookup(items: Iterable[ChecklistTemplateItem]) -> dict[str, ChecklistTemplateItem]:
    out: dict[str, ChecklistTemplateItem] = {}
    for item in items or []:
        out[item.code] = item
        if item.inspection_rule_code:
            out[item.inspection_rule_code] = item
    return out


def normalize_fail_point(text: str) -> str:
    return " ".join((text or "").strip().split())


def fail_points_to_items(fail_points: Iterable[str]) -> list[ChecklistTemplateItem]:
    out: list[ChecklistTemplateItem] = []
    for fp in fail_points or []:
        t = normalize_fail_point(fp)
        if not t:
            continue
        code = "FP_" + normalize_rule_code(t)[:32]
        out.append(
            ChecklistTemplateItem(
                code=code,
                description=t,
                category="safety",
                severity=normalize_severity("fail"),
                common_fail=True,
                inspection_rule_code=normalize_rule_code(t),
            )
        )
    return ordered_template_items(out)


def ordered_template_items(items: Iterable[ChecklistTemplateItem]) -> list[ChecklistTemplateItem]:
    deduped: dict[str, ChecklistTemplateItem] = {}
    for item in items or []:
        if not item.code:
            continue
        deduped[item.code] = item

    return sorted(
        deduped.values(),
        key=lambda item: (
            str(item.template_key or "hud_52580a"),
            str(item.template_version or "hud_52580a_2019"),
            str(item.section or ""),
            str(item.item_number or ""),
            str(item.room_scope or ""),
            str(item.category or ""),
            str(item.code or ""),
        ),
    )


def template_items_as_dicts(items: Iterable[ChecklistTemplateItem]) -> list[dict[str, Any]]:
    return [asdict(item) for item in ordered_template_items(items)]


def build_property_scoped_checklist_items(
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None,
    jurisdiction: str | None,
    template_items: Iterable[ChecklistTemplateItem],
    inspector_name: str | None = None,
    inspection_date: str | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sort_index, item in enumerate(ordered_template_items(template_items), start=1):
        rows.append(
            {
                "org_id": org_id,
                "property_id": property_id,
                "inspection_id": inspection_id,
                "inspection_date": inspection_date,
                "inspector_name": inspector_name,
                "jurisdiction": (jurisdiction or "").strip() or None,
                "template_key": item.template_key,
                "template_version": item.template_version,
                "sort_order": sort_index,
                "item_code": item.code,
                "inspection_rule_code": item.inspection_rule_code or item.code,
                "description": item.description,
                "category": item.category,
                "section": item.section,
                "item_number": item.item_number,
                "room_scope": item.room_scope,
                "severity": item.severity,
                "default_status": item.default_status,
                "result_status": item.default_status,
                "common_fail": item.common_fail,
                "not_applicable_allowed": item.not_applicable_allowed,
                "suggested_fix": item.suggested_fix,
                "notes": None,
                "evidence_json": "[]",
                "photo_references_json": "[]",
                "is_resolved": False,
                "requires_reinspection": False,
                # Step 5 additive NSPIRE fields
                "nspire_standard_key": item.nspire_standard_key,
                "nspire_standard_code": item.nspire_standard_code,
                "nspire_standard_label": item.nspire_standard_label,
                "nspire_deficiency_description": item.nspire_deficiency_description,
                "nspire_designation": item.nspire_designation,
                "correction_days": item.correction_days,
                "affirmative_habitability_requirement": item.affirmative_habitability_requirement,
                "source_name": item.source_name,
                "source_type": item.source_type,
                "source_pdf_name": item.source_pdf_name,
                "source_pdf_path": item.source_pdf_path,
                "source_citation": item.source_citation,
            }
        )
    return rows
