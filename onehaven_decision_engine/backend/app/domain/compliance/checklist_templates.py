from __future__ import annotations

from dataclasses import dataclass
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
            )
        )
    return out


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
            )
        )
    return out


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
    return out
