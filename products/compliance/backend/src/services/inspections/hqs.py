from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from sqlalchemy.orm import Session

from onehaven_platform.backend.src.models import Property

from .hqs_library import (
    build_property_inspection_packet as _build_property_inspection_packet,
    explain_hqs_rule as _explain_hqs_rule,
    get_effective_hqs_items as _get_effective_hqs_items,
    hqs_items_lookup as _hqs_items_lookup,
)


@dataclass(frozen=True)
class HqsSummary:
    total: int = 0
    todo: int = 0
    passed: int = 0
    failed: int = 0
    blocked: int = 0
    na: int = 0
    critical: int = 0
    life_threatening: int = 0
    severe: int = 0
    moderate: int = 0
    low: int = 0


def _norm_status(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"pass", "passed", "ok", "complete"}:
        return "pass"
    if raw in {"fail", "failed"}:
        return "fail"
    if raw in {"blocked", "life_threatening", "lt"}:
        return "blocked"
    if raw in {"na", "n/a", "not_applicable"}:
        return "na"
    if raw in {"todo", "pending", "unknown", ""}:
        return "todo"
    return raw


def _norm_designation(value: Any) -> str | None:
    raw = str(value or "").strip().upper()
    return raw or None


def summarize_items(items: Iterable[dict[str, Any]]) -> HqsSummary:
    total = todo = passed = failed = blocked = na = critical = 0
    lt = severe = moderate = low = 0

    for row in items or []:
        result_status = _norm_status(
            row.get("result_status")
            or row.get("status")
            or row.get("default_status")
        )
        severity = str(row.get("severity") or "").strip().lower()
        designation = _norm_designation(row.get("nspire_designation"))

        total += 1
        if result_status == "pass":
            passed += 1
        elif result_status == "fail":
            failed += 1
        elif result_status == "blocked":
            blocked += 1
        elif result_status == "na":
            na += 1
        else:
            todo += 1

        if severity == "critical" or designation == "LT":
            critical += 1
        if designation == "LT":
            lt += 1
        elif designation == "S":
            severe += 1
        elif designation == "M":
            moderate += 1
        elif designation == "L":
            low += 1

    return HqsSummary(
        total=total,
        todo=todo,
        passed=passed,
        failed=failed,
        blocked=blocked,
        na=na,
        critical=critical,
        life_threatening=lt,
        severe=severe,
        moderate=moderate,
        low=low,
    )


def top_fix_candidates(items: Iterable[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in items or []:
        result_status = _norm_status(row.get("result_status") or row.get("status") or row.get("default_status"))
        if result_status == "pass" or result_status == "na":
            continue
        designation = _norm_designation(row.get("nspire_designation"))
        severity = str(row.get("severity") or "").strip().lower()
        rank = 0
        if designation == "LT":
            rank = 400
        elif designation == "S":
            rank = 300
        elif designation == "M":
            rank = 200
        elif designation == "L":
            rank = 100
        elif severity == "critical":
            rank = 250
        elif severity == "fail":
            rank = 150
        elif severity == "warn":
            rank = 50

        rows.append(
            {
                "code": row.get("inspection_rule_code") or row.get("item_code") or row.get("code"),
                "description": row.get("description"),
                "category": row.get("category"),
                "severity": severity,
                "nspire_designation": designation,
                "correction_days": row.get("correction_days"),
                "affirmative_habitability_requirement": bool(row.get("affirmative_habitability_requirement")),
                "suggested_fix": row.get("suggested_fix"),
                "fail_reason_hint": row.get("fail_reason_hint"),
                "source_pdf_name": row.get("source_pdf_name"),
                "source_pdf_path": row.get("source_pdf_path"),
                "source_citation": row.get("source_citation") or row.get("standard_citation"),
                "score": rank,
            }
        )

    rows.sort(
        key=lambda row: (
            -int(row.get("score") or 0),
            999999 if row.get("correction_days") is None else int(row.get("correction_days") or 0),
            str(row.get("code") or ""),
        )
    )
    return rows[: max(1, int(limit or 10))]


def get_effective_hqs_items(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    profile_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _get_effective_hqs_items(db, org_id=org_id, prop=prop, profile_summary=profile_summary)


def build_property_inspection_packet(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    property_id: int | None = None,
    inspection_id: int | None = None,
    profile_summary: dict[str, Any] | None = None,
    jurisdiction: str | None = None,
    inspector_name: str | None = None,
    inspection_date: str | None = None,
) -> dict[str, Any]:
    return _build_property_inspection_packet(
        db,
        org_id=org_id,
        prop=prop,
        property_id=property_id,
        inspection_id=inspection_id,
        profile_summary=profile_summary,
        jurisdiction=jurisdiction,
        inspector_name=inspector_name,
        inspection_date=inspection_date,
    )


def hqs_items_lookup(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    profile_summary: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    return _hqs_items_lookup(db, org_id=org_id, prop=prop, profile_summary=profile_summary)


def explain_hqs_rule(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    code: str,
    profile_summary: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    return _explain_hqs_rule(db, org_id=org_id, prop=prop, code=code, profile_summary=profile_summary)
