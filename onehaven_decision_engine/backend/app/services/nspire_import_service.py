
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable

from sqlalchemy import MetaData, Table, and_, select, update
from sqlalchemy.orm import Session


SEVERITY_LT = "life_threatening"
SEVERITY_S = "severe"
SEVERITY_M = "moderate"
SEVERITY_L = "low"

DEFAULT_TEMPLATE_KEY = "nspire_hcv"
DEFAULT_TEMPLATE_VERSION = "nspire_hcv_2026"


@dataclass(frozen=True)
class NspireRuleInput:
    standard_code: str
    standard_label: str
    deficiency_description: str
    severity_code: str
    correction_days: int | None
    pass_fail: str
    inspectable_area: str | None = None
    location_scope: str | None = None
    citation: str | None = None
    source_url: str | None = None
    effective_date: date | None = None
    is_hcv_applicable: bool = True


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _norm_upper(value: Any) -> str | None:
    text = _norm_text(value)
    return text.upper() if text else None


def _norm_lower(value: Any) -> str | None:
    text = _norm_text(value)
    return text.lower() if text else None


def _nspire_table(db: Session) -> Table:
    metadata = MetaData()
    return Table("nspire_rule_catalog", metadata, autoload_with=db.bind)


def _severity_from_code(value: str | None) -> str:
    code = (_norm_upper(value) or "").strip()
    if code == "LT":
        return SEVERITY_LT
    if code == "S":
        return SEVERITY_S
    if code == "M":
        return SEVERITY_M
    if code == "L":
        return SEVERITY_L
    return SEVERITY_M


def _default_correction_days(severity: str) -> int | None:
    if severity == SEVERITY_LT:
        return 1
    if severity in {SEVERITY_S, SEVERITY_M}:
        return 30
    return None


def _stable_rule_key(
    *,
    standard_code: str,
    standard_label: str,
    deficiency_description: str,
    severity_code: str,
) -> str:
    parts = [
        _norm_upper(standard_code) or "UNKNOWN",
        (_norm_text(standard_label) or "standard").lower().replace(" ", "_"),
        (_norm_text(deficiency_description) or "deficiency").lower().replace(" ", "_"),
        _norm_upper(severity_code) or "M",
    ]
    raw = "__".join(parts)
    for ch in ["/", ",", ".", "(", ")", "'", '"', ":", ";", "*"]:
        raw = raw.replace(ch, "")
    raw = raw.replace("-", "_")
    while "__" in raw:
        raw = raw.replace("__", "_")
    return raw[:240]


def _nspire_truth_metadata(item: dict[str, Any]) -> dict[str, Any]:
    source_url = _norm_text(item.get("source_url"))
    is_pdf = bool(source_url and source_url.lower().endswith(".pdf"))
    return {
        "evidence_role": "support_only",
        "truth_role": "evidence_only",
        "truth_eligible": False,
        "projectable_truth": False,
        "requires_validation": True,
        "requires_binding_authority": True,
        "source_authority_score": 0.80 if source_url else 0.65,
        "publication_type": "pdf" if is_pdf else "official_document",
        "domain_role": "inspection_evidence",
        "operational_value": "high",
        "not_primary_truth_for_unrelated_legal_requirements": True,
    }


def import_nspire_rules(
    db: Session,
    *,
    rules: Iterable[NspireRuleInput | dict[str, Any]],
    template_key: str = DEFAULT_TEMPLATE_KEY,
    template_version: str = DEFAULT_TEMPLATE_VERSION,
    source_name: str = "HUD NSPIRE HCV Checklist",
) -> dict[str, Any]:
    table = _nspire_table(db)

    inserted = 0
    updated = 0
    seen_keys: list[str] = []
    truth_rows: list[dict[str, Any]] = []

    for raw in rules:
        item = raw if isinstance(raw, dict) else raw.__dict__

        standard_code = _norm_upper(item.get("standard_code")) or "UNKNOWN"
        standard_label = _norm_text(item.get("standard_label")) or standard_code
        deficiency_description = _norm_text(item.get("deficiency_description")) or "Unknown deficiency"
        severity_code = _norm_upper(item.get("severity_code")) or "M"
        severity = _severity_from_code(severity_code)
        rule_key = _stable_rule_key(
            standard_code=standard_code,
            standard_label=standard_label,
            deficiency_description=deficiency_description,
            severity_code=severity_code,
        )
        correction_days = item.get("correction_days")
        correction_days = (
            int(correction_days)
            if correction_days is not None
            else _default_correction_days(severity)
        )
        pass_fail = _norm_lower(item.get("pass_fail")) or ("pass" if severity == SEVERITY_L else "fail")
        truth_meta = _nspire_truth_metadata(item)

        payload = {
            "rule_key": rule_key,
            "template_key": _norm_lower(template_key) or DEFAULT_TEMPLATE_KEY,
            "template_version": _norm_text(template_version) or DEFAULT_TEMPLATE_VERSION,
            "source_name": _norm_text(source_name) or "HUD NSPIRE HCV Checklist",
            "standard_code": standard_code,
            "standard_label": standard_label,
            "deficiency_description": deficiency_description,
            "severity_code": severity_code,
            "severity_label": severity,
            "correction_days": correction_days,
            "pass_fail": pass_fail,
            "inspectable_area": _norm_text(item.get("inspectable_area")),
            "location_scope": _norm_text(item.get("location_scope")),
            "citation": _norm_text(item.get("citation")),
            "source_url": _norm_text(item.get("source_url")),
            "effective_date": item.get("effective_date") or date(2026, 1, 1),
            "is_hcv_applicable": bool(item.get("is_hcv_applicable", True)),
            "is_active": True,
            "updated_at": datetime.utcnow(),
        }

        existing = db.execute(
            select(table)
            .where(
                and_(
                    table.c.rule_key == rule_key,
                    table.c.template_key == payload["template_key"],
                    table.c.template_version == payload["template_version"],
                )
            )
            .limit(1)
        ).first()

        if existing is None:
            payload["created_at"] = datetime.utcnow()
            db.execute(table.insert().values(**payload))
            inserted += 1
        else:
            db.execute(
                update(table)
                .where(table.c.id == existing._mapping["id"])
                .values(**payload)
            )
            updated += 1

        seen_keys.append(rule_key)
        truth_rows.append(
            {
                "rule_key": rule_key,
                "standard_code": standard_code,
                "severity_label": severity,
                **truth_meta,
            }
        )

    db.flush()
    return {
        "ok": True,
        "inserted": inserted,
        "updated": updated,
        "total_processed": inserted + updated,
        "rule_keys": seen_keys,
        "truth_rows": truth_rows,
        "truth_model": "inspection_evidence_support_only",
        "projectable_truth_from_nspire_alone": False,
    }
