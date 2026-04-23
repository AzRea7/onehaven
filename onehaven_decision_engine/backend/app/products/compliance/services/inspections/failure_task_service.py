from __future__ import annotations

import json
import os
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.domain.compliance.inspection_mapping import map_inspection_code
from app.domain.compliance.top_fail_points import top_fail_points
from app.models import Inspection, InspectionItem, Property, RehabTask


def _now() -> datetime:
    return datetime.utcnow()


def _j(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False, default=str)


def _severity_rank(value: Any) -> int:
    if value is None:
        return 3
    if isinstance(value, int):
        return int(value)

    s = str(value).strip().lower()
    if s == "critical":
        return 4
    if s == "fail":
        return 3
    if s == "warn":
        return 2
    return 1


def _priority_from_item(item: InspectionItem) -> str:
    sev = _severity_rank(getattr(item, "severity", None))
    status = str(getattr(item, "result_status", "") or "").strip().lower()

    if status in {"blocked", "inconclusive"}:
        return "high"
    if sev >= 4:
        return "high"
    if sev == 3:
        return "med"
    return "low"


def _task_category_from_item(item: InspectionItem, default: str = "compliance_repair") -> str:
    code = str(getattr(item, "code", "") or "").strip().upper()
    mapped = map_inspection_code(code)
    if mapped and mapped.rehab_category:
        return str(mapped.rehab_category).strip()
    category = str(getattr(item, "category", "") or "").strip().lower()
    if category:
        return category
    return default


def _task_title_from_item(item: InspectionItem) -> str:
    code = str(getattr(item, "code", "") or "").strip().upper()
    mapped = map_inspection_code(code)
    if mapped and mapped.rehab_title:
        return str(mapped.rehab_title).strip()

    label = (
        getattr(item, "standard_label", None)
        or getattr(item, "fail_reason", None)
        or getattr(item, "code", None)
        or "Inspection remediation"
    )
    label_text = str(label).strip()
    if not label_text:
        label_text = "Inspection remediation"

    if label_text.lower().startswith("repair") or label_text.lower().startswith("install"):
        return label_text
    return f"Fix: {label_text}"


def _task_notes_from_item(
    *,
    item: InspectionItem,
    inspection: Inspection,
    property_obj: Property | None,
) -> str:
    code = str(getattr(item, "code", "") or "").strip().upper()
    mapped = map_inspection_code(code)
    fail_reason = str(getattr(item, "fail_reason", "") or "").strip()
    remediation = str(getattr(item, "remediation_guidance", "") or "").strip()
    details = str(getattr(item, "details", "") or "").strip()
    location = str(getattr(item, "location", "") or "").strip()
    result_status = str(getattr(item, "result_status", "") or "").strip().lower()
    severity = getattr(item, "severity", None)
    inspection_date = getattr(inspection, "inspection_date", None)
    inspector = getattr(inspection, "inspector", None)
    jurisdiction = getattr(inspection, "jurisdiction", None)
    template_key = getattr(inspection, "template_key", None)
    template_version = getattr(inspection, "template_version", None)

    lines = [
        "Auto-generated from inspection failure.",
        f"Inspection ID: {getattr(inspection, 'id', None)}",
        f"Inspection date: {inspection_date}",
        f"Inspector: {inspector}",
        f"Jurisdiction: {jurisdiction}",
        f"Template: {template_key}:{template_version}",
        f"Inspection code: {code or 'UNKNOWN'}",
        f"Result status: {result_status or 'unknown'}",
        f"Severity: {severity}",
    ]

    if property_obj is not None:
        lines.append(
            "Property: "
            f"{getattr(property_obj, 'address', '')}, "
            f"{getattr(property_obj, 'city', '')}, "
            f"{getattr(property_obj, 'state', '')} "
            f"{getattr(property_obj, 'zip', '')}"
        )

    if location:
        lines.append(f"Location: {location}")
    if fail_reason:
        lines.append(f"Fail reason: {fail_reason}")
    if details:
        lines.append(f"Inspector details: {details}")
    if remediation:
        lines.append(f"Remediation guidance: {remediation}")
    elif mapped and mapped.default_fail_reason:
        lines.append(f"Default issue: {mapped.default_fail_reason}")
    if mapped and mapped.rehab_category:
        lines.append(f"Mapped rehab category: {mapped.rehab_category}")

    standard_label = str(getattr(item, "standard_label", "") or "").strip()
    standard_citation = str(getattr(item, "standard_citation", "") or "").strip()
    if standard_label:
        lines.append(f"Standard: {standard_label}")
    if standard_citation:
        lines.append(f"Citation: {standard_citation}")

    return "".join(lines).strip()


def _task_exists(db: Session, *, org_id: int, property_id: int, title: str) -> bool:
    existing = db.scalar(
        select(RehabTask).where(
            RehabTask.org_id == org_id,
            RehabTask.property_id == property_id,
            RehabTask.title == title,
        )
    )
    return existing is not None


@dataclass(frozen=True)
class FailureTaskBlueprint:
    inspection_item_id: int
    code: str
    title: str
    category: str
    priority: str
    notes: str
    inspection_relevant: bool = True
    requires_reinspection: bool = True
    rehab_category: str | None = None
    result_status: str | None = None
    severity: int | None = None


def _blueprint_from_item(
    *,
    item: InspectionItem,
    inspection: Inspection,
    property_obj: Property | None,
) -> FailureTaskBlueprint | None:
    result_status = str(getattr(item, "result_status", "") or "").strip().lower()
    failed = bool(getattr(item, "failed", False))

    if result_status not in {"fail", "blocked", "inconclusive"} and not failed:
        return None

    code = str(getattr(item, "code", "") or "").strip().upper()
    mapped = map_inspection_code(code)

    return FailureTaskBlueprint(
        inspection_item_id=int(getattr(item, "id")),
        code=code,
        title=_task_title_from_item(item),
        category=_task_category_from_item(item),
        priority=_priority_from_item(item),
        notes=_task_notes_from_item(item=item, inspection=inspection, property_obj=property_obj),
        inspection_relevant=bool(getattr(item, "requires_reinspection", True)),
        requires_reinspection=bool(getattr(item, "requires_reinspection", True)),
        rehab_category=(mapped.rehab_category if mapped and mapped.rehab_category else None),
        result_status=result_status,
        severity=int(getattr(item, "severity", 0) or 0),
    )


def _latest_inspection(db: Session, *, org_id: int, property_id: int) -> Inspection | None:
    return db.scalar(
        select(Inspection)
        .where(Inspection.org_id == org_id, Inspection.property_id == property_id)
        .order_by(
            desc(Inspection.inspection_date),
            desc(Inspection.created_at),
            desc(Inspection.id),
        )
        .limit(1)
    )


def _get_property(db: Session, *, org_id: int, property_id: int) -> Property | None:
    return db.scalar(
        select(Property).where(
            Property.org_id == org_id,
            Property.id == property_id,
        )
    )


def _inspection_items(
    db: Session,
    *,
    inspection_id: int,
) -> list[InspectionItem]:
    return list(
        db.scalars(
            select(InspectionItem)
            .where(InspectionItem.inspection_id == inspection_id)
            .order_by(InspectionItem.id.asc())
        ).all()
    )


def collect_failure_task_blueprints(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
) -> dict[str, Any]:
    inspection = None
    if inspection_id is not None:
        inspection = db.scalar(
            select(Inspection).where(
                Inspection.org_id == org_id,
                Inspection.property_id == property_id,
                Inspection.id == inspection_id,
            )
        )
    if inspection is None:
        inspection = _latest_inspection(db, org_id=org_id, property_id=property_id)
    if inspection is None:
        return {
            "ok": False,
            "property_id": int(property_id),
            "inspection_id": None,
            "items": [],
            "code": "no_inspection",
        }

    property_obj = _get_property(db, org_id=org_id, property_id=property_id)
    items = _inspection_items(db, inspection_id=int(inspection.id))
    blueprints = [
        bp
        for bp in (
            _blueprint_from_item(item=item, inspection=inspection, property_obj=property_obj)
            for item in items
        )
        if bp is not None
    ]

    return {
        "ok": True,
        "property_id": int(property_id),
        "inspection_id": int(inspection.id),
        "items": [bp.__dict__.copy() for bp in blueprints],
        "count": len(blueprints),
    }


def create_failure_tasks_from_inspection(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
) -> dict[str, Any]:
    payload = collect_failure_task_blueprints(
        db,
        org_id=org_id,
        property_id=property_id,
        inspection_id=inspection_id,
    )
    if not payload.get("ok"):
        return payload

    created = 0
    created_task_ids: list[int] = []
    for item in payload.get("items", []):
        title = str(item.get("title") or "").strip()
        if not title or _task_exists(db, org_id=org_id, property_id=property_id, title=title):
            continue

        row = RehabTask(
            org_id=org_id,
            property_id=property_id,
            title=title,
            category=str(item.get("category") or item.get("rehab_category") or "compliance_repair"),
            inspection_relevant=bool(item.get("inspection_relevant", True)),
            status="blocked" if str(item.get("priority") or "").lower() == "high" else "todo",
            cost_estimate=0.0,
            vendor=None,
            deadline=None,
            notes=str(item.get("notes") or ""),
            created_at=_now(),
        )
        db.add(row)
        db.flush()
        created += 1
        created_task_ids.append(int(row.id))

    db.commit()
    return {
        **payload,
        "created": created,
        "created_task_ids": created_task_ids,
    }


def build_failure_next_actions(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
    limit: int = 5,
) -> list[str]:
    payload = collect_failure_task_blueprints(
        db,
        org_id=org_id,
        property_id=property_id,
        inspection_id=inspection_id,
    )
    if not payload.get("ok"):
        return []
    out: list[str] = []
    for row in payload.get("items", [])[: max(0, int(limit))]:
        title = str(row.get("title") or "").strip()
        if title:
            out.append(title)
    return out


def summarize_fail_points(rows: Iterable[Any], limit: int = 10) -> list[dict[str, Any]]:
    return top_fail_points(rows, limit=limit)


def create_tasks_from_photo_findings(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    findings: Iterable[dict[str, Any]],
    mark_blocking: bool = False,
) -> dict[str, Any]:
    created = 0
    created_task_ids: list[int] = []
    rows = list(findings or [])

    for finding in rows:
        title = str(
            finding.get("title")
            or finding.get("probable_failed_inspection_item")
            or finding.get("observed_issue")
            or "Resolve compliance photo finding"
        ).strip()
        if not title:
            continue
        if not title.lower().startswith("resolve") and not title.lower().startswith("fix"):
            title = f"Resolve: {title}"
        if _task_exists(db, org_id=org_id, property_id=property_id, title=title):
            continue

        priority_high = str(finding.get("severity") or "").strip().lower() in {"critical", "high"}
        blocker = bool(mark_blocking and (finding.get("hard_blocker_candidate") or priority_high))
        notes = _j(
            {
                "source": "photo_compliance_finding",
                "observed_issue": finding.get("observed_issue"),
                "probable_failed_inspection_item": finding.get("probable_failed_inspection_item"),
                "recommended_fix": finding.get("recommended_fix"),
                "requires_reinspection": finding.get("requires_reinspection"),
                "confidence": finding.get("confidence"),
                "rule_mapping": finding.get("rule_mapping"),
                "evidence_photo_ids": finding.get("evidence_photo_ids", []),
            }
        )
        row = RehabTask(
            org_id=org_id,
            property_id=property_id,
            title=title,
            category=str(finding.get("rehab_category") or "compliance_repair"),
            inspection_relevant=True,
            status="blocked" if blocker else "todo",
            cost_estimate=0.0,
            vendor=None,
            deadline=None,
            notes=notes,
            created_at=_now(),
        )
        db.add(row)
        db.flush()
        created += 1
        created_task_ids.append(int(row.id))

    db.commit()
    return {
        "ok": True,
        "property_id": int(property_id),
        "created": created,
        "created_task_ids": created_task_ids,
        "rows": rows,
    }


def create_proof_gap_tasks_from_projection(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    from app.products.compliance.services.compliance_engine.projection_service import build_property_projection_snapshot

    snapshot = build_property_projection_snapshot(db, org_id=org_id, property_id=property_id)
    obligations = list(snapshot.get("proof_obligations") or ((snapshot.get("projection") or {}).get("proof_obligations") if isinstance(snapshot.get("projection"), dict) else []) or [])
    created = []
    for item in obligations:
        status = str(item.get("proof_status") or "").strip().lower()
        if status not in {"missing", "expired", "mismatched"}:
            continue
        title = f"Upload proof: {item.get('proof_label') or item.get('rule_key') or 'compliance evidence'}"
        if _task_exists(db, org_id=org_id, property_id=property_id, title=title):
            continue
        task = RehabTask(
            org_id=int(org_id),
            property_id=int(property_id),
            title=title,
            category="compliance_proof",
            status="todo",
            notes=str(item.get("evidence_gap") or f"Resolve required proof for {item.get('rule_key')}"),
            created_at=_now(),
        )
        if hasattr(task, "inspection_relevant"):
            task.inspection_relevant = True
        if hasattr(task, "priority"):
            task.priority = "high" if bool(item.get("blocking")) else "med"
        db.add(task)
        db.flush()
        created.append(int(getattr(task, "id", 0) or 0))
    db.commit()
    return {"created_count": len(created), "created_ids": created}


# --- Step 7 additive failure task + deadlines extensions ---


def _step7_pdf_dataset_status() -> dict[str, Any]:
    roots: list[str] = []
    raw = os.getenv("POLICY_PDFS_ROOT", "") or os.getenv("POLICY_PDF_ROOTS", "") or os.getenv("POLICY_PDF_ROOT", "") or os.getenv("NSPIRE_PDF_ROOT", "")
    for piece in str(raw).split(os.pathsep):
        piece = str(piece).strip()
        if piece and Path(piece).exists():
            roots.append(str(Path(piece)))
    for fallback in (Path("backend/data/pdfs").resolve(), Path("/app/backend/data/pdfs"), Path("/mnt/data/pdfs"), Path("/mnt/data/PDFs"), Path("/mnt/data/pfs"), Path(r"/mnt/data/step67_pdf_zip/pdfs")):
        if fallback.exists() and str(fallback) not in roots:
            roots.append(str(fallback))
    return {"available": bool(roots), "roots": roots}

from datetime import timedelta as _step7_timedelta

_step7_prev_collect_failure_task_blueprints = collect_failure_task_blueprints
_step7_prev_create_failure_tasks_from_inspection = create_failure_tasks_from_inspection


def _step7_item_meta(item: InspectionItem) -> dict[str, Any]:
    try:
        import json as _json
        raw = getattr(item, "evidence_json", None)
        if isinstance(raw, dict):
            return dict(raw)
        if isinstance(raw, str) and raw.strip().startswith('{'):
            parsed = _json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}
    return {}


def _step7_deadline_days(item: InspectionItem) -> int | None:
    for attr in ("correction_days",):
        value = getattr(item, attr, None)
        if value not in {None, "", 0}:
            try:
                return int(value)
            except Exception:
                pass
    sev = _severity_rank(getattr(item, "severity", None))
    if sev >= 4:
        return 1
    if sev >= 3:
        return 30
    return None


def _step7_deadline(item: InspectionItem, inspection: Inspection) -> datetime | None:
    days = _step7_deadline_days(item)
    if days is None:
        return None
    base = getattr(inspection, "inspection_date", None) or _now()
    return base + _step7_timedelta(days=int(days))


def collect_failure_task_blueprints(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
) -> dict[str, Any]:
    payload = _step7_prev_collect_failure_task_blueprints(db, org_id=org_id, property_id=property_id, inspection_id=inspection_id)
    if not payload.get("ok"):
        return payload
    inspection = None
    iid = payload.get("inspection_id")
    if iid is not None:
        inspection = db.scalar(select(Inspection).where(Inspection.id == int(iid), Inspection.org_id == org_id, Inspection.property_id == property_id))
    if inspection is None:
        inspection = _latest_inspection(db, org_id=org_id, property_id=property_id)
    if inspection is None:
        return payload
    item_rows = {int(getattr(row, 'id')): row for row in _inspection_items(db, inspection_id=int(inspection.id))}
    enriched = []
    for row in payload.get("items", []):
        item = item_rows.get(int(row.get("inspection_item_id") or 0))
        deadline = _step7_deadline(item, inspection) if item is not None else None
        mapped = map_inspection_code(str(row.get("code") or ""))
        out = dict(row)
        out.update({
            "deadline": deadline.isoformat() if deadline else None,
            "deadline_days": _step7_deadline_days(item) if item is not None else None,
            "deadline_reason": "nspire_lt_24h" if (_step7_deadline_days(item) == 1) else ("nspire_or_fail_30d" if (_step7_deadline_days(item) == 30) else None),
            "default_fail_reason": mapped.default_fail_reason if mapped else None,
            "pdf_context_available": bool(_step7_pdf_dataset_status().get("available")),
            "pdf_dataset_status": _step7_pdf_dataset_status(),
        })
        enriched.append(out)
    payload["items"] = enriched
    payload["deadline_count"] = sum(1 for row in enriched if row.get("deadline"))
    payload["high_priority_count"] = sum(1 for row in enriched if str(row.get("priority") or "").lower() == "high")
    return payload


def create_failure_tasks_from_inspection(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
) -> dict[str, Any]:
    payload = collect_failure_task_blueprints(db, org_id=org_id, property_id=property_id, inspection_id=inspection_id)
    if not payload.get("ok"):
        return payload
    created = 0
    created_task_ids: list[int] = []
    for item in payload.get("items", []):
        title = str(item.get("title") or "").strip()
        if not title or _task_exists(db, org_id=org_id, property_id=property_id, title=title):
            continue
        row = RehabTask(
            org_id=org_id,
            property_id=property_id,
            title=title,
            category=str(item.get("category") or item.get("rehab_category") or "compliance_repair"),
            inspection_relevant=bool(item.get("inspection_relevant", True)),
            status="blocked" if str(item.get("priority") or "").lower() == "high" else "todo",
            cost_estimate=0.0,
            vendor=None,
            deadline=(datetime.fromisoformat(item["deadline"]) if item.get("deadline") else None),
            notes=str(item.get("notes") or ""),
            created_at=_now(),
        )
        if hasattr(row, "priority"):
            row.priority = str(item.get("priority") or "med")
        db.add(row)
        db.flush()
        created += 1
        created_task_ids.append(int(row.id))
    db.commit()
    return {**payload, "created": created, "created_task_ids": created_task_ids}


def build_failure_next_actions(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    payload = collect_failure_task_blueprints(db, org_id=org_id, property_id=property_id, inspection_id=inspection_id)
    if not payload.get("ok"):
        return {"ok": False, "recommended_actions": [], "code": payload.get("code")}
    items = list(payload.get("items") or [])
    items.sort(key=lambda row: (
        0 if str(row.get("priority") or "").lower() == "high" else 1,
        row.get("deadline") or "9999",
        str(row.get("title") or ""),
    ))
    recommended_actions = []
    for row in items[: max(0, int(limit))]:
        recommended_actions.append({
            "code": row.get("code"),
            "title": row.get("title"),
            "priority": row.get("priority"),
            "category": row.get("category") or row.get("rehab_category"),
            "notes": row.get("notes"),
            "deadline": row.get("deadline"),
            "deadline_days": row.get("deadline_days"),
            "requires_reinspection": bool(row.get("requires_reinspection", True)),
        })
    return {
        "ok": True,
        "property_id": int(property_id),
        "inspection_id": payload.get("inspection_id"),
        "recommended_actions": recommended_actions,
        "count": len(recommended_actions),
        "pdf_dataset_status": _step7_pdf_dataset_status(),
    }
