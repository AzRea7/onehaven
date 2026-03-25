from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session


DEADLINE_CODE_LABELS = {
    "inspection_contingency": "Inspection contingency",
    "financing_contingency": "Financing contingency",
    "appraisal": "Appraisal",
    "earnest_money": "Earnest money",
    "title_objection": "Title objection",
    "insurance_due": "Insurance due",
    "walkthrough": "Walkthrough",
    "closing_datetime": "Closing",
}


def _row_to_dict(row: Any | None) -> dict[str, Any] | None:
    if row is None:
        return None
    try:
        return dict(row._mapping)
    except Exception:
        return dict(row)


def _parse_any_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(raw, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _decorate_deadline(row: dict[str, Any]) -> dict[str, Any]:
    due_at = _parse_any_datetime(row.get("due_at"))
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    is_overdue = False
    days_remaining = None
    if due_at is not None:
        delta = due_at - now
        days_remaining = delta.days
        is_overdue = delta.total_seconds() < 0 and str(row.get("status") or "").lower() not in {"completed", "waived"}
    row["label"] = row.get("label") or DEADLINE_CODE_LABELS.get(str(row.get("code") or ""))
    row["days_remaining"] = days_remaining
    row["is_overdue"] = is_overdue
    return row


def list_deadlines(db: Session, *, org_id: int, property_id: int) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            """
            select *
            from acquisition_deadlines
            where org_id = :org_id and property_id = :property_id
            order by due_at asc nulls last, id asc
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).fetchall()
    return [_decorate_deadline(_row_to_dict(row) or {}) for row in rows]


def upsert_deadline_by_code(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    code: str,
    due_at: str,
    label: str | None = None,
    status: str | None = None,
    notes: str | None = None,
    source_document_id: int | None = None,
    confidence: float | None = None,
    extraction_version: str | None = None,
    manually_overridden: bool = False,
) -> dict[str, Any]:
    if not code.strip():
        raise HTTPException(status_code=422, detail="code is required.")
    if not due_at:
        raise HTTPException(status_code=422, detail="due_at is required.")
    parsed = _parse_any_datetime(due_at)
    if parsed is None:
        raise HTTPException(status_code=422, detail="due_at is invalid.")

    existing = _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_deadlines
                where org_id = :org_id and property_id = :property_id and code = :code
                order by id desc
                limit 1
                """
            ),
            {"org_id": int(org_id), "property_id": int(property_id), "code": code.strip()},
        ).fetchone()
    )

    params = {
        "org_id": int(org_id),
        "property_id": int(property_id),
        "code": code.strip(),
        "label": (label or DEADLINE_CODE_LABELS.get(code.strip()) or code.strip()).strip(),
        "due_at": parsed,
        "status": (status or "open").strip(),
        "notes": (notes or "").strip() or None,
        "source_document_id": source_document_id,
        "confidence": confidence,
        "extraction_version": extraction_version,
        "manually_overridden": bool(manually_overridden),
    }

    if existing:
        db.execute(
            text(
                """
                update acquisition_deadlines
                set label = :label,
                    due_at = :due_at,
                    status = :status,
                    notes = :notes,
                    source_document_id = :source_document_id,
                    confidence = :confidence,
                    extraction_version = :extraction_version,
                    manually_overridden = :manually_overridden,
                    updated_at = now()
                where id = :id
                """
            ),
            {**params, "id": int(existing["id"])},
        )
        db.commit()
        return _decorate_deadline(
            _row_to_dict(
                db.execute(text("select * from acquisition_deadlines where id = :id"), {"id": int(existing["id"])}).fetchone()
            )
            or {}
        )

    db.execute(
        text(
            """
            insert into acquisition_deadlines (
                org_id,
                property_id,
                code,
                label,
                due_at,
                status,
                notes,
                source_document_id,
                confidence,
                extraction_version,
                manually_overridden,
                created_at,
                updated_at
            )
            values (
                :org_id,
                :property_id,
                :code,
                :label,
                :due_at,
                :status,
                :notes,
                :source_document_id,
                :confidence,
                :extraction_version,
                :manually_overridden,
                now(),
                now()
            )
            """
        ),
        params,
    )
    db.commit()
    return _decorate_deadline(
        _row_to_dict(
            db.execute(
                text(
                    """
                    select *
                    from acquisition_deadlines
                    where org_id = :org_id and property_id = :property_id and code = :code
                    order by id desc
                    limit 1
                    """
                ),
                {"org_id": int(org_id), "property_id": int(property_id), "code": code.strip()},
            ).fetchone()
        )
        or {}
    )


def delete_deadline(db: Session, *, org_id: int, property_id: int, deadline_id: int) -> dict[str, Any]:
    row = _row_to_dict(
        db.execute(
            text(
                """
                select *
                from acquisition_deadlines
                where id = :deadline_id and org_id = :org_id and property_id = :property_id
                """
            ),
            {"deadline_id": int(deadline_id), "org_id": int(org_id), "property_id": int(property_id)},
        ).fetchone()
    )
    if not row:
        raise HTTPException(status_code=404, detail="Deadline not found.")
    db.execute(text("delete from acquisition_deadlines where id = :deadline_id"), {"deadline_id": int(deadline_id)})
    db.commit()
    return _decorate_deadline(row)
