from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AcquisitionDeal, Document, Task


REQUIRED_DOCUMENT_KINDS = (
    "purchase_agreement",
    "inspection_report",
    "seller_disclosures",
    "title_commitment",
    "loan_estimate",
)


def _normalize_status(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text or "unknown"


def build_acquisition_workspace_summary(
    db: Session,
    *,
    org_id: int,
    acquisition_deal_id: int,
) -> dict[str, Any]:
    deal = db.get(AcquisitionDeal, int(acquisition_deal_id))
    if deal is None:
        return {
            "ok": False,
            "error": "acquisition_deal_not_found",
            "acquisition_deal_id": int(acquisition_deal_id),
        }

    docs = list(
        db.scalars(
            select(Document).where(
                Document.org_id == int(org_id),
                Document.acquisition_deal_id == int(acquisition_deal_id),
            )
        ).all()
    )
    tasks = list(
        db.scalars(
            select(Task).where(
                Task.org_id == int(org_id),
                Task.acquisition_deal_id == int(acquisition_deal_id),
            )
        ).all()
    )

    present_kinds = {str(getattr(doc, "document_kind", "") or "").strip().lower() for doc in docs}
    missing_docs = [kind for kind in REQUIRED_DOCUMENT_KINDS if kind not in present_kinds]
    blockers = []
    if missing_docs:
        blockers.append("missing_due_diligence_documents")
    if any(_normalize_status(getattr(task, "status", None)) in {"todo", "blocked"} for task in tasks):
        blockers.append("open_acquisition_tasks")

    stage = _normalize_status(getattr(deal, "stage", None))
    status = _normalize_status(getattr(deal, "status", None))

    readiness = "ready_to_close"
    if blockers:
        readiness = "blocked"
    elif stage not in {"under_contract", "closing", "clear_to_close"}:
        readiness = "in_progress"

    return {
        "ok": True,
        "acquisition_deal_id": int(deal.id),
        "property_id": int(deal.property_id),
        "stage": stage,
        "status": status,
        "required_document_kinds": list(REQUIRED_DOCUMENT_KINDS),
        "present_document_kinds": sorted(present_kinds),
        "missing_documents": missing_docs,
        "due_diligence_blockers": blockers,
        "open_tasks": [
            {
                "task_id": int(task.id),
                "title": task.title,
                "status": task.status,
                "priority": task.priority,
            }
            for task in tasks
            if _normalize_status(getattr(task, "status", None)) not in {"done", "completed"}
        ],
        "close_readiness": readiness,
    }
