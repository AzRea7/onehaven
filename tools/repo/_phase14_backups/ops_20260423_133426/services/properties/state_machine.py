from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import and_, desc, func, select, text
from sqlalchemy.orm import Session

from app.domain.compliance.compliance_completion import compute_compliance_status
from app.domain.workflow.stages import (
    STAGES,
    clamp_stage,
    gate_for_next_stage,
    infer_transition_reason,
    next_stage,
    stage_label,
)
from app.services.pane_routing_service import build_pane_context
from app.models import (
    Deal,
    Inspection,
    InspectionItem,
    Lease,
    Property,
    PropertyChecklistItem,
    PropertyState,
    RehabTask,
    Transaction,
    UnderwritingResult,
    Valuation,
)
from app.policy_models import JurisdictionProfile
from app.products.compliance.services.inspections.failure_task_service import build_failure_next_actions
from products.acquire.backend.src.services.acquisition_tag_service import list_property_tags
from app.products.compliance.services.inspections.readiness_service import build_property_readiness_summary
from app.services.jurisdiction_task_mapper import map_profile_jurisdiction_task_dicts
from products.intelligence.backend.src.services.risk_scoring import classify_deal_candidate

log = logging.getLogger("onehaven.state_machine")


def _utcnow() -> datetime:
    return datetime.utcnow()


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _safe_json_load(value: Any, fallback: Any):
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed
        except Exception:
            return fallback
    return fallback


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
    except Exception:
        return "{}"


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _failure_recommended_actions(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        raw = value.get("recommended_actions")
    else:
        raw = value
    actions: list[dict[str, Any]] = []
    for item in _safe_list(raw):
        if isinstance(item, dict):
            actions.append(item)
        elif item is not None:
            title = str(item).strip()
            if title:
                actions.append({"title": title})
    return actions


def _row_to_dict(row: Any | None) -> dict[str, Any] | None:
    if row is None:
        return None
    try:
        return dict(row._mapping)
    except Exception:
        return dict(row)


def _current_acquisition_record(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    row = db.execute(
        text(
            """
            select *
            from acquisition_records
            where org_id = :org_id and property_id = :property_id
            order by updated_at desc nulls last, id desc
            limit 1
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).fetchone()
    return _row_to_dict(row) or {}


def normalize_decision_bucket(raw: Any) -> str:
    value = str(raw or "").strip().upper()

    if value in {"GOOD", "BUY", "PASS", "APPROVE", "APPROVED", "GOOD_DEAL", "PROCEED"}:
        return "GOOD"

    if value in {"REJECT", "FAIL", "FAILED", "NO_GO", "PASS_ON_DEAL"}:
        return "REJECT"

    if value in {"WATCH", "REVIEW", "MAYBE", "UNKNOWN", ""}:
        return "REVIEW"

    return "REVIEW"


def normalize_crime_label(score: Any) -> str:
    if score is None:
        return "UNKNOWN"
    value = _safe_float(score, 0.0)
    if value >= 80:
        return "HIGH"
    if value >= 45:
        return "MODERATE"
    return "LOW"


@dataclass(frozen=True)
class ChecklistProgress:
    total: int
    todo: int
    in_progress: int
    blocked: int
    failed: int
    done: int

    @property
    def pct_done(self) -> float:
        if self.total <= 0:
            return 0.0
        return float(self.done) / float(self.total)

    def as_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "todo": self.todo,
            "in_progress": self.in_progress,
            "blocked": self.blocked,
            "failed": self.failed,
            "done": self.done,
            "pct_done": round(self.pct_done, 4),
        }


def ensure_state_row(db: Session, *, org_id: int, property_id: int) -> PropertyState:
    row = db.scalar(
        select(PropertyState).where(
            PropertyState.org_id == org_id,
            PropertyState.property_id == property_id,
        )
    )
    if row is not None:
        return row

    now = _utcnow()
    row = PropertyState(
        org_id=org_id,
        property_id=property_id,
        current_stage="discovered",
        constraints_json=_json_dumps({}),
        outstanding_tasks_json=_json_dumps({}),
        updated_at=now,
    )
    if hasattr(row, "last_transitioned_at"):
        setattr(row, "last_transitioned_at", now)
    db.add(row)
    db.flush()
    return row


def _get_property(db: Session, *, org_id: int, property_id: int) -> Optional[Property]:
    return db.scalar(
        select(Property).where(
            Property.org_id == org_id,
            Property.id == property_id,
        )
    )


def _latest_deal(db: Session, *, org_id: int, property_id: int) -> Optional[Deal]:
    return db.scalar(
        select(Deal)
        .where(and_(Deal.org_id == org_id, Deal.property_id == property_id))
        .order_by(desc(Deal.updated_at), desc(Deal.id))
        .limit(1)
    )


def _latest_underwriting(db: Session, *, org_id: int, property_id: int) -> Optional[UnderwritingResult]:
    return db.scalar(
        select(UnderwritingResult)
        .join(Deal, Deal.id == UnderwritingResult.deal_id)
        .where(
            UnderwritingResult.org_id == org_id,
            Deal.property_id == property_id,
        )
        .order_by(desc(UnderwritingResult.created_at), desc(UnderwritingResult.id))
        .limit(1)
    )


def _latest_jurisdiction_profile(
    db: Session,
    *,
    org_id: int,
    prop: Property,
) -> Optional[JurisdictionProfile]:
    city = (getattr(prop, "city", None) or "").strip().lower() or None
    county = (getattr(prop, "county", None) or "").strip().lower() or None
    state = (getattr(prop, "state", None) or "MI").strip().upper()

    q = (
        select(JurisdictionProfile)
        .where(JurisdictionProfile.state == state)
        .where((JurisdictionProfile.org_id == org_id) | (JurisdictionProfile.org_id.is_(None)))
    )

    rows = db.scalars(q.order_by(desc(JurisdictionProfile.id))).all()
    scoped: list[JurisdictionProfile] = []

    for row in rows:
        row_city = (getattr(row, "city", None) or "").strip().lower() or None
        row_county = (getattr(row, "county", None) or "").strip().lower() or None

        if row_city is not None and row_city != city:
            continue
        if row_county is not None and row_county != county:
            continue
        scoped.append(row)

    if not scoped:
        return None

    scoped.sort(
        key=lambda r: (
            0 if getattr(r, "org_id", None) == org_id else 1,
            0 if getattr(r, "city", None) else 1,
            0 if getattr(r, "county", None) else 1,
            -(getattr(r, "id", 0) or 0),
        )
    )
    return scoped[0]

def _property_listing_hidden(prop: Optional[Property]) -> bool:
    if prop is None:
        return False
    return bool(getattr(prop, "listing_hidden", False))

def _asking_price(prop: Optional[Property], deal: Optional[Deal]) -> Optional[float]:
    for attr in ("asking_price", "list_price", "price", "offer_price", "purchase_price"):
        value = getattr(deal, attr, None) if deal is not None else None
        if value is not None:
            return _safe_float(value, 0.0)
    for attr in ("asking_price", "list_price", "price"):
        value = getattr(prop, attr, None) if prop is not None else None
        if value is not None:
            return _safe_float(value, 0.0)
    return None


def _derive_stage_from_acquisition_record(
    acquisition: dict[str, Any] | None,
    current_stage: str | None,
) -> str:
    current = str(current_stage or "").strip().lower()
    acq = acquisition or {}

    if not acq:
        return current or "discovered"

    status = str(
        acq.get("status")
        or acq.get("pursuit_status")
        or acq.get("stage")
        or ""
    ).strip().lower()
    waiting_on = str(acq.get("waiting_on") or "").strip().lower()
    next_step = str(acq.get("next_step") or "").strip().lower()

    if status in {"owned", "acquired"}:
        return "owned"
    if status in {"closing", "clear_to_close", "ready_to_close"}:
        return "closing"
    if status in {"under_contract"}:
        return "under_contract"
    if status in {"negotiating"}:
        return "negotiating"
    if status in {"offer_submitted"}:
        return "offer_submitted"
    if status in {"offer_ready", "offer"}:
        return "offer_ready"
    if status in {"offer_prep", "procurement"}:
        return "offer_prep"
    if status in {"pursuing", "active", "started", "acquisition"}:
        return "pursuing"

    if current in {"discovered", "shortlisted", "underwritten", "", None}:
        return "pursuing"

    if waiting_on or next_step:
        return "pursuing"

    return current or "pursuing"


def _decision_reason_list(uw: Optional[UnderwritingResult]) -> list[str]:
    if uw is None:
        return []
    raw = getattr(uw, "reasons_json", None)
    parsed = _safe_json_load(raw, [])
    if isinstance(parsed, list):
        return [str(x) for x in parsed]
    if parsed:
        return [str(parsed)]
    return []


def _checklist_progress(db: Session, *, org_id: int, property_id: int) -> ChecklistProgress:
    rows = db.scalars(
        select(PropertyChecklistItem.status).where(
            PropertyChecklistItem.org_id == org_id,
            PropertyChecklistItem.property_id == property_id,
        )
    ).all()

    total = len(rows)
    todo = in_progress = blocked = failed = done = 0

    for status in rows:
        st = str(status or "todo").strip().lower()
        if st == "done":
            done += 1
        elif st == "failed":
            failed += 1
        elif st == "blocked":
            blocked += 1
        elif st == "in_progress":
            in_progress += 1
        else:
            todo += 1

    return ChecklistProgress(
        total=total,
        todo=todo,
        in_progress=in_progress,
        blocked=blocked,
        failed=failed,
        done=done,
    )


def _latest_inspection(db: Session, *, org_id: int, property_id: int) -> Optional[Inspection]:
    return db.scalar(
        select(Inspection)
        .where(
            Inspection.org_id == org_id,
            Inspection.property_id == property_id,
        )
        .order_by(desc(Inspection.inspection_date), desc(Inspection.id))
        .limit(1)
    )


def _open_failed_inspection_items(db: Session, *, org_id: int, property_id: int) -> int:
    try:
        return int(
            db.scalar(
                select(func.count(InspectionItem.id))
                .select_from(InspectionItem)
                .join(Inspection, Inspection.id == InspectionItem.inspection_id)
                .where(
                    Inspection.org_id == org_id,
                    Inspection.property_id == property_id,
                    InspectionItem.failed.is_(True),
                    InspectionItem.resolved_at.is_(None),
                )
            )
            or 0
        )
    except Exception:
        return 0


def _inspection_summary(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    insp = _latest_inspection(db, org_id=org_id, property_id=property_id)
    if insp is None:
        return {
            "exists": False,
            "passed": False,
            "open_failed_items": 0,
            "latest_inspection_id": None,
            "latest_inspection_date": None,
            "result_status": "pending",
            "readiness_score": 0.0,
            "readiness_status": "unknown",
            "posture": "unknown",
        }

    open_failed = _open_failed_inspection_items(db, org_id=org_id, property_id=property_id)
    latest_date = None
    if getattr(insp, "inspection_date", None) is not None:
        try:
            latest_date = insp.inspection_date.isoformat()
        except Exception:
            latest_date = str(insp.inspection_date)

    readiness = build_property_readiness_summary(db, org_id=org_id, property_id=property_id)
    readiness_data = readiness.get("readiness") or {}
    raw = readiness.get("raw") or {}

    passed = bool(readiness_data.get("latest_inspection_passed")) and open_failed == 0

    return {
        "exists": True,
        "passed": passed,
        "open_failed_items": open_failed,
        "latest_inspection_id": getattr(insp, "id", None),
        "latest_inspection_date": latest_date,
        "result_status": readiness_data.get("result_status"),
        "readiness_score": readiness_data.get("score", 0.0),
        "readiness_status": readiness_data.get("status", "unknown"),
        "posture": readiness_data.get("posture") or raw.get("posture"),
    }


def _rehab_summary(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    rows = db.scalars(
        select(RehabTask).where(
            RehabTask.org_id == org_id,
            RehabTask.property_id == property_id,
        )
    ).all()

    total = len(rows)
    done = blocked = in_progress = todo = open_count = 0
    cost_sum = 0.0

    for row in rows:
        status = str(getattr(row, "status", "todo") or "todo").strip().lower()
        if status == "done":
            done += 1
        else:
            open_count += 1

        if status == "blocked":
            blocked += 1
        elif status == "in_progress":
            in_progress += 1
        elif status != "done":
            todo += 1

        if getattr(row, "cost_estimate", None) is not None:
            cost_sum += _safe_float(getattr(row, "cost_estimate", None), 0.0)

    return {
        "total": total,
        "todo": todo,
        "in_progress": in_progress,
        "blocked": blocked,
        "done": done,
        "open": open_count,
        "cost_estimate_sum": round(cost_sum, 2),
        "has_plan": total > 0,
        "is_complete": total > 0 and open_count == 0 and blocked == 0,
    }


def _lease_is_active(lease: Lease, now: datetime) -> bool:
    start = getattr(lease, "start_date", None)
    end = getattr(lease, "end_date", None)

    if start is None:
        return False

    if isinstance(start, datetime):
        start_ok = start <= now
    elif isinstance(start, date):
        start_ok = start <= now.date()
    else:
        try:
            start_ok = datetime.fromisoformat(str(start)) <= now
        except Exception:
            start_ok = False

    if not start_ok:
        return False

    if end is None:
        return True

    if isinstance(end, datetime):
        return end >= now
    if isinstance(end, date):
        return end >= now.date()

    try:
        return datetime.fromisoformat(str(end)) >= now
    except Exception:
        return True


def _lease_summary(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    rows = db.scalars(
        select(Lease)
        .where(
            Lease.org_id == org_id,
            Lease.property_id == property_id,
        )
        .order_by(desc(Lease.id))
    ).all()

    now = _utcnow()
    active = None
    for row in rows:
        if _lease_is_active(row, now):
            active = row
            break

    return {
        "exists": len(rows) > 0,
        "active": active is not None,
        "active_lease_id": getattr(active, "id", None) if active is not None else None,
        "count": len(rows),
    }


def _cash_summary(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    rows = db.scalars(
        select(Transaction)
        .where(
            Transaction.org_id == org_id,
            Transaction.property_id == property_id,
        )
        .order_by(desc(Transaction.id))
        .limit(1000)
    ).all()

    income = 0.0
    expense = 0.0
    latest_date: Optional[str] = None

    for row in rows:
        amount = _safe_float(getattr(row, "amount", None), 0.0)
        t = str(getattr(row, "txn_type", "") or "").strip().lower()

        if t in {"income", "rent", "hap", "voucher"}:
            income += amount
        else:
            expense += amount

        dt = getattr(row, "txn_date", None) or getattr(row, "created_at", None)
        if latest_date is None and dt is not None:
            try:
                latest_date = dt.isoformat()
            except Exception:
                latest_date = str(dt)

    return {
        "transaction_count": len(rows),
        "has_transactions": len(rows) > 0,
        "income": round(income, 2),
        "expense": round(expense, 2),
        "net": round(income - expense, 2),
        "latest_transaction_date": latest_date,
    }


def _valuation_summary(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    row = db.scalar(
        select(Valuation)
        .where(
            Valuation.org_id == org_id,
            Valuation.property_id == property_id,
        )
        .order_by(desc(Valuation.id))
        .limit(1)
    )

    if row is None:
        return {
            "exists": False,
            "valuation_id": None,
            "estimated_value": None,
            "valuation_date": None,
        }

    val_date = getattr(row, "valuation_date", None) or getattr(row, "created_at", None)
    iso_date = None
    if val_date is not None:
        try:
            iso_date = val_date.isoformat()
        except Exception:
            iso_date = str(val_date)

    value = None
    for attr in ("estimated_value", "value", "market_value"):
        raw = getattr(row, attr, None)
        if raw is not None:
            value = _safe_float(raw, 0.0)
            break

    return {
        "exists": True,
        "valuation_id": getattr(row, "id", None),
        "estimated_value": value,
        "valuation_date": iso_date,
    }


def _jurisdiction_summary(
    db: Session,
    *,
    org_id: int,
    prop: Property,
) -> dict[str, Any]:
    profile = _latest_jurisdiction_profile(db, org_id=org_id, prop=prop)

    if profile is None:
        return {
            "exists": False,
            "profile_id": None,
            "completeness_status": "missing",
            "completeness_score": 0.0,
            "missing_categories": [],
            "is_stale": True,
            "stale_reason": "missing_profile",
            "required_categories": [],
            "covered_categories": [],
            "tasks": [],
            "gate_ok": False,
            "gate_reason": "missing_jurisdiction_profile",
        }

    required_categories = _safe_json_load(getattr(profile, "required_categories_json", None), [])
    covered_categories = _safe_json_load(getattr(profile, "covered_categories_json", None), [])
    missing_categories = _safe_json_load(getattr(profile, "missing_categories_json", None), [])

    completeness_status = str(getattr(profile, "completeness_status", "missing") or "missing").strip().lower()
    completeness_score = _safe_float(getattr(profile, "completeness_score", 0.0), 0.0)
    is_stale = bool(getattr(profile, "is_stale", False))
    stale_reason = getattr(profile, "stale_reason", None)

    tasks = map_profile_jurisdiction_task_dicts(profile)

    gate_ok = completeness_status == "complete" and not is_stale
    if completeness_status != "complete":
        gate_reason = "jurisdiction_incomplete"
    elif is_stale:
        gate_reason = "jurisdiction_stale"
    else:
        gate_reason = None

    return {
        "exists": True,
        "profile_id": getattr(profile, "id", None),
        "completeness_status": completeness_status,
        "completeness_score": completeness_score,
        "missing_categories": missing_categories if isinstance(missing_categories, list) else [],
        "is_stale": is_stale,
        "stale_reason": stale_reason,
        "required_categories": required_categories if isinstance(required_categories, list) else [],
        "covered_categories": covered_categories if isinstance(covered_categories, list) else [],
        "tasks": tasks,
        "gate_ok": gate_ok,
        "gate_reason": gate_reason,
    }


def derive_stage_and_constraints(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    prop = _get_property(db, org_id=org_id, property_id=property_id)
    if prop is None:
        raise ValueError("property not found")

    deal = _latest_deal(db, org_id=org_id, property_id=property_id)
    uw = _latest_underwriting(db, org_id=org_id, property_id=property_id)
    rehab = _rehab_summary(db, org_id=org_id, property_id=property_id)
    checklist = _checklist_progress(db, org_id=org_id, property_id=property_id)
    inspection = _inspection_summary(db, org_id=org_id, property_id=property_id)
    lease = _lease_summary(db, org_id=org_id, property_id=property_id)
    cash = _cash_summary(db, org_id=org_id, property_id=property_id)
    valuation = _valuation_summary(db, org_id=org_id, property_id=property_id)
    jurisdiction = _jurisdiction_summary(db, org_id=org_id, prop=prop)
    readiness = build_property_readiness_summary(db, org_id=org_id, property_id=property_id)
    completion = compute_compliance_status(db, org_id=org_id, property_id=property_id)
    failure_actions = build_failure_next_actions(db, org_id=org_id, property_id=property_id, limit=10)
    failure_action_items = _failure_recommended_actions(failure_actions)

    existing_row = ensure_state_row(db, org_id=org_id, property_id=property_id)
    persisted_constraints = _safe_json_load(getattr(existing_row, "constraints_json", None), {})
    if not isinstance(persisted_constraints, dict):
        persisted_constraints = {}
    persisted_acquisition = _current_acquisition_record(db, org_id=org_id, property_id=property_id)

    readiness_info = readiness.get("readiness") or {}
    readiness_counts = readiness.get("counts") or {}
    completion_info = readiness.get("completion") or {}

    decision_bucket = normalize_decision_bucket(
        getattr(uw, "decision", None) if uw is not None else getattr(deal, "decision", None)
    )
    asking_price = _asking_price(prop, deal)

    projected_cashflow = None
    if uw is not None and getattr(uw, "cash_flow", None) is not None:
        projected_cashflow = _safe_float(getattr(uw, "cash_flow", None), 0.0)

    projected_dscr = None
    if uw is not None and getattr(uw, "dscr", None) is not None:
        projected_dscr = _safe_float(getattr(uw, "dscr", None), 0.0)

    risk_score = None
    if prop is not None and getattr(prop, "risk_score", None) is not None:
        risk_score = _safe_float(getattr(prop, "risk_score", None), 0.0)

    deal_filter = classify_deal_candidate(
        normalized_decision=decision_bucket,
        risk_score=risk_score,
        projected_monthly_cashflow=projected_cashflow,
        dscr=projected_dscr,
        listing_hidden=_property_listing_hidden(prop),
    )

    deal_exists = deal is not None or uw is not None
    underwriting_complete = uw is not None
    
    acquisition_ready = bool(readiness_info.get("acquisition_ready"))
    acquisition_blockers = readiness_info.get("acquisition_blockers") or []
    acquisition_next_actions = readiness_info.get("acquisition_next_actions") or []

    if not isinstance(acquisition_blockers, list):
        acquisition_blockers = []
    if not isinstance(acquisition_next_actions, list):
        acquisition_next_actions = []

    offer_ready = bool(
        underwriting_complete
        and decision_bucket != "REJECT"
        and acquisition_ready
    )

    acquired_complete = bool(
        (deal is not None and (getattr(deal, "purchase_price", None) is not None or getattr(deal, "closing_date", None) is not None))
        or lease["exists"]
        or cash["has_transactions"]
        or valuation["exists"]
        or rehab["has_plan"]
    )

    pursuit_status = str(persisted_acquisition.get("pursuit_status") or "").strip().lower()
    acquisition_stage_override = str(persisted_acquisition.get("stage") or "").strip().lower()
    manual_start_requested = bool(persisted_acquisition.get("start_requested") or persisted_acquisition.get("manual_start_approved"))
    property_tags = {
        str(row.get("tag") or "").strip().lower()
        for row in list_property_tags(db, org_id=org_id, property_id=property_id)
    }
    has_saved_tag = "saved" in property_tags
    has_shortlisted_tag = "shortlisted" in property_tags
    has_offer_candidate_tag = "offer_candidate" in property_tags
    has_agent_owner = bool(
        persisted_acquisition.get("owner_user_id")
        or persisted_acquisition.get("owner_name")
        or persisted_acquisition.get("buyer_agent_name")
        or persisted_acquisition.get("buyer_agent_email")
    )
    has_pre_offer_plan = bool(
        persisted_acquisition.get("pre_offer_notes")
        or persisted_acquisition.get("offer_strategy")
        or persisted_acquisition.get("proposed_offer_price") is not None
        or persisted_acquisition.get("finance_plan")
    )
    has_active_acquisition_record = bool(
        persisted_acquisition
        and (
            persisted_acquisition.get("id") is not None
            or persisted_acquisition.get("status")
            or persisted_acquisition.get("stage")
            or persisted_acquisition.get("pursuit_status")
            or persisted_acquisition.get("waiting_on")
            or persisted_acquisition.get("next_step")
            or persisted_acquisition.get("target_close_date")
            or persisted_acquisition.get("purchase_price") is not None
            or persisted_acquisition.get("loan_amount") is not None
            or persisted_acquisition.get("cash_to_close") is not None
            or persisted_acquisition.get("buyer_agent_name")
            or persisted_acquisition.get("buyer_agent_email")
            or persisted_acquisition.get("title_company")
            or persisted_acquisition.get("escrow_officer")
            or persisted_acquisition.get("notes")
        )
    )
    investor_marked_for_acquisition = bool(
        manual_start_requested
        or has_saved_tag
        or has_shortlisted_tag
        or has_offer_candidate_tag
    )
    start_acquisition_ready = bool(
        underwriting_complete
        and decision_bucket != "REJECT"
        and not acquired_complete
        and investor_marked_for_acquisition
    )

    acquisition_started = bool(
        has_active_acquisition_record
        or pursuit_status in {"active", "started"}
        or acquisition_stage_override in {"pursuing", "offer_prep", "offer_ready", "offer_submitted", "negotiating", "under_contract", "due_diligence", "closing", "owned"}
    )
    offer_prep_complete = bool(acquisition_started and (persisted_acquisition.get("pre_offer_criteria_checked") or persisted_acquisition.get("pre_offer_packet_started") or has_pre_offer_plan))
    offer_packet_ready = bool(offer_prep_complete and (persisted_acquisition.get("offer_packet_ready") or persisted_acquisition.get("offer_terms_finalized")))
    offer_submitted_flag = bool(persisted_acquisition.get("offer_submitted") or acquisition_stage_override in {"offer_submitted", "negotiating", "under_contract", "due_diligence", "closing"})
    negotiation_started = bool(persisted_acquisition.get("negotiation_started") or acquisition_stage_override in {"negotiating", "under_contract", "due_diligence", "closing"})
    under_contract_flag = bool(persisted_acquisition.get("under_contract") or acquisition_stage_override in {"under_contract", "due_diligence", "closing"})
    due_diligence_complete = bool(persisted_acquisition.get("due_diligence_complete") or acquisition_stage_override in {"closing"})

    rehab_complete = bool(acquired_complete and rehab["is_complete"])

    compliance_complete = bool(
        rehab_complete
        and jurisdiction["gate_ok"]
        and completion.is_compliant
        and readiness_info.get("hqs_ready", False)
        and readiness_info.get("local_ready", False)
        and readiness_info.get("voucher_ready", False)
        and readiness_info.get("lease_up_ready", False)
        and completion.latest_inspection_passed
    )

    lease_exists = bool(lease["exists"])
    tenant_complete = bool(compliance_complete and lease["active"])
    cash_complete = bool(tenant_complete and cash["has_transactions"])
    occupied_complete = bool(tenant_complete and cash_complete)
    turnover_active = bool(acquired_complete and lease_exists and not lease["active"] and not tenant_complete)

    if not deal_exists:
        current_stage = "discovered"
    elif not underwriting_complete:
        current_stage = "shortlisted"
    elif has_active_acquisition_record and not acquired_complete:
        current_stage = _derive_stage_from_acquisition_record(
            persisted_acquisition,
            acquisition_stage_override or "underwritten",
        )
    elif not acquisition_started and not acquired_complete:
        current_stage = "underwritten"
    elif acquired_complete and not rehab["has_plan"] and not rehab_complete:
        current_stage = "owned"
    elif acquired_complete and not rehab_complete:
        current_stage = "rehab"
    elif rehab_complete and not inspection["exists"]:
        current_stage = "compliance_readying"
    elif rehab_complete and not compliance_complete:
        current_stage = "inspection_pending"
    elif turnover_active:
        current_stage = "turnover"
    elif compliance_complete and not lease_exists:
        current_stage = "tenant_marketing"
    elif compliance_complete and lease_exists and not lease["active"]:
        current_stage = "tenant_screening"
    elif tenant_complete and not cash_complete:
        current_stage = "leased"
    elif occupied_complete and ((rehab.get("blocked", 0) > 0) or (inspection.get("open_failed_items", 0) > 0)):
        current_stage = "maintenance"
    elif occupied_complete:
        current_stage = "occupied"
    else:
        current_stage = clamp_stage(acquisition_stage_override or "pursuing")

    if current_stage in {"pursuing", "offer_prep", "offer_ready", "offer_submitted", "negotiating", "under_contract", "due_diligence", "closing"} and acquisition_stage_override:
        current_stage = clamp_stage(acquisition_stage_override)

    blockers: list[str] = []
    next_actions: list[str] = []

    if not underwriting_complete:
        blockers.append("missing_underwriting")
        next_actions.append("Run underwriting evaluation for the property.")
    elif decision_bucket == "REVIEW":
        blockers.append("decision_review")
        next_actions.append("Review underwriting assumptions and decide whether this property should move into Acquire.")
    elif decision_bucket == "REJECT":
        blockers.append("decision_reject")
        next_actions.append("Property is currently rejected. Update assumptions only if you want to re-underwrite.")

    if underwriting_complete and not acquired_complete and not acquisition_started:
        if decision_bucket == "REJECT":
            blockers.append("decision_reject")
            next_actions.append("Property is currently rejected. Update assumptions only if you want to re-underwrite.")
        elif not investor_marked_for_acquisition:
            blockers.append("not_marked_for_acquisition")
            next_actions.append("Mark the property as saved, shortlisted, or offer-candidate to move it into Acquire.")
        else:
            next_actions.append("Start acquisition and move the property into Pursuing.")

    if acquisition_started and current_stage == "pursuing":
        next_actions.append("Open pre-offer work and move this deal into Offer Prep.")
    if current_stage == "offer_prep" and not offer_prep_complete:
        blockers.append("offer_prep_incomplete")
        next_actions.append("Finish pre-offer criteria, ownership, and strategy inputs.")
    if current_stage == "offer_ready" and not offer_packet_ready:
        blockers.append("offer_packet_incomplete")
        next_actions.append("Finalize terms and mark the offer packet ready before submission.")
    if current_stage == "offer_submitted" and not offer_submitted_flag:
        blockers.append("offer_not_submitted")
        next_actions.append("Record the submitted offer details.")
    if current_stage == "negotiating" and not (negotiation_started or under_contract_flag):
        blockers.append("negotiation_not_started")
        next_actions.append("Record negotiation activity or move the deal back to Offer Submitted.")
    if current_stage == "under_contract" and not under_contract_flag:
        blockers.append("not_under_contract")
        next_actions.append("Record contract acceptance before due diligence.")
    if current_stage == "due_diligence" and not due_diligence_complete:
        blockers.append("due_diligence_incomplete")
        next_actions.append("Finish due diligence and clear remaining contingencies.")
    if current_stage == "closing" and not acquired_complete:
        blockers.append("not_owned")
        next_actions.append("Complete close and record ownership.")

    if acquired_complete and not rehab["has_plan"]:
        blockers.append("missing_rehab_plan")
        next_actions.append("Create rehab scope and rehab tasks for the owned property.")
    if acquired_complete and rehab["blocked"] > 0:
        blockers.append("rehab_blocked")
        next_actions.append(f"Resolve rehab blockers ({rehab['blocked']} blocked tasks).")
    elif acquired_complete and rehab["open"] > 0:
        blockers.append("rehab_open_tasks")
        next_actions.append(f"Complete rehab tasks ({rehab['open']} still open).")
    if rehab_complete and not jurisdiction["exists"]:
        blockers.append("missing_jurisdiction_profile")
        next_actions.append("Create or resolve jurisdiction profile coverage for this property market.")
    elif rehab_complete and jurisdiction["completeness_status"] != "complete":
        blockers.append("jurisdiction_incomplete")
        next_actions.append("Complete jurisdiction policy coverage before advancing compliance.")
    elif rehab_complete and jurisdiction["is_stale"]:
        blockers.append("jurisdiction_stale")
        next_actions.append("Refresh stale jurisdiction policy coverage before advancing compliance.")
    if rehab_complete and not inspection["exists"]:
        blockers.append("missing_inspection")
        next_actions.append("Schedule and record the first inspection.")
    elif rehab_complete and not completion.latest_inspection_passed:
        blockers.append("latest_inspection_not_passed")
        next_actions.append("Address failed inspection findings and pass the latest inspection.")
    if rehab_complete and inspection["open_failed_items"] > 0:
        blockers.append("inspection_open_failures")
        next_actions.append(f"Resolve failed inspection items ({inspection['open_failed_items']} still open).")
    if rehab_complete and completion.failed_count > 0:
        blockers.append("compliance_failed_items")
        next_actions.append(f"Resolve compliance failed items ({completion.failed_count} remaining).")
    if rehab_complete and completion.blocked_count > 0:
        blockers.append("compliance_blocked_items")
        next_actions.append(f"Resolve blocked compliance items ({completion.blocked_count} remaining).")
    if rehab_complete and completion.latest_readiness_status in {"critical", "needs_work", "unknown"}:
        blockers.append("inspection_readiness_not_sufficient")
        next_actions.append(f"Improve inspection readiness (current status: {completion.latest_readiness_status}, score: {completion.latest_readiness_score}).")
    if compliance_complete and not lease_exists:
        blockers.append("missing_tenant_pipeline")
        next_actions.append("Start tenant marketing or match the property to an eligible tenant.")
    if compliance_complete and lease_exists and not lease["active"]:
        blockers.append("tenant_pipeline_in_progress")
        next_actions.append("Complete screening and activate the lease.")
    if tenant_complete and not cash["has_transactions"]:
        blockers.append("missing_cash_transactions")
        next_actions.append("Record first income and expense transactions for the property.")
    if occupied_complete and not valuation["exists"]:
        blockers.append("missing_valuation")
        next_actions.append("Add a valuation snapshot for equity tracking.")
    if turnover_active:
        blockers.append("turnover_active")
        next_actions.append("Route turnover through compliance or investor review depending on the blocker profile.")

    for action in failure_action_items:
        title = str(action.get("title") or "").strip()
        if title and title not in next_actions:
            next_actions.append(title)
    for task in jurisdiction["tasks"]:
        title = str(task.get("title") or "").strip()
        if title and title not in next_actions:
            next_actions.append(title)

    start_gate = {
        "ok": start_acquisition_ready,
        "blocked_reason": None if start_acquisition_ready else "Property must be intentionally marked before entering Acquire.",
        "blockers": [
            blocker
            for blocker in blockers
            if blocker in {"decision_review", "decision_reject", "not_marked_for_acquisition"}
        ],
        "target_stage": "pursuing",
    }

    gate = gate_for_next_stage(
        current_stage=current_stage,
        decision_bucket=decision_bucket,
        deal_exists=deal_exists,
        underwriting_complete=underwriting_complete,
        start_acquisition_ready=start_acquisition_ready,
        acquisition_started=acquisition_started,
        offer_prep_complete=offer_prep_complete,
        offer_packet_ready=offer_packet_ready,
        offer_submitted_flag=offer_submitted_flag,
        negotiation_started=negotiation_started,
        under_contract_flag=under_contract_flag,
        due_diligence_complete=due_diligence_complete,
        acquired_complete=acquired_complete,
        rehab_complete=rehab_complete,
        inspection_exists=bool(inspection["exists"]),
        compliance_complete=compliance_complete,
        lease_exists=lease_exists,
        tenant_complete=tenant_complete,
        cash_complete=cash_complete,
        occupied_complete=occupied_complete,
        turnover_active=turnover_active,
    ).as_dict()

    allowed_next = gate.get("allowed_next_stage")
    gate["allowed_next_stage_label"] = stage_label(allowed_next) if allowed_next else None

    stage_completion_summary = [
        {"stage": "discovered", "label": stage_label("discovered"), "is_complete": deal_exists, "blockers": [] if deal_exists else ["not_shortlisted"]},
        {"stage": "shortlisted", "label": stage_label("shortlisted"), "is_complete": underwriting_complete, "blockers": ["missing_underwriting"] if not underwriting_complete else []},
        {"stage": "underwritten", "label": stage_label("underwritten"), "is_complete": acquisition_started or acquired_complete, "blockers": start_gate["blockers"] if not (acquisition_started or acquired_complete) else []},
        {"stage": "pursuing", "label": stage_label("pursuing"), "is_complete": offer_prep_complete or acquired_complete, "blockers": ["offer_prep_incomplete"] if acquisition_started and not (offer_prep_complete or acquired_complete) else []},
        {"stage": "offer_prep", "label": stage_label("offer_prep"), "is_complete": offer_packet_ready or acquired_complete, "blockers": ["offer_packet_incomplete"] if offer_prep_complete and not (offer_packet_ready or acquired_complete) else []},
        {"stage": "offer_ready", "label": stage_label("offer_ready"), "is_complete": offer_submitted_flag or acquired_complete, "blockers": ["offer_not_submitted"] if offer_packet_ready and not (offer_submitted_flag or acquired_complete) else []},
        {"stage": "offer_submitted", "label": stage_label("offer_submitted"), "is_complete": negotiation_started or under_contract_flag or acquired_complete, "blockers": ["negotiation_not_started"] if offer_submitted_flag and not (negotiation_started or under_contract_flag or acquired_complete) else []},
        {"stage": "negotiating", "label": stage_label("negotiating"), "is_complete": under_contract_flag or acquired_complete, "blockers": ["not_under_contract"] if negotiation_started and not (under_contract_flag or acquired_complete) else []},
        {"stage": "under_contract", "label": stage_label("under_contract"), "is_complete": due_diligence_complete or acquired_complete, "blockers": ["due_diligence_incomplete"] if under_contract_flag and not (due_diligence_complete or acquired_complete) else []},
        {"stage": "due_diligence", "label": stage_label("due_diligence"), "is_complete": acquired_complete or due_diligence_complete, "blockers": ["not_owned"] if due_diligence_complete and not acquired_complete else []},
        {"stage": "closing", "label": stage_label("closing"), "is_complete": acquired_complete, "blockers": ["not_owned"] if not acquired_complete and current_stage == "closing" else []},
        {"stage": "owned", "label": stage_label("owned"), "is_complete": acquired_complete, "blockers": ["not_owned"] if not acquired_complete else []},
        {"stage": "rehab", "label": stage_label("rehab"), "is_complete": rehab_complete, "blockers": [x for x in blockers if x.startswith("rehab_") or x == "missing_rehab_plan"]},
        {"stage": "compliance_readying", "label": stage_label("compliance_readying"), "is_complete": bool(rehab_complete and (inspection["exists"] or compliance_complete)), "blockers": [x for x in blockers if x.startswith("jurisdiction_") or x == "missing_jurisdiction_profile"]},
        {"stage": "inspection_pending", "label": stage_label("inspection_pending"), "is_complete": compliance_complete, "blockers": [x for x in blockers if "inspection" in x or "compliance_" in x]},
        {"stage": "tenant_marketing", "label": stage_label("tenant_marketing"), "is_complete": lease_exists or tenant_complete or turnover_active, "blockers": [x for x in blockers if x == "missing_tenant_pipeline"]},
        {"stage": "tenant_screening", "label": stage_label("tenant_screening"), "is_complete": tenant_complete or turnover_active, "blockers": [x for x in blockers if x == "tenant_pipeline_in_progress"]},
        {"stage": "leased", "label": stage_label("leased"), "is_complete": tenant_complete, "blockers": [x for x in blockers if "lease" in x or "tenant" in x]},
        {"stage": "occupied", "label": stage_label("occupied"), "is_complete": occupied_complete, "blockers": [x for x in blockers if "cash" in x]},
        {"stage": "turnover", "label": stage_label("turnover"), "is_complete": not turnover_active, "blockers": [x for x in blockers if x == "turnover_active"]},
        {"stage": "maintenance", "label": stage_label("maintenance"), "is_complete": bool(occupied_complete and rehab.get("blocked", 0) == 0 and inspection.get("open_failed_items", 0) == 0), "blockers": [x for x in blockers if x in {"rehab_blocked", "inspection_open_failures"}]},
    ]

    completed_count = sum(1 for row in stage_completion_summary if row["is_complete"])
    stage_completion = {
        "completed_count": completed_count,
        "total_count": len(stage_completion_summary),
        "pct_complete": round(completed_count / len(stage_completion_summary), 4),
        "by_stage": stage_completion_summary,
    }

    constraints = {
        "decision_bucket": decision_bucket,
        "asking_price": asking_price,
        "crime_score": getattr(prop, "crime_score", None),
        "crime_label": normalize_crime_label(getattr(prop, "crime_score", None)),
        "deal_filter": deal_filter,
        "is_deal_candidate": bool(deal_filter.get("is_deal_candidate")),
        "suppress_from_investor": bool(deal_filter.get("suppress_from_investor")),
        "deal_filter_status": deal_filter.get("deal_filter_status"),
        "deal_candidate_reasons": list(deal_filter.get("candidate_reasons") or []),
        "deal_suppress_reasons": list(deal_filter.get("suppress_reasons") or []),
        "hidden_reason": deal_filter.get("hidden_reason"),
        "deal_exists": deal_exists,
        "underwriting_complete": underwriting_complete,
        "offer_ready": offer_ready,
        "acquired_complete": acquired_complete,
        "rehab_complete": rehab_complete,
        "compliance_complete": compliance_complete,
        "lease_exists": lease_exists,
        "tenant_complete": tenant_complete,
        "cash_complete": cash_complete,
        "occupied_complete": occupied_complete,
        "turnover_active": turnover_active,
        "checklist": checklist.as_dict(),
        "inspection": inspection,
        "rehab": rehab,
        "lease": lease,
        "cash": cash,
        "valuation": valuation,
        "jurisdiction": jurisdiction,
        "decision_reasons": _decision_reason_list(uw),
        "readiness": readiness_info,
        "readiness_counts": readiness_counts,
        "completion": {
            "completion_pct": completion.completion_pct,
            "completion_projection_pct": completion.completion_projection_pct,
            "failed_count": completion.failed_count,
            "blocked_count": completion.blocked_count,
            "latest_inspection_passed": completion.latest_inspection_passed,
            "latest_readiness_score": completion.latest_readiness_score,
            "latest_readiness_status": completion.latest_readiness_status,
            "latest_result_status": completion.latest_result_status,
            "posture": completion.posture,
            "is_compliant": completion.is_compliant,
        },
        "readiness_summary": readiness,
        "completion_projection": completion_info,
        "acquisition": {
            **persisted_acquisition,
            "has_active_record": has_active_acquisition_record,
            "start_gate": start_gate,
            "acquisition_started": acquisition_started,
            "offer_prep_complete": offer_prep_complete,
            "offer_packet_ready": offer_packet_ready,
            "offer_submitted": offer_submitted_flag,
            "negotiation_started": negotiation_started,
            "under_contract": under_contract_flag,
            "due_diligence_complete": due_diligence_complete,
        },
    }

    pane = build_pane_context(
        current_stage=current_stage,
        constraints=constraints,
        org_id=org_id,
    )

    constraints["pane"] = {
        "current_pane": pane["current_pane"],
        "current_pane_label": pane["current_pane_label"],
        "suggested_pane": pane["suggested_pane"],
        "suggested_pane_label": pane["suggested_pane_label"],
        "route_reason": pane["route_reason"],
        "turnover_target": pane["turnover_target"],
    }

    outstanding_tasks = {
        "blockers": blockers,
        "next_actions": next_actions,
        "jurisdiction_tasks": jurisdiction["tasks"],
        "inspection_failure_actions": failure_action_items,
        "counts": {
            "rehab_open": rehab["open"],
            "rehab_blocked": rehab["blocked"],
            "inspection_open_failed_items": inspection["open_failed_items"],
            "checklist_blocked": checklist.blocked,
            "checklist_failed": checklist.failed,
            "jurisdiction_missing_categories": len(jurisdiction["missing_categories"]),
            "inspection_failed_items": readiness_counts.get("failed_items", 0),
            "inspection_blocked_items": readiness_counts.get("blocked_items", 0),
            "failed_critical_items": readiness_counts.get("failed_critical_items", 0),
        },
    }

    return {
        "property_id": property_id,
        "current_stage": current_stage,
        "suggested_stage": current_stage,
        "current_stage_label": stage_label(current_stage),
        "current_pane": pane["current_pane"],
        "current_pane_label": pane["current_pane_label"],
        "suggested_pane": pane["suggested_pane"],
        "suggested_pane_label": pane["suggested_pane_label"],
        "route_reason": pane["route_reason"],
        "allowed_panes": pane["allowed_panes"],
        "allowed_pane_labels": pane["allowed_pane_labels"],
        "suggested_next_pane": pane.get("suggested_next_pane"),
        "suggested_next_pane_label": pane.get("suggested_next_pane_label"),
        "normalized_decision": decision_bucket,
        "decision_bucket": decision_bucket,
        "constraints": constraints,
        "outstanding_tasks": outstanding_tasks,
        "next_actions": next_actions,
        "gate": gate,
        "gate_status": "OPEN" if gate.get("ok") else "BLOCKED",
        "stage_completion_summary": stage_completion,
    }


def _build_snapshot_payload(
    *,
    property_id: int,
    state: dict[str, Any],
    updated_at: Optional[datetime],
    last_transitioned_at: Optional[datetime],
) -> dict[str, Any]:
    return {
        "property_id": property_id,
        "current_stage": state["current_stage"],
        "suggested_stage": state["suggested_stage"],
        "current_stage_label": state["current_stage_label"],
        "current_pane": state["current_pane"],
        "current_pane_label": state["current_pane_label"],
        "suggested_pane": state["suggested_pane"],
        "suggested_pane_label": state["suggested_pane_label"],
        "suggested_next_pane": state.get("suggested_next_pane"),
        "suggested_next_pane_label": state.get("suggested_next_pane_label"),
        "route_reason": state["route_reason"],
        "allowed_panes": state["allowed_panes"],
        "allowed_pane_labels": state["allowed_pane_labels"],
        "normalized_decision": state["normalized_decision"],
        "decision_bucket": state["decision_bucket"],
        "gate": state["gate"],
        "gate_status": state["gate_status"],
        "constraints": state["constraints"],
        "outstanding_tasks": state["outstanding_tasks"],
        "next_actions": state["next_actions"],
        "stage_completion_summary": state["stage_completion_summary"],
        "updated_at": updated_at.isoformat() if updated_at is not None else None,
        "last_transitioned_at": last_transitioned_at.isoformat() if last_transitioned_at is not None else None,
        "transition_at": last_transitioned_at.isoformat() if last_transitioned_at is not None else None,
        "transition_reason": state.get("transition_reason"),
        "is_auto_routed": True,
        "stage_order": list(STAGES),
    }


def _attach_snapshot_to_constraints(state: dict[str, Any]) -> dict[str, Any]:
    constraints = dict(state["constraints"] or {})
    constraints["_state_snapshot"] = {
        "suggested_stage": state["suggested_stage"],
        "current_stage_label": state["current_stage_label"],
        "current_pane": state["current_pane"],
        "current_pane_label": state["current_pane_label"],
        "suggested_pane": state["suggested_pane"],
        "suggested_pane_label": state["suggested_pane_label"],
        "suggested_next_pane": state.get("suggested_next_pane"),
        "suggested_next_pane_label": state.get("suggested_next_pane_label"),
        "route_reason": state["route_reason"],
        "allowed_panes": state["allowed_panes"],
        "allowed_pane_labels": state["allowed_pane_labels"],
        "normalized_decision": state["normalized_decision"],
        "decision_bucket": state["decision_bucket"],
        "gate": state["gate"],
        "gate_status": state["gate_status"],
        "stage_completion_summary": state["stage_completion_summary"],
        "transition_reason": state.get("transition_reason"),
        "is_auto_routed": True,
        "stage_order": list(STAGES),
    }
    return constraints


def _payload_from_row_snapshot(
    row: PropertyState,
    *,
    property_id: int,
) -> Optional[dict[str, Any]]:
    constraints = _safe_json_load(getattr(row, "constraints_json", None), {})
    outstanding = _safe_json_load(getattr(row, "outstanding_tasks_json", None), {})

    if not isinstance(constraints, dict):
        constraints = {}
    if not isinstance(outstanding, dict):
        outstanding = {}

    snapshot = constraints.get("_state_snapshot")
    if not isinstance(snapshot, dict):
        return None

    current_stage = clamp_stage(getattr(row, "current_stage", None))
    updated_at = getattr(row, "updated_at", None)
    last_transitioned_at = getattr(row, "last_transitioned_at", None)

    return {
        "property_id": property_id,
        "current_stage": current_stage,
        "suggested_stage": snapshot.get("suggested_stage") or current_stage,
        "current_stage_label": snapshot.get("current_stage_label") or stage_label(current_stage),
        "current_pane": snapshot.get("current_pane"),
        "current_pane_label": snapshot.get("current_pane_label"),
        "suggested_pane": snapshot.get("suggested_pane"),
        "suggested_pane_label": snapshot.get("suggested_pane_label"),
        "suggested_next_pane": snapshot.get("suggested_next_pane"),
        "suggested_next_pane_label": snapshot.get("suggested_next_pane_label"),
        "route_reason": snapshot.get("route_reason"),
        "allowed_panes": snapshot.get("allowed_panes") or [],
        "allowed_pane_labels": snapshot.get("allowed_pane_labels") or [],
        "normalized_decision": snapshot.get("normalized_decision") or constraints.get("decision_bucket") or "REVIEW",
        "decision_bucket": snapshot.get("decision_bucket") or constraints.get("decision_bucket") or "REVIEW",
        "gate": snapshot.get("gate") or {},
        "gate_status": snapshot.get("gate_status") or "BLOCKED",
        "constraints": constraints,
        "outstanding_tasks": outstanding,
        "next_actions": outstanding.get("next_actions") or [],
        "stage_completion_summary": snapshot.get("stage_completion_summary") or {},
        "updated_at": updated_at.isoformat() if updated_at is not None else None,
        "last_transitioned_at": last_transitioned_at.isoformat() if last_transitioned_at is not None else None,
        "transition_at": last_transitioned_at.isoformat() if last_transitioned_at is not None else None,
        "transition_reason": snapshot.get("transition_reason"),
        "is_auto_routed": True,
        "stage_order": snapshot.get("stage_order") or list(STAGES),
    }


def sync_property_state(db: Session, *, org_id: int, property_id: int) -> PropertyState:
    state = derive_stage_and_constraints(db, org_id=org_id, property_id=property_id)
    row = ensure_state_row(db, org_id=org_id, property_id=property_id)

    new_stage = clamp_stage(state["current_stage"])
    old_raw = getattr(row, "current_stage", None)
    old_stage = clamp_stage(old_raw) if old_raw is not None else None
    transition_reason = infer_transition_reason(old_stage, new_stage)
    state["transition_reason"] = transition_reason

    row.current_stage = new_stage
    row.constraints_json = _json_dumps(_attach_snapshot_to_constraints(state))
    row.outstanding_tasks_json = _json_dumps(state["outstanding_tasks"])
    row.updated_at = _utcnow()

    if hasattr(row, "last_transitioned_at") and old_stage is not None and new_stage != old_stage:
        setattr(row, "last_transitioned_at", _utcnow())

    db.add(row)
    db.flush()
    return row


def get_state_payload(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    recompute: bool = True,
) -> dict[str, Any]:
    row: Optional[PropertyState]

    if recompute:
        row = sync_property_state(db, org_id=org_id, property_id=property_id)
        constraints = _safe_json_load(getattr(row, "constraints_json", None), {})
        outstanding = _safe_json_load(getattr(row, "outstanding_tasks_json", None), {})
        snapshot = derive_stage_and_constraints(db, org_id=org_id, property_id=property_id)
        snapshot["constraints"] = constraints if isinstance(constraints, dict) else snapshot["constraints"]
        snapshot["outstanding_tasks"] = outstanding if isinstance(outstanding, dict) else snapshot["outstanding_tasks"]
        return _build_snapshot_payload(
            property_id=property_id,
            state=snapshot,
            updated_at=getattr(row, "updated_at", None),
            last_transitioned_at=getattr(row, "last_transitioned_at", None),
        )

    row = ensure_state_row(db, org_id=org_id, property_id=property_id)
    payload = _payload_from_row_snapshot(row, property_id=property_id)
    if payload is not None:
        return payload

    log.info(
        "property_state_snapshot_missing_or_legacy",
        extra={"org_id": org_id, "property_id": property_id},
    )

    row = sync_property_state(db, org_id=org_id, property_id=property_id)
    payload = _payload_from_row_snapshot(row, property_id=property_id)
    if payload is not None:
        return payload

    snapshot = derive_stage_and_constraints(db, org_id=org_id, property_id=property_id)
    return _build_snapshot_payload(
        property_id=property_id,
        state=snapshot,
        updated_at=getattr(row, "updated_at", None),
        last_transitioned_at=getattr(row, "last_transitioned_at", None),
    )


def get_transition_payload(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    state = get_state_payload(db, org_id=org_id, property_id=property_id, recompute=True)

    return {
        "property_id": property_id,
        "current_stage": state["current_stage"],
        "current_stage_label": state["current_stage_label"],
        "current_pane": state["current_pane"],
        "current_pane_label": state["current_pane_label"],
        "next_stage": next_stage(state["current_stage"]),
        "next_stage_label": stage_label(next_stage(state["current_stage"])),
        "suggested_next_pane": state.get("suggested_next_pane"),
        "suggested_next_pane_label": state.get("suggested_next_pane_label"),
        "transition_reason": state.get("transition_reason"),
        "transition_at": state.get("transition_at"),
        "is_auto_routed": state.get("is_auto_routed", True),
        "decision_bucket": state["decision_bucket"],
        "gate": state["gate"],
        "gate_status": state["gate_status"],
        "constraints": state["constraints"],
        "next_actions": state["next_actions"],
        "stage_completion_summary": state["stage_completion_summary"],
    }


def compute_and_persist_stage(db: Session, *, org_id: int, property: Property) -> PropertyState:
    return sync_property_state(db, org_id=org_id, property_id=int(property.id))
