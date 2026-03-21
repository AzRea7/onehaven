from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import and_, desc, func, select
from sqlalchemy.orm import Session

from ..domain.workflow.stages import (
    STAGES,
    clamp_stage,
    gate_for_next_stage,
    stage_label,
)
from ..models import (
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
from ..policy_models import JurisdictionProfile
from .jurisdiction_task_mapper import map_profile_jurisdiction_task_dicts


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
        current_stage="deal",
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
        .where(
            (JurisdictionProfile.org_id == org_id)
            | (JurisdictionProfile.org_id.is_(None))
        )
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
        }

    open_failed = _open_failed_inspection_items(db, org_id=org_id, property_id=property_id)
    passed = bool(getattr(insp, "passed", False)) and open_failed == 0

    latest_date = None
    if getattr(insp, "inspection_date", None) is not None:
        try:
            latest_date = insp.inspection_date.isoformat()
        except Exception:
            latest_date = str(insp.inspection_date)

    return {
        "exists": True,
        "passed": passed,
        "open_failed_items": open_failed,
        "latest_inspection_id": getattr(insp, "id", None),
        "latest_inspection_date": latest_date,
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

    decision_bucket = normalize_decision_bucket(
        getattr(uw, "decision", None) if uw is not None else getattr(deal, "decision", None)
    )
    asking_price = _asking_price(prop, deal)

    deal_complete = uw is not None and decision_bucket == "GOOD"
    rehab_complete = deal_complete and rehab["is_complete"]
    compliance_complete = (
        rehab_complete
        and inspection["passed"]
        and checklist.blocked == 0
        and checklist.failed == 0
        and jurisdiction["gate_ok"]
    )
    tenant_complete = compliance_complete and lease["active"]
    cash_complete = tenant_complete and cash["has_transactions"]
    equity_complete = cash_complete and valuation["exists"]

    if not deal_complete:
        current_stage = "deal"
    elif not rehab_complete:
        current_stage = "rehab"
    elif not compliance_complete:
        current_stage = "compliance"
    elif not tenant_complete:
        current_stage = "tenant"
    elif not cash_complete:
        current_stage = "cash"
    else:
        current_stage = "equity"

    blockers: list[str] = []
    next_actions: list[str] = []

    if uw is None:
        blockers.append("missing_underwriting")
        next_actions.append("Run underwriting evaluation for the deal.")
    elif decision_bucket == "REVIEW":
        blockers.append("decision_review")
        next_actions.append("Review underwriting assumptions and either approve or reject the deal.")
    elif decision_bucket == "REJECT":
        blockers.append("decision_reject")
        next_actions.append("Deal is currently rejected. Update assumptions only if you want to re-underwrite.")

    if decision_bucket == "GOOD":
        if not rehab["has_plan"]:
            blockers.append("missing_rehab_plan")
            next_actions.append("Create rehab scope and rehab tasks.")
        elif rehab["blocked"] > 0:
            blockers.append("rehab_blocked")
            next_actions.append(f"Resolve rehab blockers ({rehab['blocked']} blocked tasks).")
        elif rehab["open"] > 0:
            blockers.append("rehab_open_tasks")
            next_actions.append(f"Complete rehab tasks ({rehab['open']} still open).")

        if rehab_complete and not jurisdiction["exists"]:
            blockers.append("missing_jurisdiction_profile")
            next_actions.append("Create or project jurisdiction profile coverage for this property market.")
        elif rehab_complete and jurisdiction["completeness_status"] != "complete":
            blockers.append("jurisdiction_incomplete")
            next_actions.append("Complete jurisdiction policy coverage before advancing compliance.")
        elif rehab_complete and jurisdiction["is_stale"]:
            blockers.append("jurisdiction_stale")
            next_actions.append("Refresh stale jurisdiction policy sources before advancing compliance.")

        if rehab_complete and not inspection["exists"]:
            blockers.append("missing_inspection")
            next_actions.append("Schedule and record the first inspection.")
        elif rehab_complete and inspection["open_failed_items"] > 0:
            blockers.append("inspection_open_failures")
            next_actions.append(
                f"Resolve failed inspection items ({inspection['open_failed_items']} still open)."
            )
        elif rehab_complete and checklist.total > 0 and (checklist.blocked > 0 or checklist.failed > 0):
            blockers.append("checklist_blockers")
            next_actions.append("Clear blocked or failed compliance checklist items.")

        if compliance_complete and not lease["active"]:
            blockers.append("missing_active_lease")
            next_actions.append("Create or activate the lease for the selected tenant.")

        if tenant_complete and not cash["has_transactions"]:
            blockers.append("missing_cash_transactions")
            next_actions.append("Record first income and expense transactions for the property.")

        if cash_complete and not valuation["exists"]:
            blockers.append("missing_valuation")
            next_actions.append("Add a valuation snapshot for equity tracking.")

    for task in jurisdiction["tasks"]:
        title = str(task.get("title") or "").strip()
        if title and title not in next_actions:
            next_actions.append(title)

    gate = gate_for_next_stage(
        current_stage=current_stage,
        decision_bucket=decision_bucket,
        deal_complete=deal_complete,
        rehab_complete=rehab_complete,
        compliance_complete=compliance_complete,
        tenant_complete=tenant_complete,
        cash_complete=cash_complete,
        equity_complete=equity_complete,
    ).as_dict()

    if current_stage == "compliance" and not jurisdiction["gate_ok"]:
        gate["ok"] = False
        gate["reason"] = jurisdiction["gate_reason"] or "jurisdiction_blocked"
        gate["allowed_next_stage"] = None

    allowed_next = gate.get("allowed_next_stage")
    if allowed_next:
        gate["allowed_next_stage_label"] = stage_label(allowed_next)
    else:
        gate["allowed_next_stage_label"] = None

    stage_completion_summary = [
        {
            "stage": "deal",
            "label": stage_label("deal"),
            "is_complete": deal_complete,
            "blockers": [x for x in blockers if x.startswith("missing_underwriting") or x.startswith("decision_")],
        },
        {
            "stage": "rehab",
            "label": stage_label("rehab"),
            "is_complete": rehab_complete,
            "blockers": [x for x in blockers if x.startswith("missing_rehab") or x.startswith("rehab_")],
        },
        {
            "stage": "compliance",
            "label": stage_label("compliance"),
            "is_complete": compliance_complete,
            "blockers": [
                x
                for x in blockers
                if "inspection" in x or "checklist" in x or x.startswith("jurisdiction_") or x == "missing_jurisdiction_profile"
            ],
        },
        {
            "stage": "tenant",
            "label": stage_label("tenant"),
            "is_complete": tenant_complete,
            "blockers": [x for x in blockers if "lease" in x or "tenant" in x],
        },
        {
            "stage": "cash",
            "label": stage_label("cash"),
            "is_complete": cash_complete,
            "blockers": [x for x in blockers if "cash" in x],
        },
        {
            "stage": "equity",
            "label": stage_label("equity"),
            "is_complete": equity_complete,
            "blockers": [x for x in blockers if "valuation" in x or "equity" in x],
        },
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
        "deal_complete": deal_complete,
        "rehab_complete": rehab_complete,
        "compliance_complete": compliance_complete,
        "tenant_complete": tenant_complete,
        "cash_complete": cash_complete,
        "equity_complete": equity_complete,
        "checklist": checklist.as_dict(),
        "inspection": inspection,
        "rehab": rehab,
        "lease": lease,
        "cash": cash,
        "valuation": valuation,
        "jurisdiction": jurisdiction,
        "decision_reasons": _decision_reason_list(uw),
    }

    outstanding_tasks = {
        "blockers": blockers,
        "next_actions": next_actions,
        "jurisdiction_tasks": jurisdiction["tasks"],
        "counts": {
            "rehab_open": rehab["open"],
            "rehab_blocked": rehab["blocked"],
            "inspection_open_failed_items": inspection["open_failed_items"],
            "checklist_blocked": checklist.blocked,
            "checklist_failed": checklist.failed,
            "jurisdiction_missing_categories": len(jurisdiction["missing_categories"]),
        },
    }

    return {
        "property_id": property_id,
        "current_stage": current_stage,
        "suggested_stage": current_stage,
        "current_stage_label": stage_label(current_stage),
        "normalized_decision": decision_bucket,
        "decision_bucket": decision_bucket,
        "constraints": constraints,
        "outstanding_tasks": outstanding_tasks,
        "next_actions": next_actions,
        "gate": gate,
        "gate_status": "OPEN" if gate.get("ok") else "BLOCKED",
        "stage_completion_summary": stage_completion,
    }


def sync_property_state(db: Session, *, org_id: int, property_id: int) -> PropertyState:
    state = derive_stage_and_constraints(db, org_id=org_id, property_id=property_id)
    row = ensure_state_row(db, org_id=org_id, property_id=property_id)

    new_stage = clamp_stage(state["current_stage"])
    old_stage = clamp_stage(getattr(row, "current_stage", None))

    row.current_stage = new_stage
    row.constraints_json = _json_dumps(state["constraints"])
    row.outstanding_tasks_json = _json_dumps(state["outstanding_tasks"])
    row.updated_at = _utcnow()

    if hasattr(row, "last_transitioned_at") and new_stage != old_stage:
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
    row: Optional[PropertyState] = None
    if recompute:
        row = sync_property_state(db, org_id=org_id, property_id=property_id)
    else:
        row = ensure_state_row(db, org_id=org_id, property_id=property_id)

    if row is None:
        raise ValueError("property state not available")

    derived = derive_stage_and_constraints(db, org_id=org_id, property_id=property_id)

    updated_at = getattr(row, "updated_at", None)
    last_transitioned_at = getattr(row, "last_transitioned_at", None)

    return {
        "property_id": property_id,
        "current_stage": derived["current_stage"],
        "suggested_stage": derived["suggested_stage"],
        "current_stage_label": derived["current_stage_label"],
        "normalized_decision": derived["normalized_decision"],
        "decision_bucket": derived["decision_bucket"],
        "gate": derived["gate"],
        "gate_status": derived["gate_status"],
        "constraints": derived["constraints"],
        "outstanding_tasks": derived["outstanding_tasks"],
        "next_actions": derived["next_actions"],
        "stage_completion_summary": derived["stage_completion_summary"],
        "updated_at": updated_at.isoformat() if updated_at is not None else None,
        "last_transitioned_at": last_transitioned_at.isoformat() if last_transitioned_at is not None else None,
        "stage_order": list(STAGES),
    }


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
        "decision_bucket": state["decision_bucket"],
        "gate": state["gate"],
        "gate_status": state["gate_status"],
        "constraints": state["constraints"],
        "next_actions": state["next_actions"],
        "stage_completion_summary": state["stage_completion_summary"],
    }


def compute_and_persist_stage(db: Session, *, org_id: int, property: Property) -> PropertyState:
    return sync_property_state(db, org_id=org_id, property_id=int(property.id))
