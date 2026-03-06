from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import AgentRun, Property
from .property_state_machine import compute_and_persist_stage, get_state_payload


@dataclass(frozen=True)
class PlannedRun:
    property_id: int
    agent_key: str
    reason: str
    idempotency_key: str


def _loads_json(val: Any):
    if val is None:
        return None
    if isinstance(val, (list, dict, int, float, bool)):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return s
    return None


def _normalize_next_actions(raw: Any) -> list[dict[str, Any]]:
    decoded = _loads_json(raw)

    if decoded is None:
        return []

    if isinstance(decoded, dict):
        out: list[dict[str, Any]] = []

        next_actions = decoded.get("next_actions")
        if isinstance(next_actions, list):
            for a in next_actions:
                if isinstance(a, dict):
                    out.append(a)
                elif isinstance(a, str):
                    out.append({"type": a})
                elif a is not None:
                    out.append({"type": "note", "value": str(a)})

        for key, value in decoded.items():
          if key == "next_actions":
              continue
          if isinstance(value, dict):
              out.append({"type": key, **value})
          elif isinstance(value, list):
              out.append({"type": key, "items": value})
          else:
              out.append({"type": key, "value": value})
        return out

    if isinstance(decoded, str):
        return [{"type": decoded}]

    if isinstance(decoded, list):
        out: list[dict[str, Any]] = []
        for a in decoded:
            if a is None:
                continue
            if isinstance(a, dict):
                out.append(a)
            elif isinstance(a, str):
                out.append({"type": a})
            else:
                out.append({"type": "note", "value": str(a)})
        return out

    return [{"type": "note", "value": str(decoded)}]


def _normalize_constraints(raw: Any) -> list[dict[str, Any]]:
    decoded = _loads_json(raw)
    if decoded is None:
        return []

    if isinstance(decoded, list):
        out: list[dict[str, Any]] = []
        for c in decoded:
            if isinstance(c, dict):
                out.append(c)
            elif isinstance(c, str):
                out.append({"type": c})
            else:
                out.append({"type": "constraint", "value": str(c)})
        return out

    if isinstance(decoded, dict):
        out: list[dict[str, Any]] = []
        for key, value in decoded.items():
            if isinstance(value, dict):
                out.append({"type": key, **value})
            else:
                out.append({"type": key, "value": value})
        return out

    if isinstance(decoded, str):
        return [{"type": decoded}]

    return [{"type": "constraint", "value": str(decoded)}]


def _fingerprint(obj) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _hour_bucket(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def plan_agent_runs(db: Session, *, org_id: int, property_id: int) -> List[PlannedRun]:
    prop = db.scalar(select(Property).where(Property.org_id == org_id).where(Property.id == property_id))
    if prop is None:
        return []

    compute_and_persist_stage(db, org_id=org_id, property=prop)
    state_payload = get_state_payload(db, org_id=org_id, property_id=property_id, recompute=True)

    bucket = _hour_bucket(datetime.utcnow())
    count = db.scalar(
        select(func.count(AgentRun.id))
        .where(AgentRun.org_id == org_id)
        .where(AgentRun.property_id == property_id)
        .where(AgentRun.created_at >= bucket)
    )
    if int(count or 0) >= int(settings.agents_max_runs_per_property_per_hour):
        return []

    next_actions = _normalize_next_actions(state_payload.get("outstanding_tasks") or state_payload.get("next_actions"))
    constraints = _normalize_constraints(state_payload.get("constraints"))
    stage = str(state_payload.get("current_stage") or "deal").strip().lower()

    planned: list[tuple[str, str]] = []

    if stage in {"deal", "import", "intake"}:
        planned.append(("deal_intake", "stage implies intake/deal work"))
        planned.append(("public_records_check", "stage implies diligence"))
        planned.append(("packet_builder", "packet readiness begins early"))

    if stage in {"decision", "acquisition"}:
        planned.append(("public_records_check", "decision/acquisition support"))
        planned.append(("packet_builder", "decision/acquisition packet support"))

    if stage in {"rehab_plan", "rehab_exec"}:
        planned.append(("timeline_nudger", "rehab work needs timeline pressure"))
        planned.append(("packet_builder", "rehab stage packet completeness"))

    if stage in {"compliance"}:
        planned.append(("hqs_precheck", "stage implies HQS readiness"))
        planned.append(("timeline_nudger", "compliance stage needs follow-through"))

    if stage in {"tenant", "lease"}:
        planned.append(("packet_builder", "tenant/lease stage packet support"))
        planned.append(("timeline_nudger", "tenant placement needs momentum"))

    if stage in {"cash", "equity"}:
        planned.append(("timeline_nudger", "cash/equity stage follow-up"))

    for a in next_actions:
        typ = str((a or {}).get("type") or "").lower()

        if "valuation_due" in typ or "missing_valuation" in typ:
            planned.append(("timeline_nudger", "valuation follow-up needed"))

        if "rent_gap" in typ or "rent_reconciliation_gap" in typ:
            planned.append(("rent_reasonableness", "rent gap requires review"))

        if "packet_incomplete" in typ:
            planned.append(("packet_builder", "packet incomplete"))

        if "needs_checklist" in typ or "missing_checklist" in typ:
            planned.append(("hqs_precheck", "checklist missing"))

        if "inspection_not_passed" in typ or "missing_inspection" in typ:
            planned.append(("hqs_precheck", "inspection follow-up needed"))

    for c in constraints:
        typ = str((c or {}).get("type") or "").lower()
        if "rent_reconciliation_gap" in typ:
            planned.append(("rent_reasonableness", "constraint indicates rent reconciliation gap"))
        if "missing_valuation" in typ or "valuation_due" in typ:
            planned.append(("timeline_nudger", "constraint indicates valuation work"))
        if "missing_checklist" in typ or "inspection_not_passed" in typ:
            planned.append(("hqs_precheck", "constraint indicates compliance work"))

    planned.append(("ops_judge", "synthesize specialist outputs into a ranked next-step plan"))

    state_blob = {
        "plan_version": getattr(settings, "decision_version", "v0"),
        "stage": stage,
        "next_actions": next_actions,
        "constraints": constraints,
        "property_id": property_id,
    }
    fp = _fingerprint(state_blob)

    out: List[PlannedRun] = []
    for agent_key, reason in planned:
        idem = f"{org_id}:{property_id}:{agent_key}:{fp}"
        out.append(
            PlannedRun(
                property_id=property_id,
                agent_key=agent_key,
                reason=reason,
                idempotency_key=idem,
            )
        )

    uniq: dict[str, PlannedRun] = {}
    for r in out:
        uniq.setdefault(r.agent_key, r)
    return list(uniq.values())


def _safe_json_dump(x: Any) -> str:
    try:
        return json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return "{}"


def on_run_terminal(db: Session, *, run_id: int, org_id: int | None = None) -> None:
    r = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id)))
    if r is None:
        return

    resolved_org_id = int(org_id) if org_id is not None else int(r.org_id)
    property_id = int(r.property_id) if getattr(r, "property_id", None) is not None else None
    if property_id is None:
        return

    prop = db.scalar(select(Property).where(Property.org_id == resolved_org_id).where(Property.id == property_id))
    if prop is None:
        return

    st = compute_and_persist_stage(db, org_id=resolved_org_id, property=prop)

    status = str(getattr(r, "status", "") or "").lower()
    agent_key = str(getattr(r, "agent_key", "") or "").lower()

    if status in {"failed", "timed_out"}:
        existing_raw = getattr(st, "outstanding_tasks_json", None)
        existing = _normalize_next_actions(existing_raw)

        def has_type(t: str) -> bool:
            return any(str(x.get("type", "")).lower() == t.lower() for x in existing if isinstance(x, dict))

        if agent_key == "rent_reasonableness" and not has_type("rent_gap"):
            existing.append({"type": "rent_gap", "source": "agent_failure", "run_id": int(r.id)})

        if agent_key in {"hqs_precheck", "packet_builder"} and not has_type("packet_incomplete"):
            existing.append({"type": "packet_incomplete", "source": "agent_failure", "run_id": int(r.id)})

        setattr(st, "outstanding_tasks_json", _safe_json_dump({"next_actions": existing}))
        db.add(st)

    db.commit()
    