from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from app.db import get_db
from app.models import AgentMessage, AgentRun, AgentSlotAssignment, Property, WorkflowEvent
from app.schemas import (
    AgentMessageCreate,
    AgentMessageOut,
    AgentRunCreate,
    AgentRunOut,
    AgentSlotAssignmentOut,
    AgentSlotAssignmentUpsert,
    AgentSlotSpecOut,
    AgentSpecOut,
)
from app.domain.agents.registry import AGENTS, AGENT_SPECS, SLOTS
from app.services.agent_engine import create_and_execute_run
from app.services.compliance_photo_analysis_service import (
    analyze_property_photos_for_compliance,
    create_compliance_tasks_from_photo_analysis,
)

try:
    from app.services.trust_service import record_signal, recompute_and_persist  # type: ignore
except Exception:  # pragma: no cover
    def record_signal(*args, **kwargs):  # type: ignore
        return None

    def recompute_and_persist(*args, **kwargs):  # type: ignore
        return None


router = APIRouter(prefix="/agents", tags=["agents"])


VALID_SLOT_KEYS = {s.slot_key for s in SLOTS}
DEFAULT_SLOT_BY_KEY = {s.slot_key: s for s in SLOTS}
VALID_OWNER_TYPES = {"human", "agent"}
VALID_SLOT_STATUSES = {"idle", "queued", "running", "blocked", "done", "failed", "disabled"}


def _utcnow() -> datetime:
    return datetime.utcnow()


def _json_dumps(v: Any) -> str:
    try:
        return json.dumps(v)
    except Exception:
        return "{}"


def _canonical_agent_key(agent_key: str) -> str:
    raw = str(agent_key or "").strip()
    if raw in AGENT_SPECS:
        spec = AGENT_SPECS[raw]
        return str(spec.get("canonical_key") or raw)
    if raw in AGENTS:
        return raw
    return raw


def _assert_property_access(db: Session, *, org_id: int, property_id: int | None) -> Property | None:
    if property_id is None:
        return None
    prop = db.scalar(select(Property).where(Property.id == int(property_id), Property.org_id == int(org_id)))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")
    return prop


def _emit_slot_event(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    property_id: int | None,
    slot_key: str,
    owner_type: str,
    assignee: str | None,
    status: str,
) -> None:
    db.add(
        WorkflowEvent(
            org_id=int(org_id),
            property_id=property_id,
            actor_user_id=int(actor_user_id),
            event_type="slot_assigned",
            payload_json=_json_dumps(
                {
                    "slot_key": slot_key,
                    "owner_type": owner_type,
                    "assignee": assignee,
                    "status": status,
                }
            ),
            created_at=_utcnow(),
        )
    )


def _record_agent_requested_signal(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    property_id: int | None,
    agent_key: str,
    run_id: int,
) -> None:
    try:
        entity_type = "property" if property_id is not None else "org"
        entity_id = str(property_id if property_id is not None else org_id)

        record_signal(
            db,
            org_id=int(org_id),
            entity_type=entity_type,
            entity_id=entity_id,
            signal_key=f"agent.{agent_key}.requested",
            value=1.0,
            meta={"run_id": int(run_id), "actor_user_id": int(actor_user_id)},
        )
        try:
            recompute_and_persist(db, int(org_id), entity_type, entity_id)
        except TypeError:
            recompute_and_persist(
                db,
                org_id=int(org_id),
                entity_type=entity_type,
                entity_id=entity_id,
            )
    except Exception:
        pass


def _record_slot_override_signal(
    db: Session,
    *,
    org_id: int,
    property_id: int | None,
    slot_key: str,
    assignee: str | None,
    status: str,
) -> None:
    try:
        entity_type = "property" if property_id is not None else "org"
        entity_id = str(property_id if property_id is not None else org_id)

        record_signal(
            db,
            org_id=int(org_id),
            entity_type=entity_type,
            entity_id=entity_id,
            signal_key="property.manual_override.slot_assignment",
            value=0.4,
            meta={"slot_key": slot_key, "assignee": assignee, "status": status},
        )
        try:
            recompute_and_persist(db, int(org_id), entity_type, entity_id)
        except TypeError:
            recompute_and_persist(
                db,
                org_id=int(org_id),
                entity_type=entity_type,
                entity_id=entity_id,
            )
    except Exception:
        pass


@router.get("", response_model=list[AgentSpecOut])
def list_agents(p=Depends(get_principal)):
    out: list[AgentSpecOut] = []
    for spec in AGENT_SPECS.values():
        out.append(
            AgentSpecOut(
                agent_key=spec["agent_key"],
                title=spec["title"],
                description=spec.get("description"),
                needs_human=bool(spec.get("needs_human", False)),
                category=spec.get("category"),
                sidebar_slots=[],
            )
        )

    if not any(getattr(a, "agent_key", None) == "compliance_photo_reviewer" for a in out):
        out.append(
            AgentSpecOut(
                agent_key="compliance_photo_reviewer",
                title="Compliance photo reviewer",
                description="Turns property photos into HQS or Section 8 fail-point candidates and recommended remediation tasks.",
                needs_human=True,
                category="compliance",
                sidebar_slots=[],
            )
        )
    return out


@router.get("/registry", response_model=dict)
def registry(p=Depends(get_principal)):
    agents = list(AGENT_SPECS.values())
    if not any(str(a.get("agent_key") or "") == "compliance_photo_reviewer" for a in agents):
        agents.append(
            {
                "agent_key": "compliance_photo_reviewer",
                "title": "Compliance photo reviewer",
                "description": "Turns property photos into likely inspection failures, rule mappings, and repair tasks.",
                "needs_human": True,
                "category": "compliance",
            }
        )

    return {
        "agents": agents,
        "slots": [
            {
                "slot_key": s.slot_key,
                "title": s.title,
                "description": s.description,
                "owner_type": s.owner_type,
                "default_status": s.default_status,
                "default_agent_key": s.default_agent_key,
                "default_payload_schema": s.default_payload_schema,
            }
            for s in SLOTS
        ],
    }


@router.post("/runs", response_model=AgentRunOut)
def create_run(payload: AgentRunCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    requested_key = str(payload.agent_key or "").strip()
    canonical_key = _canonical_agent_key(requested_key)

    if canonical_key not in AGENTS:
        raise HTTPException(status_code=404, detail="unknown agent_key")

    _assert_property_access(db, org_id=p.org_id, property_id=payload.property_id)

    dispatch = bool(getattr(payload, "dispatch", True))
    idempotency_key = getattr(payload, "idempotency_key", None)

    res = create_and_execute_run(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        agent_key=canonical_key,
        property_id=payload.property_id,
        input_payload=payload.input_json or {},
        idempotency_key=idempotency_key,
        dispatch=dispatch,
    )

    run_id = int(res["run_id"])
    run = db.scalar(select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == p.org_id))
    if not run:
        raise HTTPException(status_code=500, detail="run created but not found")

    _record_agent_requested_signal(
        db,
        org_id=int(p.org_id),
        actor_user_id=int(p.user_id),
        property_id=payload.property_id,
        agent_key=canonical_key,
        run_id=run_id,
    )
    return run


@router.get("/runs", response_model=list[AgentRunOut])
def list_runs(
    agent_key: str | None = Query(default=None),
    property_id: int | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    if property_id is not None:
        _assert_property_access(db, org_id=p.org_id, property_id=property_id)

    q = select(AgentRun).where(AgentRun.org_id == p.org_id).order_by(desc(AgentRun.id))
    if agent_key:
        q = q.where(AgentRun.agent_key == _canonical_agent_key(agent_key))
    if property_id is not None:
        q = q.where(AgentRun.property_id == property_id)
    return list(db.scalars(q.limit(int(limit))).all())


@router.post("/messages", response_model=AgentMessageOut)
def post_message(payload: AgentMessageCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    msg = AgentMessage(
        org_id=p.org_id,
        thread_key=payload.thread_key,
        sender=payload.sender,
        recipient=payload.recipient,
        message=payload.message,
        created_at=_utcnow(),
    )
    db.add(msg)
    db.commit()
    db.refresh(msg)
    return msg


@router.get("/messages", response_model=list[AgentMessageOut])
def list_messages(
    thread_key: str,
    recipient: str | None = None,
    limit: int = 200,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = (
        select(AgentMessage)
        .where(AgentMessage.org_id == p.org_id)
        .where(AgentMessage.thread_key == thread_key)
        .order_by(AgentMessage.id.asc())
    )
    if recipient:
        q = q.where(AgentMessage.recipient == recipient)
    return list(db.scalars(q.limit(int(limit))).all())


@router.get("/slots/specs", response_model=list[AgentSlotSpecOut])
def slot_specs(p=Depends(get_principal)):
    return [
        AgentSlotSpecOut(
            slot_key=s.slot_key,
            title=s.title,
            description=s.description,
            owner_type=s.owner_type,
            default_status=s.default_status,
        )
        for s in SLOTS
    ]


@router.get("/slots/assignments", response_model=list[AgentSlotAssignmentOut])
def slot_assignments(
    property_id: int | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    if property_id is not None:
        _assert_property_access(db, org_id=p.org_id, property_id=property_id)

    q = select(AgentSlotAssignment).where(AgentSlotAssignment.org_id == p.org_id).order_by(
        desc(AgentSlotAssignment.updated_at)
    )
    if property_id is not None:
        q = q.where(AgentSlotAssignment.property_id == int(property_id))

    return list(db.scalars(q.limit(int(limit))).all())


@router.post("/slots/assignments", response_model=AgentSlotAssignmentOut)
def upsert_slot_assignment(payload: AgentSlotAssignmentUpsert, db: Session = Depends(get_db), p=Depends(get_principal)):
    _assert_property_access(db, org_id=p.org_id, property_id=payload.property_id)

    slot_key = str(payload.slot_key or "").strip()
    if slot_key not in VALID_SLOT_KEYS:
        raise HTTPException(status_code=404, detail="unknown slot_key")

    spec = DEFAULT_SLOT_BY_KEY[slot_key]
    owner_type = str(payload.owner_type or spec.owner_type).strip().lower()
    status = str(payload.status or spec.default_status).strip().lower()

    if owner_type not in VALID_OWNER_TYPES:
        raise HTTPException(status_code=422, detail="invalid owner_type")
    if status not in VALID_SLOT_STATUSES:
        raise HTTPException(status_code=422, detail="invalid slot status")

    existing = db.scalar(
        select(AgentSlotAssignment)
        .where(
            AgentSlotAssignment.org_id == p.org_id,
            AgentSlotAssignment.slot_key == slot_key,
            AgentSlotAssignment.property_id == payload.property_id,
        )
        .limit(1)
    )

    now = _utcnow()

    if existing:
        existing.owner_type = owner_type
        existing.assignee = payload.assignee
        existing.status = status
        if payload.notes is not None:
            existing.notes = payload.notes
        existing.updated_at = now
        db.add(existing)

        _emit_slot_event(
            db,
            org_id=p.org_id,
            actor_user_id=p.user_id,
            property_id=payload.property_id,
            slot_key=slot_key,
            owner_type=existing.owner_type,
            assignee=existing.assignee,
            status=existing.status,
        )
        _record_slot_override_signal(
            db,
            org_id=int(p.org_id),
            property_id=payload.property_id,
            slot_key=slot_key,
            assignee=existing.assignee,
            status=existing.status,
        )

        db.commit()
        db.refresh(existing)
        return existing

    row = AgentSlotAssignment(
        org_id=p.org_id,
        slot_key=slot_key,
        property_id=payload.property_id,
        owner_type=owner_type,
        assignee=payload.assignee,
        status=status,
        notes=payload.notes,
        updated_at=now,
        created_at=now,
    )
    db.add(row)

    _emit_slot_event(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        property_id=payload.property_id,
        slot_key=slot_key,
        owner_type=row.owner_type,
        assignee=row.assignee,
        status=row.status,
    )
    _record_slot_override_signal(
        db,
        org_id=int(p.org_id),
        property_id=payload.property_id,
        slot_key=slot_key,
        assignee=row.assignee,
        status=row.status,
    )

    db.commit()
    db.refresh(row)
    return row


@router.post("/compliance-photo/preview", response_model=dict)
def preview_compliance_photo_agent(
    property_id: int,
    inspection_id: int | None = None,
    checklist_item_id: int | None = None,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _assert_property_access(db, org_id=p.org_id, property_id=property_id)
    return analyze_property_photos_for_compliance(
        db,
        org_id=int(p.org_id),
        property_id=int(property_id),
        inspection_id=int(inspection_id) if inspection_id is not None else None,
        checklist_item_id=int(checklist_item_id) if checklist_item_id is not None else None,
    )


@router.post("/compliance-photo/commit", response_model=dict)
def commit_compliance_photo_agent(
    property_id: int,
    confirmed_codes: list[str] | None = None,
    inspection_id: int | None = None,
    checklist_item_id: int | None = None,
    mark_blocking: bool = False,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _assert_property_access(db, org_id=p.org_id, property_id=property_id)
    analysis = analyze_property_photos_for_compliance(
        db,
        org_id=int(p.org_id),
        property_id=int(property_id),
        inspection_id=int(inspection_id) if inspection_id is not None else None,
        checklist_item_id=int(checklist_item_id) if checklist_item_id is not None else None,
    )
    return create_compliance_tasks_from_photo_analysis(
        db,
        org_id=int(p.org_id),
        property_id=int(property_id),
        analysis=analysis,
        confirmed_codes=confirmed_codes,
        mark_blocking=bool(mark_blocking),
    )
