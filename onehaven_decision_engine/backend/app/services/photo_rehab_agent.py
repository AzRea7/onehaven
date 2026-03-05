# onehaven_decision_engine/backend/app/services/photo_rehab_agent.py
from __future__ import annotations

from datetime import datetime
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import PropertyPhoto, RehabTask


def _now() -> datetime:
    return datetime.utcnow()


def generate_rehab_tasks_from_photos(db: Session, *, org_id: int, property_id: int) -> dict:
    """
    Stub “vision”:
    - today: deterministic placeholder tasks (so pipeline works end-to-end)
    - later: replace with real vision model (OpenAI vision / local model)
    """
    photos = list(
        db.scalars(
            select(PropertyPhoto).where(PropertyPhoto.org_id == org_id, PropertyPhoto.property_id == property_id).order_by(PropertyPhoto.id.desc())
        ).all()
    )
    if not photos:
        return {"ok": False, "code": "no_photos"}

    # Create a couple of deterministic tasks if none exist
    created = 0
    now = _now()

    candidates = [
        ("Photo review: peeling paint", "exterior", "high", True),
        ("Photo review: broken window / glazing", "windows", "high", True),
        ("Photo review: trip hazard / stairs", "safety", "med", False),
    ]

    for title, category, priority, blocking in candidates:
        existing = db.scalar(
            select(RehabTask).where(RehabTask.org_id == org_id, RehabTask.property_id == property_id, RehabTask.title == title)
        )
        if existing:
            continue
        t = RehabTask(
            org_id=org_id,
            property_id=property_id,
            title=title,
            category=category,
            status="todo",
            priority=priority,
            estimated_cost=None,
            actual_cost=None,
            notes="Generated from photos (stub). Replace with real vision classifier.",
            created_at=now,
            updated_at=now,
        )
        # best-effort “blocking_ready” if your model has it
        if hasattr(t, "blocking_ready"):
            setattr(t, "blocking_ready", bool(blocking))
        db.add(t)
        created += 1

    db.commit()
    return {"ok": True, "created": created, "photos": len(photos)}