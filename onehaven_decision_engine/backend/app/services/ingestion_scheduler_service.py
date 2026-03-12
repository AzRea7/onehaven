from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import IngestionSource


def due_sources(db: Session) -> list[IngestionSource]:
    now = datetime.utcnow()
    return list(
        db.scalars(
            select(IngestionSource).where(
                IngestionSource.is_enabled.is_(True),
                IngestionSource.next_scheduled_at.is_not(None),
                IngestionSource.next_scheduled_at <= now,
            )
        ).all()
    )
