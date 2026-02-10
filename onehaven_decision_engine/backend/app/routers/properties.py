from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from ..db import get_db
from ..models import Property
from ..schemas import PropertyCreate, PropertyOut

router = APIRouter(prefix="/properties", tags=["properties"])


@router.post("", response_model=PropertyOut)
def create_property(payload: PropertyCreate, db: Session = Depends(get_db)):
    p = Property(**payload.model_dump())
    db.add(p)
    db.commit()
    db.refresh(p)
    return p


@router.get("/{property_id}", response_model=PropertyOut)
def get_property(property_id: int, db: Session = Depends(get_db)):
    stmt = (
        select(Property)
        .where(Property.id == property_id)
        .options(
            selectinload(Property.rent_assumption),
            selectinload(Property.rent_comps),
        )
    )
    p = db.execute(stmt).scalar_one_or_none()
    if not p:
        raise HTTPException(status_code=404, detail="Property not found")
    return p
