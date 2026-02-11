# backend/app/routers/tenants.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Tenant, Lease
from ..schemas import TenantCreate, TenantOut, LeaseCreate, LeaseOut

router = APIRouter(prefix="/tenants", tags=["tenants"])


@router.post("", response_model=TenantOut)
def create_tenant(payload: TenantCreate, db: Session = Depends(get_db)):
    row = Tenant(**payload.model_dump())
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.get("", response_model=list[TenantOut])
def list_tenants(limit: int = Query(default=100, ge=1, le=2000), db: Session = Depends(get_db)):
    q = select(Tenant).order_by(desc(Tenant.id))
    return list(db.scalars(q.limit(limit)).all())


@router.post("/leases", response_model=LeaseOut)
def create_lease(payload: LeaseCreate, db: Session = Depends(get_db)):
    row = Lease(**payload.model_dump())
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.get("/leases", response_model=list[LeaseOut])
def list_leases(
    property_id: int | None = Query(default=None),
    tenant_id: int | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    q = select(Lease).order_by(desc(Lease.id))
    if property_id is not None:
        q = q.where(Lease.property_id == property_id)
    if tenant_id is not None:
        q = q.where(Lease.tenant_id == tenant_id)
    return list(db.scalars(q.limit(limit)).all())
