# backend/app/cli/seed_demo.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import Organization, AppUser, OrgMembership, Property, RentAssumption
from app.models_saas import Plan, OrgPlan


@dataclass(frozen=True)
class SeedResult:
    org_slug: str
    user_email: str
    plan_code: str
    property_id: Optional[int]


def _get_or_create_org(db: Session, slug: str, name: str) -> Organization:
    row = db.query(Organization).filter(Organization.slug == slug).one_or_none()
    if row:
        return row
    row = Organization(slug=slug, name=name)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _get_or_create_user(db: Session, email: str, display_name: str) -> AppUser:
    row = db.query(AppUser).filter(AppUser.email == email).one_or_none()
    if row:
        return row
    row = AppUser(email=email, display_name=display_name)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _ensure_membership(db: Session, org_id: int, user_id: int, role: str = "owner") -> None:
    existing = db.query(OrgMembership).filter(
        OrgMembership.org_id == int(org_id),
        OrgMembership.user_id == int(user_id),
    ).one_or_none()
    if existing:
        return
    db.add(OrgMembership(org_id=int(org_id), user_id=int(user_id), role=str(role)))
    db.commit()


def _get_or_create_plan(db: Session, code: str, name: str, limits: dict) -> Plan:
    row = db.query(Plan).filter(Plan.code == code).one_or_none()
    if row:
        # keep it stable, but allow updating limits if you want.
        row.limits_json = row.limits_json or {}
        db.commit()
        return row
    row = Plan(code=code, name=name, limits_json=limits)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _assign_org_plan(db: Session, org_id: int, plan_id: int) -> None:
    row = db.query(OrgPlan).filter(OrgPlan.org_id == int(org_id)).one_or_none()
    if row:
        row.plan_id = int(plan_id)
        db.commit()
        return
    db.add(OrgPlan(org_id=int(org_id), plan_id=int(plan_id)))
    db.commit()


def seed_demo(
    *,
    org_slug: str = "demo",
    org_name: str = "demo",
    user_email: str = "austin@demo.local",
    user_name: str = "Austin",
    plan_code: str = "free",
    create_sample_property: bool = True,
) -> SeedResult:
    db = SessionLocal()
    try:
        org = _get_or_create_org(db, org_slug, org_name)
        user = _get_or_create_user(db, user_email, user_name)
        _ensure_membership(db, org.id, user.id, role="owner")

        # Seed plans
        # Professional default: 50 external calls/day for free plan
        free_plan = _get_or_create_plan(
            db,
            code="free",
            name="Free",
            limits={
                "external_calls_per_day": 50,
            },
        )
        pro_plan = _get_or_create_plan(
            db,
            code="pro",
            name="Pro",
            limits={
                "external_calls_per_day": 500,
            },
        )

        chosen = free_plan if plan_code == "free" else pro_plan
        _assign_org_plan(db, org.id, chosen.id)

        property_id: Optional[int] = None
        if create_sample_property:
            prop = db.query(Property).filter(Property.org_id == org.id).first()
            if not prop:
                prop = Property(
                    org_id=org.id,
                    address="55 Logic Ave",
                    city="Detroit",
                    state="MI",
                    zip="48201",
                    bedrooms=3,
                    bathrooms=1.5,
                    square_feet=1200,
                )
                db.add(prop)
                db.commit()
                db.refresh(prop)

                # Seed a rent assumption with a manual override ceiling (optional)
                ra = RentAssumption(org_id=org.id, property_id=prop.id)
                db.add(ra)
                db.commit()

            property_id = int(prop.id)

        return SeedResult(org_slug=org_slug, user_email=user_email, plan_code=chosen.code, property_id=property_id)
    finally:
        db.close()
        