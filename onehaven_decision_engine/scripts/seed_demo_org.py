# backend/scripts/seed_demo_org.py
from __future__ import annotations

from datetime import datetime

from app.db import SessionLocal
from app.models import Organization, AppUser, OrgMembership, Plan, OrgSubscription


def main() -> None:
    db = SessionLocal()
    try:
        org = db.query(Organization).filter(Organization.slug == "demo").first()
        if not org:
            org = Organization(slug="demo", name="Demo Org", created_at=datetime.utcnow())
            db.add(org)
            db.commit()
            db.refresh(org)

        user = db.query(AppUser).filter(AppUser.email == "austin@demo.local").first()
        if not user:
            user = AppUser(email="austin@demo.local", display_name="Austin Demo", created_at=datetime.utcnow())
            db.add(user)
            db.commit()
            db.refresh(user)

        mem = (
            db.query(OrgMembership)
            .filter(OrgMembership.org_id == org.id, OrgMembership.user_id == user.id)
            .first()
        )
        if not mem:
            mem = OrgMembership(org_id=org.id, user_id=user.id, role="owner", created_at=datetime.utcnow())
            db.add(mem)
            db.commit()

        plan = db.query(Plan).filter(Plan.code == "free").first()
        if not plan:
            plan = Plan(code="free", name="Free", limits_json="{}", created_at=datetime.utcnow())
            db.add(plan)
            db.commit()

        sub = db.query(OrgSubscription).filter(OrgSubscription.org_id == org.id).first()
        if not sub:
            sub = OrgSubscription(
                org_id=org.id,
                plan_code="free",
                status="active",
                created_at=datetime.utcnow(),
            )
            db.add(sub)
            db.commit()

        print(
            {
                "ok": True,
                "org_id": org.id,
                "org_slug": org.slug,
                "user_id": user.id,
                "email": user.email,
                "role": "owner",
            }
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
    