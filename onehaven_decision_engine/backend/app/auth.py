# onehaven_decision_engine/backend/app/auth.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from fastapi import Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import settings
from .db import get_db
from .models import Organization, AppUser, OrgMembership
from .services.jurisdiction_rules_service import ensure_seeded_for_org


@dataclass(frozen=True)
class Principal:
    org_id: int
    org_slug: str
    user_id: int
    email: str
    role: str  # owner | operator | analyst


ROLE_ORDER = {"analyst": 1, "operator": 2, "owner": 3}


def _require_role(principal: Principal, min_role: str) -> None:
    if ROLE_ORDER.get(principal.role, 0) < ROLE_ORDER.get(min_role, 999):
        raise HTTPException(status_code=403, detail=f"Requires role >= {min_role}")


def require_operator(p: Principal = Depends(lambda: get_principal())) -> Principal:
    _require_role(p, "operator")
    return p


def require_owner(p: Principal = Depends(lambda: get_principal())) -> Principal:
    _require_role(p, "owner")
    return p


def get_principal(
    db: Session = Depends(get_db),
    x_org_slug: Optional[str] = Header(default=None, alias="X-Org-Slug"),
    x_user_email: Optional[str] = Header(default=None, alias="X-User-Email"),
    x_user_role: Optional[str] = Header(default=None, alias="X-User-Role"),
) -> Principal:
    """
    DEV auth mode:
      Requires X-Org-Slug + X-User-Email.
      Auto-provisions org + user + membership when dev_auto_provision=True.

    Phase-2 DoD hook:
      When org is created in dev, ensure jurisdiction defaults exist for that org.
    """
    if settings.auth_mode != "dev":
        raise HTTPException(status_code=500, detail="Non-dev auth mode not configured yet")

    org_slug = (x_org_slug or "").strip()
    email = (x_user_email or "").strip().lower()
    role = ((x_user_role or "owner").strip().lower() or "owner")

    if not org_slug or not email:
        raise HTTPException(
            status_code=401,
            detail="Missing auth headers. Provide X-Org-Slug and X-User-Email (and optionally X-User-Role).",
        )

    if role not in {"owner", "operator", "analyst"}:
        role = "owner"

    org = db.scalar(select(Organization).where(Organization.slug == org_slug))
    user = db.scalar(select(AppUser).where(AppUser.email == email))

    if (org is None or user is None) and not settings.dev_auto_provision:
        raise HTTPException(status_code=401, detail="Unknown org/user (auto-provision disabled).")

    now = datetime.utcnow()

    org_created = False
    if org is None:
        org = Organization(slug=org_slug, name=org_slug, created_at=now)
        db.add(org)
        db.commit()
        db.refresh(org)
        org_created = True

    if user is None:
        user = AppUser(email=email, display_name=email.split("@")[0], created_at=now)
        db.add(user)
        db.commit()
        db.refresh(user)

    mem = db.scalar(
        select(OrgMembership).where(
            OrgMembership.org_id == org.id,
            OrgMembership.user_id == user.id,
        )
    )
    if mem is None:
        mem = OrgMembership(org_id=org.id, user_id=user.id, role=role, created_at=now)
        db.add(mem)
        db.commit()
        db.refresh(mem)

    # Phase 2 DoD: on org creation, seed org-scoped jurisdiction rules (admin starts non-empty).
    if org_created:
        try:
            ensure_seeded_for_org(db, org_id=int(org.id))
        except Exception:
            # Never break auth on seed hiccup; worst case: admin sees empty and can seed manually.
            db.rollback()

    effective_role = mem.role

    return Principal(
        org_id=int(org.id),
        org_slug=str(org.slug),
        user_id=int(user.id),
        email=str(user.email),
        role=str(effective_role),
    )
