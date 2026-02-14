from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..schemas import PrincipalOut

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/me", response_model=PrincipalOut)
def me(_db: Session = Depends(get_db), p=Depends(get_principal)):
    return PrincipalOut(
        org_id=p.org_id,
        org_slug=p.org_slug,
        user_id=p.user_id,
        email=p.email,
        role=p.role,
    )
