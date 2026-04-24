
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.auth import get_principal
from app.db import get_db
from app.policy_models import PolicyAssertion, PolicySource

router = APIRouter(prefix="/policy-evidence", tags=["policy-evidence"])

ARCHIVE_MARKER = "[archived_stale_source]"


def _norm_state(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip().upper()
    return v or None


def _norm_lower(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip().lower()
    return v or None


def _norm_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip()
    return v or None


def _is_archived_source(src: PolicySource) -> bool:
    return ARCHIVE_MARKER in (src.notes or "").lower()


@router.get("/market")
def evidence_for_market(
    state: str = Query("MI"),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    include_global: bool = Query(True),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    from app.services.policy_evidence_service import evidence_for_market as _svc_evidence_for_market
    from products.compliance.backend.src.services.policy_sources.dataset_service import dataset_snapshot_for_market as _svc_dataset_snapshot_for_market
    from app.services.policy_evidence_version_service import evidence_versions_for_market as _svc_versions_for_market

    st = _norm_state(state) or "MI"
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)
    org_id = getattr(principal, "org_id", None)

    payload = _svc_evidence_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        include_global=include_global,
    )
    payload["datasets"] = _svc_dataset_snapshot_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        include_global=include_global,
    )
    payload["versions"] = _svc_versions_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        include_global=include_global,
        limit=100,
    )
    return payload


@router.get("/market/summary")
def evidence_summary_for_market_route(
    state: str = Query("MI"),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    include_global: bool = Query(True),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    from app.services.policy_evidence_service import evidence_summary_for_market
    return evidence_summary_for_market(
        db,
        org_id=getattr(principal, "org_id", None),
        state=_norm_state(state) or "MI",
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
        include_global=include_global,
    )


@router.get("/market/datasets")
def dataset_summary_for_market_route(
    state: str = Query("MI"),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    include_global: bool = Query(True),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    from products.compliance.backend.src.services.policy_sources.dataset_service import dataset_snapshot_for_market
    return dataset_snapshot_for_market(
        db,
        org_id=getattr(principal, "org_id", None),
        state=_norm_state(state) or "MI",
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
        include_global=include_global,
    )


@router.get("/market/versions")
def evidence_versions_for_market_route(
    state: str = Query("MI"),
    county: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    pha_name: Optional[str] = Query(None),
    include_global: bool = Query(True),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    from app.services.policy_evidence_version_service import evidence_versions_for_market
    return evidence_versions_for_market(
        db,
        org_id=getattr(principal, "org_id", None),
        state=_norm_state(state) or "MI",
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
        include_global=include_global,
        limit=limit,
    )
