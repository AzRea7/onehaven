from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import get_principal, require_owner
from app.db import get_db
from app.policy_models import PolicyCatalogEntry
from app.services.policy_catalog_admin_service import (
    bootstrap_market_catalog_entries,
    create_catalog_entry,
    disable_catalog_entry,
    list_catalog_entries_for_market,
    merged_catalog_for_market,
    reset_market_catalog_entries,
    source_kind_coverage_for_market,
    update_catalog_entry,
)

router = APIRouter(prefix="/policy/catalog-admin", tags=["policy-catalog-admin"])


class MarketIn(BaseModel):
    state: str = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    org_scope: bool = False
    focus: str = "se_mi_extended"


class CreateCatalogItemIn(BaseModel):
    state: str = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    program_type: Optional[str] = None
    org_scope: bool = False
    url: str
    publisher: Optional[str] = None
    title: Optional[str] = None
    notes: Optional[str] = None
    source_kind: Optional[str] = None
    is_authoritative: bool = True
    priority: int = Field(default=100, ge=1, le=1000)
    baseline_url: Optional[str] = None


class UpdateCatalogItemIn(BaseModel):
    org_scope: bool = False
    title: Optional[str] = None
    publisher: Optional[str] = None
    notes: Optional[str] = None
    source_kind: Optional[str] = None
    is_authoritative: Optional[bool] = None
    priority: Optional[int] = Field(default=None, ge=1, le=1000)
    url: Optional[str] = None
    is_active: Optional[bool] = None


def _serialize_row(row: PolicyCatalogEntry) -> dict:
    return {
        "id": row.id,
        "org_id": row.org_id,
        "state": row.state,
        "county": row.county,
        "city": row.city,
        "pha_name": row.pha_name,
        "program_type": row.program_type,
        "url": row.url,
        "publisher": row.publisher,
        "title": row.title,
        "notes": row.notes,
        "source_kind": row.source_kind,
        "is_authoritative": bool(row.is_authoritative),
        "priority": int(row.priority or 100),
        "is_active": bool(row.is_active),
        "is_override": bool(row.is_override),
        "baseline_url": row.baseline_url,
    }


@router.post("/market")
def get_market_catalog(
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if payload.org_scope else None
    merged = merged_catalog_for_market(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        focus=payload.focus,
    )
    db_rows = list_catalog_entries_for_market(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
    )
    coverage = source_kind_coverage_for_market(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        focus=payload.focus,
    )

    merged_items = [
            {
                "url": item.url,
                "state": item.state,
                "county": item.county,
                "city": item.city,
                "pha_name": item.pha_name,
                "program_type": item.program_type,
                "publisher": item.publisher,
                "title": item.title,
                "notes": item.notes,
                "source_kind": item.source_kind,
                "is_authoritative": bool(item.is_authoritative),
                "priority": int(item.priority or 100),
            }
            for item in merged
        ]
    editable_items = [_serialize_row(r) for r in db_rows]
    return {
        "ok": True,
        "merged_items": merged_items,
        "editable_items": editable_items,
        "layers": _group_catalog_layers(merged_items),
        "coverage": coverage,
        "coverage_confidence": _coverage_confidence_from_coverage(coverage),
        "missing_local_rule_areas": (coverage or {}).get("missing_local_rule_areas") or (coverage or {}).get("missing_source_kinds") or [],
        "source_evidence_count": len([item for item in merged_items if item.get("url")]),
    }


@router.post("/market/bootstrap")
def bootstrap_market(
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if payload.org_scope else None
    return {
        "ok": True,
        **bootstrap_market_catalog_entries(
            db,
            org_id=target_org_id,
            state=payload.state,
            county=payload.county,
            city=payload.city,
            pha_name=payload.pha_name,
            focus=payload.focus,
        ),
    }


@router.post("/market/reset")
def reset_market(
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if payload.org_scope else None
    return {
        "ok": True,
        **reset_market_catalog_entries(
            db,
            org_id=target_org_id,
            state=payload.state,
            county=payload.county,
            city=payload.city,
            pha_name=payload.pha_name,
        ),
    }


@router.post("/market/items")
def create_market_item(
    payload: CreateCatalogItemIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if payload.org_scope else None
    row = create_catalog_entry(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        program_type=payload.program_type,
        url=payload.url,
        publisher=payload.publisher,
        title=payload.title,
        notes=payload.notes,
        source_kind=payload.source_kind,
        is_authoritative=payload.is_authoritative,
        priority=payload.priority,
        baseline_url=payload.baseline_url,
    )
    return {"ok": True, "item": _serialize_row(row)}


@router.patch("/market/items/{item_id}")
def patch_market_item(
    item_id: int,
    payload: UpdateCatalogItemIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if payload.org_scope else None
    row = update_catalog_entry(
        db,
        item_id=item_id,
        org_id=target_org_id,
        title=payload.title,
        publisher=payload.publisher,
        notes=payload.notes,
        source_kind=payload.source_kind,
        is_authoritative=payload.is_authoritative,
        priority=payload.priority,
        url=payload.url,
        is_active=payload.is_active,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Catalog entry not found")
    return {"ok": True, "item": _serialize_row(row)}


@router.post("/market/items/{item_id}/disable")
def disable_market_item(
    item_id: int,
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    target_org_id = principal.org_id if payload.org_scope else None
    row = disable_catalog_entry(
        db,
        item_id=item_id,
        org_id=target_org_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Catalog entry not found")
    return {"ok": True, "item": _serialize_row(row)}


def _group_catalog_layers(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    buckets = {
        "state": [],
        "county": [],
        "city": [],
        "housing_authority": [],
        "org_override": [],
        "other": [],
    }
    for item in items:
        if item.get("org_id") is not None or item.get("is_override"):
            buckets["org_override"].append(item)
        elif item.get("pha_name"):
            buckets["housing_authority"].append(item)
        elif item.get("city"):
            buckets["city"].append(item)
        elif item.get("county"):
            buckets["county"].append(item)
        elif item.get("state"):
            buckets["state"].append(item)
        else:
            buckets["other"].append(item)
    return buckets


def _coverage_confidence_from_coverage(coverage: dict[str, Any]) -> str:
    raw = str((coverage or {}).get("coverage_confidence") or "").strip().lower()
    if raw in {"high", "medium", "low"}:
        return raw
    score = coverage.get("coverage_score")
    try:
        score = float(score)
    except Exception:
        score = None
    if score is None:
        return "medium" if (coverage or {}).get("missing_source_kinds") else "high"
    if score >= 0.85:
        return "high"
    if score >= 0.6:
        return "medium"
    return "low"


@router.post("/market/summary")
def get_market_catalog_summary(
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if payload.org_scope else None
    merged = merged_catalog_for_market(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        focus=payload.focus,
    )
    merged_items = [
        {
            "url": item.url,
            "state": item.state,
            "county": item.county,
            "city": item.city,
            "pha_name": item.pha_name,
            "source_kind": item.source_kind,
            "is_authoritative": bool(item.is_authoritative),
            "priority": int(item.priority or 100),
        }
        for item in merged
    ]
    coverage = source_kind_coverage_for_market(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        focus=payload.focus,
    )
    layers = _group_catalog_layers(merged_items)
    return {
        "ok": True,
        "market": {
            "state": payload.state,
            "county": payload.county,
            "city": payload.city,
            "pha_name": payload.pha_name,
        },
        "layer_counts": {key: len(value) for key, value in layers.items()},
        "coverage": coverage,
        "coverage_confidence": _coverage_confidence_from_coverage(coverage),
        "missing_local_rule_areas": (coverage or {}).get("missing_local_rule_areas") or (coverage or {}).get("missing_source_kinds") or [],
    }
