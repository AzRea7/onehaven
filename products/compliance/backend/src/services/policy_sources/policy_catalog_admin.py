# backend/app/routers/policy_catalog_admin.py
from __future__ import annotations

from typing import Any, Optional
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.auth import get_principal, require_owner
from onehaven_platform.backend.src.db import get_db
from onehaven_platform.backend.src.policy_models import PolicyCatalogEntry, JurisdictionProfile, PolicySource
from products.compliance.backend.src.services.policy_coverage.completeness_service import profile_completeness_payload
from products.compliance.backend.src.services.policy_governance.notification_service import (
    build_jurisdiction_review_queue,
    build_review_queue_entry,
    notify_unresolved_jurisdiction_gaps,
)
from products.compliance.backend.src.services.policy_sources.catalog_admin_service import (
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


def _serialize_policy_source(row: PolicySource) -> dict[str, Any]:
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "title": getattr(row, "title", None),
        "publisher": getattr(row, "publisher", None),
        "url": getattr(row, "url", None),
        "source_kind": getattr(row, "source_kind", None),
        "is_authoritative": bool(getattr(row, "is_authoritative", False)),
        "freshness_status": getattr(row, "freshness_status", None),
        "last_verified_at": getattr(row, "last_verified_at", None).isoformat() if getattr(row, "last_verified_at", None) else None,
    }


def _norm_state(value: Optional[str]) -> str:
    return (value or "MI").strip().upper()


def _norm_lower(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = value.strip().lower()
    return raw or None


def _norm_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = value.strip()
    return raw or None


def _market_profile_row(db: Session, *, org_id: int | None, state: str, county: str | None, city: str | None, pha_name: str | None) -> JurisdictionProfile | None:
    q = db.query(JurisdictionProfile).filter(JurisdictionProfile.state == _norm_state(state))
    if org_id is None:
        q = q.filter(JurisdictionProfile.org_id.is_(None))
    else:
        q = q.filter((JurisdictionProfile.org_id == org_id) | (JurisdictionProfile.org_id.is_(None)))
    q = q.filter(JurisdictionProfile.county == _norm_lower(county))
    q = q.filter(JurisdictionProfile.city == _norm_lower(city))
    q = q.filter(JurisdictionProfile.pha_name == _norm_text(pha_name))
    return q.order_by(JurisdictionProfile.org_id.desc(), JurisdictionProfile.id.desc()).first()


def _category_matrix_from_completeness(db: Session, completeness: dict[str, Any]) -> list[dict[str, Any]]:
    details = completeness.get("category_details") or {}
    statuses = completeness.get("category_statuses") or {}
    required = list(completeness.get("required_categories") or [])
    ordered = required + [cat for cat in details.keys() if cat not in required]
    out: list[dict[str, Any]] = []
    required_set = set(required)
    covered = set(completeness.get("covered_categories") or [])
    missing = set(completeness.get("missing_categories") or [])
    stale = set(completeness.get("stale_categories") or [])
    inferred = set(completeness.get("inferred_categories") or [])
    conflicting = set(completeness.get("conflicting_categories") or [])
    for category in ordered:
        detail = details.get(category) or {}
        source_ids = [int(x) for x in (detail.get("source_ids") or []) if str(x).strip()]
        source_rows = []
        if source_ids:
            rows = db.query(PolicySource).filter(PolicySource.id.in_(source_ids)).all()
            row_map = {int(row.id): row for row in rows if getattr(row, "id", None) is not None}
            source_rows = [_serialize_policy_source(row_map[source_id]) for source_id in source_ids if source_id in row_map]
        out.append(
            {
                "category": category,
                "status": detail.get("status") or statuses.get(category) or "missing",
                "expected": category in required_set,
                "covered": category in covered,
                "missing": category in missing,
                "stale": category in stale,
                "inferred": category in inferred,
                "conflicting": category in conflicting,
                "latest_verified_at": detail.get("latest_verified_at"),
                "source_count": int(detail.get("source_count") or 0),
                "authoritative_source_count": int(detail.get("authoritative_source_count") or 0),
                "assertion_count": int(detail.get("assertion_count") or 0),
                "governed_assertion_count": int(detail.get("governed_assertion_count") or 0),
                "citation_count": int(detail.get("citation_count") or 0),
                "source_ids": source_ids,
                "assertion_ids": [int(x) for x in (detail.get("assertion_ids") or []) if str(x).strip()],
                "sources": source_rows,
            }
        )
    return out




def _pdf_catalog_roots() -> list[Path]:
    roots = [
        Path.cwd() / "backend" / "data" / "pdfs",
        Path.cwd() / "pdfs",
        Path("/app/backend/data/pdfs"),
        Path("/mnt/data/pdfs"),
        Path("/mnt/data/step3_zip/pdfs"),
        Path("/mnt/data/step4_pdf_catalog/pdfs"),
        Path("/mnt/data/step67_pdf_zip/pdfs"),
        Path("/mnt/data/step8_pdf_zip/pdfs"),
    ]
    seen: set[str] = set()
    out: list[Path] = []
    for root in roots:
        try:
            if root.exists():
                key = str(root.resolve())
                if key not in seen:
                    seen.add(key)
                    out.append(root)
        except Exception:
            continue
    return out


def _pdf_catalog_payload(limit: int = 12) -> dict[str, Any]:
    files: dict[str, Path] = {}
    for root in _pdf_catalog_roots():
        try:
            for path in root.rglob("*.pdf"):
                files[str(path.resolve())] = path
        except Exception:
            continue
    rows = sorted(files.values(), key=lambda p: p.name.lower())
    names = [p.name for p in rows]
    nspire = [name for name in names if "nspire" in name.lower()]
    return {
        "available": bool(rows),
        "support_state": "pdf_catalog_backed" if rows else "no_pdf_catalog_found",
        "count": len(rows),
        "nspire_count": len(nspire),
        "sample_names": names[:limit],
        "roots": [str(r) for r in _pdf_catalog_roots()],
    }

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
    profile = _market_profile_row(db, org_id=target_org_id, state=payload.state, county=payload.county, city=payload.city, pha_name=payload.pha_name)
    completeness = profile_completeness_payload(db, profile) if profile is not None else {}
    return {
        "ok": True,
        "merged_items": merged_items,
        "editable_items": editable_items,
        "layers": _group_catalog_layers(merged_items),
        "coverage": coverage,
        "coverage_confidence": _coverage_confidence_from_coverage(coverage),
        "missing_local_rule_areas": (coverage or {}).get("missing_local_rule_areas") or (coverage or {}).get("missing_source_kinds") or [],
        "source_evidence_count": len([item for item in merged_items if item.get("url")]),
        "coverage_matrix": _category_matrix_from_completeness(db, completeness) if completeness else [],
        "expected_categories": list(completeness.get("required_categories") or []),
        "covered_categories": list(completeness.get("covered_categories") or []),
        "stale_categories": list(completeness.get("stale_categories") or []),
        "conflicting_categories": list(completeness.get("conflicting_categories") or []),
        "pdf_catalog": _pdf_catalog_payload(),
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
    profile = _market_profile_row(db, org_id=target_org_id, state=payload.state, county=payload.county, city=payload.city, pha_name=payload.pha_name)
    completeness = profile_completeness_payload(db, profile) if profile is not None else {}
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
        "coverage_matrix": _category_matrix_from_completeness(db, completeness) if completeness else [],
        "expected_categories": list(completeness.get("required_categories") or []),
        "covered_categories": list(completeness.get("covered_categories") or []),
        "stale_categories": list(completeness.get("stale_categories") or []),
        "conflicting_categories": list(completeness.get("conflicting_categories") or []),
        "pdf_catalog": _pdf_catalog_payload(),
    }


@router.post("/market/coverage-matrix")
def get_market_coverage_matrix(
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if payload.org_scope else None
    profile = _market_profile_row(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
    )
    if profile is None:
        return {
            "ok": True,
            "market": {
                "state": payload.state,
                "county": payload.county,
                "city": payload.city,
                "pha_name": payload.pha_name,
            },
            "expected_categories": [],
            "covered_categories": [],
            "missing_categories": [],
            "stale_categories": [],
            "inferred_categories": [],
            "conflicting_categories": [],
            "category_matrix": [],
        }
    completeness = profile_completeness_payload(db, profile)
    return {
        "ok": True,
        "market": {
            "state": payload.state,
            "county": payload.county,
            "city": payload.city,
            "pha_name": payload.pha_name,
        },
        "jurisdiction_profile_id": int(profile.id),
        "expected_categories": list(completeness.get("required_categories") or []),
        "covered_categories": list(completeness.get("covered_categories") or []),
        "missing_categories": list(completeness.get("missing_categories") or []),
        "stale_categories": list(completeness.get("stale_categories") or []),
        "inferred_categories": list(completeness.get("inferred_categories") or []),
        "conflicting_categories": list(completeness.get("conflicting_categories") or []),
        "category_matrix": _category_matrix_from_completeness(db, completeness),
    }


@router.get("/review-queue")
def get_jurisdiction_review_queue(
    state: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    pha_name: Optional[str] = Query(default=None),
    org_scope: bool = Query(default=False),
    only_needs_escalation: bool = Query(default=True),
    limit: Optional[int] = Query(default=None),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    target_org_id = principal.org_id if org_scope else None
    return build_jurisdiction_review_queue(
        db,
        org_id=target_org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        only_needs_escalation=only_needs_escalation,
        limit=limit,
    )


@router.get("/review-queue/{jurisdiction_profile_id}")
def get_jurisdiction_review_queue_entry(
    jurisdiction_profile_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    if profile is None:
        raise HTTPException(status_code=404, detail="Jurisdiction profile not found")
    if profile.org_id is not None and profile.org_id != principal.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    return {
        "ok": True,
        "entry": build_review_queue_entry(db, profile=profile),
    }


@router.post("/review-queue/{jurisdiction_profile_id}/escalate")
def escalate_jurisdiction_review_queue_entry(
    jurisdiction_profile_id: int,
    force: bool = Query(default=False),
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    if profile is None:
        raise HTTPException(status_code=404, detail="Jurisdiction profile not found")
    if profile.org_id is not None and profile.org_id != principal.org_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    result = notify_unresolved_jurisdiction_gaps(
        db,
        profile=profile,
        force=bool(force),
    )
    return {
        "ok": True,
        **result,
    }

# ===== Evidence-first refactor additions =====

@router.post("/market/dataset-summary")
def market_dataset_summary(
    payload: MarketIn,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    from products.compliance.backend.src.services.policy_sources.dataset_service import dataset_snapshot_for_market
    target_org_id = principal.org_id if payload.org_scope else None
    return dataset_snapshot_for_market(
        db,
        org_id=target_org_id,
        state=payload.state,
        county=payload.county,
        city=payload.city,
        pha_name=payload.pha_name,
        include_global=True,
        focus=payload.focus,
    )
