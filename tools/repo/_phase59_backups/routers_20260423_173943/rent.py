from __future__ import annotations

import json
from datetime import datetime
from typing import Any, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import get_principal
from app.clients.federal_register import FederalRegisterClient
from app.clients.govinfo import GovInfoClient
from app.config import settings
from app.db import get_db
from app.models import (
    AuditEvent,
    Property,
    RentAssumption,
    RentCalibration,
    RentComp,
    RentExplainRun,
    RentObservation,
)
from app.schemas import (
    RentAssumptionOut,
    RentAssumptionUpsert,
    RentCalibrationOut,
    RentCompOut,
    RentCompsBatchIn,
    RentCompsSummaryOut,
    RentExplainOut,
    RentObservationCreate,
    RentObservationOut,
    RentRecomputeOut,
)
from onehaven_platform.backend.src.domain.events import emit_workflow_event
from products.intelligence.backend.src.domain.rent_learning import (
    get_or_create_rent_assumption,
    recompute_rent_fields,
    summarize_comps,
    update_calibration_from_observation,
)
from app.domain.section8.rent_rules import compute_approved_ceiling, compute_rent_used, summarize_nspire_pdf_dataset
from products.intelligence.backend.src.domain.underwriting import describe_rent_cap_reason

router = APIRouter(prefix="/rent", tags=["rent"])


class RentExplainPropertiesIn(BaseModel):
    property_ids: list[int] = Field(default_factory=list)
    strategy: str = "section8"
    payment_standard_pct: float | None = Field(default=None, ge=0.5, le=1.5)
    persist: bool = True


def _norm_strategy(strategy: Optional[str]) -> str:
    s = (strategy or "section8").strip().lower()
    return s if s in {"section8", "market"} else "section8"


def _to_pos_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        f = float(v)
        return f if f > 0 else None
    except Exception:
        return None

def _payment_standard_pct_value(raw: Any) -> float:
    try:
        if raw is None:
            raw = getattr(settings, "default_payment_standard_pct", None)
        if raw is None:
            return 110.0
        value = float(raw)
        if 0 < value <= 3.0:
            return float(value * 100.0)
        return float(value)
    except Exception:
        return 110.0


def _must_get_property(db: Session, *, property_id: int, org_id: int) -> Property:
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != org_id:
        raise HTTPException(status_code=404, detail="property not found")
    return prop


def _build_section8_payload(
    *,
    prop: Property,
    ra: RentAssumption,
    strategy: str,
    payment_standard_pct: float,
) -> dict[str, Any]:
    market = _to_pos_float(getattr(ra, "market_rent_estimate", None))
    section8_fmr = _to_pos_float(getattr(ra, "section8_fmr", None))
    rr = _to_pos_float(getattr(ra, "rent_reasonableness_comp", None))
    manual_override = _to_pos_float(getattr(ra, "approved_rent_ceiling", None))

    approved_ceiling, candidates = compute_approved_ceiling(
        section8_fmr=section8_fmr,
        payment_standard_pct=payment_standard_pct,
        rent_reasonableness_comp=rr,
        manual_override=manual_override,
    )
    rent_decision = compute_rent_used(
        strategy=strategy,
        market=market,
        approved=approved_ceiling,
        candidates=candidates,
    )

    utility_allowance = _to_pos_float(
        getattr(ra, "utility_allowance", None) or getattr(prop, "utility_allowance", None)
    ) or 0.0
    rent_to_owner = rent_decision.rent_used
    gross_rent = None
    if rent_to_owner is not None:
        gross_rent = round(float(rent_to_owner) + float(utility_allowance), 2)

    gross_rent_cap = approved_ceiling
    gross_rent_compliant = None
    if gross_rent is not None and gross_rent_cap is not None:
        gross_rent_compliant = bool(gross_rent <= gross_rent_cap)

    return {
        "strategy": strategy,
        "payment_standard_pct": float(payment_standard_pct),
        "market_rent_estimate": market,
        "section8_fmr": section8_fmr,
        "rent_reasonableness_comp": rr,
        "manual_override": manual_override,
        "approved_rent_ceiling": approved_ceiling,
        "rent_used": rent_decision.rent_used,
        "cap_reason": rent_decision.cap_reason,
        "explanation": rent_decision.explanation,
        "ceiling_candidates": [{"type": c.type, "value": c.value} for c in (rent_decision.candidates or [])],
        "rent_to_owner": rent_to_owner,
        "utility_allowance": utility_allowance,
        "gross_rent": gross_rent,
        "gross_rent_cap": gross_rent_cap,
        "gross_rent_compliant": gross_rent_compliant,
        "bedrooms": int(getattr(prop, "bedrooms", 0) or 0),
        "property_type": getattr(prop, "property_type", None),
        "units": int(getattr(prop, "units", 0) or 0),
    }


def _collect_federal_updates(*, limit: int, include_public_inspection: bool) -> dict[str, Any]:
    updates: list[dict[str, Any]] = []
    errors: list[str] = []

    try:
        fr = FederalRegisterClient(timeout=30.0)
        result = fr.search_documents(
            conditions={
                "agencies": ["housing-and-urban-development-department"],
                "term": "housing choice voucher OR section 8 OR fair market rent OR nspire",
            },
            per_page=max(1, min(limit, 50)),
            page=1,
            order="newest",
        )
        for row in result.get("results", []) or []:
            updates.append({
                "source": "federal_register",
                "document_number": row.get("document_number"),
                "title": row.get("title"),
                "type": row.get("type"),
                "publication_date": row.get("publication_date"),
                "effective_on": row.get("effective_on"),
                "html_url": row.get("html_url"),
                "pdf_url": row.get("pdf_url"),
                "citation": row.get("citation"),
                "agencies": [a.get("name") for a in (row.get("agencies") or []) if isinstance(a, dict)],
            })
    except Exception as e:
        errors.append(f"FederalRegisterClient search failed: {e}")

    if include_public_inspection:
        try:
            fr = FederalRegisterClient(timeout=30.0)
            pi = fr.current_public_inspection()
            docs = pi.get("results") or pi.get("documents") or []
            for row in docs[: max(1, min(limit, 20))]:
                agencies = row.get("agencies") or []
                names = [a.get("name") for a in agencies if isinstance(a, dict)]
                agency_text = " ".join(names).lower()
                title_text = str(row.get("title") or "").lower()
                if (
                    "housing" in agency_text
                    or "urban development" in agency_text
                    or "voucher" in title_text
                    or "section 8" in title_text
                    or "fair market rent" in title_text
                    or "nspire" in title_text
                ):
                    updates.append({
                        "source": "federal_register_public_inspection",
                        "document_number": row.get("document_number"),
                        "title": row.get("title"),
                        "type": row.get("type"),
                        "publication_date": row.get("publication_date"),
                        "effective_on": row.get("effective_on"),
                        "html_url": row.get("html_url"),
                        "pdf_url": row.get("pdf_url"),
                        "citation": row.get("citation"),
                        "agencies": names,
                    })
        except Exception as e:
            errors.append(f"FederalRegisterClient public inspection failed: {e}")

    try:
        gov = GovInfoClient(timeout=30.0)
        gov_search = gov.search_collections(
            collections=["FR", "CFR"],
            query='("housing choice voucher" OR "section 8" OR "fair market rent" OR NSPIRE OR HUD)',
            page_size=max(1, min(limit, 20)),
            offset_mark="*",
        )
        packages = gov_search.get("packages") or gov_search.get("results") or []
        for row in packages:
            updates.append({
                "source": "govinfo",
                "package_id": row.get("packageId") or row.get("package_id"),
                "title": row.get("title"),
                "collection_code": row.get("collectionCode") or row.get("collection_code"),
                "date_issued": row.get("dateIssued") or row.get("date_issued"),
                "last_modified": row.get("lastModified") or row.get("last_modified"),
                "link": row.get("packageLink") or row.get("download") or row.get("granuleLink"),
            })
    except Exception as e:
        errors.append(f"GovInfoClient search failed: {e}")

    def _sort_key(row: dict[str, Any]) -> tuple[str, str]:
        return (
            str(row.get("publication_date") or row.get("date_issued") or row.get("last_modified") or ""),
            str(row.get("title") or ""),
        )

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in sorted(updates, key=_sort_key, reverse=True):
        key = str(row.get("document_number") or row.get("package_id") or row.get("title") or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return {"ok": True, "count": len(deduped[:limit]), "results": deduped[:limit], "errors": errors, "nspire_pdf_catalog": summarize_nspire_pdf_dataset()}


def _audit(
    db: Session,
    *,
    org_id: int,
    actor_user_id: Optional[int],
    action: str,
    entity_type: str,
    entity_id: str,
    before: Optional[dict],
    after: Optional[dict],
) -> None:
    ev = AuditEvent(
        org_id=org_id,
        actor_user_id=actor_user_id,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        before_json=json.dumps(before) if before is not None else None,
        after_json=json.dumps(after) if after is not None else None,
        created_at=datetime.utcnow(),
    )
    db.add(ev)


def _persist_rent_explain_run(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    strategy: str,
    cap_reason: str,
    payment_standard_pct_used: float,
    explain_payload: dict,
) -> RentExplainRun:
    run = RentExplainRun(
        org_id=org_id,
        property_id=property_id,
        strategy=strategy,
        cap_reason=str(cap_reason),
        explain_json=json.dumps(explain_payload, sort_keys=True),
        decision_version=str(getattr(settings, "decision_version", "unknown")),
        payment_standard_pct_used=float(payment_standard_pct_used),
        created_at=datetime.utcnow(),
    )
    db.add(run)
    db.flush()
    return run


@router.get("/{property_id}", response_model=RentAssumptionOut)
def get_rent_assumption(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    ra = db.execute(
        select(RentAssumption)
        .where(RentAssumption.property_id == property_id)
        .where(RentAssumption.org_id == p.org_id)
    ).scalar_one_or_none()

    if not ra:
        raise HTTPException(status_code=404, detail="rent assumption not found")
    return ra


@router.get("/assumption/{property_id}", response_model=RentAssumptionOut)
def get_rent_assumption_alias(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    return get_rent_assumption(property_id=property_id, db=db, p=p)


@router.post("/{property_id}", response_model=RentAssumptionOut)
def upsert_rent_assumption(
    property_id: int,
    payload: RentAssumptionUpsert,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, property_id)
    ra.org_id = p.org_id

    before = {
        "market_rent_estimate": ra.market_rent_estimate,
        "section8_fmr": ra.section8_fmr,
        "rent_reasonableness_comp": ra.rent_reasonableness_comp,
        "approved_rent_ceiling": ra.approved_rent_ceiling,
        "rent_used": ra.rent_used,
    }

    for k, v in payload.model_dump(exclude_unset=True).items():
        setattr(ra, k, v)

    after = {
        "market_rent_estimate": ra.market_rent_estimate,
        "section8_fmr": ra.section8_fmr,
        "rent_reasonableness_comp": ra.rent_reasonableness_comp,
        "approved_rent_ceiling": ra.approved_rent_ceiling,
        "rent_used": ra.rent_used,
    }

    if before.get("approved_rent_ceiling") != after.get("approved_rent_ceiling"):
        _audit(
            db,
            org_id=p.org_id,
            actor_user_id=p.user_id,
            action="rent_override_set",
            entity_type="rent_assumption",
            entity_id=str(property_id),
            before={"approved_rent_ceiling": before.get("approved_rent_ceiling")},
            after={"approved_rent_ceiling": after.get("approved_rent_ceiling")},
        )

    db.add(ra)
    db.commit()
    db.refresh(ra)
    return ra


@router.post("/assumption/{property_id}", response_model=RentAssumptionOut)
def upsert_rent_assumption_alias(
    property_id: int,
    payload: RentAssumptionUpsert,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return upsert_rent_assumption(property_id=property_id, payload=payload, db=db, p=p)


@router.post("/comps/{property_id}", response_model=RentCompsSummaryOut)
def add_comps_batch(property_id: int, payload: RentCompsBatchIn, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, property_id)
    ra.org_id = p.org_id

    rents: List[float] = []
    for c in payload.comps:
        comp = RentComp(
            property_id=property_id,
            source=c.source,
            address=c.address,
            url=c.url,
            rent=float(c.rent),
            bedrooms=c.bedrooms,
            bathrooms=c.bathrooms,
            square_feet=c.square_feet,
            notes=c.notes,
            created_at=datetime.utcnow(),
        )
        db.add(comp)
        rents.append(float(c.rent))

    summary = summarize_comps(rents)
    ra.rent_reasonableness_comp = summary.median_rent
    db.add(ra)
    db.commit()

    return RentCompsSummaryOut(
        property_id=property_id,
        count=summary.count,
        median_rent=summary.median_rent,
        mean_rent=summary.mean_rent,
        min_rent=summary.min_rent,
        max_rent=summary.max_rent,
    )


@router.get("/comps/{property_id}", response_model=list[RentCompOut])
def list_comps(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    rows = (
        db.execute(
            select(RentComp)
            .where(RentComp.property_id == property_id)
            .order_by(RentComp.created_at.desc(), RentComp.id.desc())
        )
        .scalars()
        .all()
    )
    return rows


@router.post("/observe", response_model=RentObservationOut)
def add_rent_observation(payload: RentObservationCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, payload.property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, payload.property_id)
    ra.org_id = p.org_id

    strategy = _norm_strategy(payload.strategy)

    obs = RentObservation(
        property_id=payload.property_id,
        strategy=strategy,
        achieved_rent=float(payload.achieved_rent),
        tenant_portion=payload.tenant_portion,
        hap_portion=payload.hap_portion,
        lease_start=payload.lease_start,
        lease_end=payload.lease_end,
        notes=payload.notes,
        created_at=datetime.utcnow(),
    )
    db.add(obs)

    update_calibration_from_observation(
        db,
        property_row=prop,
        strategy=strategy,
        predicted_market_rent=ra.market_rent_estimate,
        achieved_rent=float(payload.achieved_rent),
    )

    db.commit()
    db.refresh(obs)
    return obs


@router.get("/calibration", response_model=list[RentCalibrationOut])
def list_calibration(
    zip: str | None = Query(default=None),
    bedrooms: int | None = Query(default=None),
    strategy: str | None = Query(default=None),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = select(RentCalibration).order_by(RentCalibration.updated_at.desc())
    if zip:
        q = q.where(RentCalibration.zip == zip)
    if bedrooms is not None:
        q = q.where(RentCalibration.bedrooms == bedrooms)
    if strategy:
        q = q.where(RentCalibration.strategy == _norm_strategy(strategy))
    return db.execute(q).scalars().all()


@router.post("/recompute/{property_id}", response_model=RentRecomputeOut)
def recompute(
    property_id: int,
    strategy: str = Query(default="section8"),
    payment_standard_pct: float | None = Query(default=None, ge=0.5, le=1.5),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    strategy = _norm_strategy(strategy)
    pct = float(payment_standard_pct) if payment_standard_pct is not None else float(settings.default_payment_standard_pct)

    computed = recompute_rent_fields(
        db,
        property_id=property_id,
        strategy=strategy,
        payment_standard_pct=pct,
    )

    ra = db.execute(
        select(RentAssumption).where(
            RentAssumption.property_id == property_id,
            RentAssumption.org_id == p.org_id,
        )
    ).scalar_one_or_none()
    if not ra:
        raise HTTPException(status_code=404, detail="rent assumption not found")

    computed_ceiling = _to_pos_float(computed.get("approved_rent_ceiling"))
    computed_rent_used = computed.get("rent_used", None)
    computed_cap_reason = str(computed.get("rent_cap_reason") or "missing_rent_inputs")

    if computed_ceiling is not None:
        ra.approved_rent_ceiling = float(computed_ceiling)
    else:
        ra.approved_rent_ceiling = None

    ra.rent_used = float(computed_rent_used) if computed_rent_used is not None else None
    if hasattr(ra, "rent_cap_reason"):
        setattr(ra, "rent_cap_reason", computed_cap_reason)

    db.add(ra)
    db.commit()
    db.refresh(ra)

    return RentRecomputeOut(
        property_id=property_id,
        market_rent_estimate=ra.market_rent_estimate,
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        approved_rent_ceiling=ra.approved_rent_ceiling,
        calibrated_market_rent=computed.get("calibrated_market_rent"),
        strategy=strategy,
        rent_used=ra.rent_used,
    )


@router.post("/explain/properties", response_model=dict)
def explain_rent_properties(
    payload: RentExplainPropertiesIn = Body(...),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    strategy = _norm_strategy(payload.strategy)
    pct = float(payload.payment_standard_pct) if payload.payment_standard_pct is not None else float(settings.default_payment_standard_pct)

    seen: set[int] = set()
    property_ids: list[int] = []
    for pid in payload.property_ids:
        if int(pid) in seen:
            continue
        seen.add(int(pid))
        property_ids.append(int(pid))

    attempted = len(property_ids)
    explained = 0
    errors: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []

    for pid in property_ids:
        try:
            out = explain_rent(
                property_id=int(pid),
                strategy=strategy,
                payment_standard_pct=pct,
                persist=payload.persist,
                db=db,
                p=p,
            )
            explained += 1
            results.append(
                {
                    "property_id": int(pid),
                    "run_id": int(out.run_id),
                    "rent_used": out.rent_used,
                    "approved_rent_ceiling": out.approved_rent_ceiling,
                    "cap_reason": out.cap_reason,
                }
            )
        except Exception as e:
            errors.append({"property_id": int(pid), "error": f"{type(e).__name__}: {e}"})

    return {
        "ok": True,
        "strategy": strategy,
        "attempted": attempted,
        "explained": explained,
        "property_ids": property_ids,
        "results": results,
        "errors": errors,
    }


@router.get("/explain/{property_id}", response_model=RentExplainOut)
def explain_rent(
    property_id: int,
    strategy: str = Query("section8"),
    payment_standard_pct: float | None = Query(default=None, ge=0.5, le=1.5),
    persist: bool = Query(default=True, description="If true, persist rent_used/approved ceiling when appropriate"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, property_id)
    ra.org_id = p.org_id

    strategy = _norm_strategy(strategy)
    pct = float(payment_standard_pct) if payment_standard_pct is not None else float(settings.default_payment_standard_pct)

    computed = recompute_rent_fields(
        db,
        property_id=property_id,
        strategy=strategy,
        payment_standard_pct=pct,
    )

    fmr_adjusted = _to_pos_float(computed.get("approved_rent_ceiling"))
    market = _to_pos_float(computed.get("calibrated_market_rent") or ra.market_rent_estimate)
    approved = _to_pos_float(computed.get("approved_rent_ceiling"))
    rent_used = computed.get("rent_used", None)
    cap_reason = str(computed.get("rent_cap_reason") or "missing_rent_inputs")
    explanation = str(computed.get("explanation") or describe_rent_cap_reason(cap_reason, strategy=strategy))

    ceiling_candidates: list[dict] = []
    if approved is not None:
        ceiling_candidates.append({"type": "approved_fmr_ceiling", "value": float(approved)})
    rr = _to_pos_float(ra.rent_reasonableness_comp)
    if rr is not None:
        ceiling_candidates.append({"type": "rent_reasonableness_comp", "value": float(rr)})

    explain_payload = {
        "property_id": property_id,
        "strategy": strategy,
        "payment_standard_pct": float(pct),
        "fmr_adjusted": fmr_adjusted,
        "market_rent_estimate": market,
        "section8_fmr": ra.section8_fmr,
        "rent_reasonableness_comp": ra.rent_reasonableness_comp,
        "approved_rent_ceiling": approved,
        "rent_used": rent_used,
        "ceiling_candidates": ceiling_candidates,
        "explanation": explanation,
        "cap_reason": cap_reason,
    }
    run = _persist_rent_explain_run(
        db,
        org_id=p.org_id,
        property_id=property_id,
        strategy=strategy,
        cap_reason=cap_reason,
        payment_standard_pct_used=float(pct),
        explain_payload=explain_payload,
    )

    if persist:
        ra.rent_used = float(rent_used) if rent_used is not None else None
        ra.approved_rent_ceiling = float(approved) if approved is not None else None
        if hasattr(ra, "rent_cap_reason"):
            setattr(ra, "rent_cap_reason", cap_reason)
        db.add(ra)

    emit_workflow_event(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="rent_explained",
        property_id=property_id,
        payload={"property_id": property_id, "run_id": int(run.id), "strategy": strategy},
    )

    db.commit()

    return RentExplainOut(
        property_id=property_id,
        strategy=strategy,
        payment_standard_pct=float(pct),
        fmr_adjusted=fmr_adjusted,
        market_rent_estimate=market,
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        approved_rent_ceiling=approved,
        calibrated_market_rent=computed.get("calibrated_market_rent"),
        rent_used=rent_used,
        ceiling_candidates=ceiling_candidates,
        cap_reason=cap_reason,
        explanation=explanation,
        run_id=int(run.id),
        created_at=run.created_at,
    )


@router.get("/section8/compliance/{property_id}", response_model=dict)
def section8_compliance(
    property_id: int,
    strategy: str = Query(default="section8"),
    payment_standard_pct: float | None = Query(default=None, ge=0.5, le=1.5),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = _must_get_property(db, property_id=property_id, org_id=p.org_id)
    ra = db.execute(
        select(RentAssumption).where(
            RentAssumption.property_id == property_id,
            RentAssumption.org_id == p.org_id,
        )
    ).scalar_one_or_none()
    if not ra:
        raise HTTPException(status_code=404, detail="rent assumption not found")

    strategy = _norm_strategy(strategy)
    payment_pct = _payment_standard_pct_value(payment_standard_pct)
    payload = _build_section8_payload(
        prop=prop,
        ra=ra,
        strategy=strategy,
        payment_standard_pct=payment_pct,
    )

    compliant = bool(payload.get("gross_rent_compliant")) if payload.get("gross_rent_compliant") is not None else None
    status = "compliant" if compliant is True else "non_compliant" if compliant is False else "insufficient_data"

    return {
        "ok": True,
        "property_id": int(property_id),
        "status": status,
        **payload,
        "property": {
            "address": getattr(prop, "address", None),
            "city": getattr(prop, "city", None),
            "state": getattr(prop, "state", None),
            "zip": getattr(prop, "zip", None),
            "bedrooms": getattr(prop, "bedrooms", None),
            "units": getattr(prop, "units", None),
            "property_type": getattr(prop, "property_type", None),
        },
        "missing_inputs": [
            key
            for key in ("market_rent_estimate", "section8_fmr", "rent_reasonableness_comp")
            if payload.get(key) is None
        ],
        "nspire_pdf_catalog": summarize_nspire_pdf_dataset(),
    }


@router.get("/federal-updates", response_model=dict)
def federal_updates(
    limit: int = Query(default=10, ge=1, le=50),
    include_public_inspection: bool = Query(default=True),
    _db: Session = Depends(get_db),
    _p=Depends(get_principal),
):
    return _collect_federal_updates(
        limit=limit,
        include_public_inspection=include_public_inspection,
    )
