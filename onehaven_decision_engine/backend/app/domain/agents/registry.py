# backend/app/domain/agents/registry.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ...models import Property, Deal, RentComp, Inspection, PropertyState
from ...policy_models import JurisdictionProfile
from ...services.policy_seed import ensure_policy_seeded
from ...services.hud_fmr_service import get_cached_fmr
from ..compliance.hqs_library import load_hqs_items


@dataclass(frozen=True)
class AgentContext:
    org_id: int
    property_id: int
    run_id: int


def _loads(s: Optional[str]):
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def _json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _get_property(db: Session, *, org_id: int, property_id: int) -> Property:
    p = db.scalar(select(Property).where(Property.org_id == org_id).where(Property.id == property_id))
    if p is None:
        raise ValueError("property not found")
    return p


def _pick_jurisdiction_profile(db: Session, *, org_id: int, p: Property) -> Optional[JurisdictionProfile]:
    # deterministic matching: prefer exact city, else zip_prefix match
    q = select(JurisdictionProfile).where(JurisdictionProfile.org_id == org_id).order_by(JurisdictionProfile.effective_date.desc())
    rows = list(db.scalars(q).all())
    if not rows:
        return None

    city = (p.city or "").strip().lower()
    zp = (p.zip or "").strip()

    def score(j: JurisdictionProfile) -> int:
        s = 0
        if j.city and j.city.strip().lower() == city:
            s += 10
        if j.zip_prefix and zp.startswith(j.zip_prefix):
            s += 6
        if j.county and j.county.strip().lower() in (j.county or "").strip().lower():
            s += 1
        return s

    rows.sort(key=score, reverse=True)
    return rows[0] if score(rows[0]) > 0 else rows[0]


def agent_deal_intake(db: Session, ctx: AgentContext) -> dict:
    ensure_policy_seeded(db, org_id=ctx.org_id)

    p = _get_property(db, org_id=ctx.org_id, property_id=ctx.property_id)
    d = db.scalar(
        select(Deal)
        .where(Deal.org_id == ctx.org_id)
        .where(Deal.property_id == ctx.property_id)
        .order_by(Deal.id.desc())
    )

    jp = _pick_jurisdiction_profile(db, org_id=ctx.org_id, p=p)

    missing = []
    if not p.address: missing.append("address")
    if not p.city: missing.append("city")
    if not p.zip: missing.append("zip")
    if not p.bedrooms: missing.append("bedrooms")

    if d is None:
        missing.append("deal_record")

    disqualifiers = []
    # Keep this deterministic: point at your constitution rules without duplicating them here.
    if p.bedrooms < 2:
        disqualifiers.append("Bedrooms below constitution min_bedrooms (default 2).")

    actions = []
    actions.append(
        {
            "entity_type": "WorkflowEvent",
            "op": "recommend",
            "data": {
                "event_type": "agent.deal_intake",
                "payload": {
                    "missing_fields": missing,
                    "disqualifiers": disqualifiers,
                    "jurisdiction_profile_key": getattr(jp, "key", None),
                    "suggested_next_steps": [
                        "Confirm serving PHA + payment standard policy",
                        "Confirm utilities (tenant-paid vs owner-paid)",
                        "Run rent_reasonableness after adding comps or FMR cache",
                        "If proceeding: start packet_builder checklist now",
                    ],
                },
            },
        }
    )

    return {
        "summary": f"Deal intake completed for {p.address}, {p.city}. Missing={len(missing)} disqualifiers={len(disqualifiers)}.",
        "actions": actions,
    }


def agent_public_records_check(db: Session, ctx: AgentContext) -> dict:
    p = _get_property(db, org_id=ctx.org_id, property_id=ctx.property_id)

    # Deterministic: we do not scrape. We create follow-ups to attach parcel/taxes/ownership.
    payload = {
        "property": {"address": p.address, "city": p.city, "zip": p.zip},
        "needs": [
            {"item": "Parcel ID", "why": "Tie taxes/assessed value/ownership to the asset record."},
            {"item": "Tax amount (annual)", "why": "Underwriting correctness; avoids fantasy margins."},
            {"item": "Owner of record / deed", "why": "Prevents wholesaler confusion and title surprises."},
            {"item": "Year built / permit flags", "why": "HQS risk + rehab realism."},
        ],
        "how_to_fill": "Add fields via UI or import a county export. Later: integrate a paid public-record API behind rate limits.",
    }

    return {
        "summary": "Public records check generated required evidence list (no external calls).",
        "actions": [{"entity_type": "WorkflowEvent", "op": "recommend", "data": {"event_type": "agent.public_records_check", "payload": payload}}],
    }


def agent_rent_reasonableness(db: Session, ctx: AgentContext) -> dict:
    p = _get_property(db, org_id=ctx.org_id, property_id=ctx.property_id)
    comps = list(
        db.scalars(
            select(RentComp)
            .where(RentComp.property_id == ctx.property_id)
            .order_by(RentComp.created_at.desc())
            .limit(10)
        ).all()
    )

    jp = _pick_jurisdiction_profile(db, org_id=ctx.org_id, p=p)

    # HUD FMR cache anchor (may be missing)
    # area_name here is taken from jurisdiction notes until you standardize it.
    area_name = "Detroit-Warren-Dearborn, MI HUD Metro FMR Area" if (p.city or "").lower() == "detroit" else "Default MI Area"
    year = 2026
    beds = int(p.bedrooms)

    fmr_res = get_cached_fmr(db, state=p.state, area_name=area_name, year=year, bedrooms=beds)

    # deterministic comp math: simple similarity scoring
    def comp_score(c: RentComp) -> float:
        s = 0.0
        if c.bedrooms is not None and int(c.bedrooms) == int(p.bedrooms):
            s += 2.0
        if c.bathrooms is not None and abs(float(c.bathrooms) - float(p.bathrooms)) <= 0.5:
            s += 1.0
        if c.square_feet is not None and p.square_feet is not None:
            if abs(int(c.square_feet) - int(p.square_feet)) <= 250:
                s += 1.0
        return s

    comps_sorted = sorted(comps, key=comp_score, reverse=True)
    top = comps_sorted[:5]

    comp_rents = [float(c.rent) for c in top] if top else []
    comp_min = min(comp_rents) if comp_rents else None
    comp_max = max(comp_rents) if comp_rents else None
    comp_avg = (sum(comp_rents) / len(comp_rents)) if comp_rents else None

    # recommended rent: prefer comps avg; else fall back to FMR * payment_standard_pct
    payment_pct = float(getattr(jp, "payment_standard_pct", None) or 1.10)
    fmr_based = (float(fmr_res.fmr) * payment_pct) if fmr_res.ok and fmr_res.fmr is not None else None
    recommended = comp_avg or fmr_based

    # Regulatory comparability factors you must always mention (even in v1)
    factors = [
        "location",
        "quality/condition",
        "unit size",
        "unit type",
        "age of unit",
        "amenities",
        "services/maintenance",
        "utilities included",
    ]

    payload = {
        "jurisdiction_profile_key": getattr(jp, "key", None),
        "hud_anchor": {"ok": fmr_res.ok, "fmr": fmr_res.fmr, "reason": fmr_res.reason, "area_name": area_name, "year": year, "bedrooms": beds},
        "comps_used": [{"address": c.address, "rent": c.rent, "beds": c.bedrooms, "baths": c.bathrooms, "sqft": c.square_feet, "url": c.url} for c in top],
        "comp_range": {"min": comp_min, "max": comp_max, "avg": comp_avg},
        "recommended_gross_rent": recommended,
        "comparability_factors_considered": factors,
        "notes": "Deterministic v1: uses internal comps + cached FMR anchor. Add utility allowance logic next.",
        "needs": [] if recommended is not None else ["Add comps OR refresh HUD FMR cache for this area/year/bedroom."],
    }

    return {
        "summary": f"Rent reasonableness computed. Recommended={recommended}. Comps_used={len(top)}. HUD_cache={fmr_res.reason}.",
        "actions": [{"entity_type": "WorkflowEvent", "op": "recommend", "data": {"event_type": "agent.rent_reasonableness", "payload": payload}}],
    }


def agent_hqs_precheck(db: Session, ctx: AgentContext) -> dict:
    p = _get_property(db, org_id=ctx.org_id, property_id=ctx.property_id)
    jp = _pick_jurisdiction_profile(db, org_id=ctx.org_id, p=p)

    items = load_hqs_items(db, org_id=ctx.org_id, jurisdiction_profile_id=getattr(jp, "id", None))

    # read latest inspection failures (if any)
    latest_insp = db.scalar(
        select(Inspection)
        .where(Inspection.property_id == ctx.property_id)
        .order_by(Inspection.inspected_at.desc())
    )
    prev_failures = []
    if latest_insp is not None:
        raw = _loads(getattr(latest_insp, "results_json", None)) or {}
        prev_failures = list((raw.get("failed_codes") or [])) if isinstance(raw, dict) else []

    # deterministic "likely fails" heuristics: prioritize historical failures, then high-severity safety items
    likely = []
    for it in items:
        if it.code in prev_failures and it.severity == "fail":
            likely.append({"code": it.code, "why": "previous inspection failure"})
    if not likely:
        for it in items:
            if it.category in {"safety", "electrical"} and it.severity == "fail":
                likely.append({"code": it.code, "why": "high-impact category"})
        likely = likely[:5]

    payload = {
        "jurisdiction_profile_key": getattr(jp, "key", None),
        "checklist_total": len(items),
        "likely_fail_points": likely,
        "evidence_pack": [
            "Photos of detectors/GFCIs/panel covers",
            "Heat running video + thermostat photo",
            "Exterior steps/handrails photos",
            "Under-sink plumbing photos",
        ],
        "note": "This is a real HQS library overlay system. Expand rules + addendum as you verify local requirements.",
    }

    return {
        "summary": f"HQS precheck generated {len(items)} checklist items and {len(likely)} likely fail points.",
        "actions": [{"entity_type": "WorkflowEvent", "op": "recommend", "data": {"event_type": "agent.hqs_precheck", "payload": payload}}],
    }


def agent_packet_builder(db: Session, ctx: AgentContext) -> dict:
    p = _get_property(db, org_id=ctx.org_id, property_id=ctx.property_id)
    jp = _pick_jurisdiction_profile(db, org_id=ctx.org_id, p=p)

    packet = _loads(getattr(jp, "packet_requirements_json", None)) if jp else None
    packet_items = (packet or {}).get("packet") if isinstance(packet, dict) else []

    payload = {
        "jurisdiction_profile_key": getattr(jp, "key", None),
        "packet_items": packet_items or [],
        "missing_policy_warning": jp is None,
        "notes": "Packet requirements are jurisdiction-versioned. Add sources + last_verified_at as you confirm.",
    }
    return {
        "summary": f"Packet builder produced {len(packet_items or [])} required docs checklist items.",
        "actions": [{"entity_type": "WorkflowEvent", "op": "recommend", "data": {"event_type": "agent.packet_builder", "payload": payload}}],
    }


def agent_timeline_nudger(db: Session, ctx: AgentContext) -> dict:
    st = db.scalar(
        select(PropertyState)
        .where(PropertyState.org_id == ctx.org_id)
        .where(PropertyState.property_id == ctx.property_id)
    )
    outstanding = _loads(getattr(st, "outstanding_tasks_json", None)) if st else []
    outstanding = outstanding if isinstance(outstanding, list) else []

    payload = {
        "outstanding_tasks": outstanding,
        "rules": {
            "behavior": "Turn next_actions into operator tasks. No DB mutation in v1; only recommendations.",
            "anti_spam": f"max {getattr(settings, 'agents_max_runs_per_property_per_hour', 3)} runs/property/hour",
        },
    }
    return {
        "summary": f"Timeline nudger reviewed {len(outstanding)} outstanding tasks and generated followup recommendations.",
        "actions": [{"entity_type": "WorkflowEvent", "op": "recommend", "data": {"event_type": "agent.timeline_nudger", "payload": payload}}],
    }


AGENTS: Dict[str, Callable[[Session, AgentContext], dict]] = {
    "deal_intake": agent_deal_intake,
    "public_records_check": agent_public_records_check,
    "rent_reasonableness": agent_rent_reasonableness,
    "hqs_precheck": agent_hqs_precheck,
    "packet_builder": agent_packet_builder,
    "timeline_nudger": agent_timeline_nudger,
}