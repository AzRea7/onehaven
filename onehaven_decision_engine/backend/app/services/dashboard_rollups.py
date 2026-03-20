from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.models import Deal, Property, UnderwritingResult
from .property_state_machine import get_state_payload, normalize_decision_bucket


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def _asking_price(prop: Property, deal: Optional[Deal]) -> Optional[float]:
    for attr in ("asking_price", "list_price", "price", "offer_price", "purchase_price"):
        if deal is not None and getattr(deal, attr, None) is not None:
            return _safe_float(getattr(deal, attr, None), 0.0)
    for attr in ("asking_price", "list_price", "price"):
        if getattr(prop, attr, None) is not None:
            return _safe_float(getattr(prop, attr, None), 0.0)
    return None


def _latest_deal(db: Session, *, org_id: int, property_id: int) -> Optional[Deal]:
    return db.scalar(
        select(Deal)
        .where(Deal.org_id == org_id, Deal.property_id == property_id)
        .order_by(desc(Deal.updated_at), desc(Deal.id))
        .limit(1)
    )


def _latest_uw(db: Session, *, org_id: int, property_id: int) -> Optional[UnderwritingResult]:
    return db.scalar(
        select(UnderwritingResult)
        .join(Deal, Deal.id == UnderwritingResult.deal_id)
        .where(UnderwritingResult.org_id == org_id, Deal.property_id == property_id)
        .order_by(desc(UnderwritingResult.created_at), desc(UnderwritingResult.id))
        .limit(1)
    )


def compute_rollups(
    db: Session,
    *,
    org_id: int,
    days: int = 90,
    limit: int = 500,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    q: Optional[str] = None,
    stage: Optional[str] = None,
    decision: Optional[str] = None,
    only_red_zone: bool = False,
    exclude_red_zone: bool = False,
    min_crime_score: Optional[float] = None,
    max_crime_score: Optional[float] = None,
    min_offender_count: Optional[int] = None,
    max_offender_count: Optional[int] = None,
) -> dict[str, Any]:
    stmt = select(Property).where(Property.org_id == org_id)

    if state:
        stmt = stmt.where(Property.state == state)
    if county:
        stmt = stmt.where(func.lower(Property.county) == county.lower())
    if city:
        stmt = stmt.where(func.lower(Property.city) == city.lower())

    if q:
        like = f"%{q.strip().lower()}%"
        stmt = stmt.where(
            func.lower(
                func.concat(
                    Property.address,
                    " ",
                    Property.city,
                    " ",
                    Property.state,
                    " ",
                    Property.zip,
                )
            ).like(like)
        )

    if only_red_zone:
        stmt = stmt.where(Property.is_red_zone.is_(True))
    elif exclude_red_zone:
        stmt = stmt.where((Property.is_red_zone.is_(False)) | (Property.is_red_zone.is_(None)))

    if min_crime_score is not None:
        stmt = stmt.where(Property.crime_score.is_not(None))
        stmt = stmt.where(Property.crime_score >= float(min_crime_score))

    if max_crime_score is not None:
        stmt = stmt.where(Property.crime_score.is_not(None))
        stmt = stmt.where(Property.crime_score <= float(max_crime_score))

    if min_offender_count is not None:
        stmt = stmt.where(Property.offender_count.is_not(None))
        stmt = stmt.where(Property.offender_count >= int(min_offender_count))

    if max_offender_count is not None:
        stmt = stmt.where(Property.offender_count.is_not(None))
        stmt = stmt.where(Property.offender_count <= int(max_offender_count))

    props = list(db.scalars(stmt.order_by(desc(Property.id)).limit(max(int(limit), 500))).all())

    decision_counts: dict[str, int] = defaultdict(int)
    stage_counts: dict[str, int] = defaultdict(int)
    county_counts: dict[str, int] = defaultdict(int)

    total_asking = 0.0
    total_cashflow = 0.0
    total_dscr = 0.0
    cashflow_count = 0
    dscr_count = 0

    rows: list[dict[str, Any]] = []

    wanted_decision = normalize_decision_bucket(decision) if decision else None
    wanted_stage = (stage or "").strip().lower() or None

    for prop in props:
        state_payload = get_state_payload(db, org_id=org_id, property_id=int(prop.id), recompute=True)
        current_stage = str(state_payload.get("current_stage") or "deal")
        normalized_decision = str(state_payload.get("normalized_decision") or "REVIEW")

        if wanted_decision and normalized_decision != wanted_decision:
            continue
        if wanted_stage and current_stage != wanted_stage:
            continue

        deal_row = _latest_deal(db, org_id=org_id, property_id=int(prop.id))
        uw_row = _latest_uw(db, org_id=org_id, property_id=int(prop.id))

        asking_price = _asking_price(prop, deal_row)
        projected_cashflow = _safe_float(getattr(uw_row, "cash_flow", None), 0.0) if uw_row else None
        dscr_value = _safe_float(getattr(uw_row, "dscr", None), 0.0) if uw_row else None

        decision_counts[normalized_decision] += 1
        stage_counts[current_stage] += 1
        county_counts[str(getattr(prop, "county", None) or "unknown")] += 1

        if asking_price is not None:
            total_asking += asking_price
        if projected_cashflow is not None:
            total_cashflow += projected_cashflow
            cashflow_count += 1
        if dscr_value is not None:
            total_dscr += dscr_value
            dscr_count += 1

        rows.append(
            {
                "property_id": int(prop.id),
                "address": getattr(prop, "address", None),
                "city": getattr(prop, "city", None),
                "state": getattr(prop, "state", None),
                "county": getattr(prop, "county", None),
                "asking_price": asking_price,
                "normalized_decision": normalized_decision,
                "current_stage": current_stage,
                "gate_status": state_payload.get("gate_status"),
                "projected_monthly_cashflow": projected_cashflow,
                "dscr": dscr_value,
                "crime_score": getattr(prop, "crime_score", None),
                "crime_label": state_payload.get("constraints", {}).get("crime_label"),
                "stage_completion_summary": state_payload.get("stage_completion_summary"),
                "next_actions": state_payload.get("next_actions") or [],
            }
        )

    total_properties = len(rows)

    return {
        "ok": True,
        "as_of": None,
        "window_days": int(days),
        "filters": {
            "state": state,
            "county": county,
            "city": city,
            "q": q,
            "stage": wanted_stage,
            "decision": wanted_decision,
            "only_red_zone": only_red_zone,
            "exclude_red_zone": exclude_red_zone,
            "min_crime_score": min_crime_score,
            "max_crime_score": max_crime_score,
            "min_offender_count": min_offender_count,
            "max_offender_count": max_offender_count,
            "limit": limit,
        },
        "summary": {
            "property_count": total_properties,
            "good_count": _safe_int(decision_counts.get("GOOD")),
            "review_count": _safe_int(decision_counts.get("REVIEW")),
            "reject_count": _safe_int(decision_counts.get("REJECT")),
            "avg_asking_price": round(total_asking / total_properties, 2) if total_properties else 0.0,
            "avg_projected_monthly_cashflow": round(total_cashflow / cashflow_count, 2) if cashflow_count else 0.0,
            "avg_dscr": round(total_dscr / dscr_count, 3) if dscr_count else 0.0,
        },
        "kpis": {
            "total_homes": total_properties,
            "good_deals": _safe_int(decision_counts.get("GOOD")),
            "review_deals": _safe_int(decision_counts.get("REVIEW")),
            "rejected_deals": _safe_int(decision_counts.get("REJECT")),
            "avg_crime_score": round(
                sum(_safe_float(r.get("crime_score")) for r in rows if r.get("crime_score") is not None)
                / max(1, len([r for r in rows if r.get("crime_score") is not None])),
                2,
            )
            if any(r.get("crime_score") is not None for r in rows)
            else None,
            "avg_dscr": round(total_dscr / dscr_count, 3) if dscr_count else None,
            "avg_cashflow_estimate": round(total_cashflow / cashflow_count, 2) if cashflow_count else None,
        },
        "counts": {
            "properties": total_properties,
            "deals": total_properties,
            "rehab_tasks_total": 0,
            "rehab_tasks_open": 0,
            "transactions_window": 0,
            "valuations": 0,
        },
        "buckets": {
            "decisions": {k: int(v) for k, v in decision_counts.items()},
            "stages": {k: int(v) for k, v in stage_counts.items()},
            "counties": {k: int(v) for k, v in county_counts.items()},
        },
        "stage_counts": {k: int(v) for k, v in stage_counts.items()},
        "charts": {
            "decision_mix": [
                {"key": key, "label": key.title(), "value": int(value)}
                for key, value in sorted(decision_counts.items(), key=lambda item: item[0])
            ],
            "stage_mix": [
                {"key": key, "label": key.title(), "value": int(value)}
                for key, value in sorted(stage_counts.items(), key=lambda item: item[0])
            ],
            "county_mix": [
                {"key": key, "label": key, "value": int(value)}
                for key, value in sorted(county_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
            ],
        },
        "series": {
            "decision_mix": [
                {"key": key, "label": key.title(), "count": int(value)}
                for key, value in sorted(decision_counts.items(), key=lambda item: item[0])
            ],
            "workflow_mix": [
                {"key": key, "label": key.title(), "count": int(value)}
                for key, value in sorted(stage_counts.items(), key=lambda item: item[0])
            ],
            "county_mix": [
                {"key": key, "label": key, "count": int(value)}
                for key, value in sorted(county_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
            ],
            "cash_by_month": [],
        },
        "leaderboards": {
            "good_deals": [r for r in rows if r["normalized_decision"] == "GOOD"][:10],
            "cashflow": sorted(rows, key=lambda r: _safe_float(r.get("projected_monthly_cashflow")), reverse=True)[:10],
            "equity": [],
            "rehab_backlog": [],
            "compliance_attention": [r for r in rows if r["current_stage"] == "compliance"][:10],
        },
        "rows": rows[: int(limit)],
        "properties": rows[: int(limit)],
    }