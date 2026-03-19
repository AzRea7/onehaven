from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Optional

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.models import (
    Deal,
    Lease,
    Property,
    PropertyState,
    RehabTask,
    RentAssumption,
    Transaction,
    UnderwritingResult,
    Valuation,
)


def _num(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _as_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v)


def _iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        return v.isoformat()
    except Exception:
        return str(v)


def _month_bucket(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return f"{dt.year:04d}-{dt.month:02d}"
    if isinstance(dt, date):
        return f"{dt.year:04d}-{dt.month:02d}"
    try:
        parsed = datetime.fromisoformat(str(dt))
        return f"{parsed.year:04d}-{parsed.month:02d}"
    except Exception:
        return None


def _month_labels_back(n: int) -> list[str]:
    out: list[str] = []
    cursor = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    for _ in range(max(1, n)):
        out.append(f"{cursor.year:04d}-{cursor.month:02d}")
        if cursor.month == 1:
            cursor = cursor.replace(year=cursor.year - 1, month=12)
        else:
            cursor = cursor.replace(month=cursor.month - 1)
    out.reverse()
    return out


def _lease_is_active(lease: Lease, now: datetime) -> bool:
    start = getattr(lease, "start_date", None)
    end = getattr(lease, "end_date", None)

    if start is None:
        return False

    if isinstance(start, datetime):
        start_ok = start <= now
    elif isinstance(start, date):
        start_ok = start <= now.date()
    else:
        try:
            parsed = datetime.fromisoformat(str(start))
            start_ok = parsed <= now
        except Exception:
            start_ok = False

    if not start_ok:
        return False

    if end is None:
        return True

    if isinstance(end, datetime):
        return end >= now
    if isinstance(end, date):
        return end >= now.date()

    try:
        parsed_end = datetime.fromisoformat(str(end))
        return parsed_end >= now
    except Exception:
        return True


def _normalize_decision(raw: Any) -> str:
    value = _safe_str(raw).strip().upper()
    if value in {"PASS", "GOOD_DEAL", "GOOD", "APPROVE", "APPROVED"}:
        return "GOOD_DEAL"
    if value in {"REJECT", "FAIL", "FAILED", "NO_GO"}:
        return "REJECT"
    return "REVIEW"


def _normalize_stage(raw: Any) -> str:
    value = _safe_str(raw).strip().lower()

    mapping = {
        "deal": "deal",
        "intake": "deal",
        "sourcing": "deal",
        "procurement": "deal",
        "underwriting": "deal",
        "rehab": "rehab",
        "renovation": "rehab",
        "construction": "rehab",
        "compliance": "compliance",
        "inspection": "compliance",
        "licensing": "compliance",
        "voucher": "tenant",
        "tenant": "tenant",
        "lease": "lease",
        "leasing": "lease",
        "management": "management",
        "ops": "management",
        "cash": "cash_equity",
        "cashflow": "cash_equity",
        "equity": "cash_equity",
        "portfolio": "cash_equity",
    }

    return mapping.get(value, "deal")


def _workflow_label(stage_key: str) -> str:
    labels = {
        "deal": "Deal / Procurement",
        "rehab": "Rehab",
        "compliance": "Compliance",
        "tenant": "Tenant Placement",
        "lease": "Lease Activation",
        "management": "Management",
        "cash_equity": "Cashflow / Equity",
    }
    return labels.get(stage_key, "Deal / Procurement")


def _apply_property_filters(
    stmt,
    *,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    q: Optional[str] = None,
    only_red_zone: bool = False,
    exclude_red_zone: bool = False,
    min_crime_score: Optional[float] = None,
    max_crime_score: Optional[float] = None,
    min_offender_count: Optional[int] = None,
    max_offender_count: Optional[int] = None,
):
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

    return stmt


def compute_rollups(
    db: Session,
    *,
    org_id: int,
    days: int = 90,
    limit: int = 50,
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
    now = datetime.utcnow()
    since = now - timedelta(days=int(days))

    base_prop_stmt = select(Property).where(Property.org_id == org_id)
    base_prop_stmt = _apply_property_filters(
        base_prop_stmt,
        state=state,
        county=county,
        city=city,
        q=q,
        only_red_zone=only_red_zone,
        exclude_red_zone=exclude_red_zone,
        min_crime_score=min_crime_score,
        max_crime_score=max_crime_score,
        min_offender_count=min_offender_count,
        max_offender_count=max_offender_count,
    )

    props = db.scalars(
        base_prop_stmt.order_by(desc(Property.updated_at), desc(Property.id)).limit(max(int(limit), 500))
    ).all()

    prop_ids = [int(p.id) for p in props]
    if not prop_ids:
        return {
            "ok": True,
            "as_of": now.isoformat(),
            "window_days": int(days),
            "filters": {
                "state": state,
                "county": county,
                "city": city,
                "q": q,
                "stage": stage,
                "decision": decision,
                "only_red_zone": only_red_zone,
                "exclude_red_zone": exclude_red_zone,
                "min_crime_score": min_crime_score,
                "max_crime_score": max_crime_score,
                "min_offender_count": min_offender_count,
                "max_offender_count": max_offender_count,
            },
            "kpis": {
                "total_homes": 0,
                "good_deals": 0,
                "review_deals": 0,
                "rejected_deals": 0,
                "active_leases": 0,
                "cashflow_positive_homes": 0,
                "homes_with_valuation": 0,
                "red_zone_count": 0,
                "total_estimated_value": 0.0,
                "total_loan_balance": 0.0,
                "total_estimated_equity": 0.0,
                "rehab_open_cost_estimate": 0.0,
                "net_cash_window": 0.0,
                "avg_crime_score": None,
                "avg_dscr": None,
                "avg_cashflow_estimate": None,
            },
            "counts": {
                "properties": 0,
                "deals": 0,
                "rehab_tasks_total": 0,
                "rehab_tasks_open": 0,
                "transactions_window": 0,
                "valuations": 0,
            },
            "buckets": {"decisions": {}, "stages": {}, "counties": {}},
            "stage_counts": {},
            "series": {
                "cash_by_month": [],
                "decision_mix": [],
                "workflow_mix": [],
                "county_mix": [],
            },
            "leaderboards": {
                "good_deals": [],
                "cashflow": [],
                "equity": [],
                "rehab_backlog": [],
                "compliance_attention": [],
            },
            "properties": [],
        }

    stage_rows = db.execute(
        select(PropertyState.property_id, PropertyState.current_stage).where(
            PropertyState.org_id == org_id,
            PropertyState.property_id.in_(prop_ids),
        )
    ).all()
    stage_map: dict[int, str] = {
        int(pid): _normalize_stage(st) for pid, st in stage_rows
    }

    if stage:
        wanted_stage = _normalize_stage(stage)
        props = [p for p in props if stage_map.get(int(p.id), "deal") == wanted_stage]
        prop_ids = [int(p.id) for p in props]

    if not prop_ids:
        return {
            "ok": True,
            "as_of": now.isoformat(),
            "window_days": int(days),
            "filters": {
                "state": state,
                "county": county,
                "city": city,
                "q": q,
                "stage": stage,
                "decision": decision,
                "only_red_zone": only_red_zone,
                "exclude_red_zone": exclude_red_zone,
                "min_crime_score": min_crime_score,
                "max_crime_score": max_crime_score,
                "min_offender_count": min_offender_count,
                "max_offender_count": max_offender_count,
            },
            "kpis": {
                "total_homes": 0,
                "good_deals": 0,
                "review_deals": 0,
                "rejected_deals": 0,
                "active_leases": 0,
                "cashflow_positive_homes": 0,
                "homes_with_valuation": 0,
                "red_zone_count": 0,
                "total_estimated_value": 0.0,
                "total_loan_balance": 0.0,
                "total_estimated_equity": 0.0,
                "rehab_open_cost_estimate": 0.0,
                "net_cash_window": 0.0,
                "avg_crime_score": None,
                "avg_dscr": None,
                "avg_cashflow_estimate": None,
            },
            "counts": {
                "properties": 0,
                "deals": 0,
                "rehab_tasks_total": 0,
                "rehab_tasks_open": 0,
                "transactions_window": 0,
                "valuations": 0,
            },
            "buckets": {"decisions": {}, "stages": {}, "counties": {}},
            "stage_counts": {},
            "series": {
                "cash_by_month": [],
                "decision_mix": [],
                "workflow_mix": [],
                "county_mix": [],
            },
            "leaderboards": {
                "good_deals": [],
                "cashflow": [],
                "equity": [],
                "rehab_backlog": [],
                "compliance_attention": [],
            },
            "properties": [],
        }

    deals = db.scalars(
        select(Deal).where(Deal.org_id == org_id, Deal.property_id.in_(prop_ids))
    ).all()
    deal_ids = [int(d.id) for d in deals]
    deal_by_property: dict[int, list[Deal]] = defaultdict(list)
    for d in deals:
        deal_by_property[int(d.property_id)].append(d)

    latest_deal_by_property: dict[int, Deal] = {}
    for d in sorted(
        deals,
        key=lambda x: (getattr(x, "id", 0),),
        reverse=True,
    ):
        pid = int(d.property_id)
        if pid not in latest_deal_by_property:
            latest_deal_by_property[pid] = d

    underwriting_rows: list[UnderwritingResult] = []
    if deal_ids:
        underwriting_rows = db.scalars(
            select(UnderwritingResult)
            .where(UnderwritingResult.org_id == org_id, UnderwritingResult.deal_id.in_(deal_ids))
            .order_by(desc(UnderwritingResult.created_at), desc(UnderwritingResult.id))
        ).all()

    deal_id_to_property = {int(d.id): int(d.property_id) for d in deals}
    latest_underwriting_by_property: dict[int, UnderwritingResult] = {}
    for uw in underwriting_rows:
        pid = deal_id_to_property.get(int(uw.deal_id))
        if pid is None:
            continue
        if pid not in latest_underwriting_by_property:
            latest_underwriting_by_property[pid] = uw

    valuations = db.scalars(
        select(Valuation)
        .where(Valuation.org_id == org_id, Valuation.property_id.in_(prop_ids))
        .order_by(desc(Valuation.as_of), desc(Valuation.id))
    ).all()
    latest_valuation_by_property: dict[int, Valuation] = {}
    for v in valuations:
        pid = int(v.property_id)
        if pid not in latest_valuation_by_property:
            latest_valuation_by_property[pid] = v

    rehabs = db.scalars(
        select(RehabTask).where(RehabTask.org_id == org_id, RehabTask.property_id.in_(prop_ids))
    ).all()
    rehab_by_property: dict[int, list[RehabTask]] = defaultdict(list)
    for r in rehabs:
        rehab_by_property[int(r.property_id)].append(r)

    txns = db.scalars(
        select(Transaction).where(
            Transaction.org_id == org_id,
            Transaction.property_id.in_(prop_ids),
            Transaction.txn_date >= since,
        )
    ).all()
    txns_by_property: dict[int, list[Transaction]] = defaultdict(list)
    for t in txns:
        txns_by_property[int(t.property_id)].append(t)

    leases = db.scalars(
        select(Lease).where(Lease.org_id == org_id, Lease.property_id.in_(prop_ids))
    ).all()
    active_lease_by_property: dict[int, Lease] = {}
    for lease in sorted(
        leases,
        key=lambda x: (
            getattr(x, "start_date", None) or date.min,
            getattr(x, "id", 0),
        ),
        reverse=True,
    ):
        pid = int(lease.property_id)
        if pid in active_lease_by_property:
            continue
        if _lease_is_active(lease, now):
            active_lease_by_property[pid] = lease

    rent_rows = db.scalars(
        select(RentAssumption).where(
            RentAssumption.org_id == org_id,
            RentAssumption.property_id.in_(prop_ids),
        )
    ).all()
    latest_rent_by_property: dict[int, RentAssumption] = {}
    for rr in sorted(
        rent_rows,
        key=lambda x: (getattr(x, "id", 0),),
        reverse=True,
    ):
        pid = int(rr.property_id)
        if pid not in latest_rent_by_property:
            latest_rent_by_property[pid] = rr

    stage_counts: dict[str, int] = defaultdict(int)
    decision_buckets: dict[str, int] = defaultdict(int)
    county_buckets: dict[str, int] = defaultdict(int)

    cash_month_labels = _month_labels_back(6)
    cash_month_map: dict[str, dict[str, float]] = {
        m: {"label": m, "income": 0.0, "expense": 0.0, "capex": 0.0, "net": 0.0}
        for m in cash_month_labels
    }

    property_rows: list[dict[str, Any]] = []

    total_estimated_value = 0.0
    total_loan_balance = 0.0
    total_estimated_equity = 0.0
    rehab_open_cost_estimate = 0.0
    net_cash_window = 0.0

    good_deals = 0
    review_deals = 0
    rejected_deals = 0
    active_leases_count = 0
    cashflow_positive_homes = 0
    homes_with_valuation = 0
    red_zone_count = 0
    crime_scores: list[float] = []
    dscr_values: list[float] = []
    cashflow_estimates: list[float] = []

    for p in props:
        pid = int(p.id)

        stage_value = stage_map.get(pid, "deal")
        stage_counts[stage_value] += 1

        county_key = _safe_str(getattr(p, "county", None) or "unknown").strip() or "unknown"
        county_buckets[county_key] += 1

        uw = latest_underwriting_by_property.get(pid)
        raw_decision = _safe_str(getattr(uw, "decision", None))
        classification = _normalize_decision(raw_decision)
        decision_buckets[classification] += 1

        if classification == "GOOD_DEAL":
            good_deals += 1
        elif classification == "REVIEW":
            review_deals += 1
        else:
            rejected_deals += 1

        val = latest_valuation_by_property.get(pid)
        estimated_value = _num(getattr(val, "estimated_value", None))
        loan_balance = _num(getattr(val, "loan_balance", None))
        estimated_equity = estimated_value - loan_balance if val is not None else 0.0

        if val is not None:
            homes_with_valuation += 1
            total_estimated_value += estimated_value
            total_loan_balance += loan_balance
            total_estimated_equity += estimated_equity

        if bool(getattr(p, "is_red_zone", False)):
            red_zone_count += 1

        crime_score = getattr(p, "crime_score", None)
        if crime_score is not None:
            crime_scores.append(_num(crime_score))

        rehab_rows = rehab_by_property.get(pid, [])
        rehab_total = len(rehab_rows)
        rehab_open = 0
        rehab_blocked = 0
        rehab_done = 0
        rehab_open_cost = 0.0
        for rr in rehab_rows:
            st = _safe_str(getattr(rr, "status", None) or "todo").lower()
            if st == "done":
                rehab_done += 1
            elif st == "blocked":
                rehab_blocked += 1
                rehab_open += 1
                rehab_open_cost += _num(getattr(rr, "cost_estimate", None))
            else:
                rehab_open += 1
                rehab_open_cost += _num(getattr(rr, "cost_estimate", None))

        rehab_open_cost_estimate += rehab_open_cost

        property_txns = txns_by_property.get(pid, [])
        property_income = 0.0
        property_expense = 0.0
        property_capex = 0.0
        property_other = 0.0

        for t in property_txns:
            amount = _num(getattr(t, "amount", None))
            txn_type = _safe_str(getattr(t, "txn_type", None)).lower().strip()

            if txn_type in {"income", "rent"}:
                property_income += amount
            elif txn_type == "expense":
                property_expense += amount
            elif txn_type == "capex":
                property_capex += amount
            else:
                property_other += amount

            mb = _month_bucket(getattr(t, "txn_date", None))
            if mb and mb in cash_month_map:
                if txn_type in {"income", "rent"}:
                    cash_month_map[mb]["income"] += amount
                elif txn_type == "expense":
                    cash_month_map[mb]["expense"] += amount
                elif txn_type == "capex":
                    cash_month_map[mb]["capex"] += amount
                else:
                    cash_month_map[mb]["net"] += amount

        property_net_cash_window = property_income - property_expense - property_capex + property_other
        net_cash_window += property_net_cash_window

        if property_net_cash_window > 0:
            cashflow_positive_homes += 1

        active_lease = active_lease_by_property.get(pid)
        if active_lease is not None:
            active_leases_count += 1

        latest_deal = latest_deal_by_property.get(pid)
        latest_rent = latest_rent_by_property.get(pid)

        asking_price = _num(getattr(latest_deal, "asking_price", None))
        market_rent_estimate = _num(getattr(latest_rent, "market_rent_estimate", None))
        dscr = _num(getattr(uw, "dscr", None)) if uw is not None else None
        if dscr is not None and dscr > 0:
            dscr_values.append(float(dscr))

        # lightweight investor-facing estimate for list/dashboard display
        monthly_rehab_holdback = rehab_open_cost / 12.0 if rehab_open_cost > 0 else 0.0
        monthly_cashflow_estimate = market_rent_estimate - monthly_rehab_holdback
        if market_rent_estimate > 0:
            cashflow_estimates.append(monthly_cashflow_estimate)

        property_rows.append(
            {
                "id": pid,
                "address": getattr(p, "address", None),
                "city": getattr(p, "city", None),
                "state": getattr(p, "state", None),
                "county": getattr(p, "county", None),
                "zip": getattr(p, "zip", None),
                "bedrooms": getattr(p, "bedrooms", None),
                "bathrooms": getattr(p, "bathrooms", None),
                "square_feet": getattr(p, "square_feet", None),
                "year_built": getattr(p, "year_built", None),
                "stage": stage_value,
                "stage_label": _workflow_label(stage_value),
                "classification": classification,
                "latest_decision": classification,
                "raw_decision": raw_decision or None,
                "score": _num(getattr(uw, "score", None)) if uw is not None else None,
                "dscr": float(dscr) if dscr is not None else None,
                "asking_price": asking_price if asking_price > 0 else None,
                "cashflow_estimate": round(monthly_cashflow_estimate, 2) if market_rent_estimate > 0 else None,
                "market_rent_estimate": market_rent_estimate if market_rent_estimate > 0 else None,
                "is_red_zone": bool(getattr(p, "is_red_zone", False)),
                "crime_score": getattr(p, "crime_score", None),
                "offender_count": getattr(p, "offender_count", None),
                "rehab_total": rehab_total,
                "rehab_open": rehab_open,
                "rehab_blocked": rehab_blocked,
                "rehab_done": rehab_done,
                "rehab_open_cost": rehab_open_cost,
                "has_active_lease": active_lease is not None,
                "lease_total_rent": _num(getattr(active_lease, "total_rent", None)) if active_lease is not None else None,
                "property_income_window": property_income,
                "property_expense_window": property_expense,
                "property_capex_window": property_capex,
                "property_net_cash_window": property_net_cash_window,
                "latest_value": estimated_value if val is not None else None,
                "latest_loan_balance": loan_balance if val is not None else None,
                "estimated_equity": estimated_equity if val is not None else None,
                "latest_valuation_as_of": _iso(getattr(val, "as_of", None)) if val is not None else None,
            }
        )

    if decision:
        wanted = _normalize_decision(decision)
        property_rows = [r for r in property_rows if _safe_str(r.get("classification")) == wanted]

    property_rows.sort(
        key=lambda r: (
            0 if r.get("classification") == "GOOD_DEAL" else 1 if r.get("classification") == "REVIEW" else 2,
            -_num(r.get("score")),
            -_num(r.get("estimated_equity")),
            -_num(r.get("cashflow_estimate")),
            -_num(r.get("asking_price")),
        )
    )

    for bucket in cash_month_map.values():
        bucket["net"] = (
            _num(bucket["income"]) - _num(bucket["expense"]) - _num(bucket["capex"]) + _num(bucket["net"])
        )

    property_count = len(property_rows)
    deal_count = len(deals)
    rehab_total_count = len(rehabs)
    rehab_open_count = sum(1 for r in rehabs if _safe_str(getattr(r, "status", None) or "todo").lower() != "done")
    txn_count = len(txns)
    valuation_count = len(valuations)

    avg_crime_score = round(sum(crime_scores) / len(crime_scores), 2) if crime_scores else None
    avg_dscr = round(sum(dscr_values) / len(dscr_values), 2) if dscr_values else None
    avg_cashflow_estimate = round(sum(cashflow_estimates) / len(cashflow_estimates), 2) if cashflow_estimates else None

    decision_mix = [
        {"key": k, "label": k.replace("_", " ").title(), "count": int(v)}
        for k, v in sorted(decision_buckets.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    workflow_mix = [
        {"key": k, "label": _workflow_label(k), "count": int(v)}
        for k, v in sorted(stage_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    county_mix = [
        {"key": k, "label": k, "count": int(v)}
        for k, v in sorted(county_buckets.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
    ]

    latest_valuation = valuations[0] if valuations else None
    latest_valuation_out = None
    if latest_valuation is not None:
        latest_valuation_out = {
            "property_id": getattr(latest_valuation, "property_id", None),
            "as_of": _iso(getattr(latest_valuation, "as_of", None)),
            "estimated_value": _num(getattr(latest_valuation, "estimated_value", None)),
            "loan_balance": _num(getattr(latest_valuation, "loan_balance", None)),
            "notes": getattr(latest_valuation, "notes", None),
        }

    return {
        "ok": True,
        "as_of": now.isoformat(),
        "window_days": int(days),
        "filters": {
            "state": state,
            "county": county,
            "city": city,
            "q": q,
            "stage": stage,
            "decision": decision,
            "only_red_zone": only_red_zone,
            "exclude_red_zone": exclude_red_zone,
            "min_crime_score": min_crime_score,
            "max_crime_score": max_crime_score,
            "min_offender_count": min_offender_count,
            "max_offender_count": max_offender_count,
        },
        "kpis": {
            "total_homes": property_count,
            "good_deals": good_deals,
            "review_deals": review_deals,
            "rejected_deals": rejected_deals,
            "active_leases": active_leases_count,
            "cashflow_positive_homes": cashflow_positive_homes,
            "homes_with_valuation": homes_with_valuation,
            "red_zone_count": red_zone_count,
            "total_estimated_value": round(total_estimated_value, 2),
            "total_loan_balance": round(total_loan_balance, 2),
            "total_estimated_equity": round(total_estimated_equity, 2),
            "rehab_open_cost_estimate": round(rehab_open_cost_estimate, 2),
            "net_cash_window": round(net_cash_window, 2),
            "avg_crime_score": avg_crime_score,
            "avg_dscr": avg_dscr,
            "avg_cashflow_estimate": avg_cashflow_estimate,
        },
        "counts": {
            "properties": property_count,
            "deals": deal_count,
            "rehab_tasks_total": rehab_total_count,
            "rehab_tasks_open": rehab_open_count,
            "transactions_window": txn_count,
            "valuations": valuation_count,
        },
        "buckets": {
            "decisions": {k: int(v) for k, v in decision_buckets.items()},
            "stages": {k: int(v) for k, v in stage_counts.items()},
            "counties": {k: int(v) for k, v in county_buckets.items()},
        },
        "stage_counts": {k: int(v) for k, v in stage_counts.items()},
        "sums": {
            "rehab_open_cost_estimate": round(rehab_open_cost_estimate, 2),
            "net_cash_window": round(net_cash_window, 2),
            "total_estimated_equity": round(total_estimated_equity, 2),
        },
        "latest": {
            "valuation": latest_valuation_out,
        },
        "series": {
            "cash_by_month": [cash_month_map[m] for m in cash_month_labels],
            "decision_mix": decision_mix,
            "workflow_mix": workflow_mix,
            "county_mix": county_mix,
        },
        "leaderboards": {
            "good_deals": sorted(
                property_rows,
                key=lambda r: (
                    0 if r.get("classification") == "GOOD_DEAL" else 1 if r.get("classification") == "REVIEW" else 2,
                    -_num(r.get("score")),
                    -_num(r.get("dscr")),
                ),
            )[:10],
            "cashflow": sorted(
                property_rows,
                key=lambda r: -_num(r.get("cashflow_estimate")),
            )[:10],
            "equity": sorted(
                property_rows,
                key=lambda r: -_num(r.get("estimated_equity")),
            )[:10],
            "rehab_backlog": sorted(
                property_rows,
                key=lambda r: (-_num(r.get("rehab_open_cost")), -_as_int(r.get("rehab_open"))),
            )[:10],
            "compliance_attention": sorted(
                property_rows,
                key=lambda r: (
                    0 if r.get("stage") == "compliance" else 1,
                    -_as_int(r.get("rehab_open")),
                    -_num(r.get("crime_score")),
                ),
            )[:10],
        },
        "properties": property_rows[: int(limit)],
    }
