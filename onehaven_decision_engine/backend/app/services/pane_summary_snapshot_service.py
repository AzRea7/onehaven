from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any, Optional

from .acquisition_tag_service import count_tags_for_scope
from .property_inventory_snapshot_service import build_inventory_snapshots_for_scope
from .runtime_metrics import METRICS

log = logging.getLogger("onehaven.pane_snapshot")
PANES = ["investor", "acquisition", "compliance", "tenants", "management", "admin"]


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _stage_matches_pane(pane: str, stage: str) -> bool:
    stage = str(stage or "").strip().lower()
    if pane == "investor":
        return stage in {"discovered", "shortlisted", "underwritten", "offer"}
    if pane == "acquisition":
        return stage in {"offer", "acquired"}
    if pane == "compliance":
        return stage in {"rehab", "compliance_readying", "inspection_pending"}
    if pane == "tenants":
        return stage in {"tenant_marketing", "tenant_screening", "leased"}
    if pane == "management":
        return stage in {"occupied", "turnover", "maintenance"}
    if pane == "admin":
        return True
    return False


def _filter_rows_for_pane(rows: list[dict[str, Any]], pane: str) -> list[dict[str, Any]]:
    return rows if pane == "admin" else [row for row in rows if _stage_matches_pane(pane, str(row.get("current_stage") or ""))]


def _counter_summary(rows: list[dict[str, Any]], key: str, limit: int = 5, child_key: str | None = None) -> list[dict[str, Any]]:
    counter: dict[str, int] = defaultdict(int)
    for row in rows:
        values = row.get(key) or []
        if isinstance(values, dict) and child_key:
            values = values.get(child_key) or []
        for item in values:
            normalized = str(item or '').strip()
            if normalized:
                counter[normalized] += 1
    label = child_key or key.rstrip('s')
    return [{label: item, 'count': count} for item, count in sorted(counter.items(), key=lambda x: (-x[1], x[0]))[:limit]]


def _completeness_rollup(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for row in rows:
        for dim, status in dict(row.get('completeness_status') or {}).items():
            bucket = out.setdefault(dim, {'complete': 0, 'deferred': 0, 'missing': 0, 'partial': 0})
            normalized = str(status or 'missing').lower()
            bucket[normalized] = int(bucket.get(normalized, 0)) + 1
    return out


def _pane_kpis(pane: str, rows: list[dict[str, Any]], global_tag_counts: dict[str, int]) -> dict[str, Any]:
    total = len(rows)
    with_blockers = sum(1 for row in rows if row.get('blockers'))
    enriched = sum(1 for row in rows if row.get('completeness') == 'COMPLETE')
    partial = sum(1 for row in rows if row.get('completeness') == 'PARTIAL')
    missing = sum(1 for row in rows if row.get('completeness') == 'MISSING')
    kpis: dict[str, Any] = {
        'total_properties': total,
        'with_blockers': with_blockers,
        'enriched_count': enriched,
        'partial_count': partial,
        'missing_count': missing,
        'completeness_rollup': _completeness_rollup(rows),
    }
    if pane == 'investor':
        cashflows = [row.get('projected_monthly_cashflow') for row in rows if row.get('projected_monthly_cashflow') is not None]
        kpis.update({
            'saved_count': global_tag_counts.get('saved', 0),
            'shortlisted_count': global_tag_counts.get('shortlisted', 0),
            'review_later_count': global_tag_counts.get('review_later', 0),
            'rejected_count': global_tag_counts.get('rejected', 0),
            'offer_candidate_count': global_tag_counts.get('offer_candidate', 0),
            'avg_cashflow_estimate': round(sum(_safe_float(v) for v in cashflows) / len(cashflows), 2) if cashflows else 0.0,
        })
    return kpis


def build_pane_summary_snapshot(db, *, org_id: int, pane: str, state: Optional[str] = None, county: Optional[str] = None, city: Optional[str] = None, q: Optional[str] = None, limit: int = 200) -> dict[str, Any]:
    pane = str(pane or 'investor').strip().lower()
    if pane not in PANES:
        raise ValueError(f'unsupported pane: {pane}')
    t0 = time.perf_counter()
    scope = build_inventory_snapshots_for_scope(db, org_id=org_id, state=state, county=county, city=city, q=q, limit=limit)
    rows = _filter_rows_for_pane(list(scope.get('rows') or []), pane)
    tag_counts = count_tags_for_scope(db, org_id=org_id)
    result = {
        'pane': pane,
        'count': len(rows),
        'rows': rows,
        'kpis': _pane_kpis(pane, rows, tag_counts),
        'top_blockers': _counter_summary(rows, 'blockers', limit=6),
        'top_actions': _counter_summary(rows, 'next_actions', limit=6),
        'top_tags': [{'tag': k, 'count': v} for k, v in sorted(tag_counts.items(), key=lambda item: (-item[1], item[0])) if v > 0],
        'scope_meta': scope.get('meta') or {},
    }
    duration_ms = round((time.perf_counter() - t0) * 1000, 2)
    METRICS.observe_ms('pane_snapshot_total_ms', duration_ms, labels={'org_id': org_id, 'pane': pane})
    log.info('pane_snapshot_complete', extra={'org_id': org_id, 'pane': pane, 'rows': len(rows), 'duration_ms': duration_ms})
    return result
