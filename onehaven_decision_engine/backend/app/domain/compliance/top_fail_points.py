# backend/app/domain/compliance/top_fail_points.py
from __future__ import annotations

import json
from collections import Counter
from typing import Any, Iterable


def _extract_fail_points(row: Any) -> list[str]:
    """
    Accepts flexible input shapes because early pipelines evolve.
    Supported row shapes:
      1) {"fail_items_json": "[...json list...]"}
      2) {"typical_fail_points_json": "[...json list...]"}  (jurisdiction default patterns)
      3) {"items": [{"code": "...", "failed": true}, ...]}
      4) {"code": "...", "failed": true}  (single item)
      5) a bare string fail point
    Returns a list of fail-point strings/codes.
    """
    if row is None:
        return []

    # If it's already a string, treat as a fail point.
    if isinstance(row, str):
        t = row.strip()
        return [t] if t else []

    if not isinstance(row, dict):
        return []

    out: list[str] = []

    # single item shape
    if "code" in row:
        failed = row.get("failed")
        if failed is True or failed is None:
            code = str(row.get("code") or "").strip()
            if code:
                out.append(code)

    # items list shape
    items = row.get("items")
    if isinstance(items, list):
        for it in items:
            if not isinstance(it, dict):
                continue
            failed = it.get("failed")
            if failed is True or failed is None:
                code = str(it.get("code") or "").strip()
                if code:
                    out.append(code)

    # json strings that decode to list[str] or list[dict]
    for k in ("fail_items_json", "typical_fail_points_json"):
        raw = row.get(k)
        if not isinstance(raw, str) or not raw.strip():
            continue

        try:
            parsed = json.loads(raw)
        except Exception:
            continue

        if isinstance(parsed, list):
            for v in parsed:
                if isinstance(v, str):
                    t = v.strip()
                    if t:
                        out.append(t)
                elif isinstance(v, dict):
                    # allow dict forms like {"code": "..."} or {"item": "..."}
                    code = str(v.get("code") or v.get("item") or "").strip()
                    if code:
                        out.append(code)

    return out


def top_fail_points(rows: Iterable[Any], limit: int = 10) -> list[dict[str, int | str]]:
    """
    Returns most common fail points across rows.
    Output shape: [{"code": "GFCI_MISSING", "count": 12}, ...]
    """
    ctr: Counter[str] = Counter()

    for r in rows or []:
        for fp in _extract_fail_points(r):
            key = fp.strip().upper()
            if key:
                ctr[key] += 1

    most = ctr.most_common(max(0, int(limit)))
    return [{"code": code, "count": count} for code, count in most]