from __future__ import annotations

import json
from collections import Counter
from typing import Any, Iterable

from .inspection_rules import normalize_inspection_item_status, normalize_rule_code, rank_common_fail_points


def _extract_fail_points(row: Any) -> list[str]:
    """
    Flexible extractor for inspection, compliance, and photo-analysis fail points.

    Supported shapes:
      1) {"fail_items_json": "[.json list.]"}
      2) {"typical_fail_points_json": "[.json list.]"}
      3) {"items": [{"code": ".", "failed": true}, .]}
      4) {"code": ".", "failed": true}
      5) {"code": ".", "result_status": "fail"}
      6) {"findings": [{"code": "."}, .]}
      7) {"probable_failed_inspection_item": ".", "rule_mapping": {"code": "."}}
      8) bare string
    """
    if row is None:
        return []

    if isinstance(row, str):
        t = normalize_rule_code(row)
        return [t] if t else []

    if not isinstance(row, dict):
        return []

    out: list[str] = []

    if "code" in row:
        code = normalize_rule_code(row.get("code"))
        status = normalize_inspection_item_status(row.get("result_status") or row.get("status"), failed=row.get("failed"))
        if code and status == "fail":
            out.append(code)

    probable = row.get("probable_failed_inspection_item")
    rule_mapping = row.get("rule_mapping")
    if probable and isinstance(rule_mapping, dict):
        mapped_code = normalize_rule_code(rule_mapping.get("code"))
        if mapped_code:
            out.append(mapped_code)

    items = row.get("items")
    if isinstance(items, list):
        for it in items:
            if not isinstance(it, dict):
                continue
            code = normalize_rule_code(it.get("code"))
            status = normalize_inspection_item_status(
                it.get("result_status") or it.get("status"),
                failed=it.get("failed"),
            )
            if code and status == "fail":
                out.append(code)

    findings = row.get("findings")
    if isinstance(findings, list):
        for finding in findings:
            if not isinstance(finding, dict):
                continue
            code = normalize_rule_code(
                finding.get("code")
                or ((finding.get("rule_mapping") or {}).get("code") if isinstance(finding.get("rule_mapping"), dict) else None)
            )
            if code:
                out.append(code)

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
                    code = normalize_rule_code(v)
                    if code:
                        out.append(code)
                elif isinstance(v, dict):
                    code = normalize_rule_code(v.get("code") or v.get("item"))
                    status = normalize_inspection_item_status(
                        v.get("result_status") or v.get("status"),
                        failed=v.get("failed"),
                    )
                    if code and (status == "fail" or k == "typical_fail_points_json"):
                        out.append(code)

    return out


def top_fail_points(rows: Iterable[Any], limit: int = 10) -> list[dict[str, int | str]]:
    ctr: Counter[str] = Counter()

    for r in rows or []:
        for fp in _extract_fail_points(r):
            if fp:
                ctr[fp] += 1

    raw_ranked = [{"code": code, "count": count} for code, count in ctr.items()]
    return rank_common_fail_points(raw_ranked, limit=max(0, int(limit)))
