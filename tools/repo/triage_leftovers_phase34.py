#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path


PHASE33_REPORT = "tools/repo/remaining-leftovers-phase33-report.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cluster remaining migration leftovers into actionable groups."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE33_REPORT,
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def bucket_name(rec: dict, section: str) -> str:
    if section.startswith("backend"):
        return rec.get("bucket", "unknown")
    if section.startswith("frontend"):
        return rec.get("bucket", "unknown")
    if section == "compliance_manual_review":
        return rec.get("area", "manual_review")
    return "unknown"


def source_path(rec: dict) -> str:
    return rec.get("source", "")


def first_n_parts(path: str, n: int) -> str:
    parts = path.split("/")
    return "/".join(parts[:n]) if len(parts) >= n else path


def classify_priority(path: str) -> str:
    p = path.lower()

    if "routers" in p or "/router" in p or p.endswith("main.py") or p.endswith("app.tsx") or p.endswith("main.tsx"):
        return "high"
    if "services" in p or "domain" in p or "pages" in p:
        return "medium"
    if "components" in p or "lib" in p:
        return "medium"
    return "low"


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    report = read_json(repo_root / PHASE33_REPORT)

    sections = {
        "backend_manual_split": report.get("backend_manual_split", []),
        "backend_unclear": report.get("backend_unclear", []),
        "frontend_manual_split": report.get("frontend_manual_split", []),
        "frontend_unclear": report.get("frontend_unclear", []),
        "compliance_manual_review": report.get("compliance_manual_review", []),
    }

    grouped_by_prefix = defaultdict(list)
    grouped_by_priority = defaultdict(list)
    grouped_by_filename = Counter()
    grouped_by_section = {}
    grouped_by_owner_hint = Counter()

    for section, records in sections.items():
        grouped_by_section[section] = len(records)

        for rec in records:
            src = source_path(rec)
            prefix3 = first_n_parts(src, 3)
            prefix4 = first_n_parts(src, 4)
            priority = classify_priority(src)
            filename = Path(src).name
            reason = rec.get("reason", "")
            matched_product = rec.get("matched_product")

            grouped_by_prefix[prefix3].append({
                "section": section,
                "source": src,
                "reason": reason,
                "matched_product": matched_product,
                "priority": priority,
            })

            grouped_by_prefix[prefix4].append({
                "section": section,
                "source": src,
                "reason": reason,
                "matched_product": matched_product,
                "priority": priority,
            })

            grouped_by_priority[priority].append({
                "section": section,
                "source": src,
                "reason": reason,
                "matched_product": matched_product,
            })

            grouped_by_filename[filename] += 1

            if matched_product:
                grouped_by_owner_hint[f"product:{matched_product}"] += 1
            elif "router" in src.lower() or "routers" in src.lower():
                grouped_by_owner_hint["likely:router"] += 1
            elif "service" in src.lower() or "services" in src.lower():
                grouped_by_owner_hint["likely:service"] += 1
            elif "component" in src.lower() or "components" in src.lower():
                grouped_by_owner_hint["likely:component"] += 1
            elif "page" in src.lower() or "pages" in src.lower():
                grouped_by_owner_hint["likely:page"] += 1
            else:
                grouped_by_owner_hint["likely:misc"] += 1

    top_prefix_groups = []
    seen = set()
    for prefix, items in sorted(grouped_by_prefix.items(), key=lambda kv: len(kv[1]), reverse=True):
        if prefix in seen:
            continue
        seen.add(prefix)
        if len(items) < 2:
            continue
        top_prefix_groups.append({
            "prefix": prefix,
            "count": len(items),
            "sample_sources": [item["source"] for item in items[:10]],
            "priority_mix": dict(Counter(item["priority"] for item in items)),
            "sections": dict(Counter(item["section"] for item in items)),
        })

    top_filenames = [
        {"filename": name, "count": count}
        for name, count in grouped_by_filename.most_common(25)
    ]

    priority_summary = {
        key: len(val) for key, val in grouped_by_priority.items()
    }

    owner_summary = dict(grouped_by_owner_hint.most_common())

    payload = {
        "phase": 34,
        "summary": {
            "sections": grouped_by_section,
            "priority": priority_summary,
            "owner_hints": owner_summary,
            "top_prefix_group_count": len(top_prefix_groups),
        },
        "top_prefix_groups": top_prefix_groups[:30],
        "top_filenames": top_filenames,
        "recommended_order": [
            "high-priority router/runtime leftovers first",
            "largest same-prefix backend groups next",
            "largest same-prefix compliance manual_review groups next",
            "frontend manual_split after backend router/service cleanup",
            "remaining low-priority one-offs last",
        ],
    }

    out = repo_root / "tools" / "repo" / "leftovers-triage-phase34-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 34 complete.")
    print(f"Top prefix groups: {len(top_prefix_groups[:30])}")
    print(f"Priority summary: {priority_summary}")
    print("Report written to tools/repo/leftovers-triage-phase34-report.json")


if __name__ == "__main__":
    main()