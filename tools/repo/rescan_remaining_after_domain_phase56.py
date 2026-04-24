#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


TEXT_EXTENSIONS = {
    ".py", ".tsx", ".ts", ".js", ".jsx", ".json", ".md",
    ".yaml", ".yml", ".toml", ".ini", ".txt", ".css", ".scss", ".html",
}

LEGACY_ROOTS = [
    "onehaven_decision_engine/backend/app",
    "onehaven_decision_engine/frontend/src",
]

EXCLUDED_PREFIXES = [
    ".git/",
    "node_modules/",
    ".venv/",
    "venv/",
    "__pycache__/",
    "dist/",
    "build/",
    "tools/repo/",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rescan remaining legacy files after domain migration.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def should_exclude(rel: str) -> bool:
    return any(rel.startswith(prefix) for prefix in EXCLUDED_PREFIXES)


def classify_priority(rel: str) -> str:
    p = rel.lower()
    if "/routers/" in p or p.endswith("/main.py") or p.endswith("/app.tsx") or p.endswith("/main.tsx"):
        return "high"
    if "/services/" in p or "/domain/" in p or "/pages/" in p or "/components/" in p:
        return "medium"
    return "low"


def owner_hint(rel: str) -> str:
    p = rel.lower()

    if "/frontend/src/components/" in p:
        return "frontend_component"
    if "/frontend/src/pages/" in p:
        return "frontend_page"

    if "/backend/app/alembic" in p:
        return "migration"
    if "/backend/app/routers/" in p:
        return "router"
    if "/backend/app/domain/" in p:
        return "domain"
    if "/backend/app/services/" in p:
        return "service"
    if "/backend/app/products/compliance" in p or "jurisdiction" in p or "policy_" in p or "inspection" in p:
        return "compliance"
    if "rent" in p or "underwrit" in p or "cashflow" in p or "valuation" in p:
        return "intelligence"
    if "import" in p or "acquisition" in p or "closing" in p or "portfolio_ingestion" in p:
        return "acquire"
    if "tenant" in p or "voucher" in p or "lease_" in p or "applicant" in p:
        return "tenants"
    if "rehab" in p or "task" in p or "turnover" in p or "inventory" in p:
        return "ops"
    if "agent_" in p or "/agents/" in p or "auth" in p or "usage" in p or "virus_scanning" in p:
        return "platform"

    return "unclear"


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()

    leftovers = []

    for legacy_root in LEGACY_ROOTS:
        root = repo_root / legacy_root
        if not root.exists():
            continue

        for p in root.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() not in TEXT_EXTENSIONS:
                continue

            rel = p.relative_to(repo_root).as_posix()
            if should_exclude(rel):
                continue

            leftovers.append(
                {
                    "source": rel,
                    "priority": classify_priority(rel),
                    "owner_hint": owner_hint(rel),
                    "prefix3": "/".join(rel.split("/")[:3]),
                    "prefix4": "/".join(rel.split("/")[:4]),
                    "filename": p.name,
                }
            )

    priority_counts = Counter(item["priority"] for item in leftovers)
    owner_counts = Counter(item["owner_hint"] for item in leftovers)
    prefix_counts = Counter(item["prefix4"] for item in leftovers)
    filename_counts = Counter(item["filename"] for item in leftovers)

    top_prefix_groups = []
    for prefix, count in prefix_counts.most_common(25):
        items = [x for x in leftovers if x["prefix4"] == prefix]
        top_prefix_groups.append(
            {
                "prefix": prefix,
                "count": count,
                "priority_mix": dict(Counter(x["priority"] for x in items)),
                "owner_mix": dict(Counter(x["owner_hint"] for x in items)),
                "sample_sources": [x["source"] for x in items[:10]],
            }
        )

    payload = {
        "phase": 56,
        "summary": {
            "total_live_leftovers": len(leftovers),
            "priority": dict(priority_counts),
            "owner_hints": dict(owner_counts),
        },
        "top_prefix_groups": top_prefix_groups,
        "top_filenames": [{"filename": k, "count": v} for k, v in filename_counts.most_common(25)],
        "leftovers": leftovers,
    }

    out = repo_root / "tools" / "repo" / "remaining-after-domain-phase56-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 56 complete.")
    print(f"Live leftovers: {len(leftovers)}")
    print(f"Priority summary: {dict(priority_counts)}")
    print("Report written to tools/repo/remaining-after-domain-phase56-report.json")


if __name__ == "__main__":
    main()