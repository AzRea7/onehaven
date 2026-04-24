#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path


TEXT_EXTENSIONS = {
    ".py", ".tsx", ".ts", ".js", ".jsx", ".json", ".md",
    ".yaml", ".yml", ".toml", ".ini", ".txt", ".css",
    ".scss", ".html",
}

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

LEGACY_ROOTS = [
    "onehaven_decision_engine/backend/app",
    "onehaven_decision_engine/frontend/src",
]

KNOWN_NEW_ROOTS = [
    "apps/",
    "platform/",
    "packages/",
    "products/",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live rescan of remaining legacy leftovers.")
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


def likely_owner(rel: str) -> str:
    p = rel.lower()

    if "products/compliance" in p or "/policy_" in p or "jurisdiction" in p or "inspection" in p:
        return "compliance"
    if "tenant" in p or "voucher" in p or "applicant" in p:
        return "tenants"
    if "rent" in p or "deal" in p or "underwrit" in p or "equity" in p:
        return "intelligence"
    if "import" in p or "acquisition" in p or "closing" in p:
        return "acquire"
    if "rehab" in p or "ops" in p or "turnover" in p or "task" in p:
        return "ops"
    if "/domain/agents/" in p or "agent_" in p or "auth_service" in p or "address_normalization" in p:
        return "platform"
    if "/components/" in p:
        return "frontend_component"
    if "/pages/" in p:
        return "frontend_page"
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

            leftovers.append({
                "source": rel,
                "priority": classify_priority(rel),
                "owner_hint": likely_owner(rel),
                "prefix3": "/".join(rel.split("/")[:3]),
                "prefix4": "/".join(rel.split("/")[:4]),
                "filename": p.name,
            })

    priority_counts = Counter(item["priority"] for item in leftovers)
    owner_counts = Counter(item["owner_hint"] for item in leftovers)
    prefix_counts = Counter(item["prefix4"] for item in leftovers)
    filename_counts = Counter(item["filename"] for item in leftovers)

    top_prefix_groups = []
    for prefix, count in prefix_counts.most_common(25):
        items = [x for x in leftovers if x["prefix4"] == prefix]
        top_prefix_groups.append({
            "prefix": prefix,
            "count": count,
            "priority_mix": dict(Counter(x["priority"] for x in items)),
            "owner_mix": dict(Counter(x["owner_hint"] for x in items)),
            "sample_sources": [x["source"] for x in items[:10]],
        })

    payload = {
        "phase": 42,
        "summary": {
            "total_live_leftovers": len(leftovers),
            "priority": dict(priority_counts),
            "owner_hints": dict(owner_counts),
        },
        "top_prefix_groups": top_prefix_groups,
        "top_filenames": [{"filename": k, "count": v} for k, v in filename_counts.most_common(25)],
        "leftovers": leftovers,
    }

    out = repo_root / "tools" / "repo" / "live-leftovers-phase42-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 42 complete.")
    print(f"Live leftovers: {len(leftovers)}")
    print(f"Priority summary: {dict(priority_counts)}")
    print("Report written to tools/repo/live-leftovers-phase42-report.json")


if __name__ == "__main__":
    main()