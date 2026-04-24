#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


PHASE34_REPORT = "tools/repo/leftovers-triage-phase34-report.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract the largest medium-priority leftover groups."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--top", type=int, default=10, help="How many groups to keep.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE34_REPORT,
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    report = read_json(repo_root / PHASE34_REPORT)

    groups = []
    for group in report.get("top_prefix_groups", []):
        medium_count = group.get("priority_mix", {}).get("medium", 0)
        if medium_count > 0:
            groups.append({
                "prefix": group["prefix"],
                "count": group["count"],
                "medium_count": medium_count,
                "sections": group.get("sections", {}),
                "sample_sources": group.get("sample_sources", []),
            })

    groups.sort(key=lambda g: (g["medium_count"], g["count"]), reverse=True)
    selected = groups[:args.top]

    payload = {
        "phase": 39,
        "top": args.top,
        "group_count": len(selected),
        "groups": selected,
    }

    out = repo_root / "tools" / "repo" / "leftovers-medium-phase39-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 39 complete.")
    print(f"Groups selected: {len(selected)}")
    print("Report written to tools/repo/leftovers-medium-phase39-report.json")


if __name__ == "__main__":
    main()