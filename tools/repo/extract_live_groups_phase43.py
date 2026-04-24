#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


PHASE42_REPORT = "tools/repo/live-leftovers-phase42-report.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract top live leftover groups from Phase 42."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument(
        "--priority",
        choices=["high", "medium", "low", "all"],
        default="all",
        help="Filter groups by priority mix.",
    )
    parser.add_argument("--top", type=int, default=10, help="How many groups to return.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE42_REPORT,
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
    report = read_json(repo_root / PHASE42_REPORT)

    groups = report.get("top_prefix_groups", [])

    if args.priority != "all":
        groups = [
            g for g in groups
            if g.get("priority_mix", {}).get(args.priority, 0) > 0
        ]

    groups = sorted(
        groups,
        key=lambda g: (
            g.get("priority_mix", {}).get(args.priority, 0) if args.priority != "all" else g.get("count", 0),
            g.get("count", 0),
        ),
        reverse=True,
    )[:args.top]

    payload = {
        "phase": 43,
        "priority": args.priority,
        "group_count": len(groups),
        "groups": groups,
    }

    out = repo_root / "tools" / "repo" / f"live-groups-{args.priority}-phase43-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 43 complete.")
    print(f"Priority filter: {args.priority}")
    print(f"Groups selected: {len(groups)}")
    print(f"Report written to {out}")


if __name__ == "__main__":
    main()