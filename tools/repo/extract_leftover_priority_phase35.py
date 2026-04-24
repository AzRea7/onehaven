#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


PHASE34_REPORT = "tools/repo/leftovers-triage-phase34-report.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract leftovers by priority from Phase 34 report."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument(
        "--priority",
        required=True,
        choices=["high", "medium", "low"],
        help="Priority bucket to extract.",
    )
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

    extracted = []

    for group in report.get("top_prefix_groups", []):
        mix = group.get("priority_mix", {})
        if mix.get(args.priority, 0) > 0:
            extracted.append(group)

    payload = {
        "phase": 35,
        "priority": args.priority,
        "group_count": len(extracted),
        "groups": extracted,
    }

    out = repo_root / "tools" / "repo" / f"leftovers-{args.priority}-phase35-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 35 complete.")
    print(f"Priority: {args.priority}")
    print(f"Groups extracted: {len(extracted)}")
    print(f"Report written to {out}")


if __name__ == "__main__":
    main()