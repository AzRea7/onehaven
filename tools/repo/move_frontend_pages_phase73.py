#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path

PHASE72_REPORT = "tools/repo/frontend-pages-phase72-report.json"

@dataclass
class MoveResult:
    source: str
    target: str | None
    owner: str
    status: str
    reason: str

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()

def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()
    report_path = root / PHASE72_REPORT

    if not report_path.exists():
        raise SystemExit(f"Missing report: {report_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    results: list[MoveResult] = []

    for plan in report.get("plans", []):
        source = plan["source"]
        target = plan["target"]
        owner = plan["owner"]

        if not target:
            results.append(MoveResult(source, target, owner, "skipped", "manual_review"))
            continue

        src = root / source
        dst = root / target

        if not src.exists():
            results.append(MoveResult(source, target, owner, "skipped", "source_missing"))
            continue

        if dst.exists():
            results.append(MoveResult(source, target, owner, "skipped", "target_exists"))
            continue

        if args.dry_run:
            results.append(MoveResult(source, target, owner, "would_move", "dry_run"))
            continue

        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        results.append(MoveResult(source, target, owner, "moved", "copied_successfully"))

    payload = {
        "phase": 73,
        "dry_run": args.dry_run,
        "summary": {
            "would_move": sum(r.status == "would_move" for r in results),
            "moved": sum(r.status == "moved" for r in results),
            "skipped": sum(r.status == "skipped" for r in results),
        },
        "results": [asdict(r) for r in results],
    }

    out = root / "tools/repo/frontend-pages-phase73-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 73 complete.")
    print(f"Would move: {payload['summary']['would_move']}")
    print(f"Moved: {payload['summary']['moved']}")
    print(f"Skipped: {payload['summary']['skipped']}")
    print("Report written to tools/repo/frontend-pages-phase73-report.json")

if __name__ == "__main__":
    main()