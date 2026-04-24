#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path


PHASE60_REPORT = "tools/repo/missing-router-targets-phase60-report.json"
BACKUP_ROOT = "tools/repo/_phase61_backups"


@dataclass
class RemovalResult:
    source: str
    status: str
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remove legacy routers moved in Phase 60.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def backup_file(repo_root: Path, source: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = repo_root / BACKUP_ROOT / f"missing_routers_{ts}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    dst = backup_dir / source.name
    shutil.copy2(source, dst)
    return dst


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    report_path = repo_root / PHASE60_REPORT

    if not report_path.exists():
        raise SystemExit(f"Missing report: {report_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    results: list[RemovalResult] = []

    for item in report.get("results", []):
        if item.get("status") != "moved":
            results.append(RemovalResult(item["source"], "skipped", item.get("status", "not_moved")))
            continue

        src = repo_root / item["source"]
        if not src.exists():
            results.append(RemovalResult(item["source"], "skipped", "source_missing"))
            continue

        if args.dry_run:
            results.append(RemovalResult(item["source"], "would_remove", "moved_in_phase60"))
            continue

        backup_file(repo_root, src)
        src.unlink()
        results.append(RemovalResult(item["source"], "removed", "moved_in_phase60"))

    payload = {
        "phase": 61,
        "dry_run": args.dry_run,
        "summary": {
            "would_remove": sum(1 for r in results if r.status == "would_remove"),
            "removed": sum(1 for r in results if r.status == "removed"),
            "skipped": sum(1 for r in results if r.status == "skipped"),
        },
        "results": [asdict(r) for r in results],
    }

    out = repo_root / "tools" / "repo" / "missing-router-removal-phase61-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 61 complete.")
    print(f"Would remove: {payload['summary']['would_remove']}")
    print(f"Removed: {payload['summary']['removed']}")
    print(f"Skipped: {payload['summary']['skipped']}")
    print("Report written to tools/repo/missing-router-removal-phase61-report.json")


if __name__ == "__main__":
    main()