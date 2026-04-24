#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path


PHASE58_REPORT = "tools/repo/router-target-audit-phase58-report.json"
BACKUP_ROOT = "tools/repo/_phase59_backups"


@dataclass
class RemovalResult:
    source: str
    status: str
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remove resolved legacy routers when target is already present.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def backup_file(repo_root: Path, source: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = repo_root / BACKUP_ROOT / f"routers_{ts}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    dst = backup_dir / source.name
    shutil.copy2(source, dst)
    return dst


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    report_path = repo_root / PHASE58_REPORT

    if not report_path.exists():
        raise SystemExit(f"Missing report: {report_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    results: list[RemovalResult] = []

    for audit in report.get("audits", []):
        source = repo_root / audit["source"]
        status = audit["status"]

        if status not in {"identical", "already_migrated"}:
            results.append(RemovalResult(audit["source"], "skipped", status))
            continue

        if not source.exists():
            results.append(RemovalResult(audit["source"], "skipped", "source_missing"))
            continue

        if args.dry_run:
            results.append(RemovalResult(audit["source"], "would_remove", status))
            continue

        backup_file(repo_root, source)
        source.unlink()
        results.append(RemovalResult(audit["source"], "removed", status))

    payload = {
        "phase": 59,
        "dry_run": args.dry_run,
        "summary": {
            "would_remove": sum(1 for r in results if r.status == "would_remove"),
            "removed": sum(1 for r in results if r.status == "removed"),
            "skipped": sum(1 for r in results if r.status == "skipped"),
        },
        "results": [asdict(r) for r in results],
    }

    out = repo_root / "tools" / "repo" / "router-removal-phase59-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 59 complete.")
    print(f"Would remove: {payload['summary']['would_remove']}")
    print(f"Removed: {payload['summary']['removed']}")
    print(f"Skipped: {payload['summary']['skipped']}")
    print("Report written to tools/repo/router-removal-phase59-report.json")


if __name__ == "__main__":
    main()