#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path


PHASE62_REPORT = "tools/repo/meta-router-phase62-report.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remove legacy meta router if safe.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def backup_file(repo_root: Path, source: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = repo_root / "tools" / "repo" / "_phase63_backups" / f"meta_{ts}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    dst = backup_dir / source.name
    shutil.copy2(source, dst)
    return dst


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()

    report_path = repo_root / PHASE62_REPORT
    if not report_path.exists():
        raise SystemExit(f"Missing report: {report_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    source = repo_root / report["source"]
    status = report["status"]

    if status not in {"identical", "already_migrated"}:
        print("Phase 63 complete.")
        print(f"Skipped: {status}")
        return

    if not source.exists():
        print("Phase 63 complete.")
        print("Skipped: source_missing")
        return

    if args.dry_run:
        print(f"[DRY RUN] Would remove: {source}")
        return

    backup = backup_file(repo_root, source)
    source.unlink()

    print("Phase 63 complete.")
    print(f"Removed: {source}")
    print(f"Backup: {backup}")


if __name__ == "__main__":
    main()