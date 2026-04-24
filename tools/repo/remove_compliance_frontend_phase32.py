#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path


PHASE30_REPORT = "tools/repo/compliance-frontend-validate-phase30-report.json"
LEGACY_DIR = "onehaven_decision_engine/frontend/src/products/compliance"
NEW_DIR = "products/compliance/frontend/src"
BACKUP_ROOT = "tools/repo/_phase32_backups"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove legacy Compliance frontend after clean validation."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE30_REPORT,
        repo_root / LEGACY_DIR,
        repo_root / NEW_DIR,
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def read_report(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def count_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for p in path.rglob("*") if p.is_file())


def backup_dir(repo_root: Path, source_dir: Path) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = repo_root / BACKUP_ROOT / f"compliance_frontend_{ts}"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_dir, target)
    return target.relative_to(repo_root).as_posix()


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))

    report = read_report(repo_root / PHASE30_REPORT)
    if report.get("legacy_hit_count") != 0:
        raise SystemExit("Cannot remove legacy Compliance frontend; legacy refs still exist.")

    legacy_dir = repo_root / LEGACY_DIR
    new_dir = repo_root / NEW_DIR

    if count_files(new_dir) == 0:
        raise SystemExit("New Compliance frontend is empty; refusing removal.")

    if args.dry_run:
        print(f"[DRY RUN] Would remove: {legacy_dir}")
        return

    backup = backup_dir(repo_root, legacy_dir)
    shutil.rmtree(legacy_dir)

    print("Phase 32 complete.")
    print(f"Removed: {legacy_dir}")
    print(f"Backup: {backup}")


if __name__ == "__main__":
    main()