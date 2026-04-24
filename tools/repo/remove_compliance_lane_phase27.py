#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path


LANE_PATHS = {
    "policy_sources": {
        "legacy_dir": "onehaven_decision_engine/backend/app/products/compliance/services/policy_sources",
        "validate_report": "tools/repo/compliance-policy_sources-validate-phase25-report.json",
    },
    "policy_governance": {
        "legacy_dir": "onehaven_decision_engine/backend/app/products/compliance/services/policy_governance",
        "validate_report": "tools/repo/compliance-policy_governance-validate-phase25-report.json",
    },
    "policy_coverage": {
        "legacy_dir": "onehaven_decision_engine/backend/app/products/compliance/services/policy_coverage",
        "validate_report": "tools/repo/compliance-policy_coverage-validate-phase25-report.json",
    },
    "policy_assertions": {
        "legacy_dir": "onehaven_decision_engine/backend/app/products/compliance/services/policy_assertions",
        "validate_report": "tools/repo/compliance-policy_assertions-validate-phase25-report.json",
    },
    "inspections": {
        "legacy_dir": "onehaven_decision_engine/backend/app/products/compliance/services/inspections",
        "validate_report": "tools/repo/compliance-inspections-validate-phase25-report.json",
    },
    "compliance_engine": {
        "legacy_dir": "onehaven_decision_engine/backend/app/products/compliance/services/compliance_engine",
        "validate_report": "tools/repo/compliance-compliance_engine-validate-phase25-report.json",
    },
    "router": {
        "legacy_dir": "onehaven_decision_engine/backend/app/products/compliance/routers",
        "validate_report": "tools/repo/compliance-router-validate-phase25-report.json",
    },
}

BACKUP_ROOT = "tools/repo/_phase27_backups"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remove a clean compliance lane.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--lane", required=True, choices=sorted(LANE_PATHS.keys()))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path, lane: str) -> Path:
    repo_root = repo_root.resolve()
    cfg = LANE_PATHS[lane]
    required = [
        repo_root / cfg["validate_report"],
        repo_root / "onehaven_decision_engine",
        repo_root / "products" / "compliance",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def backup_dir(repo_root: Path, source_dir: Path, lane: str) -> str | None:
    if not source_dir.exists():
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = repo_root / BACKUP_ROOT / f"{lane}_{ts}"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_dir, target)
    return target.relative_to(repo_root).as_posix()


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root), args.lane)
    cfg = LANE_PATHS[args.lane]

    report = read_json(repo_root / cfg["validate_report"])
    if report.get("legacy_hit_count") != 0:
        raise SystemExit(
            f"Cannot remove {args.lane}; validator still reports legacy hits."
        )

    legacy_dir = repo_root / cfg["legacy_dir"]

    if args.dry_run:
        print(f"[DRY RUN] Would remove: {legacy_dir}")
        return

    backup = backup_dir(repo_root, legacy_dir, args.lane)

    if legacy_dir.exists():
        shutil.rmtree(legacy_dir)
        print(f"Removed: {legacy_dir}")
        if backup:
            print(f"Backup: {backup}")
    else:
        print("Nothing to remove.")


if __name__ == "__main__":
    main()