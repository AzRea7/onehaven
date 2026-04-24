#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path


PRODUCTS = {
    "intelligence": {
        "legacy_frontend_dir": "onehaven_decision_engine/frontend/src/products/investor_intelligence",
        "new_frontend_dir": "products/intelligence/frontend/src",
        "phase20_report": "tools/repo/intelligence-frontend-validate-phase20-report.json",
    },
    "tenants": {
        "legacy_frontend_dir": "onehaven_decision_engine/frontend/src/products/tenant",
        "new_frontend_dir": "products/tenants/frontend/src",
        "phase20_report": "tools/repo/tenants-frontend-validate-phase20-report.json",
    },
    "ops": {
        "legacy_frontend_dir": "onehaven_decision_engine/frontend/src/products/management",
        "new_frontend_dir": "products/ops/frontend/src",
        "phase20_report": "tools/repo/ops-frontend-validate-phase20-report.json",
    },
}

BACKUP_ROOT = "tools/repo/_phase22_backups"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--product", required=True, choices=PRODUCTS.keys())
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def count_files(path: Path):
    if not path.exists():
        return 0
    return sum(1 for p in path.rglob("*") if p.is_file())


def main():
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()

    cfg = PRODUCTS[args.product]

    phase20 = read_json(repo_root / cfg["phase20_report"])
    if phase20.get("legacy_hit_count") != 0:
        raise SystemExit("Still has legacy refs — cannot delete.")

    legacy_dir = repo_root / cfg["legacy_frontend_dir"]
    new_dir = repo_root / cfg["new_frontend_dir"]

    if not new_dir.exists() or count_files(new_dir) == 0:
        raise SystemExit("New frontend missing or empty.")

    if args.dry_run:
        print(f"[DRY RUN] Would remove: {legacy_dir}")
        return

    if legacy_dir.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = repo_root / BACKUP_ROOT / f"{args.product}_{ts}"
        backup.parent.mkdir(parents=True, exist_ok=True)

        shutil.copytree(legacy_dir, backup)
        shutil.rmtree(legacy_dir)

        print("Removed:", legacy_dir)
        print("Backup:", backup)
    else:
        print("Nothing to remove.")


if __name__ == "__main__":
    main()