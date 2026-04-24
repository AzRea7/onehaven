#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path


PRODUCTS = {
    "intelligence": {
        "legacy_backend_dir": "onehaven_decision_engine/backend/app/products/investor_intelligence",
        "new_backend_dir": "products/intelligence/backend/src",
        "phase12_report": "tools/repo/intelligence-backend-validate-phase12-report.json",
    },
    "tenants": {
        "legacy_backend_dir": "onehaven_decision_engine/backend/app/products/tenant",
        "new_backend_dir": "products/tenants/backend/src",
        "phase12_report": "tools/repo/tenants-backend-validate-phase12-report.json",
    },
    "ops": {
        "legacy_backend_dir": "onehaven_decision_engine/backend/app/products/management",
        "new_backend_dir": "products/ops/backend/src",
        "phase12_report": "tools/repo/ops-backend-validate-phase12-report.json",
    },
}

BACKUP_ROOT = "tools/repo/_phase14_backups"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove legacy backend product folders after clean validation."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument(
        "--product",
        required=True,
        choices=sorted(PRODUCTS.keys()),
        help="Product to remove legacy backend folder for.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path, product: str) -> Path:
    repo_root = repo_root.resolve()
    cfg = PRODUCTS[product]
    required = [
        repo_root / cfg["phase12_report"],
        repo_root / cfg["new_backend_dir"],
        repo_root / "onehaven_decision_engine",
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- " + "\n- ".join(missing)
        )
    return repo_root


def read_phase12(repo_root: Path, product: str) -> dict:
    return json.loads((repo_root / PRODUCTS[product]["phase12_report"]).read_text(encoding="utf-8"))


def dir_file_count(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for p in path.rglob("*") if p.is_file())


def validate_safe_to_remove(repo_root: Path, product: str) -> tuple[Path, Path]:
    cfg = PRODUCTS[product]
    phase12 = read_phase12(repo_root, product)

    legacy_hits = phase12.get("legacy_hit_count", 999999)
    if legacy_hits != 0:
        raise SystemExit(
            f"Cannot remove legacy backend for {product}. "
            f"Phase 12 still reports {legacy_hits} legacy hits."
        )

    legacy_dir = repo_root / cfg["legacy_backend_dir"]
    new_dir = repo_root / cfg["new_backend_dir"]

    if not new_dir.exists() or dir_file_count(new_dir) == 0:
        raise SystemExit(
            f"Cannot remove legacy backend for {product}. "
            f"New backend dir missing or empty: {new_dir}"
        )

    return legacy_dir, new_dir


def backup_dir(repo_root: Path, source_dir: Path, product: str) -> str | None:
    if not source_dir.exists():
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_target = repo_root / BACKUP_ROOT / f"{product}_{timestamp}"
    backup_target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_dir, backup_target, dirs_exist_ok=False)
    return backup_target.relative_to(repo_root).as_posix()


def write_report(repo_root: Path, product: str, payload: dict) -> None:
    out = repo_root / "tools" / "repo" / f"{product}-backend-remove-phase14-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root), args.product)

    legacy_dir, new_dir = validate_safe_to_remove(repo_root, args.product)

    payload = {
        "phase": 14,
        "product": args.product,
        "dry_run": args.dry_run,
        "legacy_backend_dir": legacy_dir.relative_to(repo_root).as_posix(),
        "new_backend_dir": new_dir.relative_to(repo_root).as_posix(),
        "legacy_backend_exists_before": legacy_dir.exists(),
        "legacy_backend_file_count_before": dir_file_count(legacy_dir),
        "new_backend_file_count": dir_file_count(new_dir),
        "backup_path": None,
        "removed": False,
    }

    if args.dry_run:
        payload["status"] = "dry_run_success"
        write_report(repo_root, args.product, payload)
        print("Phase 14 dry run complete.")
        print(f"Legacy backend folder for {args.product} is safe to remove.")
        print(f"Report written to tools/repo/{args.product}-backend-remove-phase14-report.json")
        return

    backup_path = backup_dir(repo_root, legacy_dir, args.product)
    payload["backup_path"] = backup_path

    if legacy_dir.exists():
        shutil.rmtree(legacy_dir)
        payload["removed"] = True

    payload["status"] = "success"
    payload["legacy_backend_exists_after"] = legacy_dir.exists()

    write_report(repo_root, args.product, payload)

    print("Phase 14 complete.")
    print(f"Removed legacy backend folder for {args.product}.")
    if backup_path:
        print(f"Backup: {backup_path}")
    print(f"Report written to tools/repo/{args.product}-backend-remove-phase14-report.json")


if __name__ == "__main__":
    main()