#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path


LEGACY_BACKEND_DIR = "onehaven_decision_engine/backend/app/products/acquire"
LEGACY_FRONTEND_DIR = "onehaven_decision_engine/frontend/src/products/acquire"
NEW_BACKEND_DIR = "products/acquire/backend/src"
NEW_FRONTEND_DIR = "products/acquire/frontend/src"

PHASE5_REPORT = "tools/repo/acquire-phase5-report.json"
PHASE6_REPORT = "tools/repo/acquire-phase6-report.json"
PHASE6_BACKUP_ROOT = "tools/repo/_phase6_backups"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 6 removal of legacy OneHaven Acquire folders."
    )
    parser.add_argument("--repo-root", default=".", help="Repository root.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only. No backups or deletions.",
    )
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / "tools",
        repo_root / "onehaven_decision_engine",
        repo_root / "products" / "acquire" / "backend" / "src",
        repo_root / "products" / "acquire" / "frontend" / "src",
        repo_root / PHASE5_REPORT,
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- "
            + "\n- ".join(missing)
        )
    return repo_root


def dir_file_count(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for p in path.rglob("*") if p.is_file())


def read_phase5_report(repo_root: Path) -> dict:
    report_path = repo_root / PHASE5_REPORT
    return json.loads(report_path.read_text(encoding="utf-8"))


def validate_phase5_ready(phase5_payload: dict) -> None:
    ready = phase5_payload.get("summary", {}).get("ready_to_remove_legacy", False)
    if not ready:
        raise SystemExit(
            "Phase 6 aborted.\n"
            "Phase 5 does not mark Acquire as ready_to_remove_legacy=true.\n"
            "Fix remaining legacy references before removing legacy Acquire."
        )


def ensure_non_empty_new_targets(repo_root: Path) -> tuple[Path, Path]:
    new_backend = repo_root / NEW_BACKEND_DIR
    new_frontend = repo_root / NEW_FRONTEND_DIR

    backend_count = dir_file_count(new_backend)
    frontend_count = dir_file_count(new_frontend)

    if backend_count == 0:
        raise SystemExit(
            f"Phase 6 aborted.\nNew backend Acquire directory is empty: {new_backend}"
        )
    if frontend_count == 0:
        raise SystemExit(
            f"Phase 6 aborted.\nNew frontend Acquire directory is empty: {new_frontend}"
        )

    return new_backend, new_frontend


def backup_legacy_dirs(repo_root: Path, legacy_backend: Path, legacy_frontend: Path) -> dict:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_root = repo_root / PHASE6_BACKUP_ROOT / f"acquire_{timestamp}"
    backup_backend = backup_root / "backend"
    backup_frontend = backup_root / "frontend"

    backup_root.mkdir(parents=True, exist_ok=True)

    if legacy_backend.exists():
        shutil.copytree(legacy_backend, backup_backend, dirs_exist_ok=False)
    if legacy_frontend.exists():
        shutil.copytree(legacy_frontend, backup_frontend, dirs_exist_ok=False)

    return {
        "backup_root": str(backup_root.relative_to(repo_root).as_posix()),
        "backend_backup": str(backup_backend.relative_to(repo_root).as_posix()) if backup_backend.exists() else None,
        "frontend_backup": str(backup_frontend.relative_to(repo_root).as_posix()) if backup_frontend.exists() else None,
    }


def remove_legacy_dirs(legacy_backend: Path, legacy_frontend: Path) -> dict:
    removed = {
        "backend_removed": False,
        "frontend_removed": False,
    }

    if legacy_backend.exists():
        shutil.rmtree(legacy_backend)
        removed["backend_removed"] = True

    if legacy_frontend.exists():
        shutil.rmtree(legacy_frontend)
        removed["frontend_removed"] = True

    return removed


def write_report(repo_root: Path, payload: dict) -> None:
    report_path = repo_root / PHASE6_REPORT
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))

    phase5_payload = read_phase5_report(repo_root)
    validate_phase5_ready(phase5_payload)

    new_backend, new_frontend = ensure_non_empty_new_targets(repo_root)

    legacy_backend = repo_root / LEGACY_BACKEND_DIR
    legacy_frontend = repo_root / LEGACY_FRONTEND_DIR

    payload = {
        "phase": 6,
        "product": "acquire",
        "dry_run": args.dry_run,
        "status": "pending",
        "checked": {
            "phase5_report": PHASE5_REPORT,
            "phase5_ready_to_remove_legacy": phase5_payload.get("summary", {}).get("ready_to_remove_legacy", False),
            "new_backend_exists": new_backend.exists(),
            "new_backend_file_count": dir_file_count(new_backend),
            "new_frontend_exists": new_frontend.exists(),
            "new_frontend_file_count": dir_file_count(new_frontend),
            "legacy_backend_exists": legacy_backend.exists(),
            "legacy_backend_file_count": dir_file_count(legacy_backend),
            "legacy_frontend_exists": legacy_frontend.exists(),
            "legacy_frontend_file_count": dir_file_count(legacy_frontend),
        },
        "backup": None,
        "removal": None,
    }

    if args.dry_run:
        payload["status"] = "dry_run_success"
        write_report(repo_root, payload)
        print("Phase 6 dry run complete.")
        print("Legacy Acquire would be removed.")
        print(f"Report written to {PHASE6_REPORT}")
        return

    backup_info = backup_legacy_dirs(repo_root, legacy_backend, legacy_frontend)
    removal_info = remove_legacy_dirs(legacy_backend, legacy_frontend)

    payload["backup"] = backup_info
    payload["removal"] = removal_info
    payload["status"] = "success"

    write_report(repo_root, payload)

    print("Phase 6 completed successfully.")
    print("Legacy Acquire folders removed.")
    print(f"Backup stored under: {backup_info['backup_root']}")
    print(f"Report written to {PHASE6_REPORT}")


if __name__ == "__main__":
    main()