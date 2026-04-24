#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path


MOVES = {
    "onehaven_decision_engine/backend/app/alembic": "apps/suite_api/db/alembic",
    "onehaven_decision_engine/backend/app/alembic.ini": "apps/suite_api/db/alembic.ini",
}


PATCH_FILES = [
    "apps/suite_api/db/alembic/env.py",
    "apps/suite_api/db/alembic.ini",
]


REPLS = {
    "onehaven_decision_engine/backend/app/alembic": "apps/suite_api/db/alembic",
    "onehaven_decision_engine/backend/app/alembic.ini": "apps/suite_api/db/alembic.ini",
    "script_location = alembic": "script_location = apps/suite_api/db/alembic",
    "from app.config import": "from onehaven_platform.backend.src.config import",
    "from app.db import": "from onehaven_platform.backend.src.db import",
    "from app.models import": "from onehaven_platform.backend.src.models import",
    "from app.policy_models import": "from onehaven_platform.backend.src.policy_models import",
}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--remove-source", action="store_true")
    return p.parse_args()


def backup(root: Path, src: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = root / "tools/repo/_phase90_backups" / f"alembic_{ts}" / src.relative_to(root)
    dst.parent.mkdir(parents=True, exist_ok=True)

    if src.is_dir():
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)

    return dst


def copy_path(src: Path, dst: Path, dry_run: bool, remove_source: bool) -> dict:
    if not src.exists():
        return {"source": str(src), "target": str(dst), "status": "skipped", "reason": "source_missing"}

    if dry_run:
        return {"source": str(src), "target": str(dst), "status": "would_copy", "reason": "dry_run"}

    dst.parent.mkdir(parents=True, exist_ok=True)

    if src.is_dir():
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)

    if remove_source:
        if src.is_dir():
            shutil.rmtree(src)
        else:
            src.unlink()
        status = "moved"
    else:
        status = "copied"

    return {"source": str(src), "target": str(dst), "status": status, "reason": "ok"}


def patch_files(root: Path, dry_run: bool) -> list[dict]:
    results = []

    for rel in PATCH_FILES:
        path = root / rel
        if not path.exists():
            results.append({"file": rel, "status": "skipped", "reason": "missing"})
            continue

        original = path.read_text(encoding="utf-8")
        updated = original
        replacements = 0

        for old, new in REPLS.items():
            count = updated.count(old)
            if count:
                replacements += count
                updated = updated.replace(old, new)

        if updated == original:
            results.append({"file": rel, "status": "unchanged", "replacements": 0})
            continue

        if dry_run:
            results.append({"file": rel, "status": "would_update", "replacements": replacements})
        else:
            path.write_text(updated, encoding="utf-8")
            results.append({"file": rel, "status": "updated", "replacements": replacements})

    return results


def remove_empty_legacy_app(root: Path, dry_run: bool) -> dict:
    legacy = root / "onehaven_decision_engine/backend/app"
    if not legacy.exists():
        return {"path": str(legacy), "status": "skipped", "reason": "missing"}

    remaining = [p for p in legacy.rglob("*") if p.is_file()]
    if remaining:
        return {
            "path": str(legacy),
            "status": "skipped",
            "reason": "not_empty",
            "remaining_files": [p.relative_to(root).as_posix() for p in remaining],
        }

    if dry_run:
        return {"path": str(legacy), "status": "would_remove", "reason": "empty"}

    shutil.rmtree(legacy)
    return {"path": str(legacy), "status": "removed", "reason": "empty"}


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()

    move_results = []
    for src_rel, dst_rel in MOVES.items():
        src = root / src_rel
        dst = root / dst_rel
        if src.exists() and not args.dry_run:
            backup(root, src)
        move_results.append(copy_path(src, dst, args.dry_run, args.remove_source))

    patch_results = patch_files(root, args.dry_run)
    empty_result = remove_empty_legacy_app(root, args.dry_run)

    payload = {
        "phase": 90,
        "dry_run": args.dry_run,
        "remove_source": args.remove_source,
        "moves": move_results,
        "patches": patch_results,
        "legacy_app": empty_result,
    }

    out = root / "tools/repo/alembic-relocation-phase90-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 90 complete.")
    print({
        "moves": len(move_results),
        "patches": len(patch_results),
        "legacy_app_status": empty_result["status"],
    })
    print("Report written to tools/repo/alembic-relocation-phase90-report.json")


if __name__ == "__main__":
    main()