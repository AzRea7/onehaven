#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path


COPY_DIRS = [
    # platform runtime/support
    ("onehaven_decision_engine/backend/app/services", "onehaven_platform/backend/src/services"),
    ("onehaven_decision_engine/backend/app/workers", "onehaven_platform/backend/src/jobs"),
    ("onehaven_decision_engine/backend/app/tasks", "onehaven_platform/backend/src/jobs"),
    ("onehaven_decision_engine/backend/app/clients", "onehaven_platform/backend/src/integrations"),
    ("onehaven_decision_engine/backend/app/integrations", "onehaven_platform/backend/src/integrations"),
    ("onehaven_decision_engine/backend/app/middleware", "onehaven_platform/backend/src/middleware"),
    ("onehaven_decision_engine/backend/app/domain", "onehaven_platform/backend/src/domain"),
]

COPY_FILES = [
    ("onehaven_decision_engine/backend/app/config.py", "onehaven_platform/backend/src/config/__init__.py"),
    ("onehaven_decision_engine/backend/app/db.py", "onehaven_platform/backend/src/db/__init__.py"),
    ("onehaven_decision_engine/backend/app/models.py", "onehaven_platform/backend/src/models.py"),
    ("onehaven_decision_engine/backend/app/policy_models.py", "onehaven_platform/backend/src/policy_models.py"),
    ("onehaven_decision_engine/backend/app/schemas.py", "onehaven_platform/backend/src/schemas.py"),
    ("onehaven_decision_engine/backend/app/auth.py", "onehaven_platform/backend/src/auth.py"),
    ("onehaven_decision_engine/backend/app/logging_config.py", "onehaven_platform/backend/src/logging_config.py"),
]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def copy_dir(src: Path, dst: Path, dry_run: bool) -> tuple[int, int]:
    copied = 0
    skipped = 0

    if not src.exists():
        return copied, skipped

    for file in src.rglob("*"):
        if not file.is_file():
            continue
        if "__pycache__" in file.parts or file.suffix == ".pyc":
            skipped += 1
            continue

        rel = file.relative_to(src)
        target = dst / rel

        if dry_run:
            print(f"[DRY RUN] copy {file} -> {target}")
            copied += 1
            continue

        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file, target)
        copied += 1

    return copied, skipped


def copy_file(src: Path, dst: Path, dry_run: bool) -> bool:
    if not src.exists():
        return False

    if dry_run:
        print(f"[DRY RUN] copy {src} -> {dst}")
        return True

    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def touch_package_inits(root: Path, dry_run: bool) -> int:
    count = 0
    for d in [
        root / "onehaven_platform",
        root / "onehaven_platform/backend",
        root / "onehaven_platform/backend/src",
    ]:
        for subdir in [d, *[p for p in d.rglob("*") if p.is_dir() and "__pycache__" not in p.parts]]:
            init = subdir / "__init__.py"
            if init.exists():
                continue
            if dry_run:
                print(f"[DRY RUN] touch {init}")
            else:
                init.touch()
            count += 1
    return count


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()

    copied = 0
    skipped = 0

    for src_rel, dst_rel in COPY_DIRS:
        c, s = copy_dir(root / src_rel, root / dst_rel, args.dry_run)
        copied += c
        skipped += s

    file_copied = 0
    for src_rel, dst_rel in COPY_FILES:
        if copy_file(root / src_rel, root / dst_rel, args.dry_run):
            file_copied += 1

    init_count = touch_package_inits(root, args.dry_run)

    print("Phase 81 complete.")
    print({
        "directory_files_copied": copied,
        "files_copied": file_copied,
        "pycache_skipped": skipped,
        "package_inits_created": init_count,
        "dry_run": args.dry_run,
    })


if __name__ == "__main__":
    main()