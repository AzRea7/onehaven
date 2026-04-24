#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path


TARGETS = {
    "onehaven_decision_engine/backend/app/main.py": "apps/suite_api/app/main.py",
    "onehaven_decision_engine/backend/app/auth.py": "onehaven_platform/backend/src/auth.py",
    "onehaven_decision_engine/backend/app/config.py": "onehaven_platform/backend/src/config/__init__.py",
    "onehaven_decision_engine/backend/app/db.py": "onehaven_platform/backend/src/db/__init__.py",
    "onehaven_decision_engine/backend/app/logging_config.py": "onehaven_platform/backend/src/logging_config.py",
    "onehaven_decision_engine/backend/app/models.py": "onehaven_platform/backend/src/models.py",
    "onehaven_decision_engine/backend/app/policy_models.py": "onehaven_platform/backend/src/policy_models.py",
    "onehaven_decision_engine/backend/app/schemas.py": "onehaven_platform/backend/src/schemas.py",
    "onehaven_decision_engine/backend/app/integrations/lm_studio_client.py": "onehaven_platform/backend/src/integrations/lm_studio_client.py",
    "onehaven_decision_engine/backend/app/middleware/request_id.py": "onehaven_platform/backend/src/middleware/request_id.py",
    "onehaven_decision_engine/backend/app/middleware/structured_logging.py": "onehaven_platform/backend/src/middleware/structured_logging.py",
}


COPY_DIR_TARGETS = [
    ("onehaven_decision_engine/backend/app/services", "onehaven_platform/backend/src/services"),
    ("onehaven_decision_engine/backend/app/clients", "onehaven_platform/backend/src/integrations"),
    ("onehaven_decision_engine/backend/app/workers", "onehaven_platform/backend/src/jobs"),
    ("onehaven_decision_engine/backend/app/tasks", "onehaven_platform/backend/src/jobs"),
    ("onehaven_decision_engine/backend/app/domain", "onehaven_platform/backend/src/domain"),
]


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def backup(root: Path, src: Path) -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_root = root / "tools/repo/_phase83_backups" / f"runtime_leftovers_{ts}"
    dst = backup_root / src.relative_to(root)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force-main", action="store_true")
    return p.parse_args()


def add_dir_targets(root: Path, targets: dict[str, str]) -> None:
    for src_root_rel, dst_root_rel in COPY_DIR_TARGETS:
        src_root = root / src_root_rel
        dst_root = root / dst_root_rel
        if not src_root.exists():
            continue

        for src in src_root.rglob("*"):
            if not src.is_file():
                continue
            if "__pycache__" in src.parts or src.suffix == ".pyc":
                continue

            rel = src.relative_to(src_root)
            targets[src.relative_to(root).as_posix()] = (dst_root / rel).relative_to(root).as_posix()


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()

    targets = dict(TARGETS)
    add_dir_targets(root, targets)

    results = []

    for src_rel, dst_rel in sorted(targets.items()):
        src = root / src_rel
        dst = root / dst_rel

        if not src.exists():
            results.append({"source": src_rel, "target": dst_rel, "status": "skipped", "reason": "source_missing"})
            continue

        if not dst.exists():
            results.append({"source": src_rel, "target": dst_rel, "status": "skipped", "reason": "target_missing"})
            continue

        same = sha(src) == sha(dst)

        if src_rel.endswith("/main.py") and not same and not args.force_main:
            results.append({"source": src_rel, "target": dst_rel, "status": "skipped", "reason": "main_differs_use_force_main"})
            continue

        if not same and not args.force_main:
            results.append({"source": src_rel, "target": dst_rel, "status": "skipped", "reason": "different"})
            continue

        if args.dry_run:
            results.append({"source": src_rel, "target": dst_rel, "status": "would_remove", "reason": "safe"})
            continue

        backup(root, src)
        src.unlink()
        results.append({"source": src_rel, "target": dst_rel, "status": "removed", "reason": "safe"})

    out = root / "tools/repo/runtime-leftovers-phase83-report.json"
    payload = {
        "phase": 83,
        "dry_run": args.dry_run,
        "force_main": args.force_main,
        "summary": {
            "would_remove": sum(r["status"] == "would_remove" for r in results),
            "removed": sum(r["status"] == "removed" for r in results),
            "skipped": sum(r["status"] == "skipped" for r in results),
        },
        "results": results,
    }
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 83 complete.")
    print(payload["summary"])
    print("Report written to tools/repo/runtime-leftovers-phase83-report.json")


if __name__ == "__main__":
    main()