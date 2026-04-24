#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


SOURCE = "onehaven_decision_engine/backend/app/routers/meta.py"
TARGET = "apps/suite-api/app/api/health/meta.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit legacy meta router against target.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()

    src = repo_root / SOURCE
    dst = repo_root / TARGET

    source_exists = src.exists()
    target_exists = dst.exists()
    source_hash = sha256_file(src) if source_exists else None
    target_hash = sha256_file(dst) if target_exists else None

    if source_exists and target_exists and source_hash == target_hash:
        status = "identical"
    elif source_exists and target_exists and source_hash != target_hash:
        status = "different"
    elif source_exists and not target_exists:
        status = "missing_target"
    elif not source_exists and target_exists:
        status = "already_migrated"
    else:
        status = "missing_both"

    payload = {
        "phase": 62,
        "source": SOURCE,
        "target": TARGET,
        "source_exists": source_exists,
        "target_exists": target_exists,
        "source_hash": source_hash,
        "target_hash": target_hash,
        "status": status,
    }

    out = repo_root / "tools" / "repo" / "meta-router-phase62-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 62 complete.")
    print(f"Status: {status}")
    print("Report written to tools/repo/meta-router-phase62-report.json")


if __name__ == "__main__":
    main()