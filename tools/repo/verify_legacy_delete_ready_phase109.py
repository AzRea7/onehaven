#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


CHECKS = {
    # moved data
    "storage/nspire": "required_dir",
    "storage/policy_raw": "required_dir",
    "storage/acquisition_docs": "required_dir",

    # moved infra
    "infra/docker/docker-compose.yml": "required_file",
    "infra/env/.env": "required_file",
    "infra/env/.env.example": "required_file",

    # backups created by phase 108
    "apps/suite_web": "required_dir",
    "apps/suite_api_legacy_backup": "required_dir",

    # new architecture must still exist
    "apps/suite_api": "required_dir",
    "apps/suite-web": "required_dir",
    "products": "required_dir",
    "onehaven_platform": "required_dir",
}


JUNK_SHOULD_BE_GONE = [
    "onehaven_decision_engine/frontend/node_modules",
    "onehaven_decision_engine/frontend/dist",
    "onehaven_decision_engine/.pytest_cache",
    "onehaven_decision_engine/backend/.pytest_cache",
    "onehaven_decision_engine/frontend/tsconfig.tsbuildinfo",
    "onehaven_decision_engine/backend/celerybeat-schedule",
]


def count_files(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return 1
    return sum(1 for p in path.rglob("*") if p.is_file())


def main():
    root = Path(".").resolve()

    results = []
    ok = True

    for rel, kind in CHECKS.items():
        p = root / rel

        if kind == "required_dir":
            passed = p.exists() and p.is_dir() and count_files(p) > 0
        else:
            passed = p.exists() and p.is_file()

        if not passed:
            ok = False

        results.append({
            "path": rel,
            "kind": kind,
            "exists": p.exists(),
            "file_count": count_files(p),
            "passed": passed,
        })

    junk_results = []
    for rel in JUNK_SHOULD_BE_GONE:
        p = root / rel
        gone = not p.exists()
        if not gone:
            ok = False

        junk_results.append({
            "path": rel,
            "gone": gone,
        })

    legacy = root / "onehaven_decision_engine"
    remaining_files = []
    if legacy.exists():
        remaining_files = [
            p.relative_to(root).as_posix()
            for p in legacy.rglob("*")
            if p.is_file()
        ]

    payload = {
        "phase": 109,
        "delete_ready": ok,
        "checks": results,
        "junk_checks": junk_results,
        "legacy_remaining_file_count": len(remaining_files),
        "legacy_remaining_sample": remaining_files[:200],
        "recommendation": (
            "SAFE_TO_DELETE onehaven_decision_engine"
            if ok
            else "DO_NOT_DELETE_YET review failed checks"
        ),
    }

    out = root / "tools/repo/legacy-delete-ready-phase109-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 109 complete.")
    print({
        "delete_ready": payload["delete_ready"],
        "legacy_remaining_file_count": payload["legacy_remaining_file_count"],
        "recommendation": payload["recommendation"],
    })
    print("Report written to tools/repo/legacy-delete-ready-phase109-report.json")


if __name__ == "__main__":
    main()