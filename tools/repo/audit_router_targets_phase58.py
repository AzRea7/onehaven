#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass, asdict
from pathlib import Path


ROUTER_TARGETS = {
    "agents.py": "onehaven_onehaven_platform/backend/src/agents/agents_router.py",
    "agent_runs.py": "onehaven_onehaven_platform/backend/src/agents/agent_runs_router.py",
    "api_keys.py": "onehaven_onehaven_platform/backend/src/identity/interfaces/api_keys_router.py",
    "auth.py": "onehaven_onehaven_platform/backend/src/identity/interfaces/auth_router.py",
    "automation.py": "onehaven_onehaven_platform/backend/src/workflow/automation_router.py",
    "geo.py": "onehaven_onehaven_platform/backend/src/integrations/geo_router.py",
    "health.py": "apps/suite-api/app/api/health/health.py",
    "imports.py": "products/acquire/backend/src/routers/imports.py",
    "imports_alias.py": "products/acquire/backend/src/routers/imports_alias.py",
    "ingestion.py": "onehaven_onehaven_platform/backend/src/integrations/ingestion_router.py",
    "rehab.py": "products/ops/backend/src/routers/rehab.py",
    "rent.py": "products/intelligence/backend/src/routers/rent.py",
    "trust.py": "products/compliance/backend/src/routers/trust.py",
}

ROUTER_ROOT = "onehaven_decision_engine/backend/app/routers"


@dataclass
class AuditResult:
    source: str
    target: str | None
    source_exists: bool
    target_exists: bool
    source_hash: str | None
    target_hash: str | None
    status: str
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit remaining legacy routers against intended targets.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def classify(source_exists: bool, target_exists: bool, source_hash: str | None, target_hash: str | None) -> tuple[str, str]:
    if source_exists and target_exists and source_hash == target_hash:
        return "identical", "source_and_target_match"
    if source_exists and target_exists and source_hash != target_hash:
        return "different", "source_and_target_differ"
    if source_exists and not target_exists:
        return "missing_target", "legacy_exists_but_target_missing"
    if not source_exists and target_exists:
        return "already_migrated", "target_exists_but_legacy_missing"
    return "missing_both", "neither_source_nor_target_exists"


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    router_root = repo_root / ROUTER_ROOT

    if not router_root.exists():
        raise SystemExit(f"Missing router root: {router_root}")

    audits: list[AuditResult] = []

    for src in sorted(router_root.glob("*.py")):
        name = src.name
        target = ROUTER_TARGETS.get(name)
        dst = repo_root / target if target else None

        source_exists = src.exists()
        target_exists = bool(dst and dst.exists())
        source_hash = sha256_file(src) if source_exists else None
        target_hash = sha256_file(dst) if target_exists and dst else None

        status, reason = classify(source_exists, target_exists, source_hash, target_hash)

        audits.append(
            AuditResult(
                source=src.relative_to(repo_root).as_posix(),
                target=target,
                source_exists=source_exists,
                target_exists=target_exists,
                source_hash=source_hash,
                target_hash=target_hash,
                status=status,
                reason=reason,
            )
        )

    payload = {
        "phase": 58,
        "summary": {
            "total": len(audits),
            "identical": sum(1 for a in audits if a.status == "identical"),
            "different": sum(1 for a in audits if a.status == "different"),
            "missing_target": sum(1 for a in audits if a.status == "missing_target"),
            "already_migrated": sum(1 for a in audits if a.status == "already_migrated"),
            "missing_both": sum(1 for a in audits if a.status == "missing_both"),
        },
        "audits": [asdict(a) for a in audits],
    }

    out = repo_root / "tools" / "repo" / "router-target-audit-phase58-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 58 complete.")
    print(f"Total audited: {payload['summary']['total']}")
    print(f"Identical: {payload['summary']['identical']}")
    print(f"Different: {payload['summary']['different']}")
    print(f"Missing target: {payload['summary']['missing_target']}")
    print(f"Already migrated: {payload['summary']['already_migrated']}")
    print(f"Missing both: {payload['summary']['missing_both']}")
    print("Report written to tools/repo/router-target-audit-phase58-report.json")


if __name__ == "__main__":
    main()