#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class RouterPlan:
    source: str
    target: str | None
    owner: str
    confidence: str
    reason: str


ROUTER_TARGET_RULES = {
    "agent_runs.py": ("apps/suite-api/app/api/suite/agent_runs.py", "app", "medium", "suite_runtime_router"),
    "agents.py": ("apps/suite-api/app/api/suite/agents.py", "app", "medium", "suite_runtime_router"),
    "automation.py": ("onehaven_onehaven_platform/backend/src/workflow/automation_router.py", "platform", "medium", "workflow_platform_router"),
    "geo.py": ("onehaven_onehaven_platform/backend/src/integrations/geo_router.py", "platform", "medium", "geo_platform_router"),
    "imports.py": ("products/acquire/backend/src/routers/imports.py", "product:acquire", "medium", "acquire_imports_router"),
    "imports_alias.py": ("products/acquire/backend/src/routers/imports_alias.py", "product:acquire", "medium", "acquire_imports_router"),
    "ingestion.py": ("onehaven_onehaven_platform/backend/src/integrations/ingestion_router.py", "platform", "medium", "ingestion_platform_router"),
    "meta.py": ("apps/suite-api/app/api/health/meta.py", "app", "medium", "meta_runtime_router"),
    "rehab.py": ("products/ops/backend/src/routers/rehab.py", "product:ops", "medium", "ops_rehab_router"),
    "rent.py": ("products/intelligence/backend/src/routers/rent.py", "product:intelligence", "medium", "intelligence_rent_router"),
}

SOURCE_PREFIX = "onehaven_decision_engine/backend/app/routers"
PHASE35_REPORT = "tools/repo/leftovers-high-phase35-report.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan the backend/app/routers leftover group.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE35_REPORT,
        repo_root / "onehaven_decision_engine" / "backend" / "app" / "routers",
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    router_root = repo_root / SOURCE_PREFIX

    plans: list[RouterPlan] = []

    for p in sorted(router_root.glob("*.py")):
        name = p.name
        if name in ROUTER_TARGET_RULES:
            target, owner, confidence, reason = ROUTER_TARGET_RULES[name]
            plans.append(
                RouterPlan(
                    source=str(p.relative_to(repo_root).as_posix()),
                    target=target,
                    owner=owner,
                    confidence=confidence,
                    reason=reason,
                )
            )
        else:
            plans.append(
                RouterPlan(
                    source=str(p.relative_to(repo_root).as_posix()),
                    target=None,
                    owner="manual_review",
                    confidence="low",
                    reason="no_explicit_router_mapping",
                )
            )

    payload = {
        "phase": 36,
        "group": "backend/app/routers",
        "summary": {
            "total": len(plans),
            "mapped": sum(1 for p in plans if p.target),
            "manual_review": sum(1 for p in plans if not p.target),
        },
        "plans": [asdict(p) for p in plans],
    }

    out = repo_root / "tools" / "repo" / "router-group-phase36-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 36 complete.")
    print(f"Routers planned: {len(plans)}")
    print(f"Mapped: {payload['summary']['mapped']}")
    print(f"Manual review: {payload['summary']['manual_review']}")
    print("Report written to tools/repo/router-group-phase36-report.json")


if __name__ == "__main__":
    main()
    