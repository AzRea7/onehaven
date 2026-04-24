#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE36_REPORT = "tools/repo/router-group-phase36-report.json"


@dataclass
class RouterReview:
    source: str
    suggested_target: str | None
    owner: str
    confidence: str
    reason: str


EXPLICIT_RULES = {
    "api_keys.py": ("onehaven_onehaven_platform/backend/src/identity/interfaces/api_keys_router.py", "platform", "medium", "identity_router"),
    "audit.py": ("onehaven_onehaven_platform/backend/src/audit/audit_router.py", "platform", "medium", "audit_router"),
    "auth.py": ("onehaven_onehaven_platform/backend/src/identity/interfaces/auth_router.py", "platform", "high", "identity_router"),
    "health.py": ("apps/suite-api/app/api/health/health.py", "app", "high", "health_runtime_router"),
    "meta.py": ("apps/suite-api/app/api/health/meta.py", "app", "medium", "meta_runtime_router"),
    "agents.py": ("onehaven_onehaven_platform/backend/src/agents/agents_router.py", "platform", "medium", "agents_router"),
    "agent_runs.py": ("onehaven_onehaven_platform/backend/src/agents/agent_runs_router.py", "platform", "medium", "agents_router"),
    "automation.py": ("onehaven_onehaven_platform/backend/src/workflow/automation_router.py", "platform", "medium", "workflow_router"),
    "geo.py": ("onehaven_onehaven_platform/backend/src/integrations/geo_router.py", "platform", "medium", "geo_router"),
    "ingestion.py": ("onehaven_onehaven_platform/backend/src/integrations/ingestion_router.py", "platform", "medium", "ingestion_router"),
    "imports.py": ("products/acquire/backend/src/routers/imports.py", "product:acquire", "medium", "acquire_router"),
    "imports_alias.py": ("products/acquire/backend/src/routers/imports_alias.py", "product:acquire", "medium", "acquire_router"),
    "rehab.py": ("products/ops/backend/src/routers/rehab.py", "product:ops", "medium", "ops_router"),
    "rent.py": ("products/intelligence/backend/src/routers/rent.py", "product:intelligence", "medium", "intelligence_router"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan unmapped legacy routers.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE36_REPORT,
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    report = read_json(repo_root / PHASE36_REPORT)

    reviews: list[RouterReview] = []

    for plan in report.get("plans", []):
        src = plan["source"]
        name = Path(src).name

        if plan.get("target"):
            continue

        if name in EXPLICIT_RULES:
            target, owner, confidence, reason = EXPLICIT_RULES[name]
            reviews.append(
                RouterReview(
                    source=src,
                    suggested_target=target,
                    owner=owner,
                    confidence=confidence,
                    reason=reason,
                )
            )
        else:
            reviews.append(
                RouterReview(
                    source=src,
                    suggested_target=None,
                    owner="manual_review",
                    confidence="low",
                    reason="no_explicit_router_mapping",
                )
            )

    payload = {
        "phase": 44,
        "review_count": len(reviews),
        "mapped_now": sum(1 for r in reviews if r.suggested_target),
        "still_manual": sum(1 for r in reviews if not r.suggested_target),
        "reviews": [asdict(r) for r in reviews],
    }

    out = repo_root / "tools" / "repo" / "unmapped-routers-phase44-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 44 complete.")
    print(f"Routers reviewed: {payload['review_count']}")
    print(f"Mapped now: {payload['mapped_now']}")
    print(f"Still manual: {payload['still_manual']}")
    print("Report written to tools/repo/unmapped-routers-phase44-report.json")


if __name__ == "__main__":
    main()
    