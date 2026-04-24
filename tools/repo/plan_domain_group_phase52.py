#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class DomainPlan:
    source: str
    target: str | None
    owner: str
    confidence: str
    reason: str


SOURCE_ROOT = "onehaven_decision_engine/backend/app/domain"

PLATFORM_EXACT = {
    "__init__.py": "onehaven_onehaven_platform/backend/src/domain/__init__.py",
    "audit.py": "onehaven_onehaven_platform/backend/src/domain/audit.py",
    "events.py": "onehaven_onehaven_platform/backend/src/domain/events.py",
    "fingerprint.py": "onehaven_onehaven_platform/backend/src/domain/fingerprint.py",
    "operating_truth.py": "onehaven_onehaven_platform/backend/src/domain/operating_truth.py",
    "operating_truth_enforcement.py": "onehaven_onehaven_platform/backend/src/domain/operating_truth_enforcement.py",
}

COMPLIANCE_EXACT = {
    "compliance.py": "products/compliance/backend/src/domain/compliance.py",
    "jurisdiction_scoring.py": "products/compliance/backend/src/domain/jurisdiction_scoring.py",
}

INTELLIGENCE_EXACT = {
    "cashflow.py": "products/intelligence/backend/src/domain/cashflow.py",
    "decision_engine.py": "products/intelligence/backend/src/domain/decision_engine.py",
    "rent_explain_runs.py": "products/intelligence/backend/src/domain/rent_explain_runs.py",
    "rent_learning.py": "products/intelligence/backend/src/domain/rent_learning.py",
    "underwriting.py": "products/intelligence/backend/src/domain/underwriting.py",
    "valuation_cadence.py": "products/intelligence/backend/src/domain/valuation_cadence.py",
}

ACQUIRE_EXACT = {}

OPS_EXACT = {}

TENANTS_EXACT = {}

PLATFORM_PREFIXES = [
    "agents/",
    "policy/",
]

COMPLIANCE_PREFIXES = [
    "compliance/",
]

INTELLIGENCE_PREFIXES = [
    "importers/",
]

ACQUIRE_PREFIXES = []

OPS_PREFIXES = []

TENANTS_PREFIXES = []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan backend/app/domain migration.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / SOURCE_ROOT,
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def classify(rel_path: str) -> tuple[str, str | None, str, str]:
    name = Path(rel_path).name
    lower_rel = rel_path.lower()

    if name in PLATFORM_EXACT:
        return "platform", PLATFORM_EXACT[name], "high", "platform_exact_match"
    if any(lower_rel.startswith(prefix) for prefix in PLATFORM_PREFIXES):
        return "platform", f"onehaven_onehaven_platform/backend/src/domain/{rel_path}", "high", "platform_prefix_match"

    if name in COMPLIANCE_EXACT:
        return "compliance", COMPLIANCE_EXACT[name], "high", "compliance_exact_match"
    if any(lower_rel.startswith(prefix) for prefix in COMPLIANCE_PREFIXES):
        return "compliance", f"products/compliance/backend/src/domain/{rel_path}", "medium", "compliance_prefix_match"

    if name in INTELLIGENCE_EXACT:
        return "intelligence", INTELLIGENCE_EXACT[name], "high", "intelligence_exact_match"
    if any(lower_rel.startswith(prefix) for prefix in INTELLIGENCE_PREFIXES):
        return "intelligence", f"products/intelligence/backend/src/domain/{rel_path}", "medium", "intelligence_prefix_match"

    if name in ACQUIRE_EXACT:
        return "acquire", ACQUIRE_EXACT[name], "medium", "acquire_exact_match"
    if any(lower_rel.startswith(prefix) for prefix in ACQUIRE_PREFIXES):
        return "acquire", f"products/acquire/backend/src/domain/{rel_path}", "medium", "acquire_prefix_match"

    if name in OPS_EXACT:
        return "ops", OPS_EXACT[name], "medium", "ops_exact_match"
    if any(lower_rel.startswith(prefix) for prefix in OPS_PREFIXES):
        return "ops", f"products/ops/backend/src/domain/{rel_path}", "medium", "ops_prefix_match"

    if name in TENANTS_EXACT:
        return "tenants", TENANTS_EXACT[name], "medium", "tenants_exact_match"
    if any(lower_rel.startswith(prefix) for prefix in TENANTS_PREFIXES):
        return "tenants", f"products/tenants/backend/src/domain/{rel_path}", "medium", "tenants_prefix_match"

    return "manual_review", None, "low", "no_match"


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    domain_root = repo_root / SOURCE_ROOT

    plans: list[DomainPlan] = []

    for p in sorted(domain_root.rglob("*.py")):
        rel = p.relative_to(domain_root).as_posix()
        owner, target, confidence, reason = classify(rel)
        plans.append(
            DomainPlan(
                source=p.relative_to(repo_root).as_posix(),
                target=target,
                owner=owner,
                confidence=confidence,
                reason=reason,
            )
        )

    payload = {
        "phase": 52,
        "summary": {
            "total": len(plans),
            "platform": sum(1 for p in plans if p.owner == "platform"),
            "compliance": sum(1 for p in plans if p.owner == "compliance"),
            "intelligence": sum(1 for p in plans if p.owner == "intelligence"),
            "acquire": sum(1 for p in plans if p.owner == "acquire"),
            "ops": sum(1 for p in plans if p.owner == "ops"),
            "tenants": sum(1 for p in plans if p.owner == "tenants"),
            "manual_review": sum(1 for p in plans if p.owner == "manual_review"),
        },
        "plans": [asdict(p) for p in plans],
    }

    out = repo_root / "tools" / "repo" / "domain-group-phase52-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 52 complete.")
    print(f"Total domain files planned: {payload['summary']['total']}")
    print(f"Platform: {payload['summary']['platform']}")
    print(f"Compliance: {payload['summary']['compliance']}")
    print(f"Intelligence: {payload['summary']['intelligence']}")
    print(f"Acquire: {payload['summary']['acquire']}")
    print(f"Ops: {payload['summary']['ops']}")
    print(f"Tenants: {payload['summary']['tenants']}")
    print(f"Manual review: {payload['summary']['manual_review']}")
    print("Report written to tools/repo/domain-group-phase52-report.json")


if __name__ == "__main__":
    main()