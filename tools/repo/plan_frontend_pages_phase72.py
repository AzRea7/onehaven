#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class PagePlan:
    source: str
    target: str | None
    owner: str
    confidence: str
    reason: str


SOURCE_ROOT = "onehaven_decision_engine/frontend/src/pages"

PLATFORM_EXACT = {
    "Agents.tsx": "onehaven_onehaven_platform/frontend/src/pages/Agents.tsx",
    "AgentsPanel.tsx": "onehaven_onehaven_platform/frontend/src/pages/AgentsPanel.tsx",
    "Constitution.tsx": "onehaven_onehaven_platform/frontend/src/pages/Constitution.tsx",
    "Login.tsx": "onehaven_onehaven_platform/frontend/src/pages/Login.tsx",
    "Register.tsx": "onehaven_onehaven_platform/frontend/src/pages/Register.tsx",
}

ACQUIRE_EXACT = {
    "ImportsPage.tsx": "products/acquire/frontend/src/pages/ImportsPage.tsx",
}

COMPLIANCE_EXACT = {
    "JurisdictionProfiles.tsx": "products/compliance/frontend/src/pages/JurisdictionProfiles.tsx",
    "Jurisdictions.tsx": "products/compliance/frontend/src/pages/Jurisdictions.tsx",
    "PolicyReview.tsx": "products/compliance/frontend/src/pages/PolicyReview.tsx",
}

OPS_EXACT = {
    "Property.tsx": "products/ops/frontend/src/pages/Property.tsx",
}

INTELLIGENCE_EXACT = {
    "InvestorPane.tsx": "products/intelligence/frontend/src/pages/InvestorPane.tsx",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan frontend page migration.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def classify(name: str) -> tuple[str, str | None, str, str]:
    if name in PLATFORM_EXACT:
        return "platform", PLATFORM_EXACT[name], "high", "platform_exact_match"
    if name in ACQUIRE_EXACT:
        return "acquire", ACQUIRE_EXACT[name], "high", "acquire_exact_match"
    if name in COMPLIANCE_EXACT:
        return "compliance", COMPLIANCE_EXACT[name], "high", "compliance_exact_match"
    if name in OPS_EXACT:
        return "ops", OPS_EXACT[name], "high", "ops_exact_match"
    if name in INTELLIGENCE_EXACT:
        return "intelligence", INTELLIGENCE_EXACT[name], "high", "intelligence_exact_match"

    return "manual_review", None, "low", "no_match"


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    page_root = repo_root / SOURCE_ROOT

    if not page_root.exists():
        raise SystemExit(f"Missing page root: {page_root}")

    plans: list[PagePlan] = []

    for p in sorted(page_root.glob("*.tsx")):
        owner, target, confidence, reason = classify(p.name)
        plans.append(
            PagePlan(
                source=p.relative_to(repo_root).as_posix(),
                target=target,
                owner=owner,
                confidence=confidence,
                reason=reason,
            )
        )

    payload = {
        "phase": 72,
        "summary": {
            "total": len(plans),
            "platform": sum(1 for p in plans if p.owner == "platform"),
            "acquire": sum(1 for p in plans if p.owner == "acquire"),
            "compliance": sum(1 for p in plans if p.owner == "compliance"),
            "ops": sum(1 for p in plans if p.owner == "ops"),
            "intelligence": sum(1 for p in plans if p.owner == "intelligence"),
            "manual_review": sum(1 for p in plans if p.owner == "manual_review"),
        },
        "plans": [asdict(p) for p in plans],
    }

    out = repo_root / "tools" / "repo" / "frontend-pages-phase72-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 72 complete.")
    print(f"Total pages planned: {payload['summary']['total']}")
    print(f"Platform: {payload['summary']['platform']}")
    print(f"Acquire: {payload['summary']['acquire']}")
    print(f"Compliance: {payload['summary']['compliance']}")
    print(f"Ops: {payload['summary']['ops']}")
    print(f"Intelligence: {payload['summary']['intelligence']}")
    print(f"Manual review: {payload['summary']['manual_review']}")
    print("Report written to tools/repo/frontend-pages-phase72-report.json")


if __name__ == "__main__":
    main()