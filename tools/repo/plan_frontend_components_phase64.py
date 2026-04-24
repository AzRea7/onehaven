#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class ComponentPlan:
    source: str
    target: str | None
    owner: str
    confidence: str
    reason: str


SOURCE_ROOT = "onehaven_decision_engine/frontend/src/components"

PLATFORM_EXACT = {
    "AnimatedBackdrop.tsx": "onehaven_onehaven_platform/frontend/src/components/AnimatedBackdrop.tsx",
    "AppFooter.tsx": "onehaven_onehaven_platform/frontend/src/components/AppFooter.tsx",
    "AppHeader.tsx": "onehaven_onehaven_platform/frontend/src/components/AppHeader.tsx",
    "AppSelect.tsx": "onehaven_onehaven_platform/frontend/src/components/AppSelect.tsx",
    "AppShell.tsx": "onehaven_onehaven_platform/frontend/src/components/AppShell.tsx",
    "Artwork.tsx": "onehaven_onehaven_platform/frontend/src/components/Artwork.tsx",
    "MetricCard.tsx": "onehaven_onehaven_platform/frontend/src/components/MetricCard.tsx",
    "RequireAuth.tsx": "onehaven_onehaven_platform/frontend/src/components/RequireAuth.tsx",
    "SearchBar.tsx": "onehaven_onehaven_platform/frontend/src/components/SearchBar.tsx",
    "SectionHeader.tsx": "onehaven_onehaven_platform/frontend/src/components/SectionHeader.tsx",
    "StatusBadge.tsx": "onehaven_onehaven_platform/frontend/src/components/StatusBadge.tsx",
    "TabbedPanel.tsx": "onehaven_onehaven_platform/frontend/src/components/TabbedPanel.tsx",
}

INTELLIGENCE_EXACT = {
    "DealCard.tsx": "products/intelligence/frontend/src/components/DealCard.tsx",
    "DealFilterBar.tsx": "products/intelligence/frontend/src/components/DealFilterBar.tsx",
    "RentExplainPanel.tsx": "products/intelligence/frontend/src/components/RentExplainPanel.tsx",
}

COMPLIANCE_EXACT = {
    "JurisdictionCoverageBadge.tsx": "products/compliance/frontend/src/components/JurisdictionCoverageBadge.tsx",
    "PropertyJurisdictionRulesPanel.tsx": "products/compliance/frontend/src/components/PropertyJurisdictionRulesPanel.tsx",
    "InspectionReadiness.tsx": "products/compliance/frontend/src/components/InspectionReadiness.tsx",
    "ComplianceDocumentUploader.tsx": "products/compliance/frontend/src/components/ComplianceDocumentUploader.tsx",
    "ComplianceDocumentStack.tsx": "products/compliance/frontend/src/components/ComplianceDocumentStack.tsx",
    "CompliancePhotoFindingsPanel.tsx": "products/compliance/frontend/src/components/CompliancePhotoFindingsPanel.tsx",
}

OPS_EXACT = {
    "TaskBoard.tsx": "products/ops/frontend/src/components/TaskBoard.tsx",
    "InspectionCalendar.tsx": "products/ops/frontend/src/components/InspectionCalendar.tsx",
    "LeasePanel.tsx": "products/ops/frontend/src/components/LeasePanel.tsx",
}

TENANTS_EXACT = {
    "TenantCard.tsx": "products/tenants/frontend/src/components/TenantCard.tsx",
    "VoucherStatusBadge.tsx": "products/tenants/frontend/src/components/VoucherStatusBadge.tsx",
}

ACQUIRE_EXACT = {
    "ImportWizard.tsx": "products/acquire/frontend/src/components/ImportWizard.tsx",
    "CloseChecklist.tsx": "products/acquire/frontend/src/components/CloseChecklist.tsx",
}

PLATFORM_PREFIXES = [
    "AgentRun",
    "AgentRuns",
    "AgentSlots",
    "App",
]

INTELLIGENCE_PREFIXES = [
    "Deal",
    "Rent",
    "Valuation",
    "Cashflow",
]

COMPLIANCE_PREFIXES = [
    "Compliance",
    "Inspection",
    "Jurisdiction",
    "PropertyJurisdiction",
]

OPS_PREFIXES = [
    "Task",
    "Lease",
    "PropertyPhoto",
    "Rehab",
]

TENANTS_PREFIXES = [
    "Tenant",
    "Voucher",
    "Applicant",
]

ACQUIRE_PREFIXES = [
    "Import",
    "Closing",
    "Checklist",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan frontend component migration.")
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


def starts_with_any(name: str, prefixes: list[str]) -> bool:
    return any(name.startswith(prefix) for prefix in prefixes)


def classify(name: str) -> tuple[str, str | None, str, str]:
    if name in PLATFORM_EXACT:
        return "platform", PLATFORM_EXACT[name], "high", "platform_exact_match"
    if starts_with_any(name, PLATFORM_PREFIXES):
        return "platform", f"onehaven_onehaven_platform/frontend/src/components/{name}", "medium", "platform_prefix_match"

    if name in INTELLIGENCE_EXACT:
        return "intelligence", INTELLIGENCE_EXACT[name], "high", "intelligence_exact_match"
    if starts_with_any(name, INTELLIGENCE_PREFIXES):
        return "intelligence", f"products/intelligence/frontend/src/components/{name}", "medium", "intelligence_prefix_match"

    if name in COMPLIANCE_EXACT:
        return "compliance", COMPLIANCE_EXACT[name], "high", "compliance_exact_match"
    if starts_with_any(name, COMPLIANCE_PREFIXES):
        return "compliance", f"products/compliance/frontend/src/components/{name}", "medium", "compliance_prefix_match"

    if name in OPS_EXACT:
        return "ops", OPS_EXACT[name], "high", "ops_exact_match"
    if starts_with_any(name, OPS_PREFIXES):
        return "ops", f"products/ops/frontend/src/components/{name}", "medium", "ops_prefix_match"

    if name in TENANTS_EXACT:
        return "tenants", TENANTS_EXACT[name], "high", "tenants_exact_match"
    if starts_with_any(name, TENANTS_PREFIXES):
        return "tenants", f"products/tenants/frontend/src/components/{name}", "medium", "tenants_prefix_match"

    if name in ACQUIRE_EXACT:
        return "acquire", ACQUIRE_EXACT[name], "high", "acquire_exact_match"
    if starts_with_any(name, ACQUIRE_PREFIXES):
        return "acquire", f"products/acquire/frontend/src/components/{name}", "medium", "acquire_prefix_match"

    return "manual_review", None, "low", "no_match"


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    component_root = repo_root / SOURCE_ROOT

    plans: list[ComponentPlan] = []

    for p in sorted(component_root.glob("*.tsx")):
        owner, target, confidence, reason = classify(p.name)
        plans.append(
            ComponentPlan(
                source=p.relative_to(repo_root).as_posix(),
                target=target,
                owner=owner,
                confidence=confidence,
                reason=reason,
            )
        )

    payload = {
        "phase": 64,
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

    out = repo_root / "tools" / "repo" / "frontend-components-phase64-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 64 complete.")
    print(f"Total components planned: {payload['summary']['total']}")
    print(f"Platform: {payload['summary']['platform']}")
    print(f"Compliance: {payload['summary']['compliance']}")
    print(f"Intelligence: {payload['summary']['intelligence']}")
    print(f"Acquire: {payload['summary']['acquire']}")
    print(f"Ops: {payload['summary']['ops']}")
    print(f"Tenants: {payload['summary']['tenants']}")
    print(f"Manual review: {payload['summary']['manual_review']}")
    print("Report written to tools/repo/frontend-components-phase64-report.json")


if __name__ == "__main__":
    main()