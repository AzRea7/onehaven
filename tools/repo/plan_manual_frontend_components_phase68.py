#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE64_REPORT = "tools/repo/frontend-components-phase64-report.json"


@dataclass
class ComponentPlan:
    source: str
    target: str | None
    owner: str
    confidence: str
    reason: str


PLATFORM_EXACT = {
    "AuroraBackground.tsx": "onehaven_onehaven_platform/frontend/src/components/AuroraBackground.tsx",
    "EmptyState.tsx": "onehaven_onehaven_platform/frontend/src/components/EmptyState.tsx",
    "FilterBar.tsx": "onehaven_onehaven_platform/frontend/src/components/FilterBar.tsx",
    "GlassCard.tsx": "onehaven_onehaven_platform/frontend/src/components/GlassCard.tsx",
    "GlobalFilters.tsx": "onehaven_onehaven_platform/frontend/src/components/GlobalFilters.tsx",
    "Golem.tsx": "onehaven_onehaven_platform/frontend/src/components/Golem.tsx",
    "KpiCard.tsx": "onehaven_onehaven_platform/frontend/src/components/KpiCard.tsx",
    "PageHero.tsx": "onehaven_onehaven_platform/frontend/src/components/PageHero.tsx",
    "PageShell.tsx": "onehaven_onehaven_platform/frontend/src/components/PageShell.tsx",
    "PaneSwitcher.tsx": "onehaven_onehaven_platform/frontend/src/components/PaneSwitcher.tsx",
    "Shell.tsx": "onehaven_onehaven_platform/frontend/src/components/Shell.tsx",
    "Spinner.tsx": "onehaven_onehaven_platform/frontend/src/components/Spinner.tsx",
    "StatCard.tsx": "onehaven_onehaven_platform/frontend/src/components/StatCard.tsx",
    "StatPill.tsx": "onehaven_onehaven_platform/frontend/src/components/StatPill.tsx",
    "Surface.tsx": "onehaven_onehaven_platform/frontend/src/components/Surface.tsx",
    "VirtualList.tsx": "onehaven_onehaven_platform/frontend/src/components/VirtualList.tsx",
}

ACQUIRE_EXACT = {
    "IngestionErrorsDrawer.tsx": "products/acquire/frontend/src/components/IngestionErrorsDrawer.tsx",
    "IngestionLaunchCard.tsx": "products/acquire/frontend/src/components/IngestionLaunchCard.tsx",
    "IngestionRunsPanel.tsx": "products/acquire/frontend/src/components/IngestionRunsPanel.tsx",
    "IngestionSourcesPanel.tsx": "products/acquire/frontend/src/components/IngestionSourcesPanel.tsx",
}

COMPLIANCE_EXACT = {
    "MarketSourcePackModal.tsx": "products/compliance/frontend/src/components/MarketSourcePackModal.tsx",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan manual-review frontend components.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def classify(source: str) -> tuple[str, str | None, str, str]:
    name = Path(source).name

    if name in PLATFORM_EXACT:
        return "platform", PLATFORM_EXACT[name], "high", "manual_platform_exact"
    if name in ACQUIRE_EXACT:
        return "acquire", ACQUIRE_EXACT[name], "high", "manual_acquire_exact"
    if name in COMPLIANCE_EXACT:
        return "compliance", COMPLIANCE_EXACT[name], "medium", "manual_compliance_exact"

    return "manual_review", None, "low", "still_unmapped"


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    report_path = repo_root / PHASE64_REPORT

    if not report_path.exists():
        raise SystemExit(f"Missing report: {report_path}")

    phase64 = read_json(report_path)
    plans: list[ComponentPlan] = []

    for item in phase64.get("plans", []):
        if item.get("owner") != "manual_review":
            continue

        source = item["source"]
        owner, target, confidence, reason = classify(source)

        plans.append(
            ComponentPlan(
                source=source,
                target=target,
                owner=owner,
                confidence=confidence,
                reason=reason,
            )
        )

    payload = {
        "phase": 68,
        "summary": {
            "total": len(plans),
            "platform": sum(1 for p in plans if p.owner == "platform"),
            "acquire": sum(1 for p in plans if p.owner == "acquire"),
            "compliance": sum(1 for p in plans if p.owner == "compliance"),
            "manual_review": sum(1 for p in plans if p.owner == "manual_review"),
        },
        "plans": [asdict(p) for p in plans],
    }

    out = repo_root / "tools" / "repo" / "manual-frontend-components-phase68-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 68 complete.")
    print(f"Total manual components planned: {payload['summary']['total']}")
    print(f"Platform: {payload['summary']['platform']}")
    print(f"Acquire: {payload['summary']['acquire']}")
    print(f"Compliance: {payload['summary']['compliance']}")
    print(f"Manual review: {payload['summary']['manual_review']}")
    print("Report written to tools/repo/manual-frontend-components-phase68-report.json")


if __name__ == "__main__":
    main()