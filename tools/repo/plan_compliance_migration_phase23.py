#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class ComplianceRecord:
    source: str
    area: str
    suggested_target: str | None
    confidence: str
    reason: str
    lane: str


BACKEND_ROOTS = [
    "onehaven_decision_engine/backend/app/products/compliance",
    "onehaven_decision_engine/backend/app/services",
    "onehaven_decision_engine/backend/app/domain",
    "onehaven_decision_engine/backend/app/routers",
]

FRONTEND_ROOTS = [
    "onehaven_decision_engine/frontend/src/products/compliance",
    "onehaven_decision_engine/frontend/src/components",
    "onehaven_decision_engine/frontend/src/pages",
]

COMPLIANCE_KEYWORDS = {
    "policy_assertions": ["truth_resolution", "assertion", "cleanup_service"],
    "policy_coverage": ["coverage", "expected_universe", "lockout", "health_service"],
    "policy_governance": ["rules_service", "governance"],
    "policy_sources": ["catalog", "crawl", "dataset", "fetch", "source"],
    "inspections": ["inspection", "nspire", "hqs", "readiness", "template", "failure_task"],
    "compliance_engine": ["brief_service", "fix_plan", "inspection_risk", "recommendation", "revenue_risk"],
    "documents": ["document", "photo"],
    "router": ["router", "markets", "policy_evidence", "policy_catalog_admin", "jurisdiction_profiles"],
    "frontend_components": ["Compliance", "Inspection", "Jurisdiction", "Policy"],
    "frontend_pages": ["CompliancePane"],
}

TARGET_MAP = {
    "policy_assertions": "products/compliance/backend/src/services/policy_assertions",
    "policy_coverage": "products/compliance/backend/src/services/policy_coverage",
    "policy_governance": "products/compliance/backend/src/services/policy_governance",
    "policy_sources": "products/compliance/backend/src/services/policy_sources",
    "inspections": "products/compliance/backend/src/services/inspections",
    "compliance_engine": "products/compliance/backend/src/services/compliance_engine",
    "documents": "products/compliance/backend/src/services",
    "router": "products/compliance/backend/src/routers",
    "frontend_components": "products/compliance/frontend/src/components",
    "frontend_pages": "products/compliance/frontend/src/pages",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan Compliance-specific migration lanes."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / "onehaven_decision_engine",
        repo_root / "products" / "compliance",
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- " + "\n- ".join(missing)
        )
    return repo_root


def collect_files(repo_root: Path) -> list[Path]:
    files: list[Path] = []
    for rel in BACKEND_ROOTS + FRONTEND_ROOTS:
        root = repo_root / rel
        if not root.exists():
            continue
        for p in root.rglob("*"):
            if p.is_file() and p.suffix.lower() in {".py", ".tsx", ".ts"}:
                files.append(p)
    return sorted(files)


def classify_file(repo_root: Path, path: Path) -> ComplianceRecord:
    rel = path.relative_to(repo_root).as_posix()
    name = path.name.lower()

    for area, keywords in COMPLIANCE_KEYWORDS.items():
        for keyword in keywords:
            if keyword.lower() in rel.lower() or keyword.lower() in name:
                target_base = TARGET_MAP.get(area)
                target = f"{target_base}/{path.name}" if target_base else None
                lane = "frontend" if rel.endswith((".tsx", ".ts")) else "backend"
                return ComplianceRecord(
                    source=rel,
                    area=area,
                    suggested_target=target,
                    confidence="medium",
                    reason=f"matched_keyword:{keyword}",
                    lane=lane,
                )

    return ComplianceRecord(
        source=rel,
        area="manual_review",
        suggested_target=None,
        confidence="low",
        reason="no_specific_compliance_lane_match",
        lane="frontend" if rel.endswith((".tsx", ".ts")) else "backend",
    )


def summarize(records: list[ComplianceRecord]) -> dict:
    summary: dict[str, int] = {}
    for rec in records:
        summary[rec.area] = summary.get(rec.area, 0) + 1
    return summary


def write_report(repo_root: Path, payload: dict) -> None:
    out = repo_root / "tools" / "repo" / "compliance-migration-phase23-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))

    files = collect_files(repo_root)
    records = [classify_file(repo_root, p) for p in files]

    payload = {
        "phase": 23,
        "description": "Compliance-specific migration planning report",
        "file_count": len(records),
        "summary": summarize(records),
        "records": [asdict(r) for r in records],
        "next_steps": [
            "Move backend compliance lanes in small batches by area",
            "Validate imports after each area",
            "Move frontend compliance files after backend areas stabilize",
            "Handle manual_review items last",
        ],
    }

    write_report(repo_root, payload)

    print("Phase 23 planning complete.")
    print(f"Files scanned: {len(records)}")
    print("Report written to tools/repo/compliance-migration-phase23-report.json")


if __name__ == "__main__":
    main()