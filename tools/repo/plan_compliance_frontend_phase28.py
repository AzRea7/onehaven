#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class ComplianceFrontendRecord:
    source: str
    area: str
    suggested_target: str | None
    confidence: str
    reason: str


SCAN_ROOTS = [
    "onehaven_decision_engine/frontend/src/products/compliance",
    "onehaven_decision_engine/frontend/src/components",
    "onehaven_decision_engine/frontend/src/pages",
]

KEYWORDS = {
    "components": [
        "ComplianceDocumentStack",
        "ComplianceDocumentUploader",
        "CompliancePhotoFindingsPanel",
        "ComplianceReminderPanel",
        "InspectionReadiness",
        "InspectionSchedulerModal",
        "InspectionTimelineCard",
        "JurisdictionCoverageBadge",
        "PropertyJurisdictionRulesPanel",
    ],
    "pages": [
        "CompliancePane",
    ],
}

TARGETS = {
    "components": "products/compliance/frontend/src/components",
    "pages": "products/compliance/frontend/src/pages",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan Compliance frontend migration."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / "onehaven_decision_engine",
        repo_root / "products" / "compliance" / "frontend" / "src",
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def collect_files(repo_root: Path) -> list[Path]:
    files: list[Path] = []
    for rel in SCAN_ROOTS:
        root = repo_root / rel
        if not root.exists():
            continue
        for p in root.rglob("*"):
            if p.is_file() and p.suffix.lower() in {".tsx", ".ts"}:
                files.append(p)
    return sorted(files)


def classify(path: Path, repo_root: Path) -> ComplianceFrontendRecord:
    rel = path.relative_to(repo_root).as_posix()
    name = path.name

    for area, keywords in KEYWORDS.items():
        for keyword in keywords:
            if keyword in name or keyword.lower() in rel.lower():
                return ComplianceFrontendRecord(
                    source=rel,
                    area=area,
                    suggested_target=f"{TARGETS[area]}/{name}",
                    confidence="medium",
                    reason=f"matched_keyword:{keyword}",
                )

    return ComplianceFrontendRecord(
        source=rel,
        area="manual_review",
        suggested_target=None,
        confidence="low",
        reason="no_compliance_frontend_keyword_match",
    )


def write_report(repo_root: Path, payload: dict) -> None:
    out = repo_root / "tools" / "repo" / "compliance-frontend-phase28-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    files = collect_files(repo_root)
    records = [classify(p, repo_root) for p in files]

    payload = {
        "phase": 28,
        "file_count": len(records),
        "summary": {
            "components": sum(1 for r in records if r.area == "components"),
            "pages": sum(1 for r in records if r.area == "pages"),
            "manual_review": sum(1 for r in records if r.area == "manual_review"),
        },
        "records": [asdict(r) for r in records],
    }

    write_report(repo_root, payload)

    print("Phase 28 planning complete.")
    print(f"Files scanned: {len(records)}")
    print(f"Components: {payload['summary']['components']}")
    print(f"Pages: {payload['summary']['pages']}")
    print(f"Manual review: {payload['summary']['manual_review']}")
    print("Report written to tools/repo/compliance-frontend-phase28-report.json")


if __name__ == "__main__":
    main()