#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path


LANE_RULES = {
    "policy_sources": {
        "old": [
            "app.products.compliance.services.policy_sources",
            "backend.app.products.compliance.services.policy_sources",
        ],
        "new": "products.compliance.backend.src.services.policy_sources",
        "phase25_report": "tools/repo/compliance-policy_sources-validate-phase25-report.json",
    },
    "policy_governance": {
        "old": [
            "app.products.compliance.services.policy_governance",
            "backend.app.products.compliance.services.policy_governance",
        ],
        "new": "products.compliance.backend.src.services.policy_governance",
        "phase25_report": "tools/repo/compliance-policy_governance-validate-phase25-report.json",
    },
    "policy_coverage": {
        "old": [
            "app.products.compliance.services.policy_coverage",
            "backend.app.products.compliance.services.policy_coverage",
        ],
        "new": "products.compliance.backend.src.services.policy_coverage",
        "phase25_report": "tools/repo/compliance-policy_coverage-validate-phase25-report.json",
    },
    "policy_assertions": {
        "old": [
            "app.products.compliance.services.policy_assertions",
            "backend.app.products.compliance.services.policy_assertions",
        ],
        "new": "products.compliance.backend.src.services.policy_assertions",
        "phase25_report": "tools/repo/compliance-policy_assertions-validate-phase25-report.json",
    },
    "inspections": {
        "old": [
            "app.products.compliance.services.inspections",
            "backend.app.products.compliance.services.inspections",
        ],
        "new": "products.compliance.backend.src.services.inspections",
        "phase25_report": "tools/repo/compliance-inspections-validate-phase25-report.json",
    },
    "compliance_engine": {
        "old": [
            "app.products.compliance.services.compliance_engine",
            "backend.app.products.compliance.services.compliance_engine",
        ],
        "new": "products.compliance.backend.src.services.compliance_engine",
        "phase25_report": "tools/repo/compliance-compliance_engine-validate-phase25-report.json",
    },
    "documents": {
        "old": [
            "app.products.compliance.services.compliance_document_service",
            "backend.app.products.compliance.services.compliance_document_service",
            "app.products.compliance.services.compliance_photo_analysis_service",
            "backend.app.products.compliance.services.compliance_photo_analysis_service",
            "app.products.compliance.services.workflow_gate_service",
            "backend.app.products.compliance.services.workflow_gate_service",
            "app.products.compliance.services.property_compliance_resolution_service",
            "backend.app.products.compliance.services.property_compliance_resolution_service",
        ],
        "new": "products.compliance.backend.src.services",
        "phase25_report": "tools/repo/compliance-documents-validate-phase25-report.json",
    },
    "router": {
        "old": [
            "app.products.compliance.routers",
            "backend.app.products.compliance.routers",
        ],
        "new": "products.compliance.backend.src.routers",
        "phase25_report": "tools/repo/compliance-router-validate-phase25-report.json",
    },
}

TEXT_EXTENSIONS = {
    ".py", ".tsx", ".ts", ".js", ".jsx", ".json", ".md",
    ".yaml", ".yml", ".toml", ".ini", ".txt", ".css",
    ".scss", ".html",
}

EXCLUDED_PREFIXES = [
    ".git/",
    "node_modules/",
    ".venv/",
    "venv/",
    "__pycache__/",
    "dist/",
    "build/",
    "tools/repo/",
]

EXCLUDED_EXACT = {
    ".git",
    "node_modules",
    ".venv",
    "venv",
    "__pycache__",
    "dist",
    "build",
    "tools/repo",
}


@dataclass
class RewriteRecord:
    file: str
    replacements: list[dict[str, str]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rewrite imports for one compliance migration lane."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--lane", required=True, choices=sorted(LANE_RULES.keys()))
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path, lane: str) -> Path:
    repo_root = repo_root.resolve()
    cfg = LANE_RULES[lane]
    required = [
        repo_root / "onehaven_decision_engine",
        repo_root / "products" / "compliance",
        repo_root / cfg["phase25_report"],
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- "
            + "\n- ".join(missing)
        )
    return repo_root


def read_phase25(repo_root: Path, lane: str) -> dict:
    return json.loads(
        (repo_root / LANE_RULES[lane]["phase25_report"]).read_text(encoding="utf-8")
    )


def should_exclude(path: Path, repo_root: Path) -> bool:
    rel = path.relative_to(repo_root).as_posix()
    if rel in EXCLUDED_EXACT:
        return True
    return any(rel.startswith(prefix) for prefix in EXCLUDED_PREFIXES)


def collect_files(repo_root: Path) -> list[Path]:
    files = []
    for p in repo_root.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in TEXT_EXTENSIONS:
            continue
        if should_exclude(p, repo_root):
            continue
        files.append(p)
    return files


def apply_rewrites(content: str, lane: str) -> tuple[str, list[dict[str, str]]]:
    cfg = LANE_RULES[lane]
    updated = content
    replacements: list[dict[str, str]] = []

    for old_mod in cfg["old"]:
        pattern = re.compile(rf"\b{re.escape(old_mod)}\b")
        new_text, count = pattern.subn(cfg["new"], updated)
        if count > 0:
            replacements.append(
                {
                    "rule": "lane_import_rewrite",
                    "pattern": pattern.pattern,
                    "replacement": cfg["new"],
                    "count": str(count),
                }
            )
        updated = new_text

    return updated, replacements


def rewrite_repo(repo_root: Path, lane: str, dry_run: bool) -> list[RewriteRecord]:
    rewrites: list[RewriteRecord] = []

    for file_path in collect_files(repo_root):
        original = file_path.read_text(encoding="utf-8")
        updated, replacements = apply_rewrites(original, lane)

        if updated != original:
            if not dry_run:
                file_path.write_text(updated, encoding="utf-8")

            rewrites.append(
                RewriteRecord(
                    file=file_path.relative_to(repo_root).as_posix(),
                    replacements=replacements,
                )
            )

    return rewrites


def write_report(repo_root: Path, lane: str, payload: dict) -> None:
    out = repo_root / "tools" / "repo" / f"compliance-{lane}-rewrite-phase26-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root), args.lane)
    phase25 = read_phase25(repo_root, args.lane)

    rewrites = rewrite_repo(repo_root, args.lane, args.dry_run)

    payload = {
        "phase": 26,
        "lane": args.lane,
        "dry_run": args.dry_run,
        "phase25_legacy_hit_count_before": phase25.get("legacy_hit_count"),
        "rewrite_file_count": len(rewrites),
        "rewrites": [asdict(r) for r in rewrites],
    }

    write_report(repo_root, args.lane, payload)

    print("Phase 26 complete.")
    print(f"Lane: {args.lane}")
    print(f"Phase 25 hits before rewrite: {phase25.get('legacy_hit_count')}")
    print(f"Files changed: {len(rewrites)}")
    print(f"Report written to tools/repo/compliance-{args.lane}-rewrite-phase26-report.json")


if __name__ == "__main__":
    main()