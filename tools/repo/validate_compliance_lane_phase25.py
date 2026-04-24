#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


LANE_PATTERNS = {
    "policy_sources": [
        r"\bapp\.products\.compliance\.services\.policy_sources\b",
        r"\bbackend\.app\.products\.compliance\.services\.policy_sources\b",
    ],
    "policy_governance": [
        r"\bapp\.products\.compliance\.services\.policy_governance\b",
        r"\bbackend\.app\.products\.compliance\.services\.policy_governance\b",
    ],
    "policy_coverage": [
        r"\bapp\.products\.compliance\.services\.policy_coverage\b",
        r"\bbackend\.app\.products\.compliance\.services\.policy_coverage\b",
    ],
    "policy_assertions": [
        r"\bapp\.products\.compliance\.services\.policy_assertions\b",
        r"\bbackend\.app\.products\.compliance\.services\.policy_assertions\b",
    ],
    "inspections": [
        r"\bapp\.products\.compliance\.services\.inspections\b",
        r"\bbackend\.app\.products\.compliance\.services\.inspections\b",
    ],
    "compliance_engine": [
        r"\bapp\.products\.compliance\.services\.compliance_engine\b",
        r"\bbackend\.app\.products\.compliance\.services\.compliance_engine\b",
    ],
    "documents": [
        r"\bapp\.products\.compliance\.services\.compliance_document_service\b",
        r"\bapp\.products\.compliance\.services\.compliance_photo_analysis_service\b",
        r"\bbackend\.app\.products\.compliance\.services\.compliance_document_service\b",
        r"\bbackend\.app\.products\.compliance\.services\.compliance_photo_analysis_service\b",
    ],
    "router": [
        r"\bapp\.products\.compliance\.routers\b",
        r"\bbackend\.app\.products\.compliance\.routers\b",
    ],
    "frontend_components": [
        r'(["\'])@/products/compliance/',
        r'(["\'])src/products/compliance/',
    ],
    "frontend_pages": [
        r'(["\'])@/products/compliance/',
        r'(["\'])src/products/compliance/',
    ],
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate one Compliance lane after movement."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--lane", required=True, choices=sorted(LANE_PATTERNS.keys()))
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / "onehaven_decision_engine",
        repo_root / "products" / "compliance",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- "
            + "\n- ".join(missing)
        )
    return repo_root


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


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    patterns = [(re.compile(p), p) for p in LANE_PATTERNS[args.lane]]

    hits = []
    for file_path in collect_files(repo_root):
        text = file_path.read_text(encoding="utf-8")
        lines = text.splitlines()
        rel = file_path.relative_to(repo_root).as_posix()

        for pattern, label in patterns:
            for i, line in enumerate(lines, 1):
                for match in pattern.finditer(line):
                    hits.append(
                        {
                            "file": rel,
                            "pattern": label,
                            "match": match.group(0),
                            "line": i,
                        }
                    )

    payload = {
        "phase": 25,
        "lane": args.lane,
        "legacy_hit_count": len(hits),
        "legacy_hits": hits,
        "status": "success" if len(hits) == 0 else "needs_attention",
    }

    out = repo_root / "tools" / "repo" / f"compliance-{args.lane}-validate-phase25-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 25 complete.")
    print(f"Lane: {args.lane}")
    print(f"Remaining legacy hits: {len(hits)}")
    print(f"Report written to tools/repo/compliance-{args.lane}-validate-phase25-report.json")


if __name__ == "__main__":
    main()