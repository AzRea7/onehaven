#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


TEXT_EXTENSIONS = {
    ".tsx", ".ts", ".js", ".jsx", ".json", ".md",
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
    "products/",
    "onehaven_decision_engine/frontend/src/products/",
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
    "products",
}

LEGACY_FRONTEND_PATTERNS = [
    (re.compile(r'(["\'])@/components/'), "legacy_components_alias"),
    (re.compile(r'(["\'])@/lib/'), "legacy_lib_alias"),
    (re.compile(r'(["\'])@/pages/'), "legacy_pages_alias"),
    (re.compile(r'(["\'])src/components/'), "legacy_src_components"),
    (re.compile(r'(["\'])src/lib/'), "legacy_src_lib"),
    (re.compile(r'(["\'])src/pages/'), "legacy_src_pages"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate remaining legacy frontend core refs.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / "onehaven_decision_engine",
        repo_root / "apps",
        repo_root / "platform",
        repo_root / "packages",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- " + "\n- ".join(missing)
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

    hits = []
    for file_path in collect_files(repo_root):
        text = file_path.read_text(encoding="utf-8")
        lines = text.splitlines()
        rel = file_path.relative_to(repo_root).as_posix()

        for pattern, label in LEGACY_FRONTEND_PATTERNS:
            for i, line in enumerate(lines, 1):
                for match in pattern.finditer(line):
                    hits.append(
                        {
                            "file": rel,
                            "type": label,
                            "match": match.group(0),
                            "line": i,
                        }
                    )

    payload = {
        "phase": 18,
        "legacy_hit_count": len(hits),
        "legacy_hits": hits,
        "status": "success" if len(hits) == 0 else "needs_attention",
    }

    out = repo_root / "tools" / "repo" / "frontend-core-validate-phase18-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 18 complete.")
    print(f"Remaining legacy frontend core hits: {len(hits)}")
    print("Report written to tools/repo/frontend-core-validate-phase18-report.json")


if __name__ == "__main__":
    main()