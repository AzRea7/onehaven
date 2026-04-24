#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE30_REPORT = "tools/repo/compliance-frontend-validate-phase30-report.json"

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
        description="Rewrite remaining Compliance frontend refs."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE30_REPORT,
        repo_root / "products" / "compliance" / "frontend" / "src",
        repo_root / "onehaven_decision_engine",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def read_phase30(repo_root: Path) -> dict:
    return json.loads((repo_root / PHASE30_REPORT).read_text(encoding="utf-8"))


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


def apply_rewrites(content: str) -> tuple[str, list[dict[str, str]]]:
    rules = [
        (
            re.compile(r'(["\'])@/products/compliance/'),
            r'\1products/compliance/frontend/src/',
            "legacy_alias_to_new_path",
        ),
        (
            re.compile(r'(["\'])src/products/compliance/'),
            r'\1products/compliance/frontend/src/',
            "legacy_src_to_new_path",
        ),
    ]

    updated = content
    replacements: list[dict[str, str]] = []

    for pattern, replacement, label in rules:
        new_text, count = pattern.subn(replacement, updated)
        if count > 0:
            replacements.append(
                {
                    "rule": label,
                    "pattern": pattern.pattern,
                    "replacement": replacement,
                    "count": str(count),
                }
            )
        updated = new_text

    return updated, replacements


def rewrite_only_flagged_files(repo_root: Path, dry_run: bool) -> list[RewriteRecord]:
    phase30 = read_phase30(repo_root)
    flagged = sorted({hit["file"] for hit in phase30.get("legacy_hits", [])})

    rewrites: list[RewriteRecord] = []

    for rel in flagged:
        file_path = repo_root / rel
        if not file_path.exists() or not file_path.is_file():
            continue
        if should_exclude(file_path, repo_root):
            continue

        original = file_path.read_text(encoding="utf-8")
        updated, replacements = apply_rewrites(original)

        if updated != original:
            if not dry_run:
                file_path.write_text(updated, encoding="utf-8")

            rewrites.append(
                RewriteRecord(
                    file=rel,
                    replacements=replacements,
                )
            )

    return rewrites


def write_report(repo_root: Path, payload: dict) -> None:
    out = repo_root / "tools" / "repo" / "compliance-frontend-rewrite-phase31-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    phase30 = read_phase30(repo_root)

    rewrites = rewrite_only_flagged_files(repo_root, args.dry_run)

    payload = {
        "phase": 31,
        "dry_run": args.dry_run,
        "phase30_legacy_hit_count_before": phase30.get("legacy_hit_count"),
        "rewrite_file_count": len(rewrites),
        "rewrites": [asdict(r) for r in rewrites],
    }

    write_report(repo_root, payload)

    print("Phase 31 complete.")
    print(f"Phase 30 hits before rewrite: {phase30.get('legacy_hit_count')}")
    print(f"Files changed: {len(rewrites)}")
    print("Report written to tools/repo/compliance-frontend-rewrite-phase31-report.json")


if __name__ == "__main__":
    main()