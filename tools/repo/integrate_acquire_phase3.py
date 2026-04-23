#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path


# ============================================================
# PHASE 3: ACQUIRE INTEGRATION
# ------------------------------------------------------------
# Safe-by-default behavior:
# - rewrites ONLY references to Acquire outside the moved Acquire tree
# - does NOT touch other products
# - does NOT create shims
# - validates remaining legacy Acquire references after rewrite
# - writes a report
# ============================================================


@dataclass
class RewriteRecord:
    file: str
    replacements: list[dict[str, str]]


TEXT_EXTENSIONS = {
    ".py",
    ".tsx",
    ".ts",
    ".js",
    ".jsx",
    ".json",
    ".md",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".txt",
    ".css",
    ".scss",
    ".html",
    ".sh",
}

# old -> new Acquire references used OUTSIDE the moved Acquire product
REWRITE_RULES = [
    (
        re.compile(r"\bapp\.products\.acquire\b"),
        "products.acquire.backend.src",
        "python_abs_product_import",
    ),
    (
        re.compile(r"\bsrc/products/acquire\b"),
        "products/acquire/frontend/src",
        "frontend_src_product_reference",
    ),
]

LEGACY_PATTERNS = [
    (re.compile(r"\bapp\.products\.acquire\b"), "python legacy acquire import"),
    (re.compile(r"\bsrc/products/acquire\b"), "frontend legacy acquire path"),
]

EXCLUDED_ROOTS = [
    "products/acquire/backend/src",
    "products/acquire/frontend/src",
    ".git",
    "node_modules",
    ".venv",
    "venv",
    "__pycache__",
    "dist",
    "build",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 3 integration for OneHaven Acquire.")
    parser.add_argument("--repo-root", default=".", help="Repository root.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only. No file changes.",
    )
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / "tools",
        repo_root / "products" / "acquire" / "backend" / "src",
        repo_root / "products" / "acquire" / "frontend" / "src",
        repo_root / "onehaven_decision_engine",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- "
            + "\n- ".join(missing)
        )
    return repo_root


def is_text_file(path: Path) -> bool:
    return path.suffix.lower() in TEXT_EXTENSIONS


def should_exclude(path: Path, repo_root: Path) -> bool:
    rel = path.relative_to(repo_root).as_posix()
    for prefix in EXCLUDED_ROOTS:
        if rel == prefix or rel.startswith(prefix + "/"):
            return True
    return False


def collect_text_files(repo_root: Path) -> list[Path]:
    files: list[Path] = []
    for p in repo_root.rglob("*"):
        if not p.is_file():
            continue
        if not is_text_file(p):
            continue
        if should_exclude(p, repo_root):
            continue
        files.append(p)
    return files


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def apply_rewrites(content: str) -> tuple[str, list[dict[str, str]]]:
    updated = content
    replacements: list[dict[str, str]] = []

    for pattern, replacement, label in REWRITE_RULES:
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


def rewrite_repo(repo_root: Path, dry_run: bool) -> list[RewriteRecord]:
    rewrites: list[RewriteRecord] = []

    for file_path in collect_text_files(repo_root):
        original = read_text(file_path)
        updated, replacements = apply_rewrites(original)
        if updated != original:
            if not dry_run:
                write_text(file_path, updated)
            rewrites.append(
                RewriteRecord(
                    file=str(file_path.relative_to(repo_root).as_posix()),
                    replacements=replacements,
                )
            )

    return rewrites


def scan_remaining_legacy_references(repo_root: Path) -> list[dict[str, str]]:
    hits: list[dict[str, str]] = []

    for file_path in collect_text_files(repo_root):
        content = read_text(file_path)
        rel = file_path.relative_to(repo_root).as_posix()

        for pattern, label in LEGACY_PATTERNS:
            for match in pattern.finditer(content):
                hits.append(
                    {
                        "file": rel,
                        "type": label,
                        "match": match.group(0),
                    }
                )

    return hits


def write_report(repo_root: Path, payload: dict) -> None:
    report_path = repo_root / "tools" / "repo" / "acquire-phase3-report.json"
    write_text(report_path, json.dumps(payload, indent=2))


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))

    rewrites = rewrite_repo(repo_root, args.dry_run)
    remaining = scan_remaining_legacy_references(repo_root)

    status = "success" if not remaining else "failed_validation"
    payload = {
        "status": status,
        "phase": 3,
        "product": "acquire",
        "dry_run": args.dry_run,
        "rewrite_file_count": len(rewrites),
        "rewrites": [asdict(r) for r in rewrites],
        "remaining_legacy_references": remaining,
    }
    write_report(repo_root, payload)

    if args.dry_run:
        print("Phase 3 dry run complete.")
        print(f"Files that would be updated: {len(rewrites)}")
        print("Report written to tools/repo/acquire-phase3-report.json")
        return

    if remaining:
        raise SystemExit(
            "Phase 3 finished rewrites but validation found remaining legacy Acquire references.\n"
            "See tools/repo/acquire-phase3-report.json"
        )

    print("Phase 3 completed successfully.")
    print("No shims created.")
    print("Report written to tools/repo/acquire-phase3-report.json")


if __name__ == "__main__":
    main()