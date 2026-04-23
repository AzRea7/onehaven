#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class FilePatchResult:
    file: str
    changed: bool
    replacements: list[dict[str, str]]


TEXT_EXTENSIONS = {".py", ".tsx", ".ts", ".js", ".jsx", ".json", ".md"}

# Only runtime/bootstrap-ish files.
CANDIDATE_FILES = [
    "onehaven_decision_engine/backend/app/main.py",
    "onehaven_decision_engine/backend/app/__init__.py",
    "onehaven_decision_engine/backend/app/routers/__init__.py",
    "onehaven_decision_engine/frontend/src/App.tsx",
    "onehaven_decision_engine/frontend/src/main.tsx",
    "onehaven_decision_engine/frontend/src/pages/ImportsPage.tsx",
    "onehaven_decision_engine/frontend/src/pages/PolicyReview.tsx",
]

# Strict Acquire runtime rewrites only.
REWRITE_RULES = [
    # Backend imports
    (
        re.compile(r"(?<!\.)\bapp\.products\.acquire\.routers\b"),
        "products.acquire.backend.src.routers",
        "backend_router_import",
    ),
    (
        re.compile(r"(?<!\.)\bapp\.products\.acquire\.services\b"),
        "products.acquire.backend.src.services",
        "backend_service_import",
    ),
    (
        re.compile(r"(?<!\.)\bapp\.products\.acquire\b"),
        "products.acquire.backend.src",
        "backend_product_import",
    ),
    # Relative backend imports in app/main.py style files
    (
        re.compile(r"(?<!\.)\bfrom\s+\.products\.acquire(\b|\.)"),
        lambda m: m.group(0).replace(".products.acquire", "products.acquire.backend.src"),
        "backend_relative_product_import",
    ),
    # Frontend imports
    (
        re.compile(r'(["\'])src/products/acquire/'),
        lambda m: f"{m.group(1)}products/acquire/frontend/src/",
        "frontend_src_import",
    ),
    (
        re.compile(r'(["\'])@/products/acquire/'),
        lambda m: f"{m.group(1)}products/acquire/frontend/src/",
        "frontend_alias_import",
    ),
]

LEGACY_PATTERNS = [
    (re.compile(r"(?<!\.)\bapp\.products\.acquire\b"), "legacy backend acquire import"),
    (re.compile(r"(?<!\.)\bfrom\s+\.products\.acquire(\b|\.)"), "legacy backend relative acquire import"),
    (re.compile(r'(["\'])src/products/acquire/'), "legacy frontend acquire import"),
    (re.compile(r'(["\'])@/products/acquire/'), "legacy frontend alias acquire import"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 4 runtime registration cleanup for Acquire.")
    parser.add_argument("--repo-root", default=".", help="Repository root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes only.")
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


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def backup_file(path: Path, repo_root: Path) -> Path:
    rel = path.relative_to(repo_root)
    backup_root = repo_root / "tools" / "repo" / "_phase4_backups"
    backup_path = backup_root / rel
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, backup_path)
    return backup_path


def apply_rewrites(content: str) -> tuple[str, list[dict[str, str]]]:
    updated = content
    replacements: list[dict[str, str]] = []

    for pattern, replacement, label in REWRITE_RULES:
        if callable(replacement):
            new_text, count = pattern.subn(replacement, updated)
            if count > 0:
                replacements.append(
                    {
                        "rule": label,
                        "pattern": pattern.pattern,
                        "replacement": "<callable>",
                        "count": str(count),
                    }
                )
            updated = new_text
        else:
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


def patch_candidate_files(repo_root: Path, dry_run: bool) -> tuple[list[FilePatchResult], list[str]]:
    results: list[FilePatchResult] = []
    touched_backups: list[str] = []

    for rel in CANDIDATE_FILES:
        path = repo_root / rel
        if not path.exists() or not path.is_file() or not is_text_file(path):
            continue

        original = read_text(path)
        updated, replacements = apply_rewrites(original)
        changed = updated != original

        if changed and not dry_run:
            backup_path = backup_file(path, repo_root)
            touched_backups.append(str(backup_path.relative_to(repo_root).as_posix()))
            write_text(path, updated)

        results.append(
            FilePatchResult(
                file=str(path.relative_to(repo_root).as_posix()),
                changed=changed,
                replacements=replacements,
            )
        )

    return results, touched_backups


def scan_remaining_legacy_runtime_refs(repo_root: Path) -> list[dict[str, str]]:
    hits: list[dict[str, str]] = []

    for rel in CANDIDATE_FILES:
        path = repo_root / rel
        if not path.exists() or not path.is_file() or not is_text_file(path):
            continue

        content = read_text(path)
        for pattern, label in LEGACY_PATTERNS:
            for match in pattern.finditer(content):
                hits.append(
                    {
                        "file": str(path.relative_to(repo_root).as_posix()),
                        "type": label,
                        "match": match.group(0),
                    }
                )

    return hits


def write_report(repo_root: Path, payload: dict) -> None:
    report_path = repo_root / "tools" / "repo" / "acquire-phase4-report.json"
    write_text(report_path, json.dumps(payload, indent=2))


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))

    patch_results, backups = patch_candidate_files(repo_root, args.dry_run)
    remaining = scan_remaining_legacy_runtime_refs(repo_root)

    payload = {
        "phase": 4,
        "product": "acquire",
        "dry_run": args.dry_run,
        "candidate_files": CANDIDATE_FILES,
        "patched_files": [asdict(r) for r in patch_results],
        "backup_files": backups,
        "remaining_legacy_runtime_references": remaining,
        "status": "success" if not remaining else "failed_validation",
    }
    write_report(repo_root, payload)

    if args.dry_run:
        print("Phase 4 dry run complete.")
        print(f"Files inspected: {len(patch_results)}")
        print("Report written to tools/repo/acquire-phase4-report.json")
        return

    if remaining:
        raise SystemExit(
            "Phase 4 completed edits but remaining legacy Acquire runtime references were found.\n"
            "See tools/repo/acquire-phase4-report.json"
        )

    print("Phase 4 completed successfully.")
    print("No shims created.")
    print("Backups written under tools/repo/_phase4_backups/")
    print("Report written to tools/repo/acquire-phase4-report.json")


if __name__ == "__main__":
    main()