#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path


# ============================================================
# PHASE 2: ACQUIRE PRODUCT MOVE
# ------------------------------------------------------------
# Safe-by-default behavior:
# - moves ONE product only: acquire
# - copies into new destination first
# - rewrites imports ONLY inside moved files
# - does NOT create shims
# - does NOT delete legacy files unless --remove-legacy is passed
# - aborts if destination already contains non-empty source files
# ============================================================


@dataclass(frozen=True)
class ProductMovePlan:
    product_slug: str
    display_name: str
    legacy_backend: str
    legacy_frontend: str
    target_backend: str
    target_frontend: str


@dataclass
class RewriteRecord:
    file: str
    replacements: list[dict[str, str]]


PLAN = ProductMovePlan(
    product_slug="acquire",
    display_name="OneHaven Acquire",
    legacy_backend="backend/app/products/acquire",
    legacy_frontend="frontend/products/acquire/frontend/src",
    target_backend="products/acquire/backend/src",
    target_frontend="products/acquire/frontend/src",
)

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

IMPORT_REWRITES = [
    (
        re.compile(r"\bapp\.products\.acquire\b"),
        "products.acquire.backend.src",
        "python_abs_product_import",
    ),
    (
        re.compile(r"\bfrom\s+app\.products\.acquire(\b|\.)"),
        lambda m: m.group(0).replace(
            "products.acquire.backend.src",
            "products.acquire.backend.src",
        ),
        "python_from_product_import",
    ),
    (
        re.compile(r"\bsrc/products/acquire\b"),
        "products/acquire/frontend/src",
        "frontend_src_product_reference",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 2 mover for OneHaven Acquire."
    )
    parser.add_argument("--repo-root", default=".", help="Repository root or legacy root.")
    parser.add_argument(
        "--remove-legacy",
        action="store_true",
        help="Delete legacy acquire folders after successful copy and rewrite.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow writing into target directories if they already exist but are empty.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview actions only. No file changes.",
    )
    return parser.parse_args()


def find_legacy_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()

    candidate = repo_root / "onehaven_decision_engine"
    if candidate.exists() and (candidate / "backend").exists() and (candidate / "frontend").exists():
        return candidate

    if (repo_root / "backend").exists() and (repo_root / "frontend").exists():
        return repo_root

    raise SystemExit(
        "Could not locate legacy root.\n"
        f"Checked:\n"
        f"- {candidate}\n"
        f"- {repo_root} (for backend/ and frontend/)\n"
        "Run from repo root or pass --repo-root correctly."
    )


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def is_text_file(path: Path) -> bool:
    return path.suffix.lower() in TEXT_EXTENSIONS


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def copy_tree(src: Path, dst: Path) -> None:
    if not src.exists():
        raise SystemExit(f"Missing source directory: {src}")
    ensure_dir(dst.parent)
    shutil.copytree(src, dst, dirs_exist_ok=True)


def dir_has_files(path: Path) -> bool:
    return path.exists() and any(p.is_file() for p in path.rglob("*"))


def validate_target_dir(dst: Path, force: bool) -> None:
    if not dst.exists():
        return

    if dir_has_files(dst):
        raise SystemExit(
            f"Refusing to write into non-empty target directory: {dst}\n"
            "Move or clean it first."
        )

    if not force:
        return


def collect_text_files(root: Path) -> list[Path]:
    return [p for p in root.rglob("*") if p.is_file() and is_text_file(p)]


def apply_rewrites_to_content(content: str) -> tuple[str, list[dict[str, str]]]:
    records: list[dict[str, str]] = []
    updated = content

    for pattern, replacement, label in IMPORT_REWRITES:
        if callable(replacement):
            new_text, count = pattern.subn(replacement, updated)
            if count > 0:
                records.append(
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
                records.append(
                    {
                        "rule": label,
                        "pattern": pattern.pattern,
                        "replacement": replacement,
                        "count": str(count),
                    }
                )
            updated = new_text

    return updated, records


def rewrite_batch(root: Path) -> list[RewriteRecord]:
    results: list[RewriteRecord] = []
    for file_path in collect_text_files(root):
        original = read_text(file_path)
        updated, records = apply_rewrites_to_content(original)
        if updated != original:
            write_text(file_path, updated)
            results.append(
                RewriteRecord(
                    file=str(file_path),
                    replacements=records,
                )
            )
    return results


def scan_for_legacy_imports(root: Path) -> list[dict[str, str]]:
    checks = [
        (re.compile(r"\bapp\.products\.acquire\b"), "python legacy product import"),
        (re.compile(r"\bsrc/products/acquire\b"), "frontend legacy product path"),
    ]

    hits: list[dict[str, str]] = []
    for file_path in collect_text_files(root):
        content = read_text(file_path)
        for pattern, label in checks:
            for match in pattern.finditer(content):
                hits.append(
                    {
                        "file": str(file_path),
                        "type": label,
                        "match": match.group(0),
                    }
                )
    return hits


def remove_path(path: Path, dry_run: bool) -> None:
    if not path.exists():
        return
    if dry_run:
        return
    shutil.rmtree(path)


def write_report(repo_root: Path, payload: dict) -> None:
    report_path = repo_root / "tools/repo/acquire-phase2-report.json"
    write_text(report_path, json.dumps(payload, indent=2))


def print_plan(repo_root: Path, legacy_root: Path) -> None:
    print("Phase 2 move plan")
    print("-----------------")
    print(f"Repo root:     {repo_root}")
    print(f"Legacy root:   {legacy_root}")
    print(f"Product:       {PLAN.display_name} ({PLAN.product_slug})")
    print(f"Backend from:  {legacy_root / PLAN.legacy_backend}")
    print(f"Frontend from: {legacy_root / PLAN.legacy_frontend}")
    print(f"Backend to:    {repo_root / PLAN.target_backend}")
    print(f"Frontend to:   {repo_root / PLAN.target_frontend}")
    print("")


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    legacy_root = find_legacy_root(repo_root)

    actual_repo_root = repo_root
    if (repo_root / "backend").exists() and (repo_root / "frontend").exists():
        actual_repo_root = repo_root.parent if (repo_root.parent / "tools").exists() else repo_root

    legacy_backend = legacy_root / PLAN.legacy_backend
    legacy_frontend = legacy_root / PLAN.legacy_frontend
    target_backend = actual_repo_root / PLAN.target_backend
    target_frontend = actual_repo_root / PLAN.target_frontend

    print_plan(actual_repo_root, legacy_root)

    if not legacy_backend.exists():
        raise SystemExit(f"Missing legacy backend product directory: {legacy_backend}")
    if not legacy_frontend.exists():
        raise SystemExit(f"Missing legacy frontend product directory: {legacy_frontend}")

    validate_target_dir(target_backend, args.force)
    validate_target_dir(target_frontend, args.force)

    if args.dry_run:
        print("Dry run only. No files will be changed.")
        print("Validation passed.")
        return

    copy_tree(legacy_backend, target_backend)
    copy_tree(legacy_frontend, target_frontend)

    backend_rewrites = rewrite_batch(target_backend)
    frontend_rewrites = rewrite_batch(target_frontend)

    backend_legacy_hits = scan_for_legacy_imports(target_backend)
    frontend_legacy_hits = scan_for_legacy_imports(target_frontend)

    unresolved_hits = backend_legacy_hits + frontend_legacy_hits
    if unresolved_hits:
        payload = {
            "status": "failed_validation",
            "product": PLAN.product_slug,
            "reason": "legacy import references remain after rewrite",
            "unresolved_hits": unresolved_hits,
        }
        write_report(actual_repo_root, payload)
        raise SystemExit(
            "Phase 2 aborted after copy because unresolved legacy import references remain.\n"
            "See tools/repo/acquire-phase2-report.json"
        )

    if args.remove_legacy:
        remove_path(legacy_backend, dry_run=False)
        remove_path(legacy_frontend, dry_run=False)

    payload = {
        "status": "success",
        "product": PLAN.product_slug,
        "display_name": PLAN.display_name,
        "legacy_root": str(legacy_root),
        "repo_root": str(actual_repo_root),
        "legacy_removed": args.remove_legacy,
        "backend": {
            "from": str(legacy_backend),
            "to": str(target_backend),
            "rewrite_count": len(backend_rewrites),
            "rewrites": [asdict(r) for r in backend_rewrites],
        },
        "frontend": {
            "from": str(legacy_frontend),
            "to": str(target_frontend),
            "rewrite_count": len(frontend_rewrites),
            "rewrites": [asdict(r) for r in frontend_rewrites],
        },
    }
    write_report(actual_repo_root, payload)

    print("Phase 2 completed successfully.")
    print("No shims created.")
    if args.remove_legacy:
        print("Legacy acquire folders were removed.")
    else:
        print("Legacy acquire folders were preserved.")
    print("Report written to tools/repo/acquire-phase2-report.json")


if __name__ == "__main__":
    main()