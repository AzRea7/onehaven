#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path


PRODUCTS = {
    "intelligence": {
        "legacy_frontend_alias": "@/products/investor_intelligence",
        "legacy_frontend_src": "src/products/investor_intelligence",
        "new_frontend_path": "products/intelligence/frontend/src",
        "phase20_report": "tools/repo/intelligence-frontend-validate-phase20-report.json",
    },
    "acquire": {
        "legacy_frontend_alias": "@/products/acquire",
        "legacy_frontend_src": "src/products/acquire",
        "new_frontend_path": "products/acquire/frontend/src",
        "phase20_report": "tools/repo/acquire-phase5-report.json",
    },
    "compliance": {
        "legacy_frontend_alias": "@/products/compliance",
        "legacy_frontend_src": "src/products/compliance",
        "new_frontend_path": "products/compliance/frontend/src",
        "phase20_report": "tools/repo/compliance-frontend-validate-phase20-report.json",
    },
    "tenants": {
        "legacy_frontend_alias": "@/products/tenant",
        "legacy_frontend_src": "src/products/tenant",
        "new_frontend_path": "products/tenants/frontend/src",
        "phase20_report": "tools/repo/tenants-frontend-validate-phase20-report.json",
    },
    "ops": {
        "legacy_frontend_alias": "@/products/management",
        "legacy_frontend_src": "src/products/management",
        "new_frontend_path": "products/ops/frontend/src",
        "phase20_report": "tools/repo/ops-frontend-validate-phase20-report.json",
    },
}

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
        description="Rewrite legacy frontend product refs to new product frontend paths."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument(
        "--product",
        required=True,
        choices=["intelligence", "tenants", "ops", "compliance"],
        help="Product to rewrite.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path, product: str) -> Path:
    repo_root = repo_root.resolve()
    cfg = PRODUCTS[product]
    required = [
        repo_root / "onehaven_decision_engine",
        repo_root / cfg["phase20_report"],
        repo_root / "products",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- " + "\n- ".join(missing)
        )
    return repo_root


def read_phase20(repo_root: Path, product: str) -> dict:
    return json.loads((repo_root / PRODUCTS[product]["phase20_report"]).read_text(encoding="utf-8"))


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


def apply_rewrites(content: str, product: str) -> tuple[str, list[dict[str, str]]]:
    cfg = PRODUCTS[product]

    rules = [
        (
            re.compile(rf'(["\']){re.escape(cfg["legacy_frontend_alias"])}/'),
            rf'\1{cfg["new_frontend_path"]}/',
            "legacy_frontend_alias",
        ),
        (
            re.compile(rf'(["\']){re.escape(cfg["legacy_frontend_src"])}/'),
            rf'\1{cfg["new_frontend_path"]}/',
            "legacy_frontend_src",
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


def rewrite_repo(repo_root: Path, product: str, dry_run: bool) -> list[RewriteRecord]:
    rewrites: list[RewriteRecord] = []

    for file_path in collect_files(repo_root):
        original = file_path.read_text(encoding="utf-8")
        updated, replacements = apply_rewrites(original, product)

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


def write_report(repo_root: Path, product: str, payload: dict) -> None:
    out = repo_root / "tools" / "repo" / f"{product}-frontend-rewrite-phase21-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root), args.product)
    phase20 = read_phase20(repo_root, args.product)

    rewrites = rewrite_repo(repo_root, args.product, args.dry_run)

    payload = {
        "phase": 21,
        "product": args.product,
        "dry_run": args.dry_run,
        "phase20_legacy_hit_count_before": phase20.get("legacy_hit_count", None),
        "rewrite_file_count": len(rewrites),
        "rewrites": [asdict(r) for r in rewrites],
    }

    write_report(repo_root, args.product, payload)

    print("Phase 21 complete.")
    print(f"Product: {args.product}")
    print(f"Phase 20 hits before rewrite: {phase20.get('legacy_hit_count')}")
    print(f"Files changed: {len(rewrites)}")
    print(f"Report written to tools/repo/{args.product}-frontend-rewrite-phase21-report.json")


if __name__ == "__main__":
    main()