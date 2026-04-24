#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


PRODUCTS = {
    "intelligence": {
        "legacy_frontend_alias": "@/products/investor_intelligence",
        "legacy_frontend_src": "src/products/investor_intelligence",
        "legacy_frontend_dir": "onehaven_decision_engine/frontend/src/products/investor_intelligence",
        "new_frontend_dir": "products/intelligence/frontend/src",
    },
    "acquire": {
        "legacy_frontend_alias": "@/products/acquire",
        "legacy_frontend_src": "src/products/acquire",
        "legacy_frontend_dir": "onehaven_decision_engine/frontend/src/products/acquire",
        "new_frontend_dir": "products/acquire/frontend/src",
    },
    "compliance": {
        "legacy_frontend_alias": "@/products/compliance",
        "legacy_frontend_src": "src/products/compliance",
        "legacy_frontend_dir": "onehaven_decision_engine/frontend/src/products/compliance",
        "new_frontend_dir": "products/compliance/frontend/src",
    },
    "tenants": {
        "legacy_frontend_alias": "@/products/tenant",
        "legacy_frontend_src": "src/products/tenant",
        "legacy_frontend_dir": "onehaven_decision_engine/frontend/src/products/tenant",
        "new_frontend_dir": "products/tenants/frontend/src",
    },
    "ops": {
        "legacy_frontend_alias": "@/products/management",
        "legacy_frontend_src": "src/products/management",
        "legacy_frontend_dir": "onehaven_decision_engine/frontend/src/products/management",
        "new_frontend_dir": "products/ops/frontend/src",
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate remaining legacy frontend product references."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument(
        "--product",
        required=True,
        choices=sorted(PRODUCTS.keys()),
        help="Product to validate.",
    )
    return parser.parse_args()


def ensure_repo_root(repo_root: Path, product: str) -> Path:
    repo_root = repo_root.resolve()
    cfg = PRODUCTS[product]
    required = [
        repo_root / "onehaven_decision_engine",
        repo_root / cfg["new_frontend_dir"],
        repo_root / "tools" / "repo",
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
    repo_root = ensure_repo_root(Path(args.repo_root), args.product)
    cfg = PRODUCTS[args.product]

    patterns = [
        (re.compile(rf'(["\']){re.escape(cfg["legacy_frontend_alias"])}/'), "legacy_frontend_alias"),
        (re.compile(rf'(["\']){re.escape(cfg["legacy_frontend_src"])}/'), "legacy_frontend_src"),
    ]

    hits = []
    for file_path in collect_files(repo_root):
        text = file_path.read_text(encoding="utf-8")
        lines = text.splitlines()
        rel = file_path.relative_to(repo_root).as_posix()

        for pattern, label in patterns:
            for i, line in enumerate(lines, 1):
                for match in pattern.finditer(line):
                    hits.append({
                        "file": rel,
                        "type": label,
                        "match": match.group(0),
                        "line": i,
                    })

    payload = {
        "phase": 20,
        "product": args.product,
        "legacy_hit_count": len(hits),
        "legacy_hits": hits,
        "legacy_frontend_dir_exists": (repo_root / cfg["legacy_frontend_dir"]).exists(),
        "new_frontend_dir_exists": (repo_root / cfg["new_frontend_dir"]).exists(),
        "status": "success" if len(hits) == 0 else "needs_attention",
    }

    out = repo_root / "tools" / "repo" / f"{args.product}-frontend-validate-phase20-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 20 complete.")
    print(f"Product: {args.product}")
    print(f"Remaining legacy frontend hits: {len(hits)}")
    print(f"Report written to {out}")


if __name__ == "__main__":
    main()