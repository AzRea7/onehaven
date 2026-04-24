#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


PRODUCTS = {
    "intelligence": {
        "legacy_backend": "onehaven_decision_engine/backend/app/products/investor_intelligence",
        "legacy_frontend": "onehaven_decision_engine/frontend/src/products/investor_intelligence",
        "new_backend": "products/intelligence/backend/src",
        "new_frontend": "products/intelligence/frontend/src",
        "legacy_backend_mod": "products.intelligence.backend.src",
        "legacy_frontend_path": "src/products/investor_intelligence",
    },
    "tenants": {
        "legacy_backend": "onehaven_decision_engine/backend/app/products/tenant",
        "legacy_frontend": "onehaven_decision_engine/frontend/src/products/tenant",
        "new_backend": "products/tenants/backend/src",
        "new_frontend": "products/tenants/frontend/src",
        "legacy_backend_mod": "products.tenants.backend.src",
        "legacy_frontend_path": "src/products/tenant",
    },
    "ops": {
        "legacy_backend": "onehaven_decision_engine/backend/app/products/management",
        "legacy_frontend": "onehaven_decision_engine/frontend/src/products/management",
        "new_backend": "products/ops/backend/src",
        "new_frontend": "products/ops/frontend/src",
        "legacy_backend_mod": "products.ops.backend.src",
        "legacy_frontend_path": "src/products/management",
    },
    "compliance": {
        "legacy_backend": "onehaven_decision_engine/backend/app/products/compliance",
        "legacy_frontend": "onehaven_decision_engine/frontend/src/products/compliance",
        "new_backend": "products/compliance/backend/src",
        "new_frontend": "products/compliance/frontend/src",
        "legacy_backend_mod": "app.products.compliance",
        "legacy_frontend_path": "src/products/compliance",
    },
}

TEXT_EXTENSIONS = {
    ".py", ".tsx", ".ts", ".js", ".jsx", ".json", ".md",
    ".yaml", ".yml", ".toml", ".ini", ".txt", ".css",
    ".scss", ".html", ".sh",
}

EXCLUDED = {
    ".git", "node_modules", ".venv", "venv", "__pycache__", "dist", "build"
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate migrated product.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--product", required=True, choices=sorted(PRODUCTS.keys()))
    return parser.parse_args()


def should_skip(path: Path, repo_root: Path) -> bool:
    rel = path.relative_to(repo_root).as_posix()
    first = rel.split("/", 1)[0]
    return first in EXCLUDED


def collect_files(repo_root: Path) -> list[Path]:
    out = []
    for p in repo_root.rglob("*"):
        if p.is_file() and p.suffix.lower() in TEXT_EXTENSIONS and not should_skip(p, repo_root):
            out.append(p)
    return out


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    cfg = PRODUCTS[args.product]

    patterns = [
        (re.compile(rf"\b{re.escape(cfg['legacy_backend_mod'])}\b"), "legacy_backend_import"),
        (re.compile(rf'(["\'])@/{re.escape(cfg["legacy_frontend_path"])}/'), "legacy_frontend_alias"),
        (re.compile(rf'(["\']){re.escape(cfg["legacy_frontend_path"])}/'), "legacy_frontend_src"),
    ]

    hits = []
    for file_path in collect_files(repo_root):
        text = file_path.read_text(encoding="utf-8")
        lines = text.splitlines()
        for pattern, label in patterns:
            for i, line in enumerate(lines, 1):
                for match in pattern.finditer(line):
                    hits.append({
                        "file": file_path.relative_to(repo_root).as_posix(),
                        "type": label,
                        "match": match.group(0),
                        "line": i,
                    })

    payload = {
        "product": args.product,
        "legacy_backend_exists": (repo_root / cfg["legacy_backend"]).exists(),
        "legacy_frontend_exists": (repo_root / cfg["legacy_frontend"]).exists(),
        "new_backend_exists": (repo_root / cfg["new_backend"]).exists(),
        "new_frontend_exists": (repo_root / cfg["new_frontend"]).exists(),
        "legacy_hit_count": len(hits),
        "legacy_hits": hits,
        "ready_to_remove_legacy": len(hits) == 0,
    }

    out = repo_root / "tools" / "repo" / f"{args.product}-validation-report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Validation complete for {args.product}")
    print(f"Legacy hit count: {len(hits)}")
    print(f"Report: {out}")


if __name__ == "__main__":
    main()