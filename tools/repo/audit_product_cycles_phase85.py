#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path


PRODUCT_PREFIXES = {
    "compliance": "products.compliance.",
    "ops": "products.ops.",
    "acquire": "products.acquire.",
    "intelligence": "products.intelligence.",
    "tenants": "products.tenants.",
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    return parser.parse_args()


def owner_for_module(module: str) -> str | None:
    for owner, prefix in PRODUCT_PREFIXES.items():
        if module.startswith(prefix):
            return owner
    if module.startswith("onehaven_platform."):
        return "platform"
    if module.startswith("apps."):
        return "app"
    return None


def file_owner(path: Path) -> str | None:
    parts = path.as_posix()
    if "/products/compliance/" in parts:
        return "compliance"
    if "/products/ops/" in parts:
        return "ops"
    if "/products/acquire/" in parts:
        return "acquire"
    if "/products/intelligence/" in parts:
        return "intelligence"
    if "/products/tenants/" in parts:
        return "tenants"
    if "/onehaven_platform/" in parts:
        return "platform"
    if "/apps/" in parts:
        return "app"
    return None


def imports_from_file(path: Path) -> list[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    modules = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                modules.append(alias.name)

    return modules


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()

    edges = []
    risky = []

    for base in ["products", "onehaven_platform", "apps"]:
        scan_root = root / base
        if not scan_root.exists():
            continue

        for path in scan_root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue

            src_owner = file_owner(path)
            if not src_owner:
                continue

            for module in imports_from_file(path):
                dst_owner = owner_for_module(module)
                if not dst_owner or dst_owner == src_owner:
                    continue

                edge = {
                    "file": path.relative_to(root).as_posix(),
                    "from_owner": src_owner,
                    "to_owner": dst_owner,
                    "module": module,
                }
                edges.append(edge)

                if src_owner in {"ops", "compliance"} and dst_owner in {"ops", "compliance"}:
                    risky.append(edge)

    payload = {
        "phase": 85,
        "summary": {
            "cross_owner_edges": len(edges),
            "ops_compliance_risky_edges": len(risky),
        },
        "risky_edges": risky,
        "edges": edges,
    }

    out = root / "tools/repo/product-cycle-audit-phase85-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 85 complete.")
    print(payload["summary"])
    print("Report written to tools/repo/product-cycle-audit-phase85-report.json")


if __name__ == "__main__":
    main()