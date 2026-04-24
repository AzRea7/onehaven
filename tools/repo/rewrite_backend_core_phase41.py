#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path


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

REWRITE_RULES = [
    # platform agents/domain/services
    (
        re.compile(r"\bapp\.domain\.agents\b"),
        "onehaven_platform.backend.src.domain.agents",
    ),
    (
        re.compile(r"\bbackend\.app\.domain\.agents\b"),
        "onehaven_platform.backend.src.domain.agents",
    ),
    (
        re.compile(r"\bapp\.domain\.policy\b"),
        "onehaven_platform.backend.src.domain.policy",
    ),
    (
        re.compile(r"\bbackend\.app\.domain\.policy\b"),
        "onehaven_platform.backend.src.domain.policy",
    ),
    (
        re.compile(r"\bapp\.services\.agent_"),
        "onehaven_platform.backend.src.services.agent_",
    ),
    (
        re.compile(r"\bbackend\.app\.services\.agent_"),
        "onehaven_platform.backend.src.services.agent_",
    ),
    (
        re.compile(r"\bapp\.services\.auth_service\b"),
        "onehaven_platform.backend.src.services.auth_service",
    ),
    (
        re.compile(r"\bbackend\.app\.services\.auth_service\b"),
        "onehaven_platform.backend.src.services.auth_service",
    ),
    (
        re.compile(r"\bapp\.services\.address_normalization\b"),
        "onehaven_platform.backend.src.services.address_normalization",
    ),
    (
        re.compile(r"\bbackend\.app\.services\.address_normalization\b"),
        "onehaven_platform.backend.src.services.address_normalization",
    ),
    (
        re.compile(r"\bapp\.services\.budget_service\b"),
        "onehaven_platform.backend.src.services.budget_service",
    ),
    (
        re.compile(r"\bbackend\.app\.services\.budget_service\b"),
        "onehaven_platform.backend.src.services.budget_service",
    ),
    (
        re.compile(r"\bapp\.services\.agent_orchestrator\b"),
        "onehaven_platform.backend.src.services.agent_orchestrator",
    ),
    (
        re.compile(r"\bbackend\.app\.services\.agent_orchestrator\b"),
        "onehaven_platform.backend.src.services.agent_orchestrator",
    ),
    (
        re.compile(r"\bapp\.services\.agent_orchestrator_runtime\b"),
        "onehaven_platform.backend.src.services.agent_orchestrator_runtime",
    ),
    (
        re.compile(r"\bbackend\.app\.services\.agent_orchestrator_runtime\b"),
        "onehaven_platform.backend.src.services.agent_orchestrator_runtime",
    ),

    # product lanes moved by phase 40
    (
        re.compile(r"\bapp\.services\.rehab\b"),
        "products.ops.backend.src.services.rehab",
    ),
    (
        re.compile(r"\bbackend\.app\.services\.rehab\b"),
        "products.ops.backend.src.services.rehab",
    ),
    (
        re.compile(r"\bapp\.routers\.rehab\b"),
        "products.ops.backend.src.routers.rehab",
    ),
    (
        re.compile(r"\bbackend\.app\.routers\.rehab\b"),
        "products.ops.backend.src.routers.rehab",
    ),
    (
        re.compile(r"\bapp\.services\.rent\b"),
        "products.intelligence.backend.src.services.rent",
    ),
    (
        re.compile(r"\bbackend\.app\.services\.rent\b"),
        "products.intelligence.backend.src.services.rent",
    ),
    (
        re.compile(r"\bapp\.routers\.rent\b"),
        "products.intelligence.backend.src.routers.rent",
    ),
    (
        re.compile(r"\bbackend\.app\.routers\.rent\b"),
        "products.intelligence.backend.src.routers.rent",
    ),
    (
        re.compile(r"\bapp\.routers\.imports\b"),
        "products.acquire.backend.src.routers.imports",
    ),
    (
        re.compile(r"\bbackend\.app\.routers\.imports\b"),
        "products.acquire.backend.src.routers.imports",
    ),
    (
        re.compile(r"\bapp\.routers\.imports_alias\b"),
        "products.acquire.backend.src.routers.imports_alias",
    ),
    (
        re.compile(r"\bbackend\.app\.routers\.imports_alias\b"),
        "products.acquire.backend.src.routers.imports_alias",
    ),
    (
        re.compile(r"\bapp\.services\.tenant\b"),
        "products.tenants.backend.src.services.tenant",
    ),
    (
        re.compile(r"\bbackend\.app\.services\.tenant\b"),
        "products.tenants.backend.src.services.tenant",
    ),
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def should_exclude(path: Path, repo_root: Path) -> bool:
    rel = path.relative_to(repo_root).as_posix()
    return any(rel.startswith(prefix) for prefix in EXCLUDED_PREFIXES)


def collect_files(repo_root: Path) -> list[Path]:
    out = []
    for p in repo_root.rglob("*"):
        if p.is_file() and p.suffix.lower() in TEXT_EXTENSIONS and not should_exclude(p, repo_root):
            out.append(p)
    return out


def main():
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()

    changed = 0
    files_changed = 0

    for file_path in collect_files(repo_root):
        original = file_path.read_text(encoding="utf-8")
        updated = original

        for pattern, replacement in REWRITE_RULES:
            updated, count = pattern.subn(replacement, updated)
            changed += count

        if updated != original:
            files_changed += 1
            if not args.dry_run:
                file_path.write_text(updated, encoding="utf-8")

    print("Phase 41 complete.")
    print(f"Files changed: {files_changed}")
    print(f"Total replacements: {changed}")


if __name__ == "__main__":
    main()