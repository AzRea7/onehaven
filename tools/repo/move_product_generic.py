#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass(frozen=True)
class ProductConfig:
    slug: str
    display_name: str
    legacy_backend: str
    legacy_frontend: str
    target_backend: str
    target_frontend: str


PRODUCTS: dict[str, ProductConfig] = {
    "intelligence": ProductConfig(
        slug="intelligence",
        display_name="OneHaven Intelligence",
        legacy_backend="backend/app/products/investor_intelligence",
        legacy_frontend="frontend/src/products/investor_intelligence",
        target_backend="products/intelligence/backend/src",
        target_frontend="products/intelligence/frontend/src",
    ),
    "tenants": ProductConfig(
        slug="tenants",
        display_name="OneHaven Tenants",
        legacy_backend="backend/app/products/tenant",
        legacy_frontend="frontend/src/products/tenant",
        target_backend="products/tenants/backend/src",
        target_frontend="products/tenants/frontend/src",
    ),
    "ops": ProductConfig(
        slug="ops",
        display_name="OneHaven Ops",
        legacy_backend="backend/app/products/management",
        legacy_frontend="frontend/src/products/management",
        target_backend="products/ops/backend/src",
        target_frontend="products/ops/frontend/src",
    ),
    "compliance": ProductConfig(
        slug="compliance",
        display_name="OneHaven Compliance",
        legacy_backend="backend/app/products/compliance",
        legacy_frontend="frontend/src/products/compliance",
        target_backend="products/compliance/backend/src",
        target_frontend="products/compliance/frontend/src",
    ),
}


TEXT_EXTENSIONS = {
    ".py", ".tsx", ".ts", ".js", ".jsx", ".json", ".md",
    ".yaml", ".yml", ".toml", ".ini", ".txt", ".css",
    ".scss", ".html", ".sh",
}


@dataclass
class RewriteRecord:
    file: str
    replacements: list[dict[str, str]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generic product mover.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument(
        "--product",
        required=True,
        choices=sorted(PRODUCTS.keys()),
        help="Product slug to move.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--remove-legacy", action="store_true")
    return parser.parse_args()


def find_legacy_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    candidate = repo_root / "onehaven_decision_engine"
    if candidate.exists() and (candidate / "backend").exists() and (candidate / "frontend").exists():
        return candidate
    if (repo_root / "backend").exists() and (repo_root / "frontend").exists():
        return repo_root
    raise SystemExit(
        f"Could not locate legacy root.\nChecked:\n- {candidate}\n- {repo_root}"
    )


def actual_repo_root_from(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    if (repo_root / "onehaven_decision_engine").exists():
        return repo_root
    if (repo_root / "backend").exists() and (repo_root / "frontend").exists():
        parent = repo_root.parent
        if (parent / "tools").exists():
            return parent
    return repo_root


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def dir_has_files(path: Path) -> bool:
    return path.exists() and any(p.is_file() for p in path.rglob("*"))


def validate_target_dir(dst: Path) -> None:
    if not dst.exists():
        return
    if dir_has_files(dst):
        raise SystemExit(f"Refusing to write into non-empty target directory: {dst}")


def is_text_file(path: Path) -> bool:
    return path.suffix.lower() in TEXT_EXTENSIONS


def collect_text_files(root: Path) -> list[Path]:
    return [p for p in root.rglob("*") if p.is_file() and is_text_file(p)]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def copy_tree(src: Path, dst: Path) -> None:
    ensure_dir(dst.parent)
    shutil.copytree(src, dst, dirs_exist_ok=True)


def product_patterns(config: ProductConfig) -> tuple[str, str]:
    legacy_backend_mod = config.legacy_backend.replace("/", ".")
    legacy_frontend_path = f"src/products/{config.legacy_frontend.split('/')[-1]}"
    return legacy_backend_mod, legacy_frontend_path


def apply_rewrites(content: str, config: ProductConfig) -> tuple[str, list[dict[str, str]]]:
    legacy_backend_mod, legacy_frontend_path = product_patterns(config)
    new_backend_mod = config.target_backend.replace("/", ".")
    new_frontend_path = config.target_frontend

    rewrites = [
        (
            re.compile(rf"\b{re.escape(legacy_backend_mod)}\b"),
            new_backend_mod,
            "backend_product_import",
        ),
        (
            re.compile(rf'(["\'])@/{re.escape(legacy_frontend_path)}/'),
            lambda m: f"{m.group(1)}{new_frontend_path}/",
            "frontend_alias_import",
        ),
        (
            re.compile(rf'(["\']){re.escape(legacy_frontend_path)}/'),
            lambda m: f"{m.group(1)}{new_frontend_path}/",
            "frontend_src_import",
        ),
    ]

    updated = content
    records: list[dict[str, str]] = []
    for pattern, replacement, label in rewrites:
        if callable(replacement):
            new_text, count = pattern.subn(replacement, updated)
            rep = "<callable>"
        else:
            new_text, count = pattern.subn(replacement, updated)
            rep = replacement
        if count > 0:
            records.append({
                "rule": label,
                "pattern": pattern.pattern,
                "replacement": rep,
                "count": str(count),
            })
        updated = new_text

    return updated, records


def rewrite_batch(root: Path, config: ProductConfig) -> list[RewriteRecord]:
    results: list[RewriteRecord] = []
    for file_path in collect_text_files(root):
        original = read_text(file_path)
        updated, records = apply_rewrites(original, config)
        if updated != original:
            write_text(file_path, updated)
            results.append(RewriteRecord(str(file_path), records))
    return results


def scan_for_legacy_refs(root: Path, config: ProductConfig) -> list[dict[str, str]]:
    legacy_backend_mod, legacy_frontend_path = product_patterns(config)
    checks = [
        (re.compile(rf"\b{re.escape(legacy_backend_mod)}\b"), "legacy_backend_import"),
        (re.compile(rf'(["\'])@/{re.escape(legacy_frontend_path)}/'), "legacy_frontend_alias"),
        (re.compile(rf'(["\']){re.escape(legacy_frontend_path)}/'), "legacy_frontend_src"),
    ]
    hits: list[dict[str, str]] = []
    for file_path in collect_text_files(root):
        content = read_text(file_path)
        for pattern, label in checks:
            for match in pattern.finditer(content):
                hits.append({
                    "file": str(file_path),
                    "type": label,
                    "match": match.group(0),
                })
    return hits


def write_report(repo_root: Path, product: str, payload: dict) -> None:
    path = repo_root / "tools" / "repo" / f"{product}-generic-move-report.json"
    write_text(path, json.dumps(payload, indent=2))


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    legacy_root = find_legacy_root(repo_root)
    actual_repo_root = actual_repo_root_from(repo_root)
    config = PRODUCTS[args.product]

    legacy_backend = legacy_root / config.legacy_backend
    legacy_frontend = legacy_root / config.legacy_frontend
    target_backend = actual_repo_root / config.target_backend
    target_frontend = actual_repo_root / config.target_frontend

    print(f"Moving {config.display_name}")
    print(f"Backend:  {legacy_backend} -> {target_backend}")
    print(f"Frontend: {legacy_frontend} -> {target_frontend}")

    if not legacy_backend.exists():
        raise SystemExit(f"Missing backend path: {legacy_backend}")
    if not legacy_frontend.exists():
        raise SystemExit(f"Missing frontend path: {legacy_frontend}")

    validate_target_dir(target_backend)
    validate_target_dir(target_frontend)

    if args.dry_run:
        print("Dry run only. No changes made.")
        return

    copy_tree(legacy_backend, target_backend)
    copy_tree(legacy_frontend, target_frontend)

    backend_rewrites = rewrite_batch(target_backend, config)
    frontend_rewrites = rewrite_batch(target_frontend, config)

    unresolved = scan_for_legacy_refs(target_backend, config) + scan_for_legacy_refs(target_frontend, config)

    payload = {
        "status": "success" if not unresolved else "failed_validation",
        "product": config.slug,
        "display_name": config.display_name,
        "legacy_backend": str(legacy_backend),
        "legacy_frontend": str(legacy_frontend),
        "target_backend": str(target_backend),
        "target_frontend": str(target_frontend),
        "backend_rewrites": [asdict(r) for r in backend_rewrites],
        "frontend_rewrites": [asdict(r) for r in frontend_rewrites],
        "unresolved_hits": unresolved,
    }
    write_report(actual_repo_root, config.slug, payload)

    if unresolved:
        raise SystemExit(
            f"Move completed but unresolved legacy references remain.\n"
            f"See tools/repo/{config.slug}-generic-move-report.json"
        )

    if args.remove_legacy:
        shutil.rmtree(legacy_backend)
        shutil.rmtree(legacy_frontend)

    print("Move completed successfully.")


if __name__ == "__main__":
    main()