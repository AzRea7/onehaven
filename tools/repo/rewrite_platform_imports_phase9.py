#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE8_REPORT = "tools/repo/platform-move-phase8-report.json"

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

EXCLUDED_ROOTS = {
    ".git",
    "node_modules",
    ".venv",
    "venv",
    "__pycache__",
    "dist",
    "build",
    "tools/repo/_phase4_backups",
    "tools/repo/_phase6_backups",
}


@dataclass
class RewriteRecord:
    file: str
    replacements: list[dict[str, str]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rewrite imports for moved platform files based on Phase 8 report."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / "onehaven_decision_engine",
        repo_root / "platform",
        repo_root / PHASE8_REPORT,
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- "
            + "\n- ".join(missing)
        )
    return repo_root


def read_phase8(repo_root: Path) -> dict:
    path = repo_root / PHASE8_REPORT
    return json.loads(path.read_text(encoding="utf-8"))


def is_text_file(path: Path) -> bool:
    return path.suffix.lower() in TEXT_EXTENSIONS


def should_exclude(path: Path, repo_root: Path) -> bool:
    rel = path.relative_to(repo_root).as_posix()
    first = rel.split("/", 1)[0]
    return first in EXCLUDED_ROOTS or rel in EXCLUDED_ROOTS


def collect_files(repo_root: Path) -> list[Path]:
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


def build_rewrite_rules(moved_results: list[dict]) -> list[tuple[re.Pattern[str], str, str]]:
    rules: list[tuple[re.Pattern[str], str, str]] = []

    for item in moved_results:
        if item.get("status") != "moved":
            continue

        source = item["source"]   # e.g. backend/app/config.py
        target = item["target"]   # e.g. onehaven_onehaven_platform/backend/src/config/config.py

        if not source.endswith(".py") or not target.endswith(".py"):
            continue

        old_mod = source[:-3].replace("/", ".")
        new_mod = target[:-3].replace("/", ".")

        rules.append((
            re.compile(rf"\b{re.escape(old_mod)}\b"),
            new_mod,
            f"rewrite:{old_mod}->{new_mod}",
        ))

    return rules


def apply_rules(content: str, rules: list[tuple[re.Pattern[str], str, str]]) -> tuple[str, list[dict[str, str]]]:
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


def rewrite_repo(repo_root: Path, rules: list[tuple[re.Pattern[str], str, str]], dry_run: bool) -> list[RewriteRecord]:
    results: list[RewriteRecord] = []

    for file_path in collect_files(repo_root):
        original = read_text(file_path)
        updated, replacements = apply_rules(original, rules)

        if updated != original:
            if not dry_run:
                write_text(file_path, updated)

            results.append(
                RewriteRecord(
                    file=file_path.relative_to(repo_root).as_posix(),
                    replacements=replacements,
                )
            )

    return results


def write_report(repo_root: Path, payload: dict) -> None:
    out = repo_root / "tools/repo/platform-import-rewrite-phase9-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    phase8 = read_phase8(repo_root)

    moved_results = phase8.get("results", [])
    rules = build_rewrite_rules(moved_results)

    rewrites = rewrite_repo(repo_root, rules, args.dry_run)

    payload = {
        "phase": 9,
        "description": "Rewrite imports for moved platform files",
        "dry_run": args.dry_run,
        "rule_count": len(rules),
        "rewrite_file_count": len(rewrites),
        "rewrites": [asdict(r) for r in rewrites],
    }

    write_report(repo_root, payload)

    print("Phase 9 complete.")
    print(f"Rewrite rules: {len(rules)}")
    print(f"Files changed: {len(rewrites)}")
    print("Report written to tools/repo/platform-import-rewrite-phase9-report.json")


if __name__ == "__main__":
    main()