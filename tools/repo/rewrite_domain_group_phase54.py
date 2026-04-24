#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE53_REPORT = "tools/repo/domain-group-phase53-report.json"

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


@dataclass
class RewriteRecord:
    file: str
    replacements: list[dict[str, str]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rewrite legacy app.domain.* refs using Phase 53 report."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE53_REPORT,
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def should_exclude(path: Path, repo_root: Path) -> bool:
    rel = path.relative_to(repo_root).as_posix()
    return any(rel.startswith(prefix) for prefix in EXCLUDED_PREFIXES)


def collect_files(repo_root: Path) -> list[Path]:
    files = []
    for p in repo_root.rglob("*"):
        if p.is_file() and p.suffix.lower() in TEXT_EXTENSIONS and not should_exclude(p, repo_root):
            files.append(p)
    return files


def source_to_legacy_modules(source: str) -> list[str]:
    if not source.endswith(".py"):
        return []

    rel_after_app = source
    marker = "onehaven_decision_engine/backend/app/"
    if rel_after_app.startswith(marker):
        rel_after_app = rel_after_app[len(marker):]

    if not rel_after_app.startswith("domain/"):
        return []

    no_ext = rel_after_app[:-3]  # remove .py
    mod_tail = no_ext.replace("/", ".")

    names = [
        f"app.{mod_tail}",
        f"backend.app.{mod_tail}",
    ]

    # Also support app.domain.<stem> and backend.app.domain.<stem> direct forms.
    stem = Path(source).stem
    names.append(f"app.domain.{stem}")
    names.append(f"backend.app.domain.{stem}")

    return list(dict.fromkeys(names))


def target_to_module(target: str) -> str | None:
    if not target or not target.endswith(".py"):
        return None
    return target[:-3].replace("/", ".")


def build_rules(report: dict) -> list[tuple[re.Pattern[str], str, str]]:
    rules: list[tuple[re.Pattern[str], str, str]] = []
    seen: set[tuple[str, str]] = set()

    for item in report.get("results", []):
        target = item.get("target")
        source = item.get("source")
        if not target or not source:
            continue
        if item.get("status") not in {"moved", "skipped"}:
            continue

        replacement = target_to_module(target)
        if not replacement:
            continue

        for legacy in source_to_legacy_modules(source):
            key = (legacy, replacement)
            if key in seen:
                continue
            seen.add(key)
            rules.append(
                (
                    re.compile(rf"\b{re.escape(legacy)}\b"),
                    replacement,
                    legacy,
                )
            )

    return rules


def apply_rules(text: str, rules: list[tuple[re.Pattern[str], str, str]]) -> tuple[str, list[dict[str, str]]]:
    updated = text
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


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    report = read_json(repo_root / PHASE53_REPORT)
    rules = build_rules(report)

    rewrites: list[RewriteRecord] = []

    for file_path in collect_files(repo_root):
        original = file_path.read_text(encoding="utf-8")
        updated, replacements = apply_rules(original, rules)

        if updated != original:
            if not args.dry_run:
                file_path.write_text(updated, encoding="utf-8")

            rewrites.append(
                RewriteRecord(
                    file=file_path.relative_to(repo_root).as_posix(),
                    replacements=replacements,
                )
            )

    payload = {
        "phase": 54,
        "dry_run": args.dry_run,
        "rule_count": len(rules),
        "rewrite_file_count": len(rewrites),
        "rewrites": [asdict(r) for r in rewrites],
    }

    out = repo_root / "tools" / "repo" / "domain-group-phase54-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 54 complete.")
    print(f"Rules: {payload['rule_count']}")
    print(f"Files changed: {payload['rewrite_file_count']}")
    print("Report written to tools/repo/domain-group-phase54-report.json")


if __name__ == "__main__":
    main()