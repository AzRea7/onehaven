#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE48_REPORT = "tools/repo/services-group-phase48-report.json"
PHASE50_REPORT = "tools/repo/services-group-phase50-report.json"

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
        description="Rewrite legacy app.services.* refs using Phase 48 + Phase 50 reports."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE48_REPORT,
        repo_root / PHASE50_REPORT,
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


def build_service_mapping(phase48: dict) -> dict[str, str]:
    mapping: dict[str, str] = {}

    for item in phase48.get("results", []):
        target = item.get("target")
        source = item.get("source")
        if not target or not source:
            continue
        if not source.endswith(".py") or not target.endswith(".py"):
            continue

        service_name = Path(source).stem
        target_module = target.replace("/", ".")[:-3]
        mapping[service_name] = target_module

    return mapping


def build_patterns_from_hits(phase50: dict, mapping: dict[str, str]) -> list[tuple[re.Pattern[str], str, str]]:
    patterns: list[tuple[re.Pattern[str], str, str]] = []
    seen: set[tuple[str, str]] = set()

    for hit in phase50.get("legacy_hits", []):
        legacy = hit.get("pattern", "")
        if not legacy.startswith("app.services.") and not legacy.startswith("backend.app.services."):
            continue

        service_name = legacy.split(".")[-1]
        target_module = mapping.get(service_name)
        if not target_module:
            continue

        key = (legacy, target_module)
        if key in seen:
            continue
        seen.add(key)

        patterns.append(
            (
                re.compile(rf"\b{re.escape(legacy)}\b"),
                target_module,
                legacy,
            )
        )

    return patterns


def collect_flagged_files(repo_root: Path, phase50: dict) -> list[Path]:
    files = []
    seen = set()

    for hit in phase50.get("legacy_hits", []):
        rel = hit.get("file")
        if not rel or rel in seen:
            continue
        seen.add(rel)

        p = repo_root / rel
        if not p.exists() or not p.is_file():
            continue
        if p.suffix.lower() not in TEXT_EXTENSIONS:
            continue
        if should_exclude(p, repo_root):
            continue
        files.append(p)

    return files


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

    phase48 = read_json(repo_root / PHASE48_REPORT)
    phase50 = read_json(repo_root / PHASE50_REPORT)

    mapping = build_service_mapping(phase48)
    rules = build_patterns_from_hits(phase50, mapping)
    files = collect_flagged_files(repo_root, phase50)

    rewrites: list[RewriteRecord] = []

    for file_path in files:
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
        "phase": 51,
        "dry_run": args.dry_run,
        "mapping_count": len(mapping),
        "rule_count": len(rules),
        "flagged_file_count": len(files),
        "rewrite_file_count": len(rewrites),
        "rewrites": [asdict(r) for r in rewrites],
    }

    out = repo_root / "tools" / "repo" / "services-group-phase51-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 51 complete.")
    print(f"Mappings: {payload['mapping_count']}")
    print(f"Rules: {payload['rule_count']}")
    print(f"Flagged files scanned: {payload['flagged_file_count']}")
    print(f"Files changed: {payload['rewrite_file_count']}")
    print("Report written to tools/repo/services-group-phase51-report.json")


if __name__ == "__main__":
    main()