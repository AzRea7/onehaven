#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE18_REPORT = "tools/repo/frontend-core-validate-phase18-report.json"


@dataclass
class RewriteRecord:
    file: str
    replacements: list[dict[str, str]]


SAFE_REPLACEMENTS = [
    # alias imports to new homes
    (re.compile(r'(["\'])@/components/'), r'\1packages/ui/src/components/', "alias_components_to_package"),
    (re.compile(r'(["\'])@/lib/auth(["\'])'), r'\1onehaven_onehaven_platform/frontend/src/auth/auth\2', "alias_auth_to_platform"),
    (re.compile(r'(["\'])@/lib/authFlow(["\'])'), r'\1onehaven_onehaven_platform/frontend/src/auth/authFlow\2', "alias_authflow_to_platform"),
    (re.compile(r'(["\'])@/pages/'), r'\1apps/suite-web/src/routes/', "alias_pages_to_suite_routes"),

    # plain src references
    (re.compile(r'(["\'])src/components/'), r'\1packages/ui/src/components/', "src_components_to_package"),
    (re.compile(r'(["\'])src/lib/auth(["\'])'), r'\1onehaven_onehaven_platform/frontend/src/auth/auth\2', "src_auth_to_platform"),
    (re.compile(r'(["\'])src/lib/authFlow(["\'])'), r'\1onehaven_onehaven_platform/frontend/src/auth/authFlow\2', "src_authflow_to_platform"),
    (re.compile(r'(["\'])src/pages/'), r'\1apps/suite-web/src/routes/', "src_pages_to_suite_routes"),

    # non-src relative-ish bare paths inside config/import strings
    (re.compile(r'(["\'])components/'), r'\1packages/ui/src/components/', "bare_components_to_package"),
    (re.compile(r'(["\'])lib/auth(["\'])'), r'\1onehaven_onehaven_platform/frontend/src/auth/auth\2', "bare_auth_to_platform"),
    (re.compile(r'(["\'])lib/authFlow(["\'])'), r'\1onehaven_onehaven_platform/frontend/src/auth/authFlow\2', "bare_authflow_to_platform"),
    (re.compile(r'(["\'])pages/'), r'\1apps/suite-web/src/routes/', "bare_pages_to_suite_routes"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Targeted frontend residual fixer based on Phase 18 report."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE18_REPORT,
        repo_root / "apps",
        repo_root / "platform",
        repo_root / "packages",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- " + "\n- ".join(missing)
        )
    return repo_root


def read_phase18(repo_root: Path) -> dict:
    return json.loads((repo_root / PHASE18_REPORT).read_text(encoding="utf-8"))


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def apply_safe_replacements(content: str) -> tuple[str, list[dict[str, str]]]:
    updated = content
    replacements: list[dict[str, str]] = []

    for pattern, replacement, label in SAFE_REPLACEMENTS:
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


def rewrite_only_reported_files(repo_root: Path, report: dict, dry_run: bool) -> list[RewriteRecord]:
    files = sorted({hit["file"] for hit in report.get("legacy_hits", [])})
    results: list[RewriteRecord] = []

    for rel in files:
        path = repo_root / rel
        if not path.exists() or not path.is_file():
            continue

        original = read_text(path)
        updated, replacements = apply_safe_replacements(original)

        if updated != original:
            if not dry_run:
                write_text(path, updated)
            results.append(
                RewriteRecord(
                    file=rel,
                    replacements=replacements,
                )
            )

    return results


def write_report(repo_root: Path, payload: dict) -> None:
    out = repo_root / "tools" / "repo" / "frontend-core-fix-phase185-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    phase18 = read_phase18(repo_root)

    rewrites = rewrite_only_reported_files(repo_root, phase18, args.dry_run)

    payload = {
        "phase": "18.5",
        "dry_run": args.dry_run,
        "phase18_hit_count_before": phase18.get("legacy_hit_count", None),
        "rewrite_file_count": len(rewrites),
        "rewrites": [asdict(r) for r in rewrites],
    }

    write_report(repo_root, payload)

    print("Phase 18.5 complete.")
    print(f"Phase 18 hits before fix: {phase18.get('legacy_hit_count')}")
    print(f"Files changed: {len(rewrites)}")
    print("Report written to tools/repo/frontend-core-fix-phase185-report.json")


if __name__ == "__main__":
    main()