#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE16_REPORT = "tools/repo/frontend-core-move-phase16-report.json"

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
        description="Rewrite imports for moved frontend core files based on Phase 16."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE16_REPORT,
        repo_root / "onehaven_decision_engine",
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


def read_phase16(repo_root: Path) -> dict:
    return json.loads((repo_root / PHASE16_REPORT).read_text(encoding="utf-8"))


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


def build_rules(moved_results: list[dict]) -> list[tuple[re.Pattern[str], str, str]]:
    rules: list[tuple[re.Pattern[str], str, str]] = []

    for item in moved_results:
        if item.get("status") != "moved":
            continue

        source = item["source"]   # e.g. frontend/src/lib/auth.tsx
        target = item["target"]   # e.g. onehaven_onehaven_platform/frontend/src/auth/auth.tsx

        src_no_ext = source.rsplit(".", 1)[0]
        dst_no_ext = target.rsplit(".", 1)[0]

        src_dir = Path(src_no_ext).as_posix()
        dst_dir = Path(dst_no_ext).as_posix()

        src_name = Path(src_no_ext).name
        dst_name = Path(dst_no_ext).name

        # "@/lib/auth" -> "onehaven_onehaven_platform/frontend/src/auth/auth"
        rules.append((
            re.compile(rf'(["\'])@/{re.escape(src_dir)}/?'),
            f'"{dst_dir}/',
            f"alias_path:{src_dir}->{dst_dir}",
        ))

        # "src/lib/auth" -> "onehaven_onehaven_platform/frontend/src/auth/auth"
        rules.append((
            re.compile(rf'(["\']){re.escape(src_dir)}/?'),
            f'"{dst_dir}/',
            f"src_path:{src_dir}->{dst_dir}",
        ))

        # "@/components/AppShell" -> "onehaven_onehaven_platform/frontend/src/shell/AppShell"
        short_src = f"@/{source.split('frontend/src/', 1)[-1].rsplit('.', 1)[0]}"
        short_dst = target.rsplit(".", 1)[0]
        rules.append((
            re.compile(rf'(["\']){re.escape(short_src)}(["\'])'),
            f'"{short_dst}"',
            f"short_alias:{short_src}->{short_dst}",
        ))

        # "src/components/AppShell" -> "onehaven_onehaven_platform/frontend/src/shell/AppShell"
        short_src_plain = source.split("frontend/src/", 1)[-1].rsplit(".", 1)[0]
        rules.append((
            re.compile(rf'(["\']){re.escape(short_src_plain)}(["\'])'),
            f'"{short_dst}"',
            f"short_src:{short_src_plain}->{short_dst}",
        ))

        # Relative imports by basename only, e.g. "./AppShell" or "../AppShell"
        if src_name == dst_name:
            rules.append((
                re.compile(rf'(["\'])(\./|\.\./)+{re.escape(src_name)}(["\'])'),
                f'"{short_dst}"',
                f"relative_basename:{src_name}->{short_dst}",
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
    rewrites: list[RewriteRecord] = []

    for file_path in collect_files(repo_root):
        original = file_path.read_text(encoding="utf-8")
        updated, replacements = apply_rules(original, rules)

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


def write_report(repo_root: Path, payload: dict) -> None:
    out = repo_root / "tools" / "repo" / "frontend-core-rewrite-phase17-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    phase16 = read_phase16(repo_root)

    rules = build_rules(phase16.get("results", []))
    rewrites = rewrite_repo(repo_root, rules, args.dry_run)

    payload = {
        "phase": 17,
        "description": "Rewrite imports for moved frontend core files",
        "dry_run": args.dry_run,
        "rule_count": len(rules),
        "rewrite_file_count": len(rewrites),
        "rewrites": [asdict(r) for r in rewrites],
    }

    write_report(repo_root, payload)

    print("Phase 17 complete.")
    print(f"Rewrite rules: {len(rules)}")
    print(f"Files changed: {len(rewrites)}")
    print("Report written to tools/repo/frontend-core-rewrite-phase17-report.json")


if __name__ == "__main__":
    main()