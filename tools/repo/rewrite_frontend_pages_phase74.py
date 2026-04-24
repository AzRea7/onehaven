#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, re
from dataclasses import dataclass, asdict
from pathlib import Path

PHASE73_REPORT = "tools/repo/frontend-pages-phase73-report.json"
TEXT_EXTENSIONS = {".tsx", ".ts", ".js", ".jsx"}

@dataclass
class RewriteRecord:
    file: str
    replacements: list[dict[str, str]]

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()

def collect_files(root: Path):
    skip = {".git", "node_modules", "dist", "build"}
    return [
        p for p in root.rglob("*")
        if p.is_file()
        and p.suffix in TEXT_EXTENSIONS
        and not any(part in skip for part in p.parts)
        and "tools/repo" not in p.as_posix()
    ]

def build_rules(report: dict):
    rules = []
    seen = set()

    for item in report.get("results", []):
        source = item.get("source")
        target = item.get("target")
        if not source or not target:
            continue

        name = Path(source).stem
        target_no_ext = target.removesuffix(".tsx").removesuffix(".ts")

        legacy_forms = [
            f"@/pages/{name}",
            f"src/pages/{name}",
            f"../pages/{name}",
            f"./pages/{name}",
        ]

        for legacy in legacy_forms:
            key = (legacy, target_no_ext)
            if key in seen:
                continue
            seen.add(key)
            rules.append((re.compile(re.escape(legacy)), target_no_ext, legacy))

    return rules

def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()
    report_path = root / PHASE73_REPORT

    if not report_path.exists():
        raise SystemExit(f"Missing report: {report_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    rules = build_rules(report)

    rewrites = []

    for file_path in collect_files(root):
        original = file_path.read_text(encoding="utf-8")
        updated = original
        replacements = []

        for pattern, replacement, label in rules:
            updated2, count = pattern.subn(replacement, updated)
            if count:
                replacements.append({
                    "rule": label,
                    "replacement": replacement,
                    "count": str(count),
                })
            updated = updated2

        if updated != original:
            if not args.dry_run:
                file_path.write_text(updated, encoding="utf-8")
            rewrites.append(RewriteRecord(
                file=file_path.relative_to(root).as_posix(),
                replacements=replacements,
            ))

    payload = {
        "phase": 74,
        "dry_run": args.dry_run,
        "rule_count": len(rules),
        "rewrite_file_count": len(rewrites),
        "rewrites": [asdict(r) for r in rewrites],
    }

    out = root / "tools/repo/frontend-pages-phase74-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 74 complete.")
    print(f"Rules: {payload['rule_count']}")
    print(f"Files changed: {payload['rewrite_file_count']}")
    print("Report written to tools/repo/frontend-pages-phase74-report.json")

if __name__ == "__main__":
    main()