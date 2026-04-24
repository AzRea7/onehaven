#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


PHASE65_REPORT = "tools/repo/frontend-components-phase65-report.json"

TEXT_EXTENSIONS = {".tsx", ".ts", ".js", ".jsx"}

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate remaining legacy frontend component refs.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE65_REPORT,
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


def build_patterns(report: dict) -> list[tuple[re.Pattern[str], str]]:
    patterns = []
    seen = set()

    for item in report.get("results", []):
        source = item.get("source")
        target = item.get("target")
        if not source or not target:
            continue

        name = Path(source).stem
        legacy_forms = [
            f'@/components/{name}',
            f'src/components/{name}',
            f'../components/{name}',
            f'./components/{name}',
        ]

        for legacy in legacy_forms:
            if legacy in seen:
                continue
            seen.add(legacy)
            patterns.append((re.compile(re.escape(legacy)), legacy))

    return patterns


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    report = read_json(repo_root / PHASE65_REPORT)
    patterns = build_patterns(report)

    hits = []

    for file_path in collect_files(repo_root):
        text = file_path.read_text(encoding="utf-8")
        lines = text.splitlines()
        rel = file_path.relative_to(repo_root).as_posix()

        for regex, label in patterns:
            for i, line in enumerate(lines, 1):
                for match in regex.finditer(line):
                    hits.append(
                        {
                            "file": rel,
                            "pattern": label,
                            "match": match.group(0),
                            "line": i,
                        }
                    )

    payload = {
        "phase": 67,
        "legacy_hit_count": len(hits),
        "legacy_hits": hits,
        "status": "success" if len(hits) == 0 else "needs_attention",
    }

    out = repo_root / "tools" / "repo" / "frontend-components-phase67-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 67 complete.")
    print(f"Remaining legacy component hits: {len(hits)}")
    print("Report written to tools/repo/frontend-components-phase67-report.json")


if __name__ == "__main__":
    main()