#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate remaining legacy domain refs after Phase 54.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
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


def source_to_patterns(source: str) -> list[tuple[re.Pattern[str], str]]:
    if not source.endswith(".py"):
        return []

    rel_after_app = source
    marker = "onehaven_decision_engine/backend/app/"
    if rel_after_app.startswith(marker):
        rel_after_app = rel_after_app[len(marker):]

    if not rel_after_app.startswith("domain/"):
        return []

    no_ext = rel_after_app[:-3]
    mod_tail = no_ext.replace("/", ".")
    stem = Path(source).stem

    legacy = [
        f"app.{mod_tail}",
        f"backend.app.{mod_tail}",
        f"app.domain.{stem}",
        f"backend.app.domain.{stem}",
    ]

    return [(re.compile(rf"\b{re.escape(item)}\b"), item) for item in dict.fromkeys(legacy)]


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    report = read_json(repo_root / PHASE53_REPORT)

    patterns: list[tuple[re.Pattern[str], str]] = []
    for item in report.get("results", []):
        source = item.get("source")
        target = item.get("target")
        if not source or not target:
            continue
        patterns.extend(source_to_patterns(source))

    # dedupe patterns by label
    deduped = {}
    for regex, label in patterns:
        deduped[label] = regex

    hits = []

    for file_path in collect_files(repo_root):
        text = file_path.read_text(encoding="utf-8")
        lines = text.splitlines()
        rel = file_path.relative_to(repo_root).as_posix()

        for label, regex in deduped.items():
            for i, line in enumerate(lines, 1):
                for match in regex.finditer(line):
                    hits.append({
                        "file": rel,
                        "pattern": label,
                        "match": match.group(0),
                        "line": i,
                    })

    payload = {
        "phase": 55,
        "legacy_hit_count": len(hits),
        "legacy_hits": hits,
        "status": "success" if len(hits) == 0 else "needs_attention",
    }

    out = repo_root / "tools" / "repo" / "domain-group-phase55-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 55 complete.")
    print(f"Remaining legacy domain hits: {len(hits)}")
    print("Report written to tools/repo/domain-group-phase55-report.json")


if __name__ == "__main__":
    main()