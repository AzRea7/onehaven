#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate remaining legacy platform refs.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE8_REPORT,
        repo_root / "onehaven_decision_engine",
        repo_root / "platform",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- " + "\n- ".join(missing)
        )
    return repo_root


def read_phase8(repo_root: Path) -> dict:
    return json.loads((repo_root / PHASE8_REPORT).read_text(encoding="utf-8"))


def should_exclude(path: Path, repo_root: Path) -> bool:
    rel = path.relative_to(repo_root).as_posix()
    first = rel.split("/", 1)[0]
    return first in EXCLUDED_ROOTS or rel in EXCLUDED_ROOTS


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


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    phase8 = read_phase8(repo_root)

    patterns: list[tuple[re.Pattern[str], str]] = []
    for item in phase8.get("results", []):
        if item.get("status") != "moved":
            continue
        source = item["source"]
        if source.endswith(".py"):
            old_mod = source[:-3].replace("/", ".")
            patterns.append((re.compile(rf"\b{re.escape(old_mod)}\b"), old_mod))

    hits = []
    for file_path in collect_files(repo_root):
        text = file_path.read_text(encoding="utf-8")
        lines = text.splitlines()
        rel = file_path.relative_to(repo_root).as_posix()

        for pattern, label in patterns:
            for i, line in enumerate(lines, 1):
                for match in pattern.finditer(line):
                    hits.append(
                        {
                            "file": rel,
                            "legacy_import": label,
                            "match": match.group(0),
                            "line": i,
                        }
                    )

    payload = {
        "phase": 10,
        "legacy_hit_count": len(hits),
        "legacy_hits": hits,
        "status": "success" if len(hits) == 0 else "needs_attention",
    }

    out = repo_root / "tools/repo/platform-validate-phase10-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 10 complete.")
    print(f"Remaining legacy platform hits: {len(hits)}")
    print("Report written to tools/repo/platform-validate-phase10-report.json")


if __name__ == "__main__":
    main()