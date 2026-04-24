#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


CHECKS = {
    "legacy_backend_main": "onehaven_decision_engine/backend/app/main.py",
    "legacy_frontend_app": "onehaven_decision_engine/frontend/src/App.tsx",
    "legacy_frontend_main": "onehaven_decision_engine/frontend/src/main.tsx",
    "suite_api_main": "apps/suite-api/app/main.py",
    "suite_web_app": "apps/suite-web/src/app/App.tsx",
    "suite_web_main": "apps/suite-web/src/bootstrap/main.tsx",
    "worker_main": "apps/worker/app/worker_main.py",
}

IMPORT_PATTERNS = [
    r"\bonehaven_decision_engine\.backend\.app\b",
    r"\bapp\.products\.",
    r'["\']@/products/',
    r'["\']src/products/',
    r'["\']@/components/',
    r'["\']@/lib/',
    r'["\']@/pages/',
]

TEXT_SUFFIXES = {".py", ".ts", ".tsx", ".js", ".jsx"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit runtime entrypoints.")
    parser.add_argument("--repo-root", default=".")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / "onehaven_decision_engine",
        repo_root / "apps",
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def scan_file(repo_root: Path, rel: str) -> dict:
    path = repo_root / rel
    if not path.exists():
        return {"file": rel, "exists": False, "hits": []}

    text = read_text(path)
    lines = text.splitlines()
    hits = []

    for pattern in IMPORT_PATTERNS:
        regex = re.compile(pattern)
        for i, line in enumerate(lines, 1):
            for match in regex.finditer(line):
                hits.append({
                    "line": i,
                    "match": match.group(0),
                    "text": line.strip(),
                })

    return {
        "file": rel,
        "exists": True,
        "hits": hits,
    }


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))

    results = {name: scan_file(repo_root, rel) for name, rel in CHECKS.items()}

    payload = {
        "phase": 38,
        "summary": {
            "files_checked": len(results),
            "files_with_hits": sum(1 for r in results.values() if r["hits"]),
        },
        "results": results,
    }

    out = repo_root / "tools" / "repo" / "runtime-entrypoints-phase38-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 38 complete.")
    print(f"Files checked: {payload['summary']['files_checked']}")
    print(f"Files with hits: {payload['summary']['files_with_hits']}")
    print("Report written to tools/repo/runtime-entrypoints-phase38-report.json")


if __name__ == "__main__":
    main()