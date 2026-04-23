#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class Hit:
    file: str
    type: str
    match: str
    line: int


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

EXCLUDED_ROOTS = [
    ".git",
    "node_modules",
    ".venv",
    "venv",
    "__pycache__",
    "dist",
    "build",
    "tools/repo/_phase4_backups",
]

LEGACY_PATTERNS = [
    (re.compile(r"\bapp\.products\.acquire\b"), "legacy_backend_import"),
    (re.compile(r"\bfrom\s+\.products\.acquire(\b|\.)"), "legacy_backend_relative_import"),
    (re.compile(r'(["\'])src/products/acquire/'), "legacy_frontend_src_import"),
    (re.compile(r'(["\'])@/products/acquire/'), "legacy_frontend_alias_import"),
]

NEW_PATTERNS = [
    (re.compile(r"\bproducts\.acquire\.backend\.src\b"), "new_backend_import"),
    (re.compile(r'(["\'])products/acquire/frontend/src/'), "new_frontend_import"),
]

RUNTIME_FILES = [
    "onehaven_decision_engine/backend/app/main.py",
    "onehaven_decision_engine/backend/app/__init__.py",
    "onehaven_decision_engine/backend/app/routers/__init__.py",
    "onehaven_decision_engine/frontend/src/App.tsx",
    "onehaven_decision_engine/frontend/src/main.tsx",
    "onehaven_decision_engine/frontend/src/pages/ImportsPage.tsx",
    "onehaven_decision_engine/frontend/src/pages/PolicyReview.tsx",
]

LEGACY_BACKEND_DIR = "onehaven_decision_engine/backend/app/products/acquire"
LEGACY_FRONTEND_DIR = "onehaven_decision_engine/frontend/src/products/acquire"
NEW_BACKEND_DIR = "products/acquire/backend/src"
NEW_FRONTEND_DIR = "products/acquire/frontend/src"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 5 smoke validation for OneHaven Acquire migration."
    )
    parser.add_argument("--repo-root", default=".", help="Repository root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / "tools",
        repo_root / "onehaven_decision_engine",
        repo_root / "products",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- "
            + "\n- ".join(missing)
        )
    return repo_root


def is_text_file(path: Path) -> bool:
    return path.suffix.lower() in TEXT_EXTENSIONS


def should_exclude(path: Path, repo_root: Path) -> bool:
    rel = path.relative_to(repo_root).as_posix()
    for prefix in EXCLUDED_ROOTS:
        if rel == prefix or rel.startswith(prefix + "/"):
            return True
    return False


def collect_text_files(repo_root: Path) -> list[Path]:
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


def scan_patterns(repo_root: Path, patterns: list[tuple[re.Pattern[str], str]]) -> list[Hit]:
    hits: list[Hit] = []
    for file_path in collect_text_files(repo_root):
        rel = file_path.relative_to(repo_root).as_posix()
        content = read_text(file_path)
        lines = content.splitlines()

        for pattern, label in patterns:
            for line_no, line in enumerate(lines, start=1):
                for match in pattern.finditer(line):
                    hits.append(
                        Hit(
                            file=rel,
                            type=label,
                            match=match.group(0),
                            line=line_no,
                        )
                    )
    return hits


def scan_runtime_files(repo_root: Path) -> list[dict]:
    results: list[dict] = []
    for rel in RUNTIME_FILES:
        path = repo_root / rel
        if not path.exists() or not path.is_file() or not is_text_file(path):
            results.append(
                {
                    "file": rel,
                    "exists": False,
                    "legacy_hits": [],
                    "new_hits": [],
                }
            )
            continue

        content = read_text(path)
        legacy_hits = []
        new_hits = []

        for pattern, label in LEGACY_PATTERNS:
            for match in pattern.finditer(content):
                legacy_hits.append(
                    {
                        "type": label,
                        "match": match.group(0),
                    }
                )

        for pattern, label in NEW_PATTERNS:
            for match in pattern.finditer(content):
                new_hits.append(
                    {
                        "type": label,
                        "match": match.group(0),
                    }
                )

        results.append(
            {
                "file": rel,
                "exists": True,
                "legacy_hits": legacy_hits,
                "new_hits": new_hits,
            }
        )
    return results


def dir_summary(path: Path) -> dict:
    return {
        "path": str(path),
        "exists": path.exists(),
        "file_count": sum(1 for p in path.rglob("*") if p.is_file()) if path.exists() else 0,
    }


def write_report(repo_root: Path, payload: dict) -> None:
    report_path = repo_root / "tools" / "repo" / "acquire-phase5-report.json"
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    repo_root = ensure_repo_root(Path(parse_args().repo_root))

    legacy_backend = repo_root / LEGACY_BACKEND_DIR
    legacy_frontend = repo_root / LEGACY_FRONTEND_DIR
    new_backend = repo_root / NEW_BACKEND_DIR
    new_frontend = repo_root / NEW_FRONTEND_DIR

    legacy_hits = scan_patterns(repo_root, LEGACY_PATTERNS)
    new_hits = scan_patterns(repo_root, NEW_PATTERNS)
    runtime_results = scan_runtime_files(repo_root)

    legacy_runtime_count = sum(len(item["legacy_hits"]) for item in runtime_results if item["exists"])
    new_runtime_count = sum(len(item["new_hits"]) for item in runtime_results if item["exists"])

    legacy_backend_summary = dir_summary(legacy_backend)
    legacy_frontend_summary = dir_summary(legacy_frontend)
    new_backend_summary = dir_summary(new_backend)
    new_frontend_summary = dir_summary(new_frontend)

    ready_to_remove_legacy = (
        new_backend_summary["exists"]
        and new_frontend_summary["exists"]
        and new_backend_summary["file_count"] > 0
        and new_frontend_summary["file_count"] > 0
        and len(legacy_hits) == 0
    )

    status = "success" if ready_to_remove_legacy else "needs_attention"

    payload = {
        "phase": 5,
        "product": "acquire",
        "status": status,
        "summary": {
            "legacy_reference_count": len(legacy_hits),
            "new_reference_count": len(new_hits),
            "legacy_runtime_reference_count": legacy_runtime_count,
            "new_runtime_reference_count": new_runtime_count,
            "ready_to_remove_legacy": ready_to_remove_legacy,
        },
        "directories": {
            "legacy_backend": legacy_backend_summary,
            "legacy_frontend": legacy_frontend_summary,
            "new_backend": new_backend_summary,
            "new_frontend": new_frontend_summary,
        },
        "runtime_files": runtime_results,
        "legacy_hits": [asdict(hit) for hit in legacy_hits],
        "new_hits": [asdict(hit) for hit in new_hits],
        "next_action": (
            "Legacy Acquire can be removed."
            if ready_to_remove_legacy
            else "Do not remove legacy Acquire yet. Resolve remaining legacy references first."
        ),
    }

    write_report(repo_root, payload)

    print("Phase 5 validation complete.")
    print(f"Status: {status}")
    print(f"Legacy reference count: {len(legacy_hits)}")
    print(f"Ready to remove legacy Acquire: {ready_to_remove_legacy}")
    print("Report written to tools/repo/acquire-phase5-report.json")


if __name__ == "__main__":
    main()