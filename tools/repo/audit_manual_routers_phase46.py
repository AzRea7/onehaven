#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE44_REPORT = "tools/repo/unmapped-routers-phase44-report.json"

MANUAL_ROUTER_TARGETS = {
    "metrics.py": "onehaven_onehaven_platform/backend/src/observability/metrics_router.py",
    "ops.py": "products/ops/backend/src/routers/ops.py",
    "photos.py": "products/compliance/backend/src/routers/photos.py",
    "policy_seed.py": "products/compliance/backend/src/routers/policy_seed.py",
    "rent_enrich.py": "products/intelligence/backend/src/routers/rent_enrich.py",
    "streetview.py": "onehaven_onehaven_platform/backend/src/integrations/streetview_router.py",
    "trust.py": "products/compliance/backend/src/routers/trust.py",
    "workflow.py": "onehaven_onehaven_platform/backend/src/workflow/workflow_router.py",
}


@dataclass
class AuditResult:
    source: str
    target: str
    source_exists: bool
    target_exists: bool
    source_hash: str | None
    target_hash: str | None
    status: str
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit manual-review routers against intended targets.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE44_REPORT,
        repo_root / "onehaven_decision_engine",
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def classify(source_exists: bool, target_exists: bool, source_hash: str | None, target_hash: str | None) -> tuple[str, str]:
    if source_exists and target_exists and source_hash == target_hash:
        return "identical", "source_and_target_match"
    if source_exists and target_exists and source_hash != target_hash:
        return "different", "source_and_target_differ"
    if source_exists and not target_exists:
        return "missing_target", "legacy_exists_but_target_missing"
    if not source_exists and target_exists:
        return "already_migrated", "target_exists_but_legacy_missing"
    return "missing_both", "neither_source_nor_target_exists"


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    report = read_json(repo_root / PHASE44_REPORT)

    audits: list[AuditResult] = []

    for review in report.get("reviews", []):
        if review.get("suggested_target"):
            continue

        source = review["source"]
        name = Path(source).name
        target = MANUAL_ROUTER_TARGETS.get(name)
        if not target:
            continue

        src = repo_root / source
        dst = repo_root / target

        source_exists = src.exists()
        target_exists = dst.exists()
        source_hash = sha256_file(src) if source_exists and src.is_file() else None
        target_hash = sha256_file(dst) if target_exists and dst.is_file() else None

        status, reason = classify(source_exists, target_exists, source_hash, target_hash)

        audits.append(
            AuditResult(
                source=source,
                target=target,
                source_exists=source_exists,
                target_exists=target_exists,
                source_hash=source_hash,
                target_hash=target_hash,
                status=status,
                reason=reason,
            )
        )

    payload = {
        "phase": 46,
        "summary": {
            "total": len(audits),
            "identical": sum(1 for a in audits if a.status == "identical"),
            "different": sum(1 for a in audits if a.status == "different"),
            "missing_target": sum(1 for a in audits if a.status == "missing_target"),
            "already_migrated": sum(1 for a in audits if a.status == "already_migrated"),
            "missing_both": sum(1 for a in audits if a.status == "missing_both"),
        },
        "audits": [asdict(a) for a in audits],
    }

    out = repo_root / "tools" / "repo" / "manual-routers-phase46-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 46 complete.")
    print(f"Total audited: {payload['summary']['total']}")
    print(f"Identical: {payload['summary']['identical']}")
    print(f"Different: {payload['summary']['different']}")
    print(f"Missing target: {payload['summary']['missing_target']}")
    print(f"Already migrated: {payload['summary']['already_migrated']}")
    print("Report written to tools/repo/manual-routers-phase46-report.json")


if __name__ == "__main__":
    main()