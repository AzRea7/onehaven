#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass, asdict
from pathlib import Path


SOURCE_ROOT = "onehaven_decision_engine/backend/app/products/compliance"

TARGET_MAP = {
    "__init__.py": "products/compliance/backend/src/__init__.py",
    "services/__init__.py": "products/compliance/backend/src/services/__init__.py",
    "services/compliance_document_service.py": "products/compliance/backend/src/services/compliance_document_service.py",
    "services/compliance_photo_analysis_service.py": "products/compliance/backend/src/services/compliance_photo_analysis_service.py",
    "services/compliance_service.py": "products/compliance/backend/src/services/compliance_service.py",
    "services/property_compliance_resolution_service.py": "products/compliance/backend/src/services/property_compliance_resolution_service.py",
    "services/workflow_gate_service.py": "products/compliance/backend/src/services/workflow_gate_service.py",
    "services/policy_pipeline/__init__.py": "products/compliance/backend/src/services/policy_pipeline/__init__.py",
    "services/policy_pipeline/pipeline_service.py": "products/compliance/backend/src/services/policy_pipeline/pipeline_service.py",
}


@dataclass
class Audit:
    source: str
    target: str | None
    status: str
    source_exists: bool
    target_exists: bool
    source_hash: str | None
    target_hash: str | None


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    return p.parse_args()


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()
    source_root = root / SOURCE_ROOT

    if not source_root.exists():
        raise SystemExit(f"Missing source root: {source_root}")

    audits = []

    for src in sorted(source_root.rglob("*")):
        if not src.is_file():
            continue

        rel = src.relative_to(source_root).as_posix()
        target = TARGET_MAP.get(rel)
        dst = root / target if target else None

        source_exists = src.exists()
        target_exists = bool(dst and dst.exists())
        source_hash = sha(src) if source_exists else None
        target_hash = sha(dst) if target_exists and dst else None

        if not target:
            status = "unmapped"
        elif source_exists and target_exists and source_hash == target_hash:
            status = "identical"
        elif source_exists and target_exists:
            status = "different"
        elif source_exists and not target_exists:
            status = "missing_target"
        elif not source_exists and target_exists:
            status = "already_migrated"
        else:
            status = "missing_both"

        audits.append(Audit(
            source=src.relative_to(root).as_posix(),
            target=target,
            status=status,
            source_exists=source_exists,
            target_exists=target_exists,
            source_hash=source_hash,
            target_hash=target_hash,
        ))

    payload = {
        "phase": 76,
        "summary": {
            "total": len(audits),
            "identical": sum(a.status == "identical" for a in audits),
            "different": sum(a.status == "different" for a in audits),
            "missing_target": sum(a.status == "missing_target" for a in audits),
            "unmapped": sum(a.status == "unmapped" for a in audits),
            "already_migrated": sum(a.status == "already_migrated" for a in audits),
        },
        "audits": [asdict(a) for a in audits],
    }

    out = root / "tools/repo/residual-compliance-phase76-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 76 complete.")
    print(payload["summary"])
    print("Report written to tools/repo/residual-compliance-phase76-report.json")


if __name__ == "__main__":
    main()