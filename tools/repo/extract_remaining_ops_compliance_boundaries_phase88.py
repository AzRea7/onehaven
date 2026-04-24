#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path


MOVES = {
    "products/ops/backend/src/services/dashboard_rollups.py":
        "onehaven_platform/backend/src/services/dashboard_rollup_service.py",

    "products/ops/backend/src/services/photo_rehab_agent.py":
        "onehaven_platform/backend/src/services/photo_rehab_service.py",

    "products/compliance/backend/src/domain/jurisdiction_scoring.py":
        "onehaven_platform/backend/src/services/jurisdiction_scoring_service.py",

    "products/compliance/backend/src/domain/inspection/compliance_completion.py":
        "onehaven_platform/backend/src/services/compliance_completion_service.py",

    "products/compliance/backend/src/services/inspections/failure_task_service.py":
        "onehaven_platform/backend/src/services/inspection_failure_task_service.py",

    "products/compliance/backend/src/services/inspections/readiness_service.py":
        "onehaven_platform/backend/src/services/inspection_readiness_service.py",

    "products/compliance/backend/src/services/jurisdiction_task_mapper.py":
        "onehaven_platform/backend/src/services/jurisdiction_task_mapper_service.py",
}


REPLS = {
    "products.ops.backend.src.services.dashboard_rollups":
        "onehaven_platform.backend.src.services.dashboard_rollup_service",

    "products.ops.backend.src.services.photo_rehab_agent":
        "onehaven_platform.backend.src.services.photo_rehab_service",

    "products.compliance.backend.src.domain.jurisdiction_scoring":
        "onehaven_platform.backend.src.services.jurisdiction_scoring_service",

    "products.compliance.backend.src.domain.inspection.compliance_completion":
        "onehaven_platform.backend.src.services.compliance_completion_service",

    "products.compliance.backend.src.services.inspections.failure_task_service":
        "onehaven_platform.backend.src.services.inspection_failure_task_service",

    "products.compliance.backend.src.services.inspections.readiness_service":
        "onehaven_platform.backend.src.services.inspection_readiness_service",

    "products.compliance.backend.src.services.jurisdiction_task_mapper":
        "onehaven_platform.backend.src.services.jurisdiction_task_mapper_service",
}


# Facade imports from products.compliance.backend.src.services are ambiguous.
# This replaces them only in the known risky Ops files.
TARGETED_SERVICE_FACADE_REPLS = {
    "products/ops/backend/src/routers/ops.py": {
        "from products.compliance.backend.src.services import":
            "from onehaven_platform.backend.src.services.compliance_projection_service import",
    },
    "products/ops/backend/src/routers/properties.py": {
        "from products.compliance.backend.src.services import":
            "from onehaven_platform.backend.src.services.compliance_projection_service import",
    },
    "products/ops/backend/src/routers/rehab.py": {
        "from products.compliance.backend.src.services import":
            "from onehaven_platform.backend.src.services.compliance_projection_service import",
    },
    "products/ops/backend/src/services/photo_rehab_agent.py": {
        "from products.compliance.backend.src.services import":
            "from onehaven_platform.backend.src.services.compliance_projection_service import",
    },
    "products/ops/backend/src/services/properties/inventory_snapshot_service.py": {
        "from products.compliance.backend.src.services import":
            "from onehaven_platform.backend.src.services.compliance_projection_service import",
    },
    "products/ops/backend/src/services/stage_guard.py": {
        "from products.compliance.backend.src.services import":
            "from onehaven_platform.backend.src.services.compliance_projection_service import",
    },
}


@dataclass
class Action:
    kind: str
    source: str
    target: str | None
    status: str
    reason: str


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--remove-source", action="store_true")
    return parser.parse_args()


def copy_boundaries(root: Path, dry_run: bool, remove_source: bool) -> list[Action]:
    actions: list[Action] = []

    for src_rel, dst_rel in MOVES.items():
        src = root / src_rel
        dst = root / dst_rel

        if not src.exists():
            actions.append(Action("move", src_rel, dst_rel, "skipped", "source_missing"))
            continue

        if dry_run:
            actions.append(Action("move", src_rel, dst_rel, "would_copy", "dry_run"))
            continue

        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

        if remove_source:
            src.unlink()
            actions.append(Action("move", src_rel, dst_rel, "moved", "copied_and_removed_source"))
        else:
            actions.append(Action("move", src_rel, dst_rel, "copied", "source_preserved"))

    return actions


def rewrite_global_imports(root: Path, dry_run: bool) -> list[Action]:
    actions: list[Action] = []

    for scan_root in ["apps", "products", "onehaven_platform"]:
        base = root / scan_root
        if not base.exists():
            continue

        for path in base.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue

            original = path.read_text(encoding="utf-8")
            updated = original
            count = 0

            for old, new in REPLS.items():
                c = updated.count(old)
                if c:
                    count += c
                    updated = updated.replace(old, new)

            if updated != original:
                rel = path.relative_to(root).as_posix()
                if dry_run:
                    actions.append(Action("rewrite", rel, None, "would_update", f"{count}_replacements"))
                else:
                    path.write_text(updated, encoding="utf-8")
                    actions.append(Action("rewrite", rel, None, "updated", f"{count}_replacements"))

    return actions


def rewrite_targeted_service_facades(root: Path, dry_run: bool) -> list[Action]:
    actions: list[Action] = []

    for file_rel, repls in TARGETED_SERVICE_FACADE_REPLS.items():
        path = root / file_rel

        if not path.exists():
            actions.append(Action("facade_rewrite", file_rel, None, "skipped", "file_missing"))
            continue

        original = path.read_text(encoding="utf-8")
        updated = original
        count = 0

        for old, new in repls.items():
            c = updated.count(old)
            if c:
                count += c
                updated = updated.replace(old, new)

        if updated == original:
            continue

        if dry_run:
            actions.append(Action("facade_rewrite", file_rel, None, "would_update", f"{count}_replacements"))
        else:
            path.write_text(updated, encoding="utf-8")
            actions.append(Action("facade_rewrite", file_rel, None, "updated", f"{count}_replacements"))

    return actions


def patch_platform_internal_imports(root: Path, dry_run: bool) -> list[Action]:
    actions: list[Action] = []

    patch_repls = {
        "from products.compliance.backend.src.services import":
            "from onehaven_platform.backend.src.services.compliance_projection_service import",
        "from products.ops.backend.src.services.properties.state_machine import":
            "from onehaven_platform.backend.src.services.state_machine_service import",
        "from products.ops.backend.src.services.stage_guard import":
            "from onehaven_platform.backend.src.services.stage_guard_service import",
        "from products.ops.backend.src.services.property_photo_service import":
            "from onehaven_platform.backend.src.services.photo_service import",
        "from products.compliance.backend.src.domain.inspection.compliance_completion import":
            "from onehaven_platform.backend.src.services.compliance_completion_service import",
        "from products.compliance.backend.src.services.inspections.failure_task_service import":
            "from onehaven_platform.backend.src.services.inspection_failure_task_service import",
        "from products.compliance.backend.src.services.inspections.readiness_service import":
            "from onehaven_platform.backend.src.services.inspection_readiness_service import",
        "from products.compliance.backend.src.services.jurisdiction_task_mapper import":
            "from onehaven_platform.backend.src.services.jurisdiction_task_mapper_service import",
    }

    for dst_rel in MOVES.values():
        path = root / dst_rel
        if not path.exists():
            continue

        original = path.read_text(encoding="utf-8")
        updated = original
        count = 0

        for old, new in patch_repls.items():
            c = updated.count(old)
            if c:
                count += c
                updated = updated.replace(old, new)

        if updated == original:
            continue

        if dry_run:
            actions.append(Action("platform_patch", dst_rel, None, "would_update", f"{count}_replacements"))
        else:
            path.write_text(updated, encoding="utf-8")
            actions.append(Action("platform_patch", dst_rel, None, "updated", f"{count}_replacements"))

    return actions


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()

    actions: list[Action] = []
    actions.extend(copy_boundaries(root, args.dry_run, args.remove_source))
    actions.extend(rewrite_global_imports(root, args.dry_run))
    actions.extend(rewrite_targeted_service_facades(root, args.dry_run))
    actions.extend(patch_platform_internal_imports(root, args.dry_run))

    payload = {
        "phase": 88,
        "dry_run": args.dry_run,
        "remove_source": args.remove_source,
        "summary": {
            "moves": sum(a.kind == "move" for a in actions),
            "rewrites": sum(a.kind == "rewrite" for a in actions),
            "facade_rewrites": sum(a.kind == "facade_rewrite" for a in actions),
            "platform_patches": sum(a.kind == "platform_patch" for a in actions),
            "skipped": sum(a.status == "skipped" for a in actions),
        },
        "actions": [asdict(a) for a in actions],
    }

    out = root / "tools/repo/remaining-boundaries-phase88-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 88 complete.")
    print(payload["summary"])
    print("Report written to tools/repo/remaining-boundaries-phase88-report.json")


if __name__ == "__main__":
    main()