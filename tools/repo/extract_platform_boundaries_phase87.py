#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path


MOVES = {
    "products/ops/backend/src/services/properties/state_machine.py":
        "onehaven_platform/backend/src/services/state_machine_service.py",

    "products/ops/backend/src/services/stage_guard.py":
        "onehaven_platform/backend/src/services/stage_guard_service.py",

    "products/ops/backend/src/services/property_photo_service.py":
        "onehaven_platform/backend/src/services/photo_service.py",

    "products/compliance/backend/src/services/compliance_engine/projection_service.py":
        "onehaven_platform/backend/src/services/compliance_projection_service.py",
}


REPLS = {
    "products.ops.backend.src.services.properties.state_machine":
        "onehaven_platform.backend.src.services.state_machine_service",

    "products.ops.backend.src.services.stage_guard":
        "onehaven_platform.backend.src.services.stage_guard_service",

    "products.ops.backend.src.services.property_photo_service":
        "onehaven_platform.backend.src.services.photo_service",

    "products.compliance.backend.src.services.compliance_engine.projection_service":
        "onehaven_platform.backend.src.services.compliance_projection_service",
}


CONTRACTS = {
    "onehaven_platform/backend/src/contracts/state_machine_contract.py": '''from __future__ import annotations

from typing import Any, Protocol


class StateMachineContract(Protocol):
    def get_state_payload(self, *args: Any, **kwargs: Any) -> dict[str, Any]: ...
    def get_transition_payload(self, *args: Any, **kwargs: Any) -> dict[str, Any]: ...
    def transition_property_state(self, *args: Any, **kwargs: Any) -> dict[str, Any]: ...
''',

    "onehaven_platform/backend/src/contracts/compliance_contract.py": '''from __future__ import annotations

from typing import Any, Protocol


class ComplianceContract(Protocol):
    def build_property_jurisdiction_blocker(self, *args: Any, **kwargs: Any) -> dict[str, Any]: ...
    def build_workflow_summary(self, *args: Any, **kwargs: Any) -> dict[str, Any]: ...
    def build_property_projection_snapshot(self, *args: Any, **kwargs: Any) -> dict[str, Any]: ...
''',

    "onehaven_platform/backend/src/contracts/photo_contract.py": '''from __future__ import annotations

from typing import Any, Protocol


class PhotoContract(Protocol):
    def list_property_photos(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]: ...
    def create_property_photo(self, *args: Any, **kwargs: Any) -> dict[str, Any]: ...
    def delete_property_photo(self, *args: Any, **kwargs: Any) -> dict[str, Any]: ...
''',

    "onehaven_platform/backend/src/contracts/workflow_contract.py": '''from __future__ import annotations

from typing import Any, Protocol


class WorkflowContract(Protocol):
    def assert_stage_transition_allowed(self, *args: Any, **kwargs: Any) -> None: ...
    def build_stage_guard_payload(self, *args: Any, **kwargs: Any) -> dict[str, Any]: ...
''',
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


def write_contracts(root: Path, dry_run: bool) -> list[Action]:
    actions: list[Action] = []

    for rel, content in CONTRACTS.items():
        target = root / rel

        if dry_run:
            actions.append(Action("contract", rel, rel, "would_write", "dry_run"))
            continue

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        actions.append(Action("contract", rel, rel, "written", "created_or_replaced"))

    return actions


def move_files(root: Path, dry_run: bool, remove_source: bool) -> list[Action]:
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


def rewrite_imports(root: Path, dry_run: bool) -> list[Action]:
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


def patch_platform_imports(root: Path, dry_run: bool) -> list[Action]:
    """
    After moving files into platform, patch their own internal imports so they do not import through products.
    """
    actions: list[Action] = []

    platform_repls = {
        "from products.ops.backend.src.services.properties.state_machine import":
            "from onehaven_platform.backend.src.services.state_machine_service import",

        "from products.ops.backend.src.services.stage_guard import":
            "from onehaven_platform.backend.src.services.stage_guard_service import",

        "from products.ops.backend.src.services.property_photo_service import":
            "from onehaven_platform.backend.src.services.photo_service import",

        "from products.compliance.backend.src.services.compliance_engine.projection_service import":
            "from onehaven_platform.backend.src.services.compliance_projection_service import",

        "from app.models import":
            "from onehaven_platform.backend.src.models import",
        "from app.db import":
            "from onehaven_platform.backend.src.db import",
        "from app.config import":
            "from onehaven_platform.backend.src.config import",
        "from app.schemas import":
            "from onehaven_platform.backend.src.schemas import",
        "from app.policy_models import":
            "from onehaven_platform.backend.src.policy_models import",
    }

    for rel in MOVES.values():
        path = root / rel
        if not path.exists():
            continue

        original = path.read_text(encoding="utf-8")
        updated = original
        count = 0

        for old, new in platform_repls.items():
            c = updated.count(old)
            if c:
                count += c
                updated = updated.replace(old, new)

        if updated != original:
            if dry_run:
                actions.append(Action("platform_patch", rel, None, "would_update", f"{count}_replacements"))
            else:
                path.write_text(updated, encoding="utf-8")
                actions.append(Action("platform_patch", rel, None, "updated", f"{count}_replacements"))

    return actions


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()

    actions: list[Action] = []
    actions.extend(write_contracts(root, args.dry_run))
    actions.extend(move_files(root, args.dry_run, args.remove_source))
    actions.extend(rewrite_imports(root, args.dry_run))
    actions.extend(patch_platform_imports(root, args.dry_run))

    payload = {
        "phase": 87,
        "dry_run": args.dry_run,
        "remove_source": args.remove_source,
        "summary": {
            "contracts": sum(a.kind == "contract" for a in actions),
            "moves": sum(a.kind == "move" for a in actions),
            "rewrites": sum(a.kind == "rewrite" for a in actions),
            "platform_patches": sum(a.kind == "platform_patch" for a in actions),
            "skipped": sum(a.status == "skipped" for a in actions),
        },
        "actions": [asdict(a) for a in actions],
    }

    out = root / "tools/repo/platform-boundaries-phase87-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 87 complete.")
    print(payload["summary"])
    print("Report written to tools/repo/platform-boundaries-phase87-report.json")


if __name__ == "__main__":
    main()