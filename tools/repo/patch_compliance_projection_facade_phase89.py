#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


TARGET = Path("onehaven_platform/backend/src/services/compliance_projection_service.py")

PATCH = r'''
# ---------------------------------------------------------------------------
# Compatibility facade exports
# ---------------------------------------------------------------------------
# Ops now imports compliance boundary helpers from this platform module instead
# of importing products.compliance.backend.src.services directly. These wrappers
# are intentionally lazy to avoid recreating Ops <-> Compliance import cycles.

from typing import Any as _Any


def build_property_jurisdiction_blocker(*args: _Any, **kwargs: _Any) -> _Any:
    from products.compliance.backend.src.services.workflow_gate_service import (
        build_property_jurisdiction_blocker as _impl,
    )

    return _impl(*args, **kwargs)


def build_workflow_summary(*args: _Any, **kwargs: _Any) -> _Any:
    from products.compliance.backend.src.services.workflow_gate_service import (
        build_workflow_summary as _impl,
    )

    return _impl(*args, **kwargs)


def build_property_document_stack(*args: _Any, **kwargs: _Any) -> _Any:
    from products.compliance.backend.src.services.compliance_document_service import (
        build_property_document_stack as _impl,
    )

    return _impl(*args, **kwargs)


def analyze_property_photos_for_compliance(*args: _Any, **kwargs: _Any) -> _Any:
    from products.compliance.backend.src.services.compliance_photo_analysis_service import (
        analyze_property_photos_for_compliance as _impl,
    )

    return _impl(*args, **kwargs)


def create_compliance_tasks_from_photo_analysis(*args: _Any, **kwargs: _Any) -> _Any:
    from products.compliance.backend.src.services.compliance_photo_analysis_service import (
        create_compliance_tasks_from_photo_analysis as _impl,
    )

    return _impl(*args, **kwargs)


def apply_inspection_form_results(*args: _Any, **kwargs: _Any) -> _Any:
    from products.compliance.backend.src.services.compliance_service import (
        apply_inspection_form_results as _impl,
    )

    return _impl(*args, **kwargs)


def build_property_compliance_summary(*args: _Any, **kwargs: _Any) -> _Any:
    from products.compliance.backend.src.services.compliance_service import (
        build_property_compliance_summary as _impl,
    )

    return _impl(*args, **kwargs)
'''


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()
    target = root / TARGET

    if not target.exists():
        raise SystemExit(f"Missing target: {target}")

    text = target.read_text(encoding="utf-8")

    marker = "# Compatibility facade exports"
    if marker in text:
        print("Phase 89 complete.")
        print({"status": "skipped", "reason": "facade_already_present"})
        return

    updated = text.rstrip() + "\n\n" + PATCH.strip() + "\n"

    if args.dry_run:
        print("Phase 89 complete.")
        print({"status": "would_patch", "target": str(target)})
        return

    target.write_text(updated, encoding="utf-8")

    print("Phase 89 complete.")
    print({"status": "patched", "target": str(target)})


if __name__ == "__main__":
    main()