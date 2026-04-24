from __future__ import annotations

from typing import Any


def build_property_jurisdiction_blocker(*args: Any, **kwargs: Any) -> Any:
    from products.compliance.backend.src.services.workflow_gate_service import (
        build_property_jurisdiction_blocker as impl,
    )
    return impl(*args, **kwargs)


def build_workflow_summary(*args: Any, **kwargs: Any) -> Any:
    from products.compliance.backend.src.services.workflow_gate_service import (
        build_workflow_summary as impl,
    )
    return impl(*args, **kwargs)


def build_property_document_stack(*args: Any, **kwargs: Any) -> Any:
    from products.compliance.backend.src.services.compliance_document_service import (
        build_property_document_stack as impl,
    )
    return impl(*args, **kwargs)


def analyze_property_photos_for_compliance(*args: Any, **kwargs: Any) -> Any:
    from products.compliance.backend.src.services.compliance_photo_analysis_service import (
        analyze_property_photos_for_compliance as impl,
    )
    return impl(*args, **kwargs)


def create_compliance_tasks_from_photo_analysis(*args: Any, **kwargs: Any) -> Any:
    from products.compliance.backend.src.services.compliance_photo_analysis_service import (
        create_compliance_tasks_from_photo_analysis as impl,
    )
    return impl(*args, **kwargs)


def evaluate_trust(*args: Any, **kwargs: Any) -> Any:
    from onehaven_platform.backend.src.adapters.compliance_adapter import evaluate_trust as impl
    return impl(*args, **kwargs)

def __getattr__(name: str) -> Any:
    import importlib

    # Keep narrow modules first. The broad services package imports workflow_gate_service,
    # which can re-enter state_machine_service during import and create a partial-init cycle.
    modules = [
        "products.compliance.backend.src.domain.inspection.inspection_mapping",
        "products.compliance.backend.src.domain.inspection.top_fail_points",
        "products.compliance.backend.src.domain.inspection.hqs",
        "products.compliance.backend.src.domain.inspection.inspection_rules",
        "products.compliance.backend.src.domain.inspection.hqs_library",
        "products.compliance.backend.src.services.inspection_scheduling_service",
        "products.compliance.backend.src.services.policy_governance.refresh_service",
        "products.compliance.backend.src.services.policy_governance.notification_service",
        "products.compliance.backend.src.services.policy_coverage.health_service",
        "products.compliance.backend.src.services.compliance_document_service",
        "products.compliance.backend.src.services.compliance_photo_analysis_service",
        "products.compliance.backend.src.services.compliance_service",
        "products.compliance.backend.src.services.workflow_gate_service",
        "products.compliance.backend.src.services.trust_service",
        "products.compliance.backend.src.services.jurisdiction_profile_service",
        "products.compliance.backend.src.services",
    ]

    last_error: Exception | None = None

    for module_name in modules:
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            last_error = exc
            continue

        if name in getattr(module, "__dict__", {}):
            return module.__dict__[name]

    if last_error is not None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}; last import error: {last_error!r}")

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
