from __future__ import annotations

from typing import Any, Protocol


class WorkflowContract(Protocol):
    def assert_stage_transition_allowed(self, *args: Any, **kwargs: Any) -> None: ...
    def build_stage_guard_payload(self, *args: Any, **kwargs: Any) -> dict[str, Any]: ...
