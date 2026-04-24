from __future__ import annotations

from typing import Any


def build_inventory_snapshots_for_scope(*args: Any, **kwargs: Any) -> Any:
    from products.ops.backend.src.services.properties.inventory_snapshot_service import build_inventory_snapshots_for_scope as impl
    return impl(*args, **kwargs)
