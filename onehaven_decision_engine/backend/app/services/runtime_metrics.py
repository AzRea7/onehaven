# backend/app/services/runtime_metrics.py
from __future__ import annotations

import threading


class _Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}

    def inc(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._counters[name] = int(self._counters.get(name, 0)) + int(n)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counters)


METRICS = _Metrics()