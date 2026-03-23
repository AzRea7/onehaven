from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator


def _normalize_label_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _metric_key(name: str, labels: dict[str, Any] | None = None) -> str:
    if not labels:
        return name
    parts = [f"{k}={_normalize_label_value(v)}" for k, v in sorted(labels.items(), key=lambda x: x[0])]
    return f"{name}|{'|'.join(parts)}"


@dataclass
class _TimerStat:
    count: int = 0
    total_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0

    def add(self, duration_ms: float) -> None:
        value = float(duration_ms)
        if self.count == 0:
            self.count = 1
            self.total_ms = value
            self.min_ms = value
            self.max_ms = value
            return

        self.count += 1
        self.total_ms += value
        if value < self.min_ms:
            self.min_ms = value
        if value > self.max_ms:
            self.max_ms = value

    def snapshot(self) -> dict[str, float | int]:
        avg_ms = round(self.total_ms / self.count, 2) if self.count > 0 else 0.0
        return {
            "count": int(self.count),
            "total_ms": round(float(self.total_ms), 2),
            "avg_ms": avg_ms,
            "min_ms": round(float(self.min_ms), 2) if self.count > 0 else 0.0,
            "max_ms": round(float(self.max_ms), 2) if self.count > 0 else 0.0,
        }


class _Metrics:
    """
    In-memory runtime metrics collector.

    Supports:
    - counters: increment-only integer metrics
    - gauges: latest numeric value
    - timings: aggregated duration stats (count/total/avg/min/max)
    - labeled metrics via metric name expansion
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}
        self._timers: dict[str, _TimerStat] = {}

    def inc(self, name: str, n: int = 1, *, labels: dict[str, Any] | None = None) -> None:
        key = _metric_key(name, labels)
        with self._lock:
            self._counters[key] = int(self._counters.get(key, 0)) + int(n)

    def set_gauge(self, name: str, value: float | int, *, labels: dict[str, Any] | None = None) -> None:
        key = _metric_key(name, labels)
        with self._lock:
            self._gauges[key] = float(value)

    def observe_ms(self, name: str, duration_ms: float | int, *, labels: dict[str, Any] | None = None) -> None:
        key = _metric_key(name, labels)
        with self._lock:
            stat = self._timers.get(key)
            if stat is None:
                stat = _TimerStat()
                self._timers[key] = stat
            stat.add(float(duration_ms))

    def time_start(self) -> float:
        return time.perf_counter()

    def time_end(self, name: str, start: float, *, labels: dict[str, Any] | None = None) -> float:
        duration_ms = round((time.perf_counter() - float(start)) * 1000, 2)
        self.observe_ms(name, duration_ms, labels=labels)
        return duration_ms

    @contextmanager
    def timer(self, name: str, *, labels: dict[str, Any] | None = None) -> Iterator[dict[str, float]]:
        started = time.perf_counter()
        holder: dict[str, float] = {}
        try:
            yield holder
        finally:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            holder["duration_ms"] = duration_ms
            self.observe_ms(name, duration_ms, labels=labels)

    def get_counter(self, name: str, *, labels: dict[str, Any] | None = None) -> int:
        key = _metric_key(name, labels)
        with self._lock:
            return int(self._counters.get(key, 0))

    def get_gauge(self, name: str, *, labels: dict[str, Any] | None = None) -> float | None:
        key = _metric_key(name, labels)
        with self._lock:
            value = self._gauges.get(key)
            return None if value is None else float(value)

    def get_timer(self, name: str, *, labels: dict[str, Any] | None = None) -> dict[str, float | int] | None:
        key = _metric_key(name, labels)
        with self._lock:
            stat = self._timers.get(key)
            return None if stat is None else stat.snapshot()

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._timers.clear()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": {k: round(float(v), 2) for k, v in self._gauges.items()},
                "timers": {k: v.snapshot() for k, v in self._timers.items()},
            }


METRICS = _Metrics()
