"""Diagnostics aggregation utilities for observability exports."""

from collections.abc import Iterable
from typing import Any

from sqlspec.storage.pipeline import get_recent_storage_events, get_storage_bridge_diagnostics


class TelemetryDiagnostics:
    """Aggregates lifecycle counters with storage bridge metrics."""

    __slots__ = ("_lifecycle_sections",)

    def __init__(self) -> None:
        self._lifecycle_sections: list[tuple[str, dict[str, int]]] = []

    def add_lifecycle_snapshot(self, config_key: str, counters: dict[str, int]) -> None:
        """Store lifecycle counters for later snapshot generation."""

        if not counters:
            return
        self._lifecycle_sections.append((config_key, counters))

    def snapshot(self) -> "dict[str, Any]":
        """Return aggregated diagnostics payload."""

        payload: dict[str, Any] = get_storage_bridge_diagnostics()
        recent_jobs = get_recent_storage_events()
        if recent_jobs:
            payload["storage_bridge.recent_jobs"] = recent_jobs
        for _prefix, counters in self._lifecycle_sections:
            for metric, value in counters.items():
                payload[metric] = payload.get(metric, 0) + value
        return payload


def collect_diagnostics(sections: Iterable[tuple[str, dict[str, int]]]) -> dict[str, Any]:
    """Convenience helper for aggregating sections without constructing a class."""

    diag = TelemetryDiagnostics()
    for prefix, counters in sections:
        diag.add_lifecycle_snapshot(prefix, counters)
    return diag.snapshot()


__all__ = ("TelemetryDiagnostics", "collect_diagnostics")
