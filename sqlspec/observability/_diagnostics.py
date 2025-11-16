"""Diagnostics aggregation utilities for observability exports."""

from collections.abc import Iterable
from typing import Any

from sqlspec.storage.pipeline import StorageDiagnostics, get_recent_storage_events, get_storage_bridge_diagnostics


class TelemetryDiagnostics:
    """Aggregates lifecycle counters, custom metrics, and storage telemetry."""

    __slots__ = ("_lifecycle_sections", "_metrics")

    def __init__(self) -> None:
        self._lifecycle_sections: list[tuple[str, dict[str, float]]] = []
        self._metrics: StorageDiagnostics = {}

    def add_lifecycle_snapshot(self, config_key: str, counters: dict[str, int]) -> None:
        """Store lifecycle counters for later snapshot generation."""

        if not counters:
            return
        float_counters = {metric: float(value) for metric, value in counters.items()}
        self._lifecycle_sections.append((config_key, float_counters))

    def add_metric_snapshot(self, metrics: StorageDiagnostics) -> None:
        """Store custom metric snapshots."""

        for key, value in metrics.items():
            if key in self._metrics:
                self._metrics[key] += value
            else:
                self._metrics[key] = value

    def snapshot(self) -> "dict[str, Any]":
        """Return aggregated diagnostics payload."""

        numeric_payload: dict[str, float] = {}

        def _increment(metric: str, amount: float) -> None:
            numeric_payload[metric] = numeric_payload.get(metric, 0.0) + amount

        for key, value in get_storage_bridge_diagnostics().items():
            _increment(key, float(value))
        for _prefix, counters in self._lifecycle_sections:
            for metric, value in counters.items():
                _increment(metric, value)
        for metric, value in self._metrics.items():
            _increment(metric, float(value))

        payload: dict[str, Any] = dict(numeric_payload)
        recent_jobs = get_recent_storage_events()
        if recent_jobs:
            payload["storage_bridge.recent_jobs"] = recent_jobs
        return payload


def collect_diagnostics(sections: Iterable[tuple[str, dict[str, int]]]) -> dict[str, Any]:
    """Convenience helper for aggregating sections without constructing a class."""

    diag = TelemetryDiagnostics()
    for prefix, counters in sections:
        diag.add_lifecycle_snapshot(prefix, counters)
    return diag.snapshot()


__all__ = ("TelemetryDiagnostics", "collect_diagnostics")
