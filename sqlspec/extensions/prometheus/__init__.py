"""Prometheus metrics helpers that integrate with the observability statement observers."""

from collections.abc import Iterable
from typing import Any

from sqlspec.observability import ObservabilityConfig
from sqlspec.observability._observer import StatementEvent, StatementObserver
from sqlspec.typing import Counter, Histogram
from sqlspec.utils.module_loader import ensure_prometheus

__all__ = ("PrometheusStatementObserver", "enable_metrics")


class PrometheusStatementObserver:
    """Statement observer that records Prometheus metrics."""

    __slots__ = ("_counters", "_duration", "_label_names", "_rows")

    def __init__(
        self,
        *,
        namespace: str = "sqlspec",
        subsystem: str = "driver",
        registry: Any | None = None,
        label_names: Iterable[str] = ("driver", "operation"),
        duration_buckets: tuple[float, ...] | None = None,
    ) -> None:
        self._label_names = tuple(label_names)
        self._counters = Counter(
            "query_total",
            "Total SQL statements executed",
            labelnames=self._label_names,
            namespace=namespace,
            subsystem=subsystem,
            registry=registry,
        )
        histogram_kwargs: dict[str, Any] = {}
        if duration_buckets is not None:
            histogram_kwargs["buckets"] = duration_buckets

        self._duration = Histogram(
            "query_duration_seconds",
            "SQL execution time in seconds",
            labelnames=self._label_names,
            namespace=namespace,
            subsystem=subsystem,
            registry=registry,
            **histogram_kwargs,
        )
        self._rows = Histogram(
            "query_rows",
            "Rows affected per statement",
            labelnames=self._label_names,
            namespace=namespace,
            subsystem=subsystem,
            registry=registry,
        )

    def __call__(self, event: StatementEvent) -> None:
        label_values = self._label_values(event)
        self._counters.labels(*label_values).inc()
        self._duration.labels(*label_values).observe(max(event.duration_s, 0.0))
        if event.rows_affected is not None:
            self._rows.labels(*label_values).observe(float(event.rows_affected))

    def _label_values(self, event: StatementEvent) -> tuple[str, ...]:
        values: list[str] = []
        for name in self._label_names:
            if name == "driver":
                values.append(event.driver)
            elif name == "operation":
                values.append(event.operation or "EXECUTE")
            elif name == "adapter":
                values.append(event.adapter)
            elif name == "bind_key":
                values.append(event.bind_key or "default")
            else:
                values.append(getattr(event, name, ""))
        return tuple(values)


def enable_metrics(
    *,
    base_config: ObservabilityConfig | None = None,
    namespace: str = "sqlspec",
    subsystem: str = "driver",
    registry: Any | None = None,
    label_names: Iterable[str] = ("driver", "operation"),
    duration_buckets: tuple[float, ...] | None = None,
) -> ObservabilityConfig:
    """Attach a Prometheus-backed statement observer to the provided config."""

    ensure_prometheus()

    observer = PrometheusStatementObserver(
        namespace=namespace,
        subsystem=subsystem,
        registry=registry,
        label_names=label_names,
        duration_buckets=duration_buckets,
    )

    config = base_config.copy() if base_config else ObservabilityConfig()
    existing: list[StatementObserver] = list(config.statement_observers or ())
    existing.append(observer)
    config.statement_observers = tuple(existing)
    return config
