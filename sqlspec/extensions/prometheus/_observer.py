"""Prometheus statement observer implementation."""

from collections.abc import Iterable
from typing import Any

from sqlspec.observability._common import resolve_db_system
from sqlspec.observability._config import ObservabilityConfig, StatementObserver
from sqlspec.observability._observer import StatementEvent
from sqlspec.typing import Counter, Histogram
from sqlspec.utils.module_loader import ensure_prometheus

__all__ = ("PrometheusStatementObserver", "enable_metrics")


_ALLOWED_LABEL_NAMES = frozenset({
    "adapter",
    "bind_key",
    "db_system",
    "driver",
    "execution_mode",
    "is_many",
    "is_script",
    "operation",
    "prepared_statement",
    "storage_backend",
    "transaction_state",
})


class PrometheusStatementObserver:
    """Statement observer that records Prometheus metrics."""

    __slots__ = ("_counters", "_duration", "_label_names", "_rows")

    def __init__(
        self,
        *,
        namespace: str = "sqlspec",
        subsystem: str = "driver",
        registry: Any | None = None,
        label_names: Iterable[str] = ("db_system", "operation"),
        duration_buckets: tuple[float, ...] | None = None,
    ) -> None:
        resolved_labels = tuple(label_names)
        disallowed = [name for name in resolved_labels if name not in _ALLOWED_LABEL_NAMES]
        if disallowed:
            disallowed_names = ", ".join(sorted(set(disallowed)))
            msg = f"High-cardinality Prometheus label names are not allowed: {disallowed_names}"
            raise ValueError(msg)
        self._label_names = resolved_labels
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
        payload: dict[str, Any] | None = None
        for name in self._label_names:
            match name:
                case "db_system":
                    db_system = event.db_system
                    if db_system is None:
                        db_system = resolve_db_system(event.adapter)
                    values.append(db_system)
                case "driver":
                    values.append(event.driver)
                case "operation":
                    values.append(event.operation or "EXECUTE")
                case "adapter":
                    values.append(event.adapter)
                case "bind_key":
                    values.append(event.bind_key or "default")
                case _:
                    if payload is None:
                        payload = event.as_dict()
                    value = payload.get(name)
                    values.append("" if value is None else str(value))
        return tuple(values)


def enable_metrics(
    *,
    base_config: ObservabilityConfig | None = None,
    namespace: str = "sqlspec",
    subsystem: str = "driver",
    registry: Any | None = None,
    label_names: Iterable[str] = ("db_system", "operation"),
    duration_buckets: tuple[float, ...] | None = None,
) -> ObservabilityConfig:
    """Attach a Prometheus-backed statement observer to the provided config."""

    ensure_prometheus()

    observer: StatementObserver = PrometheusStatementObserver(
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
