"""Statement observer primitives for SQL execution events."""

import logging
from collections.abc import Callable
from time import time
from typing import Any

from sqlspec.utils.logging import get_logger

__all__ = ("StatementEvent", "create_event", "default_statement_observer", "format_statement_event")


logger = get_logger("sqlspec.observability")

_LOG_SQL_MAX_CHARS = 2000
_LOG_PARAMETERS_MAX_ITEMS = 100


StatementObserver = Callable[["StatementEvent"], None]


class StatementEvent:
    """Structured payload describing a SQL execution."""

    __slots__ = (
        "adapter",
        "bind_key",
        "correlation_id",
        "driver",
        "duration_s",
        "execution_mode",
        "is_many",
        "is_script",
        "operation",
        "parameters",
        "rows_affected",
        "sql",
        "started_at",
        "storage_backend",
    )

    def __init__(
        self,
        *,
        sql: str,
        parameters: Any,
        driver: str,
        adapter: str,
        bind_key: "str | None",
        operation: str,
        execution_mode: "str | None",
        is_many: bool,
        is_script: bool,
        rows_affected: "int | None",
        duration_s: float,
        started_at: float,
        correlation_id: "str | None",
        storage_backend: "str | None",
    ) -> None:
        self.sql = sql
        self.parameters = parameters
        self.driver = driver
        self.adapter = adapter
        self.bind_key = bind_key
        self.operation = operation
        self.execution_mode = execution_mode
        self.is_many = is_many
        self.is_script = is_script
        self.rows_affected = rows_affected
        self.duration_s = duration_s
        self.started_at = started_at
        self.correlation_id = correlation_id
        self.storage_backend = storage_backend

    def __hash__(self) -> int:  # pragma: no cover - explicit to mirror dataclass behavior
        msg = "StatementEvent objects are mutable and unhashable"
        raise TypeError(msg)

    def as_dict(self) -> "dict[str, Any]":
        """Return event payload as a dictionary."""

        return {
            "sql": self.sql,
            "parameters": self.parameters,
            "driver": self.driver,
            "adapter": self.adapter,
            "bind_key": self.bind_key,
            "operation": self.operation,
            "execution_mode": self.execution_mode,
            "is_many": self.is_many,
            "is_script": self.is_script,
            "rows_affected": self.rows_affected,
            "duration_s": self.duration_s,
            "started_at": self.started_at,
            "correlation_id": self.correlation_id,
            "storage_backend": self.storage_backend,
        }

    def __repr__(self) -> str:
        return (
            f"StatementEvent(sql={self.sql!r}, parameters={self.parameters!r}, driver={self.driver!r}, adapter={self.adapter!r}, bind_key={self.bind_key!r}, "
            f"operation={self.operation!r}, execution_mode={self.execution_mode!r}, is_many={self.is_many!r}, is_script={self.is_script!r}, rows_affected={self.rows_affected!r}, "
            f"duration_s={self.duration_s!r}, started_at={self.started_at!r}, correlation_id={self.correlation_id!r}, storage_backend={self.storage_backend!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StatementEvent):
            return NotImplemented
        return (
            self.sql == other.sql
            and self.parameters == other.parameters
            and self.driver == other.driver
            and self.adapter == other.adapter
            and self.bind_key == other.bind_key
            and self.operation == other.operation
            and self.execution_mode == other.execution_mode
            and self.is_many == other.is_many
            and self.is_script == other.is_script
            and self.rows_affected == other.rows_affected
            and self.duration_s == other.duration_s
            and self.started_at == other.started_at
            and self.correlation_id == other.correlation_id
            and self.storage_backend == other.storage_backend
        )


def format_statement_event(event: StatementEvent) -> str:
    """Create a concise human-readable representation of a statement event."""

    classification = []
    if event.is_script:
        classification.append("script")
    if event.is_many:
        classification.append("many")
    mode_label = ",".join(classification) if classification else "single"
    rows_label = "rows=%s" % (event.rows_affected if event.rows_affected is not None else "unknown")
    duration_label = f"{event.duration_s:.6f}s"
    return (
        f"[{event.driver}] {event.operation} ({mode_label}, {rows_label}, duration={duration_label})\n"
        f"SQL: {event.sql}\nParameters: {event.parameters}"
    )


def default_statement_observer(event: StatementEvent) -> None:
    """Log statement execution payload when no custom observer is supplied."""

    sql_preview, sql_truncated, sql_length = _truncate_text(event.sql, max_chars=_LOG_SQL_MAX_CHARS)
    sql_preview = sql_preview.replace("\n", " ").strip()

    extra: dict[str, object | None] = {
        "driver": event.driver,
        "adapter": event.adapter,
        "bind_key": event.bind_key,
        "operation": event.operation,
        "execution_mode": event.execution_mode,
        "is_many": event.is_many,
        "is_script": event.is_script,
        "rows_affected": event.rows_affected,
        "duration_s": event.duration_s,
        "started_at": event.started_at,
        "correlation_id": event.correlation_id,
        "storage_backend": event.storage_backend,
        "sql": sql_preview,
        "sql_length": sql_length,
        "sql_truncated": sql_truncated,
    }

    params_summary = _summarize_parameters(event.parameters)
    if params_summary:
        extra.update(params_summary)

    if logger.isEnabledFor(logging.DEBUG):
        params, params_truncated = _maybe_truncate_parameters(event.parameters, max_items=_LOG_PARAMETERS_MAX_ITEMS)
        if params_truncated:
            extra["parameters_truncated"] = True
        extra["parameters"] = params

    rows_label = event.rows_affected if event.rows_affected is not None else "unknown"
    logger.info(
        "[%s] %s duration=%.3fms rows=%s sql=%s",
        event.driver,
        event.operation,
        event.duration_s * 1000,
        rows_label,
        sql_preview,
        extra=extra,
    )


def _truncate_text(value: str, *, max_chars: int) -> tuple[str, bool, int]:
    length = len(value)
    if length <= max_chars:
        return value, False, length
    return value[:max_chars], True, length


def _summarize_parameters(parameters: Any) -> "dict[str, str | int | None]":
    if parameters is None:
        return {"parameters_type": None, "parameters_size": None}
    if isinstance(parameters, dict):
        return {"parameters_type": "dict", "parameters_size": len(parameters)}
    if isinstance(parameters, list):
        return {"parameters_type": "list", "parameters_size": len(parameters)}
    if isinstance(parameters, tuple):
        return {"parameters_type": "tuple", "parameters_size": len(parameters)}
    return {"parameters_type": type(parameters).__name__, "parameters_size": None}


def _maybe_truncate_parameters(parameters: Any, *, max_items: int) -> tuple[Any, bool]:
    if isinstance(parameters, dict):
        if len(parameters) <= max_items:
            return parameters, False
        truncated = dict(list(parameters.items())[:max_items])
        return truncated, True
    if isinstance(parameters, list):
        if len(parameters) <= max_items:
            return parameters, False
        return parameters[:max_items], True
    if isinstance(parameters, tuple):
        if len(parameters) <= max_items:
            return parameters, False
        return parameters[:max_items], True
    return parameters, False


def create_event(
    *,
    sql: str,
    parameters: Any,
    driver: str,
    adapter: str,
    bind_key: "str | None",
    operation: str,
    execution_mode: "str | None",
    is_many: bool,
    is_script: bool,
    rows_affected: "int | None",
    duration_s: float,
    correlation_id: "str | None",
    storage_backend: "str | None" = None,
    started_at: float | None = None,
) -> StatementEvent:
    """Factory helper used by runtime to build statement events."""

    return StatementEvent(
        sql=sql,
        parameters=parameters,
        driver=driver,
        adapter=adapter,
        bind_key=bind_key,
        operation=operation,
        execution_mode=execution_mode,
        is_many=is_many,
        is_script=is_script,
        rows_affected=rows_affected,
        duration_s=duration_s,
        started_at=started_at if started_at is not None else time(),
        correlation_id=correlation_id,
        storage_backend=storage_backend,
    )
