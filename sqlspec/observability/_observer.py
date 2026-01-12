"""Statement observer primitives for SQL execution events."""

import logging
from collections.abc import Callable
from time import time
from typing import Any

from sqlspec.observability._config import LoggingConfig
from sqlspec.utils.logging import get_logger

__all__ = (
    "StatementEvent",
    "create_event",
    "create_statement_observer",
    "default_statement_observer",
    "format_statement_event",
)


logger = get_logger("sqlspec.observability")

_DEFAULT_LOGGING_CONFIG = LoggingConfig()


StatementObserver = Callable[["StatementEvent"], None]


class StatementEvent:
    """Structured payload describing a SQL execution."""

    __slots__ = (
        "adapter",
        "bind_key",
        "correlation_id",
        "db_system",
        "driver",
        "duration_s",
        "execution_mode",
        "is_many",
        "is_script",
        "operation",
        "parameters",
        "prepared_statement",
        "rows_affected",
        "sampled",
        "span_id",
        "sql",
        "sql_hash",
        "sql_original_length",
        "sql_truncated",
        "started_at",
        "storage_backend",
        "trace_id",
        "transaction_state",
    )

    def __init__(
        self,
        *,
        sql: str,
        parameters: Any,
        driver: str,
        adapter: str,
        bind_key: "str | None",
        db_system: "str | None",
        operation: str,
        execution_mode: "str | None",
        is_many: bool,
        is_script: bool,
        rows_affected: "int | None",
        duration_s: float,
        started_at: float,
        correlation_id: "str | None",
        storage_backend: "str | None",
        sql_hash: "str | None",
        sql_truncated: bool,
        sql_original_length: "int | None",
        transaction_state: "str | None",
        prepared_statement: "bool | None",
        trace_id: "str | None",
        span_id: "str | None",
        sampled: bool = True,
    ) -> None:
        self.sql = sql
        self.parameters = parameters
        self.driver = driver
        self.adapter = adapter
        self.bind_key = bind_key
        self.db_system = db_system
        self.operation = operation
        self.execution_mode = execution_mode
        self.is_many = is_many
        self.is_script = is_script
        self.rows_affected = rows_affected
        self.duration_s = duration_s
        self.started_at = started_at
        self.correlation_id = correlation_id
        self.storage_backend = storage_backend
        self.sql_hash = sql_hash
        self.sql_truncated = sql_truncated
        self.sql_original_length = sql_original_length
        self.transaction_state = transaction_state
        self.prepared_statement = prepared_statement
        self.trace_id = trace_id
        self.span_id = span_id
        self.sampled = sampled

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
            "db_system": self.db_system,
            "operation": self.operation,
            "execution_mode": self.execution_mode,
            "is_many": self.is_many,
            "is_script": self.is_script,
            "rows_affected": self.rows_affected,
            "duration_s": self.duration_s,
            "started_at": self.started_at,
            "correlation_id": self.correlation_id,
            "storage_backend": self.storage_backend,
            "sql_hash": self.sql_hash,
            "sql_truncated": self.sql_truncated,
            "sql_original_length": self.sql_original_length,
            "transaction_state": self.transaction_state,
            "prepared_statement": self.prepared_statement,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "sampled": self.sampled,
        }

    def __repr__(self) -> str:
        return (
            f"StatementEvent(sql={self.sql!r}, parameters={self.parameters!r}, driver={self.driver!r}, adapter={self.adapter!r}, bind_key={self.bind_key!r}, db_system={self.db_system!r}, "
            f"operation={self.operation!r}, execution_mode={self.execution_mode!r}, is_many={self.is_many!r}, is_script={self.is_script!r}, rows_affected={self.rows_affected!r}, duration_s={self.duration_s!r}, "
            f"started_at={self.started_at!r}, correlation_id={self.correlation_id!r}, storage_backend={self.storage_backend!r}, sql_hash={self.sql_hash!r}, sql_truncated={self.sql_truncated!r}, "
            f"sql_original_length={self.sql_original_length!r}, transaction_state={self.transaction_state!r}, prepared_statement={self.prepared_statement!r}, trace_id={self.trace_id!r}, span_id={self.span_id!r}, sampled={self.sampled!r})"
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
            and self.db_system == other.db_system
            and self.operation == other.operation
            and self.execution_mode == other.execution_mode
            and self.is_many == other.is_many
            and self.is_script == other.is_script
            and self.rows_affected == other.rows_affected
            and self.duration_s == other.duration_s
            and self.started_at == other.started_at
            and self.correlation_id == other.correlation_id
            and self.storage_backend == other.storage_backend
            and self.sql_hash == other.sql_hash
            and self.sql_truncated == other.sql_truncated
            and self.sql_original_length == other.sql_original_length
            and self.transaction_state == other.transaction_state
            and self.prepared_statement == other.prepared_statement
            and self.trace_id == other.trace_id
            and self.span_id == other.span_id
            and self.sampled == other.sampled
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


def create_statement_observer(logging_config: "LoggingConfig") -> StatementObserver:
    """Create a statement observer bound to the provided logging config."""

    def observer(event: StatementEvent) -> None:
        _emit_otel_statement_log(event, logging_config)

    return observer


def default_statement_observer(event: StatementEvent) -> None:
    """Log statement execution payload when no custom observer is supplied."""

    _emit_otel_statement_log(event, _DEFAULT_LOGGING_CONFIG)


def _emit_otel_statement_log(event: StatementEvent, logging_config: "LoggingConfig") -> None:
    sql_preview, sql_truncated, sql_length = _truncate_text(event.sql, max_chars=logging_config.sql_truncation_length)
    if event.sql_original_length is not None:
        sql_length = event.sql_original_length
        sql_truncated = event.sql_truncated
    sql_preview = sql_preview.replace("\n", " ").strip()

    extra: dict[str, object | None] = {
        "db.system": event.db_system,
        "db.operation": event.operation,
        "sqlspec.driver": event.driver,
        "sqlspec.bind_key": event.bind_key,
        "sqlspec.transaction_state": event.transaction_state,
        "sqlspec.prepared_statement": event.prepared_statement,
        "execution_mode": event.execution_mode,
        "is_many": event.is_many,
        "is_script": event.is_script,
        "duration_ms": event.duration_s * 1000,
        "rows_affected": event.rows_affected,
        "started_at": event.started_at,
        "storage_backend": event.storage_backend,
        "db.statement": sql_preview,
        "db.statement.truncated": sql_truncated,
        "db.statement.length": sql_length,
        "db.statement.preview_length": len(sql_preview),
        "sql_truncated": sql_truncated,
        "sql_length": sql_length,
        "sql_preview_length": len(sql_preview),
    }

    if event.trace_id:
        extra["trace_id"] = event.trace_id
    if event.span_id:
        extra["span_id"] = event.span_id
    if event.correlation_id:
        extra["correlation_id"] = event.correlation_id
    if event.sql_hash:
        extra["db.statement.hash"] = event.sql_hash
        extra["sql_hash"] = event.sql_hash

    params_summary = _summarize_parameters(event.parameters)
    if params_summary:
        extra.update(params_summary)

    if event.is_many and isinstance(event.parameters, (list, tuple)):
        extra["batch_size"] = len(event.parameters)

    if logger.isEnabledFor(logging.DEBUG):
        params, params_truncated = _maybe_truncate_parameters(
            event.parameters, max_items=logging_config.parameter_truncation_count
        )
        if params_truncated:
            extra["parameters_truncated"] = True
        extra["parameters"] = params

    logger.info("db.query", extra=extra)


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
    db_system: "str | None" = None,
    sql_hash: "str | None" = None,
    sql_truncated: bool = False,
    sql_original_length: "int | None" = None,
    transaction_state: "str | None" = None,
    prepared_statement: "bool | None" = None,
    trace_id: "str | None" = None,
    span_id: "str | None" = None,
    sampled: bool = True,
) -> StatementEvent:
    """Factory helper used by runtime to build statement events."""

    return StatementEvent(
        sql=sql,
        parameters=parameters,
        driver=driver,
        adapter=adapter,
        bind_key=bind_key,
        db_system=db_system,
        operation=operation,
        execution_mode=execution_mode,
        is_many=is_many,
        is_script=is_script,
        rows_affected=rows_affected,
        duration_s=duration_s,
        started_at=started_at if started_at is not None else time(),
        correlation_id=correlation_id,
        storage_backend=storage_backend,
        sql_hash=sql_hash,
        sql_truncated=sql_truncated,
        sql_original_length=sql_original_length,
        transaction_state=transaction_state,
        prepared_statement=prepared_statement,
        trace_id=trace_id,
        span_id=span_id,
        sampled=sampled,
    )
