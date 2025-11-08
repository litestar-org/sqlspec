"""Statement observer primitives for SQL execution events."""

from collections.abc import Callable
from time import time
from typing import Any

from sqlspec.utils.logging import get_logger

__all__ = ("StatementEvent", "create_event", "default_statement_observer", "format_statement_event")


logger = get_logger("sqlspec.observability")


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

    logger.info(format_statement_event(event), extra={"correlation_id": event.correlation_id})


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
