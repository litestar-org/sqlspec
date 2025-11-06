"""Statement observer primitives for SQL execution events."""

from collections.abc import Callable
from dataclasses import dataclass
from time import time
from typing import Any

from sqlspec.utils.logging import get_logger

__all__ = ("StatementEvent", "create_event", "default_statement_observer", "format_statement_event")


logger = get_logger("sqlspec.observability")


StatementObserver = Callable[["StatementEvent"], None]


@dataclass(slots=True)
class StatementEvent:
    """Structured payload describing a SQL execution."""

    sql: str
    parameters: Any
    driver: str
    adapter: str
    bind_key: "str | None"
    operation: str
    execution_mode: "str | None"
    is_many: bool
    is_script: bool
    rows_affected: "int | None"
    duration_s: float
    started_at: float
    correlation_id: "str | None"
    storage_backend: "str | None"

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
