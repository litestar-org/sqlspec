"""Base classes for adapter-specific event queue stores."""

import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

if TYPE_CHECKING:
    from sqlspec.config import DatabaseConfigProtocol

ConfigT = TypeVar("ConfigT", bound="DatabaseConfigProtocol[Any, Any, Any]")

__all__ = ("BaseEventQueueStore", "normalize_event_channel_name", "normalize_queue_table_name")

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def normalize_queue_table_name(name: str) -> str:
    """Validate schema-qualified identifiers and return normalized name."""

    segments = name.split(".")
    for segment in segments:
        if not _IDENTIFIER_PATTERN.match(segment):
            msg = f"Invalid events table name: {name}"
            raise ValueError(msg)
    return name


def normalize_event_channel_name(name: str) -> str:
    """Validate event channel identifiers and return normalized name."""

    if not _IDENTIFIER_PATTERN.match(name):
        msg = f"Invalid events channel name: {name}"
        raise ValueError(msg)
    return name


class BaseEventQueueStore(ABC, Generic[ConfigT]):
    """Base class for adapter-specific event queue DDL generators."""

    __slots__ = ("_config", "_extension_settings", "_table_name")

    def __init__(self, config: ConfigT) -> None:
        self._config = config
        extension_config = cast("dict[str, Any]", config.extension_config)
        self._extension_settings = cast("dict[str, Any]", extension_config.get("events", {}))
        table_name = self._extension_settings.get("queue_table", "sqlspec_event_queue")
        self._table_name = normalize_queue_table_name(str(table_name))

    @property
    def table_name(self) -> str:
        """Return the configured queue table name."""

        return self._table_name

    @property
    def settings(self) -> "dict[str, Any]":
        """Return extension settings for adapters to inspect."""

        return self._extension_settings

    def create_statements(self) -> "list[str]":
        """Return statements required to create the queue table and indexes."""

        statements = [self._wrap_create_statement(self._build_create_table_sql(), "table")]
        index_statement = self._build_index_sql()
        if index_statement:
            statements.append(self._wrap_create_statement(index_statement, "index"))
        return statements

    def drop_statements(self) -> "list[str]":
        """Return statements required to drop queue artifacts."""

        return [self._wrap_drop_statement(f"DROP TABLE {self.table_name}")]

    def _build_create_table_sql(self) -> str:
        payload_type, metadata_type, timestamp_type = self._column_types()
        table_clause = self._table_clause()
        return (
            f"CREATE TABLE {self.table_name} ("
            "event_id VARCHAR(64) PRIMARY KEY,"
            " channel VARCHAR(128) NOT NULL,"
            f" payload_json {payload_type} NOT NULL,"
            f" metadata_json {metadata_type},"
            " status VARCHAR(32) NOT NULL DEFAULT 'pending',"
            f" available_at {timestamp_type} NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            f" lease_expires_at {timestamp_type},"
            " attempts INTEGER NOT NULL DEFAULT 0,"
            f" created_at {timestamp_type} NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            f" acknowledged_at {timestamp_type}"
            f") {table_clause}"
        )

    def _build_index_sql(self) -> str | None:
        index_name = self._index_name()
        return f"CREATE INDEX {index_name} ON {self.table_name}(channel, status, available_at)"

    def _table_clause(self) -> str:
        return ""

    def _index_name(self) -> str:
        return f"idx_{self.table_name.replace('.', '_')}_channel_status"

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        if object_type == "table":
            return statement.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
        if object_type == "index":
            return statement.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS", 1)
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        return statement.replace("DROP TABLE", "DROP TABLE IF EXISTS", 1)

    @abstractmethod
    def _column_types(self) -> "tuple[str, str, str]":
        """Return payload, metadata, and timestamp column types for the adapter."""
