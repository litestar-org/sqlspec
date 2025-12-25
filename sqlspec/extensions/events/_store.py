"""Base classes for adapter-specific event queue stores."""

import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from sqlspec.exceptions import EventChannelError

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
            raise EventChannelError(msg)
    return name


def normalize_event_channel_name(name: str) -> str:
    """Validate event channel identifiers and return normalized name."""
    if not _IDENTIFIER_PATTERN.match(name):
        msg = f"Invalid events channel name: {name}"
        raise EventChannelError(msg)
    return name


class BaseEventQueueStore(ABC, Generic[ConfigT]):
    """Base class for adapter-specific event queue DDL generators.

    This class provides a hook-based pattern for DDL generation. Adapters only
    need to override `_column_types()` and optionally any hook methods for
    dialect-specific variations:

    - `_string_type(length)`: String type syntax (default: VARCHAR(N))
    - `_integer_type()`: Integer type syntax (default: INTEGER)
    - `_timestamp_default()`: Timestamp default expression (default: CURRENT_TIMESTAMP)
    - `_primary_key_syntax()`: Inline PRIMARY KEY clause (default: empty, PK on column)
    - `_table_clause()`: Additional table options (default: empty)

    For complex dialects (Oracle PL/SQL, BigQuery CLUSTER BY), adapters may
    override `_build_create_table_sql()` directly.
    """

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

    def _string_type(self, length: int) -> str:
        """Return string type syntax for the given length.

        Override for dialects with different string type syntax.

        Args:
            length: Maximum string length.

        Returns:
            String type declaration (e.g., VARCHAR(64), STRING(64)).
        """
        return f"VARCHAR({length})"

    def _integer_type(self) -> str:
        """Return integer type syntax.

        Override for dialects with different integer type syntax.

        Returns:
            Integer type declaration (e.g., INTEGER, INT64).
        """
        return "INTEGER"

    def _timestamp_default(self) -> str:
        """Return timestamp default expression.

        Override for dialects requiring different default syntax.

        Returns:
            Default timestamp expression (e.g., CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(6)).
        """
        return "CURRENT_TIMESTAMP"

    def _primary_key_syntax(self) -> str:
        """Return inline PRIMARY KEY clause for table definition.

        Override for dialects that require PRIMARY KEY at the end of CREATE TABLE
        instead of on the column definition (e.g., Spanner).

        Returns:
            Empty string for column-level PK, or " PRIMARY KEY (event_id)" for table-level.
        """
        return ""

    def _build_create_table_sql(self) -> str:
        """Build CREATE TABLE SQL using hook methods.

        Most adapters should NOT override this method. Instead, override the
        hook methods (_string_type, _integer_type, _timestamp_default, etc.)
        for dialect-specific variations.

        Only override this method for complex dialects that require entirely
        different DDL structure (e.g., Oracle PL/SQL blocks, BigQuery CLUSTER BY).
        """
        payload_type, metadata_type, timestamp_type = self._column_types()
        string_64 = self._string_type(64)
        string_128 = self._string_type(128)
        string_32 = self._string_type(32)
        integer_type = self._integer_type()
        ts_default = self._timestamp_default()
        pk_inline = self._primary_key_syntax()
        table_clause = self._table_clause()

        pk_column = " PRIMARY KEY" if not pk_inline else ""

        return (
            f"CREATE TABLE {self.table_name} ("
            f"event_id {string_64}{pk_column},"
            f" channel {string_128} NOT NULL,"
            f" payload_json {payload_type} NOT NULL,"
            f" metadata_json {metadata_type},"
            f" status {string_32} NOT NULL DEFAULT 'pending',"
            f" available_at {timestamp_type} NOT NULL DEFAULT {ts_default},"
            f" lease_expires_at {timestamp_type},"
            f" attempts {integer_type} NOT NULL DEFAULT 0,"
            f" created_at {timestamp_type} NOT NULL DEFAULT {ts_default},"
            f" acknowledged_at {timestamp_type}"
            f"){pk_inline}{table_clause}"
        )

    def _build_index_sql(self) -> str | None:
        """Build CREATE INDEX SQL for queue operations."""
        index_name = self._index_name()
        return f"CREATE INDEX {index_name} ON {self.table_name}(channel, status, available_at)"

    def _table_clause(self) -> str:
        """Return additional table options clause.

        Override for dialects that need options after the column definitions
        (e.g., BigQuery CLUSTER BY, Oracle INMEMORY).
        """
        return ""

    def _index_name(self) -> str:
        """Return the index name for the queue table."""
        return f"idx_{self.table_name.replace('.', '_')}_channel_status"

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        """Wrap CREATE statement with IF NOT EXISTS.

        Override for dialects that don't support IF NOT EXISTS (e.g., Spanner).
        """
        if object_type == "table":
            return statement.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
        if object_type == "index":
            return statement.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS", 1)
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        """Wrap DROP statement with IF EXISTS.

        Override for dialects that don't support IF EXISTS (e.g., Spanner).
        """
        return statement.replace("DROP TABLE", "DROP TABLE IF EXISTS", 1)

    @abstractmethod
    def _column_types(self) -> "tuple[str, str, str]":
        """Return payload, metadata, and timestamp column types for the adapter.

        Args:
            None

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
