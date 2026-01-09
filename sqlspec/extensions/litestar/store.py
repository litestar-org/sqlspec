"""Base session store classes for Litestar integration."""

import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final, Generic, TypeVar, cast

from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import has_extension_config

if TYPE_CHECKING:
    from types import TracebackType


ConfigT = TypeVar("ConfigT")


logger = get_logger("sqlspec.extensions.litestar.store")

__all__ = ("BaseSQLSpecStore",)

VALID_TABLE_NAME_PATTERN: Final = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
MAX_TABLE_NAME_LENGTH: Final = 63


class BaseSQLSpecStore(ABC, Generic[ConfigT]):
    """Base class for SQLSpec-backed Litestar session stores.

    Implements the litestar.stores.base.Store protocol for server-side session
    storage using SQLSpec database adapters.

    This abstract base class provides common functionality for all database-specific
    store implementations including:
    - Connection management via SQLSpec configs
    - Session expiration calculation
    - Table creation utilities

    Subclasses must implement dialect-specific SQL queries.

    Args:
        config: SQLSpec database configuration with extension_config["litestar"] settings.

    Example:
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.adapters.asyncpg.litestar.store import AsyncpgStore

        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://..."},
            extension_config={"litestar": {"session_table": "my_sessions"}}
        )
        store = AsyncpgStore(config)
        await store.create_table()

    Notes:
        Configuration is read from config.extension_config["litestar"]:
        - session_table: Table name (default: "litestar_session")
    """

    __slots__ = ("_config", "_table_name")

    def __init__(self, config: ConfigT) -> None:
        """Initialize the session store.

        Args:
            config: SQLSpec database configuration.

        Notes:
            Reads table_name from config.extension_config["litestar"]["session_table"].
            Defaults to "litestar_session" if not specified.
        """
        self._config = config
        self._table_name = self._get_table_name_from_config()
        self._validate_table_name(self._table_name)

    def _get_table_name_from_config(self) -> str:
        """Extract table name from config.extension_config.

        Returns:
            Table name for the session store.

        Notes:
            Accepts ``session_table: True`` for default name or a string for custom name.
        """
        default_name = "litestar_session"
        if has_extension_config(self._config):
            extension_config = cast("dict[str, dict[str, Any]]", self._config.extension_config)
            litestar_config: dict[str, Any] = extension_config.get("litestar", {})
            session_table = litestar_config.get("session_table", default_name)
            if session_table is True:
                return default_name
            return str(session_table)
        return default_name

    @property
    def config(self) -> ConfigT:
        """Return the database configuration."""
        return self._config

    @property
    def table_name(self) -> str:
        """Return the session table name."""
        return self._table_name

    @abstractmethod
    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given and the value had an initial expiry time set, renew the
                expiry time for ``renew_for`` seconds. If the value has not been set
                with an expiry time this is a no-op.

        Returns:
            Session data as bytes if found and not expired, None otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data (will be converted to bytes if string).
            expires_in: Time in seconds or timedelta before expiration.
        """
        raise NotImplementedError

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        raise NotImplementedError

    @abstractmethod
    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        raise NotImplementedError

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.
        """
        raise NotImplementedError

    @abstractmethod
    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.
        """
        raise NotImplementedError

    @abstractmethod
    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        raise NotImplementedError

    @abstractmethod
    def _get_create_table_sql(self) -> str:
        """Get the CREATE TABLE SQL for this database dialect.

        Returns:
            SQL statement to create the sessions table.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_drop_table_sql(self) -> "list[str]":
        """Get the DROP TABLE SQL statements for this database dialect.

        Returns:
            List of SQL statements to drop the table and all indexes.
            Order matters: drop indexes before table.

        Notes:
            Should use IF EXISTS or dialect-specific error handling
            to allow idempotent migrations.
        """
        raise NotImplementedError

    async def __aenter__(self) -> "BaseSQLSpecStore":
        """Enter context manager."""
        return self

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        """Exit context manager."""
        return

    def _log_table_created(self) -> None:
        logger.debug(
            "Litestar session table ready",
            extra={"db.system": resolve_db_system(type(self).__name__), "session_table": self._table_name},
        )

    def _log_delete_all(self) -> None:
        logger.debug(
            "Litestar sessions cleared",
            extra={"db.system": resolve_db_system(type(self).__name__), "session_table": self._table_name},
        )

    def _log_delete_expired(self, count: int) -> None:
        logger.debug(
            "Litestar sessions expired cleanup",
            extra={
                "db.system": resolve_db_system(type(self).__name__),
                "session_table": self._table_name,
                "deleted_sessions": count,
            },
        )

    def _calculate_expires_at(self, expires_in: "int | timedelta | None") -> "datetime | None":
        """Calculate expiration timestamp from expires_in.

        Args:
            expires_in: Seconds or timedelta until expiration.

        Returns:
            UTC datetime of expiration, or None if no expiration.
        """
        if expires_in is None:
            return None

        expires_in_seconds = int(expires_in.total_seconds()) if isinstance(expires_in, timedelta) else expires_in

        if expires_in_seconds <= 0:
            return None

        return datetime.now(timezone.utc) + timedelta(seconds=expires_in_seconds)

    def _value_to_bytes(self, value: "str | bytes") -> bytes:
        """Convert value to bytes if needed.

        Args:
            value: String or bytes value.

        Returns:
            Value as bytes.
        """
        if isinstance(value, str):
            return value.encode("utf-8")
        return value

    @staticmethod
    def _validate_table_name(table_name: str) -> None:
        """Validate table name for SQL safety.

        Args:
            table_name: Table name to validate.

        Raises:
            ValueError: If table name is invalid.

        Notes:
            - Must start with letter or underscore
            - Can only contain letters, numbers, and underscores
            - Maximum length is 63 characters (PostgreSQL limit)
            - Prevents SQL injection in table names
        """
        if not table_name:
            msg = "Table name cannot be empty"
            raise ValueError(msg)

        if len(table_name) > MAX_TABLE_NAME_LENGTH:
            msg = f"Table name too long: {len(table_name)} chars (max {MAX_TABLE_NAME_LENGTH})"
            raise ValueError(msg)

        if not VALID_TABLE_NAME_PATTERN.match(table_name):
            msg = f"Invalid table name: {table_name!r}. Must start with letter/underscore and contain only alphanumeric characters and underscores"
            raise ValueError(msg)
