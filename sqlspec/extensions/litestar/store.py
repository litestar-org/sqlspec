"""Base session store classes for Litestar integration."""

import random
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from types import TracebackType

    from sqlspec.config import DatabaseConfigProtocol

logger = get_logger("extensions.litestar.store")

__all__ = ("BaseSQLSpecStore",)


class BaseSQLSpecStore(ABC):
    """Base class for SQLSpec-backed Litestar session stores.

    Implements the litestar.stores.base.Store protocol for server-side session
    storage using SQLSpec database adapters.

    This abstract base class provides common functionality for all database-specific
    store implementations including:
    - Connection management via SQLSpec configs
    - Session expiration calculation
    - Automatic cleanup of expired sessions
    - Table creation utilities

    Subclasses must implement dialect-specific SQL queries.

    Args:
        config: SQLSpec database configuration (async or sync).
        table_name: Name of the session table. Defaults to "sessions".
        cleanup_probability: Probability (0.0-1.0) of running cleanup on each set().
            Defaults to 0.01 (1% chance). Set to 0 to disable automatic cleanup.

    Example:
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.adapters.asyncpg.litestar.store import AsyncPGStore

        config = AsyncpgConfig(pool_config={"dsn": "postgresql://..."})
        store = AsyncPGStore(config)
        await store.create_table()
    """

    __slots__ = ("_cleanup_probability", "_config", "_table_name")

    def __init__(
        self,
        config: "DatabaseConfigProtocol[Any, Any, Any]",
        table_name: str = "sessions",
        cleanup_probability: float = 0.01,
    ) -> None:
        """Initialize the session store.

        Args:
            config: SQLSpec database configuration.
            table_name: Name of the session table.
            cleanup_probability: Probability of cleanup on set (0.0-1.0).
        """
        self._config = config
        self._table_name = table_name
        self._cleanup_probability = max(0.0, min(1.0, cleanup_probability))

    @property
    def config(self) -> "DatabaseConfigProtocol[Any, Any, Any]":
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

    async def __aenter__(self) -> "BaseSQLSpecStore":
        """Enter context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: "type[BaseException] | None",
        exc_val: "BaseException | None",
        exc_tb: "TracebackType | None",
    ) -> None:
        """Exit context manager."""

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

    def _should_cleanup(self) -> bool:
        """Determine if cleanup should run based on probability.

        Returns:
            True if cleanup should run this time.

        Note:
            Uses random.random() which is not cryptographically secure,
            but is sufficient for probabilistic cleanup scheduling.
        """
        if self._cleanup_probability <= 0:
            return False

        return random.random() < self._cleanup_probability  # noqa: S311

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
