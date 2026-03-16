"""Psqlpy adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

from psqlpy import Connection as _PsqlpyConnection

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
    from sqlspec.core import StatementConfig

    PsqlpyConnection: TypeAlias = _PsqlpyConnection

if not TYPE_CHECKING:
    PsqlpyConnection = _PsqlpyConnection


class PsqlpyCursor:
    """Context manager for psqlpy cursor management."""

    __slots__ = ("_in_use", "connection")

    def __init__(self, connection: "PsqlpyConnection") -> None:
        self.connection = connection
        self._in_use = False

    async def __aenter__(self) -> "PsqlpyConnection":
        """Enter cursor context.

        Returns:
            Psqlpy connection object
        """
        self._in_use = True
        return self.connection

    async def __aexit__(self, *_: Any) -> None:
        """Exit cursor context.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback
        """
        self._in_use = False

    def is_in_use(self) -> bool:
        """Check if cursor is currently in use.

        Returns:
            True if cursor is in use, False otherwise
        """
        return self._in_use


class PsqlpySessionContext:
    """Async context manager for psqlpy sessions.

    This class is intentionally excluded from mypyc compilation to avoid ABI
    boundary issues. It receives callables from uncompiled config classes and
    instantiates compiled Driver objects, acting as a bridge between compiled
    and uncompiled code.

    Uses callable-based connection management to decouple from config implementation.
    """

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[[Any], Any]",
        statement_config: "StatementConfig | Callable[[], StatementConfig]",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[PsqlpyDriver], PsqlpyDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: PsqlpyDriver | None = None

    async def __aenter__(self) -> "PsqlpyDriver":
        from sqlspec.adapters.psqlpy.driver import PsqlpyDriver

        self._connection = await self._acquire_connection()
        statement_config = self._statement_config() if callable(self._statement_config) else self._statement_config
        self._driver = PsqlpyDriver(
            connection=self._connection, statement_config=statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = ("PsqlpyConnection", "PsqlpyCursor", "PsqlpySessionContext")
