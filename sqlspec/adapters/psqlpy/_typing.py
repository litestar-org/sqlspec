"""Psqlpy adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

from sqlspec.typing import import_optional_attr


class _PsqlpyUnavailableError(Exception):
    """Fallback psqlpy exception base when optional exception classes are unavailable."""


if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from psqlpy import Connection as _PsqlpyConnection
    from psqlpy import Listener as _PsqlpyListener
    from psqlpy.exceptions import DatabaseError as _PsqlpyDatabaseError
    from psqlpy.exceptions import DataError as _PsqlpyDataError
    from psqlpy.exceptions import Error as _PsqlpyError
    from psqlpy.exceptions import IntegrityError as _PsqlpyIntegrityError
    from psqlpy.exceptions import NotSupportedError as _PsqlpyNotSupportedError
    from psqlpy.exceptions import OperationalError as _PsqlpyOperationalError

    from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
    from sqlspec.core import StatementConfig

    PsqlpyConnection: TypeAlias = _PsqlpyConnection
    PsqlpyDataError: TypeAlias = _PsqlpyDataError
    PsqlpyDatabaseError: TypeAlias = _PsqlpyDatabaseError
    PsqlpyError: TypeAlias = _PsqlpyError
    PsqlpyIntegrityError: TypeAlias = _PsqlpyIntegrityError
    PsqlpyListener: TypeAlias = _PsqlpyListener
    PsqlpyNotSupportedError: TypeAlias = _PsqlpyNotSupportedError
    PsqlpyOperationalError: TypeAlias = _PsqlpyOperationalError

if not TYPE_CHECKING:
    PsqlpyConnection = import_optional_attr("psqlpy", "Connection") or Any
    PsqlpyDataError = import_optional_attr("psqlpy.exceptions", "DataError") or _PsqlpyUnavailableError
    PsqlpyDatabaseError = import_optional_attr("psqlpy.exceptions", "DatabaseError") or _PsqlpyUnavailableError
    PsqlpyError = import_optional_attr("psqlpy.exceptions", "Error") or _PsqlpyUnavailableError
    PsqlpyIntegrityError = import_optional_attr("psqlpy.exceptions", "IntegrityError") or _PsqlpyUnavailableError
    PsqlpyListener = import_optional_attr("psqlpy", "Listener") or Any
    PsqlpyNotSupportedError = import_optional_attr("psqlpy.exceptions", "NotSupportedError") or _PsqlpyUnavailableError
    PsqlpyOperationalError = import_optional_attr("psqlpy.exceptions", "OperationalError") or _PsqlpyUnavailableError

__all__ = (
    "PsqlpyConnection",
    "PsqlpyCursor",
    "PsqlpyDataError",
    "PsqlpyDatabaseError",
    "PsqlpyError",
    "PsqlpyIntegrityError",
    "PsqlpyListener",
    "PsqlpyNotSupportedError",
    "PsqlpyOperationalError",
    "PsqlpySessionContext",
)


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
