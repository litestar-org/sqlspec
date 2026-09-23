"""IBM Db2 adapter type definitions and context managers."""

import contextlib
from typing import TYPE_CHECKING, Any

from sqlspec.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.core import StatementConfig

ibm_db = import_optional("ibm_db")
ibm_db_dbi = import_optional("ibm_db_dbi")

IBM_DB_INSTALLED = ibm_db is not None
IBM_DB_DBI_INSTALLED = ibm_db_dbi is not None

if TYPE_CHECKING:
    Db2Connection: TypeAlias = Any
    Db2RawCursor: TypeAlias = Any
    Db2QueryParams: TypeAlias = Any
    Db2Error: TypeAlias = type[Exception]
else:
    Db2Connection = Any
    Db2RawCursor = Any
    Db2QueryParams = Any
    Db2Error = getattr(ibm_db_dbi, "Error", Exception) if ibm_db_dbi is not None else Exception

__all__ = (
    "IBM_DB_DBI_INSTALLED",
    "IBM_DB_INSTALLED",
    "Db2Connection",
    "Db2Cursor",
    "Db2Error",
    "Db2QueryParams",
    "Db2RawCursor",
    "Db2SessionContext",
    "ibm_db",
    "ibm_db_dbi",
)


class Db2Cursor:
    """Context manager for Db2 cursor operations."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: Any) -> None:
        """Initialize the cursor context manager.

        Args:
            connection: Physical Db2 connection instance.
        """
        self.connection = connection
        self.cursor: Any = None

    def __enter__(self) -> Any:
        """Acquire a cursor from the connection.

        Returns:
            Any: Cursor object ready for statement execution.
        """
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        """Close cursor and suppress errors on cleanup."""
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()


class Db2SessionContext:
    """Synchronous context manager for Db2 sessions."""

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
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig",
        driver_features: dict[str, Any],
        prepare_driver: "Callable[[Any], Any]",
    ) -> None:
        """Initialize the session context manager.

        Args:
            acquire_connection: Factory callable to checkout connection from pool.
            release_connection: Callback to return connection to pool.
            statement_config: SQL compilation configuration.
            driver_features: Feature flags and hooks for driver instance.
            prepare_driver: Hook to customize or decorate driver before yielding.
        """
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: Any = None

    def __enter__(self) -> Any:
        """Checkout connection and build initialized driver adapter.

        Returns:
            Any: Initialized driver adapter.
        """
        import importlib

        driver_module = importlib.import_module("sqlspec.adapters.db2.driver")
        driver_cls = driver_module.Db2Driver

        self._connection = self._acquire_connection()
        self._driver = driver_cls(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        """Return connection to pool on session completion."""
        if self._connection is not None:
            self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        return None
