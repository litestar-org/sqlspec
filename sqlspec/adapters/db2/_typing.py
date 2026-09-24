"""IBM Db2 adapter type definitions and context managers."""

import contextlib
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import import_optional_attr
from sqlspec.utils.module_loader import import_optional


class _Db2UnavailableError(Exception):
    """Fallback Db2 exception base when ibm_db_dbi is not installed."""


if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from ibm_db_dbi import AsyncConnection as _Db2AsyncConnection
    from ibm_db_dbi import AsyncCursor as _Db2AsyncCursor
    from ibm_db_dbi import Connection as _Db2Connection
    from ibm_db_dbi import Cursor as _Db2Cursor
    from ibm_db_dbi import Error as _Db2Error

    from sqlspec.adapters.db2.driver import Db2AsyncDriver, Db2SyncDriver
    from sqlspec.core import StatementConfig

    Db2SyncConnection: TypeAlias = _Db2Connection
    Db2RawCursor: TypeAlias = _Db2Cursor
    Db2AsyncConnection: TypeAlias = _Db2AsyncConnection
    Db2AsyncRawCursor: TypeAlias = _Db2AsyncCursor
    Db2Error: TypeAlias = _Db2Error

if not TYPE_CHECKING:
    Db2SyncConnection = import_optional_attr("ibm_db_dbi", "Connection") or Any
    Db2RawCursor = import_optional_attr("ibm_db_dbi", "Cursor") or Any
    Db2AsyncConnection = import_optional_attr("ibm_db_dbi", "AsyncConnection") or Any
    Db2AsyncRawCursor = import_optional_attr("ibm_db_dbi", "AsyncCursor") or Any
    Db2Error = import_optional_attr("ibm_db_dbi", "Error") or _Db2UnavailableError

ibm_db = import_optional("ibm_db")
ibm_db_dbi = import_optional("ibm_db_dbi")

__all__ = (
    "Db2AsyncConnection",
    "Db2AsyncCursor",
    "Db2AsyncRawCursor",
    "Db2AsyncSessionContext",
    "Db2Error",
    "Db2RawCursor",
    "Db2SyncConnection",
    "Db2SyncCursor",
    "Db2SyncSessionContext",
    "connection_autocommit_enabled",
    "ibm_db",
    "ibm_db_dbi",
)


def connection_autocommit_enabled(connection: Any) -> bool:
    """Return whether a Db2 connection is currently in autocommit mode.

    ``ibm_db_dbi.Connection`` has no autocommit getter, so the mode is read from the underlying
    ``ibm_db`` handle.

    Args:
        connection: ``ibm_db_dbi`` connection exposing ``conn_handler``.

    Returns:
        bool: True when autocommit is on.

    Raises:
        MissingDependencyError: When ibm_db is not installed.
    """
    if ibm_db is None:
        raise MissingDependencyError(package="ibm_db", install_package="db2")
    return bool(ibm_db.autocommit(connection.conn_handler))


class Db2SyncCursor:
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


class Db2SyncSessionContext:
    """Synchronous context manager for Db2 sessions.

    On exit, work left open by the session is rolled back before the connection is released: an
    active transaction always, and any pending unit of work when the connection's autocommit
    baseline is off.
    """

    __slots__ = (
        "_acquire_connection",
        "_autocommit_baseline",
        "_begin_transaction",
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
        *,
        autocommit_baseline: bool = True,
        begin_transaction: bool = False,
    ) -> None:
        """Initialize the session context manager.

        Args:
            acquire_connection: Factory callable to checkout connection from pool.
            release_connection: Callback to return connection to pool.
            statement_config: SQL compilation configuration.
            driver_features: Feature flags and hooks for driver instance.
            prepare_driver: Hook to customize or decorate driver before yielding.
            autocommit_baseline: Autocommit mode the pool opens connections in.
            begin_transaction: Begin a transaction before yielding the driver.
        """
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._autocommit_baseline = autocommit_baseline
        self._begin_transaction = begin_transaction
        self._connection: Any = None
        self._driver: Db2SyncDriver | None = None

    def __enter__(self) -> Any:
        """Checkout connection and build initialized driver adapter.

        Returns:
            Any: Initialized driver adapter.
        """
        from sqlspec.adapters.db2.driver import Db2SyncDriver

        self._connection = self._acquire_connection()
        self._driver = Db2SyncDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        if self._begin_transaction:
            try:
                self._driver.begin()
            except BaseException as exc:
                self._release_connection(self._connection, exc_type=type(exc), exc_val=exc, exc_tb=exc.__traceback__)
                self._connection = None
                self._driver = None
                raise
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        """Roll back open work, then return the connection to the pool."""
        if self._connection is None:
            return None
        try:
            if self._driver is not None:
                self._driver.release_open_work(autocommit_baseline=self._autocommit_baseline)
        finally:
            self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
            self._driver = None
        return None


class Db2AsyncCursor:
    """Async context manager for ``ibm_db_dbi.AsyncCursor`` operations."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: Any) -> None:
        """Initialize the cursor context manager.

        Args:
            connection: ``ibm_db_dbi.AsyncConnection`` instance.
        """
        self.connection = connection
        self.cursor: Any = None

    async def __aenter__(self) -> Any:
        """Open a cursor on the connection.

        Returns:
            Any: Async cursor ready for statement execution.
        """
        self.cursor = await self.connection.cursor()
        return self.cursor

    async def __aexit__(self, *_: Any) -> None:
        """Close the cursor, suppressing cleanup errors."""
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                await self.cursor.close()


class Db2AsyncSessionContext:
    """Asynchronous context manager for Db2 sessions.

    On exit, work left open by the session is rolled back before the connection is released: an
    active transaction always, and any pending unit of work when the connection's autocommit
    baseline is off.
    """

    __slots__ = (
        "_acquire_connection",
        "_autocommit_baseline",
        "_begin_transaction",
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
        *,
        autocommit_baseline: bool = True,
        begin_transaction: bool = False,
    ) -> None:
        """Initialize the session context manager.

        Args:
            acquire_connection: Coroutine function that checks a connection out of the pool.
            release_connection: Coroutine function that returns the connection to the pool.
            statement_config: SQL compilation configuration.
            driver_features: Feature flags and hooks for driver instance.
            prepare_driver: Hook to customize or decorate driver before yielding.
            autocommit_baseline: Autocommit mode the pool opens connections in.
            begin_transaction: Begin a transaction before yielding the driver.
        """
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._autocommit_baseline = autocommit_baseline
        self._begin_transaction = begin_transaction
        self._connection: Any = None
        self._driver: Db2AsyncDriver | None = None

    async def __aenter__(self) -> Any:
        """Check a connection out and build the initialized driver adapter.

        Returns:
            Any: Initialized driver adapter.
        """
        from sqlspec.adapters.db2.driver import Db2AsyncDriver

        self._connection = await self._acquire_connection()
        self._driver = Db2AsyncDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        if self._begin_transaction:
            try:
                await self._driver.begin()
            except BaseException as exc:
                await self._release_connection(
                    self._connection, exc_type=type(exc), exc_val=exc, exc_tb=exc.__traceback__
                )
                self._connection = None
                self._driver = None
                raise
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        """Roll back open work, then return the connection to the pool."""
        if self._connection is None:
            return None
        try:
            if self._driver is not None:
                await self._driver.release_open_work(autocommit_baseline=self._autocommit_baseline)
        finally:
            await self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
            self._driver = None
        return None
