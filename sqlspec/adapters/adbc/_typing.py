# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
"""ADBC adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

from adbc_driver_manager.dbapi import Connection

_AdbcConnection = Connection

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from sqlspec.adapters.adbc.driver import AdbcDriver
    from sqlspec.core import StatementConfig

    AdbcConnection: TypeAlias = _AdbcConnection

if not TYPE_CHECKING:
    AdbcConnection = _AdbcConnection


class AdbcSessionContext:
    """Sync context manager for ADBC sessions.

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
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[AdbcDriver], AdbcDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: AdbcDriver | None = None

    def __enter__(self) -> "AdbcDriver":
        from sqlspec.adapters.adbc.driver import AdbcDriver

        self._connection = self._acquire_connection()
        self._driver = AdbcDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = ("AdbcConnection", "AdbcSessionContext")
