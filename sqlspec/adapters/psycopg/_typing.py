"""Psycopg adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any, Protocol

from psycopg.rows import DictRow as PsycopgDictRow

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from psycopg import AsyncConnection, Connection

    from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, Statement, StatementConfig

    PsycopgSyncConnection: TypeAlias = Connection[PsycopgDictRow]
    PsycopgAsyncConnection: TypeAlias = AsyncConnection[PsycopgDictRow]
else:
    from psycopg import AsyncConnection, Connection

    PsycopgSyncConnection = Connection
    PsycopgAsyncConnection = AsyncConnection


class PsycopgPipelineDriver(Protocol):
    """Protocol for psycopg pipeline driver methods used in stack execution."""

    statement_config: "StatementConfig"

    def prepare_statement(
        self,
        statement: "SQL | Statement | QueryBuilder",
        parameters: Any,
        *,
        statement_config: "StatementConfig | None" = None,
        kwargs: "dict[str, Any] | None" = None,
    ) -> "SQL": ...

    def _get_compiled_sql(self, statement: "SQL", statement_config: "StatementConfig") -> "tuple[str, Any]": ...


class PsycopgSyncSessionContext:
    """Sync context manager for psycopg sessions.

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
        prepare_driver: "Callable[[PsycopgSyncDriver], PsycopgSyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: PsycopgSyncDriver | None = None

    def __enter__(self) -> "PsycopgSyncDriver":
        from sqlspec.adapters.psycopg.driver import PsycopgSyncDriver

        self._connection = self._acquire_connection()
        self._driver = PsycopgSyncDriver(
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


class PsycopgAsyncSessionContext:
    """Async context manager for psycopg sessions.

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
        prepare_driver: "Callable[[PsycopgAsyncDriver], PsycopgAsyncDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: PsycopgAsyncDriver | None = None

    async def __aenter__(self) -> "PsycopgAsyncDriver":
        from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver

        self._connection = await self._acquire_connection()
        self._driver = PsycopgAsyncDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = (
    "PsycopgAsyncConnection",
    "PsycopgAsyncSessionContext",
    "PsycopgDictRow",
    "PsycopgPipelineDriver",
    "PsycopgSyncConnection",
    "PsycopgSyncSessionContext",
)
