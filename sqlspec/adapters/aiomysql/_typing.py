"""aiomysql adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

from aiomysql import Connection  # pyright: ignore
from aiomysql.cursors import Cursor as _AiomysqlCursor  # pyright: ignore

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import Protocol, TypeAlias

    from sqlspec.adapters.aiomysql.driver import AiomysqlDriver
    from sqlspec.core import StatementConfig

    class AiomysqlConnectionProtocol(Protocol):
        def cursor(self) -> "AiomysqlRawCursor": ...

        async def commit(self) -> Any: ...

        async def rollback(self) -> Any: ...

        def close(self) -> Any: ...

    AiomysqlConnection: TypeAlias = AiomysqlConnectionProtocol
    AiomysqlRawCursor: TypeAlias = _AiomysqlCursor

if not TYPE_CHECKING:
    AiomysqlConnection = Connection
    AiomysqlRawCursor = _AiomysqlCursor


class AiomysqlCursor:
    """Context manager for aiomysql cursor operations.

    Provides automatic cursor acquisition and cleanup for database operations.
    """

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AiomysqlConnection") -> None:
        self.connection = connection
        self.cursor: AiomysqlRawCursor | None = None

    async def __aenter__(self) -> "AiomysqlRawCursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, *_: Any) -> None:
        if self.cursor is not None:
            await self.cursor.close()


class AiomysqlSessionContext:
    """Async context manager for aiomysql sessions.

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
        prepare_driver: "Callable[[AiomysqlDriver], AiomysqlDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: AiomysqlDriver | None = None

    async def __aenter__(self) -> "AiomysqlDriver":
        from sqlspec.adapters.aiomysql.driver import AiomysqlDriver

        self._connection = await self._acquire_connection()
        self._driver = AiomysqlDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = ("AiomysqlConnection", "AiomysqlCursor", "AiomysqlRawCursor", "AiomysqlSessionContext")
