"""aiomysql adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from typing import TYPE_CHECKING, Any

import aiomysql as _aiomysql
from aiomysql import Connection
from aiomysql import Error as _AiomysqlError
from aiomysql import MySQLError as _AiomysqlMySQLError
from aiomysql import Pool as _AiomysqlPool
from aiomysql import ProgrammingError as AiomysqlProgrammingError
from aiomysql import SSCursor as AiomysqlSSCursor
from aiomysql.cursors import RE_INSERT_VALUES as AIOMYSQL_INSERT_VALUES_PATTERN
from aiomysql.cursors import Cursor as _AiomysqlCursor
from aiomysql.cursors import DictCursor as _AiomysqlDictCursor
from pymysql.constants import FIELD_TYPE as _PYMYSQL_FIELD_TYPE

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType
    from typing import Protocol, TypeAlias

    from pymysql.err import Error as _PymysqlError
    from pymysql.err import MySQLError as _PymysqlMySQLError

    from sqlspec.adapters.aiomysql.driver import AiomysqlDriver
    from sqlspec.core import StatementConfig

    class AiomysqlConnectionProtocol(Protocol):
        async def cursor(self, cursor: "type[AiomysqlRawCursor] | None" = None) -> "AiomysqlRawCursor": ...

        async def commit(self) -> object: ...

        async def rollback(self) -> object: ...

        def close(self) -> object: ...

        def get_transaction_status(self) -> bool: ...

    class AiomysqlModuleProtocol(Protocol):
        async def create_pool(self, **kwargs: Any) -> "AiomysqlPool": ...

        async def connect(self, **kwargs: Any) -> "AiomysqlConnection": ...

    class AiomysqlFieldTypeProtocol(Protocol):
        JSON: int

    AiomysqlConnection: TypeAlias = AiomysqlConnectionProtocol
    AiomysqlModule: TypeAlias = AiomysqlModuleProtocol
    AiomysqlRawCursor: TypeAlias = _AiomysqlCursor
    AiomysqlDictCursor: TypeAlias = _AiomysqlDictCursor
    AiomysqlFieldType: TypeAlias = AiomysqlFieldTypeProtocol
    AiomysqlPool: TypeAlias = _AiomysqlPool
    AiomysqlPymysqlError: TypeAlias = _PymysqlError
    AiomysqlPymysqlMySQLError: TypeAlias = _PymysqlMySQLError

if not TYPE_CHECKING:
    AiomysqlConnection = Connection
    AiomysqlModule = _aiomysql
    AiomysqlRawCursor = _AiomysqlCursor
    AiomysqlDictCursor = _AiomysqlDictCursor
    AiomysqlFieldType = _PYMYSQL_FIELD_TYPE
    AiomysqlPool = _AiomysqlPool
    AiomysqlPymysqlError = _AiomysqlError
    AiomysqlPymysqlMySQLError = _AiomysqlMySQLError

__all__ = (
    "AIOMYSQL_INSERT_VALUES_PATTERN",
    "AiomysqlConnection",
    "AiomysqlCursor",
    "AiomysqlDictCursor",
    "AiomysqlFieldType",
    "AiomysqlModule",
    "AiomysqlPool",
    "AiomysqlProgrammingError",
    "AiomysqlPymysqlError",
    "AiomysqlPymysqlMySQLError",
    "AiomysqlRawCursor",
    "AiomysqlSSCursor",
    "AiomysqlSessionContext",
)


class AiomysqlCursor:
    """Context manager for aiomysql cursor operations.

    Provides automatic cursor acquisition and cleanup for database operations.

    The optional ``cursor_class`` argument forces a specific cursor type regardless of the user's
    ``cursor_class`` setting in ``AiomysqlConnectionParams``. This lets
    first-party store code (ADK, Litestar, Events) that relies on positional
    row access and must not be broken when a user configures ``DictCursor`` at
    the connection level.
    """

    __slots__ = ("connection", "cursor", "cursor_class")

    def __init__(self, connection: "AiomysqlConnection", cursor_class: "type[AiomysqlRawCursor] | None" = None) -> None:
        self.connection = connection
        self.cursor_class = cursor_class
        self.cursor: AiomysqlRawCursor | None = None

    async def __aenter__(self) -> "AiomysqlRawCursor":
        if self.cursor_class is None:
            self.cursor = await self.connection.cursor()
        else:
            self.cursor = await self.connection.cursor(self.cursor_class)
        return self.cursor

    async def __aexit__(self, *_: object) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
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
        acquire_connection: "Callable[[], Awaitable[AiomysqlConnection]]",
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[AiomysqlDriver], AiomysqlDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: AiomysqlConnection | None = None
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
            await self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        return None
