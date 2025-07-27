# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast

from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor
from sqlglot.dialects.dialect import DialectType

from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from typing_extensions import TypeAlias


__all__ = ("OracleAsyncConnection", "OracleAsyncDriver", "OracleSyncConnection", "OracleSyncDriver")

if TYPE_CHECKING:
    OracleSyncConnection: TypeAlias = Connection
    OracleAsyncConnection: TypeAlias = AsyncConnection
else:
    # Direct assignment for mypyc runtime
    OracleSyncConnection = Connection
    OracleAsyncConnection = AsyncConnection

logger = get_logger("adapters.oracledb")


class OracleSyncDriver(SyncDriverAdapterBase):
    """Oracle Sync Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "oracle"
    parameter_config = DriverParameterConfig(
        supported_parameter_styles=[
            ParameterStyle.POSITIONAL_COLON,  # :1, :2
            ParameterStyle.NAMED_COLON,       # :name
        ],
        default_parameter_style=ParameterStyle.NAMED_COLON,
        type_coercion_map={
            # Oracle has good native type support
            # Add any specific type mappings as needed
        },
        has_native_list_expansion=False,  # Oracle doesn't handle lists natively
    )

    def __init__(self, connection: OracleSyncConnection, config: Optional[SQLConfig] = None) -> None:
        super().__init__(connection=connection, config=config)

    @contextmanager
    def with_cursor(self, connection: Optional[OracleSyncConnection] = None) -> Generator[Cursor, None, None]:
        conn_to_use = connection or self.connection
        cursor: Cursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _perform_execute(self, cursor: Cursor, statement: "SQL") -> None:
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        if statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            cursor.executemany(sql, prepared_params)
        else:
            prepared_params = self._prepare_driver_parameters(params)
            cursor.execute(sql, prepared_params or {})

    def _build_result(self, cursor: Cursor, statement: "SQL") -> "SQLResult":
        if self.returns_rows(statement.expression):
            return self._build_select_result(cursor, statement)
        return self._build_modify_result(cursor, statement)

    def _build_select_result(self, cursor: Cursor, statement: "SQL") -> "SQLResult":
        fetched_data = cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]
        data = cast("list[dict[str, Any]]", [dict(zip(column_names, row)) for row in fetched_data])

        return SQLResult(
            statement=statement,
            data=data,
            column_names=column_names,
            rows_affected=cursor.rowcount,
            operation_type="SELECT",
        )

    def _build_modify_result(self, cursor: Cursor, statement: "SQL") -> "SQLResult":
        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=cursor.rowcount,
            operation_type=self._determine_operation_type(statement),
            metadata={"status_message": "OK"},
        )

    def begin(self, connection: Optional[Any] = None) -> None:
        """Begin a database transaction."""
        # Oracle uses implicit transactions, but we can use a savepoint
        # or explicit begin if the driver supports it
        # Oracle typically doesn't need explicit BEGIN, transactions start implicitly

    def rollback(self, connection: Optional[Any] = None) -> None:
        """Rollback the current transaction."""
        conn = connection or self.connection
        conn.rollback()

    def commit(self, connection: Optional[Any] = None) -> None:
        """Commit the current transaction."""
        conn = connection or self.connection
        conn.commit()


class OracleAsyncDriver(AsyncDriverAdapterBase):
    """Oracle Async Driver Adapter. Refactored for new protocol."""

    dialect: DialectType = "oracle"
    parameter_config = DriverParameterConfig(
        supported_parameter_styles=[
            ParameterStyle.POSITIONAL_COLON,  # :1, :2
            ParameterStyle.NAMED_COLON,       # :name
        ],
        default_parameter_style=ParameterStyle.NAMED_COLON,
        type_coercion_map={
            # Oracle has good native type support
            # Add any specific type mappings as needed
        },
        has_native_list_expansion=False,  # Oracle doesn't handle lists natively
    )

    def __init__(self, connection: OracleAsyncConnection, config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)

    @asynccontextmanager
    async def with_cursor(
        self, connection: Optional[OracleAsyncConnection] = None
    ) -> AsyncGenerator[AsyncCursor, None]:
        conn_to_use = connection or self.connection
        cursor: AsyncCursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    async def _perform_execute(self, cursor: AsyncCursor, statement: "SQL") -> None:
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        if statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            await cursor.executemany(sql, prepared_params)
        else:
            prepared_params = self._prepare_driver_parameters(params)
            await cursor.execute(sql, prepared_params or {})

    async def _build_result(self, cursor: AsyncCursor, statement: "SQL") -> "SQLResult":
        if self.returns_rows(statement.expression):
            return await self._build_select_result(cursor, statement)
        return self._build_modify_result(cursor, statement)

    async def _build_select_result(self, cursor: AsyncCursor, statement: "SQL") -> "SQLResult":  # type: ignore[override]
        fetched_data = await cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]
        data = cast("list[dict[str, Any]]", [dict(zip(column_names, row)) for row in fetched_data])

        return SQLResult(
            statement=statement,
            data=data,
            column_names=column_names,
            rows_affected=cursor.rowcount,
            operation_type="SELECT",
        )

    def _build_modify_result(self, cursor: AsyncCursor, statement: "SQL") -> "SQLResult":
        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=cursor.rowcount,
            operation_type=self._determine_operation_type(statement),
            metadata={"status_message": "OK"},
        )

    async def begin(self, connection: Optional[Any] = None) -> None:
        """Begin a database transaction."""
        # Oracle uses implicit transactions, but we can use a savepoint
        # or explicit begin if the driver supports it
        # Oracle typically doesn't need explicit BEGIN, transactions start implicitly

    async def rollback(self, connection: Optional[Any] = None) -> None:
        """Rollback the current transaction."""
        conn = connection or self.connection
        await conn.rollback()

    async def commit(self, connection: Optional[Any] = None) -> None:
        """Commit the current transaction."""
        conn = connection or self.connection
        await conn.commit()
