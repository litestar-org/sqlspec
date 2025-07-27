# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional

from psycopg import AsyncConnection, Connection

from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from psycopg.rows import DictRow as PsycopgDictRow
    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias


logger = get_logger("adapters.psycopg")

__all__ = ("PsycopgAsyncConnection", "PsycopgAsyncDriver", "PsycopgSyncConnection", "PsycopgSyncDriver")

if TYPE_CHECKING:
    PsycopgSyncConnection: TypeAlias = Connection[PsycopgDictRow]
    PsycopgAsyncConnection: TypeAlias = AsyncConnection[PsycopgDictRow]
else:
    PsycopgSyncConnection = Connection
    PsycopgAsyncConnection = AsyncConnection


class PsycopgSyncDriver(SyncDriverAdapterBase):
    """Psycopg Sync Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "postgres"
    parameter_config = DriverParameterConfig(
        supported_parameter_styles=[
            ParameterStyle.POSITIONAL_PYFORMAT,  # %s
            ParameterStyle.NAMED_PYFORMAT,  # %(name)s
            ParameterStyle.NUMERIC,  # $1 (also supported!)
        ],
        default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        type_coercion_map={
            # Psycopg handles most types natively
            # Add any specific type mappings as needed
        },
        has_native_list_expansion=True,  # Psycopg handles lists/tuples natively
        force_style_conversion=True,  # SQLGlot doesn't generate pyformat
    )

    def __init__(self, connection: PsycopgSyncConnection, config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)

    @contextmanager
    def with_cursor(self, connection: PsycopgSyncConnection) -> Generator[Any, None, None]:
        with connection.cursor() as cursor:
            yield cursor

    def _perform_execute(self, cursor: Any, statement: "SQL") -> None:
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        if statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            cursor.executemany(sql, prepared_params)
        else:
            prepared_params = self._prepare_driver_parameters(params)
            cursor.execute(sql, prepared_params or ())

    def _build_result(self, cursor: Any, statement: "SQL") -> "SQLResult":
        if self.returns_rows(statement.expression):
            return self._build_select_result(cursor, statement)
        return self._build_modify_result(cursor, statement)

    def _build_select_result(self, cursor: Any, statement: "SQL") -> "SQLResult":
        fetched_data = cursor.fetchall()
        column_names = [col.name for col in cursor.description or []]
        return SQLResult(
            statement=statement,
            data=fetched_data,
            column_names=column_names,
            rows_affected=len(fetched_data),
            operation_type="SELECT",
        )

    def _build_modify_result(self, cursor: Any, statement: "SQL") -> "SQLResult":
        operation_type = self._determine_operation_type(statement)
        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=cursor.rowcount or 0,
            operation_type=operation_type,
            metadata={"status_message": cursor.statusmessage or "OK"},
        )

    def begin(self, connection: "Optional[Any]" = None) -> None:
        """Begin transaction using psycopg-specific method."""
        conn = connection or self.connection
        conn.execute("BEGIN")

    def rollback(self, connection: "Optional[Any]" = None) -> None:
        """Rollback transaction using psycopg-specific method."""
        conn = connection or self.connection
        conn.rollback()

    def commit(self, connection: "Optional[Any]" = None) -> None:
        """Commit transaction using psycopg-specific method."""
        conn = connection or self.connection
        conn.commit()


class PsycopgAsyncDriver(AsyncDriverAdapterBase):
    """Psycopg Async Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "postgres"
    parameter_config = DriverParameterConfig(
        supported_parameter_styles=[
            ParameterStyle.POSITIONAL_PYFORMAT,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NUMERIC,
        ],
        default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        type_coercion_map={},
        has_native_list_expansion=True,
        force_style_conversion=True,
    )

    def __init__(self, connection: PsycopgAsyncConnection, config: Optional[SQLConfig] = None) -> None:
        super().__init__(connection=connection, config=config)

    @asynccontextmanager
    async def with_cursor(self, connection: PsycopgAsyncConnection) -> AsyncGenerator[Any, None]:
        cursor = await connection.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    async def _perform_execute(self, cursor: Any, statement: "SQL") -> None:
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        if statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            await cursor.executemany(sql, prepared_params)
        else:
            prepared_params = self._prepare_driver_parameters(params)
            await cursor.execute(sql, prepared_params or ())

    async def _build_result(self, cursor: Any, statement: "SQL") -> "SQLResult":
        if self.returns_rows(statement.expression):
            return await self._build_select_result(cursor, statement)
        return self._build_modify_result(cursor, statement)

    async def _build_select_result(self, cursor: Any, statement: "SQL") -> "SQLResult":
        fetched_data = await cursor.fetchall()
        column_names = [col.name for col in cursor.description or []]
        return SQLResult(
            statement=statement,
            data=fetched_data,
            column_names=column_names,
            rows_affected=len(fetched_data),
            operation_type="SELECT",
        )

    def _build_modify_result(self, cursor: Any, statement: "SQL") -> "SQLResult":
        operation_type = self._determine_operation_type(statement)
        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=cursor.rowcount or 0,
            operation_type=operation_type,
            metadata={"status_message": cursor.statusmessage or "OK"},
        )

    async def begin(self, connection: "Optional[Any]" = None) -> None:
        """Begin transaction using psycopg-specific method."""
        conn = connection or self.connection
        await conn.execute("BEGIN")

    async def rollback(self, connection: "Optional[Any]" = None) -> None:
        """Rollback transaction using psycopg-specific method."""
        conn = connection or self.connection
        await conn.rollback()

    async def commit(self, connection: "Optional[Any]" = None) -> None:
        """Commit transaction using psycopg-specific method."""
        conn = connection or self.connection
        await conn.commit()
