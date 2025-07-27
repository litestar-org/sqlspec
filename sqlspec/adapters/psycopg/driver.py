# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional

from psycopg import AsyncConnection, Connection

from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
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

    def _extract_select_data(self, cursor: Any) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = cursor.fetchall()
        column_names = [col.name for col in cursor.description or []]
        # Data is already in dict format from DictRow
        return fetched_data, column_names, len(fetched_data)

    def _extract_execute_rowcount(self, cursor: Any) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else 0

    def begin(self) -> None:
        """Begin transaction using psycopg-specific method."""
        self.connection.execute("BEGIN")

    def rollback(self) -> None:
        """Rollback transaction using psycopg-specific method."""
        self.connection.rollback()

    def commit(self) -> None:
        """Commit transaction using psycopg-specific method."""
        self.connection.commit()


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
        async with connection.cursor() as cursor:
            yield cursor

    async def _perform_execute(self, cursor: Any, statement: "SQL") -> None:
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        if statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            await cursor.executemany(sql, prepared_params)
        else:
            prepared_params = self._prepare_driver_parameters(params)
            await cursor.execute(sql, prepared_params or ())

    async def _extract_select_data(self, cursor: Any) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = await cursor.fetchall()
        column_names = [col.name for col in cursor.description or []]
        # Data is already in dict format from DictRow
        return fetched_data, column_names, len(fetched_data)

    def _extract_execute_rowcount(self, cursor: Any) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else 0

    async def begin(self) -> None:
        """Begin transaction using psycopg-specific method."""
        await self.connection.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback transaction using psycopg-specific method."""
        await self.connection.rollback()

    async def commit(self) -> None:
        """Commit transaction using psycopg-specific method."""
        await self.connection.commit()
