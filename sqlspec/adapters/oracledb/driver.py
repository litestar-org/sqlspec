# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any, Optional, cast

from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor
from sqlglot.dialects.dialect import DialectType

from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from typing_extensions import TypeAlias


__all__ = ("OracleAsyncConnection", "OracleAsyncDriver", "OracleSyncConnection", "OracleSyncDriver")

if TYPE_CHECKING:
    OracleSyncConnection: TypeAlias = Connection
    OracleAsyncConnection: TypeAlias = AsyncConnection
else:
    # Use Any for mypyc runtime to avoid "unreachable code" error
    OracleSyncConnection = Any
    OracleAsyncConnection = Any

logger = get_logger("adapters.oracledb")


class _OracleCursorManager:
    """Context manager for Oracle cursor management."""

    def __init__(self, connection: OracleSyncConnection) -> None:
        self.connection = connection
        self.cursor: Optional[Cursor] = None

    def __enter__(self) -> Cursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.cursor is not None:
            self.cursor.close()


class OracleSyncDriver(SyncDriverAdapterBase):
    """Oracle Sync Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "oracle"
    parameter_config: DriverParameterConfig

    def __init__(self, connection: OracleSyncConnection, config: Optional[SQLConfig] = None) -> None:
        super().__init__(connection=connection, config=config)
        self.parameter_config = DriverParameterConfig(
            supported_parameter_styles=[
                ParameterStyle.POSITIONAL_COLON,  # :1, :2
                ParameterStyle.NAMED_COLON,  # :name
            ],
            default_parameter_style=ParameterStyle.NAMED_COLON,
            type_coercion_map={
                # Oracle has good native type support
                # Add any specific type mappings as needed
            },
            has_native_list_expansion=False,  # Oracle doesn't handle lists natively
        )

    def with_cursor(self, connection: OracleSyncConnection) -> _OracleCursorManager:
        return _OracleCursorManager(connection)

    def _perform_execute(self, cursor: Cursor, statement: "SQL") -> None:
        if statement.is_script:
            # Scripts use STATIC compilation to transpile parameters automatically
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            # Oracle doesn't have executescript - execute statements one by one
            statements = self._split_script_statements(sql, strip_trailing_semicolon=True)
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    cursor.execute(stmt)
        else:
            # Enable intelligent parameter conversion - Oracle supports both POSITIONAL_COLON and NAMED_COLON
            sql, params = statement.compile()

            if statement.is_many:
                # For execute_many, params is already a list of parameter sets
                prepared_params = self._prepare_driver_parameters_many(params) if params else []
                cursor.executemany(sql, prepared_params)
            else:
                prepared_params = self._prepare_driver_parameters(params)
                cursor.execute(sql, prepared_params or {})

    def _extract_select_data(self, cursor: Cursor) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]
        data = cast("list[dict[str, Any]]", [dict(zip(column_names, row)) for row in fetched_data])
        return data, column_names, len(data)

    def _extract_execute_rowcount(self, cursor: Cursor) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else 0

    def begin(self) -> None:
        """Begin a database transaction."""
        # Oracle uses implicit transactions, but we can use a savepoint
        # or explicit begin if the driver supports it
        # Oracle typically doesn't need explicit BEGIN, transactions start implicitly

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.connection.rollback()

    def commit(self) -> None:
        """Commit the current transaction."""
        self.connection.commit()


class _AsyncOracleCursorManager:
    def __init__(self, connection: "OracleAsyncConnection") -> None:
        self.connection = connection
        self.cursor: Optional[AsyncCursor] = None

    async def __aenter__(self) -> AsyncCursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.cursor:
            self.cursor.close()  # oracledb's AsyncCursor.close() is not async


class OracleAsyncDriver(AsyncDriverAdapterBase):
    """Oracle Async Driver Adapter. Refactored for new protocol."""

    dialect: DialectType = "oracle"
    parameter_config: DriverParameterConfig

    def __init__(self, connection: OracleAsyncConnection, config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)
        self.parameter_config = DriverParameterConfig(
            supported_parameter_styles=[
                ParameterStyle.POSITIONAL_COLON,  # :1, :2
                ParameterStyle.NAMED_COLON,  # :name
            ],
            default_parameter_style=ParameterStyle.NAMED_COLON,
            type_coercion_map={
                # Oracle has good native type support
                # Add any specific type mappings as needed
            },
            has_native_list_expansion=False,  # Oracle doesn't handle lists natively
        )

    def with_cursor(self, connection: "OracleAsyncConnection") -> "_AsyncOracleCursorManager":
        return _AsyncOracleCursorManager(connection)

    async def _perform_execute(self, cursor: AsyncCursor, statement: "SQL") -> None:
        if statement.is_script:
            # Scripts use STATIC compilation to transpile parameters automatically
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            # Oracle doesn't have executescript - execute statements one by one
            statements = self._split_script_statements(sql, strip_trailing_semicolon=True)
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    await cursor.execute(stmt)
        else:
            # Enable intelligent parameter conversion - Oracle supports both POSITIONAL_COLON and NAMED_COLON
            sql, params = statement.compile()

            if statement.is_many:
                # For execute_many, params is already a list of parameter sets
                prepared_params = self._prepare_driver_parameters_many(params) if params else []
                await cursor.executemany(sql, prepared_params)
            else:
                prepared_params = self._prepare_driver_parameters(params)
                await cursor.execute(sql, prepared_params or {})

    async def _extract_select_data(self, cursor: AsyncCursor) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = await cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]
        data = cast("list[dict[str, Any]]", [dict(zip(column_names, row)) for row in fetched_data])
        return data, column_names, len(data)

    def _extract_execute_rowcount(self, cursor: AsyncCursor) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else 0

    async def begin(self) -> None:
        """Begin a database transaction."""
        # Oracle uses implicit transactions, but we can use a savepoint
        # or explicit begin if the driver supports it
        # Oracle typically doesn't need explicit BEGIN, transactions start implicitly

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.connection.rollback()

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self.connection.commit()
