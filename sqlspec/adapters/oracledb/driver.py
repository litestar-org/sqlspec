# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any, Optional, cast

from oracledb import AsyncCursor, Cursor

from sqlspec.adapters.oracledb._types import OracleAsyncConnection, OracleSyncConnection
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.statement.sql import StatementConfig

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL

# Shared Oracle statement configurations
oracledb_sync_statement_config = StatementConfig(
    dialect="oracle",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_COLON,
        supported_parameter_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON},
        type_coercion_map={},
        has_native_list_expansion=False,
        needs_static_script_compilation=True,
    ),
)

oracledb_statement_config = StatementConfig(
    dialect="oracle",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_COLON,
        supported_parameter_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON},
        type_coercion_map={},  # Oracle specific type mappings
        has_native_list_expansion=False,
        needs_static_script_compilation=True,  # Oracle requires static compilation for scripts
    ),
)

__all__ = ("OracleAsyncDriver", "OracleSyncDriver", "oracledb_statement_config", "oracledb_sync_statement_config")


class OracleSyncCursor:
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

    def __init__(
        self,
        connection: OracleSyncConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        # Set default Oracle-specific configuration
        if statement_config is None:
            statement_config = oracledb_sync_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: OracleSyncConnection) -> OracleSyncCursor:
        return OracleSyncCursor(connection)

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for Oracle-specific special operations.

        Oracle doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.
        """
        return None

    def _execute_many(self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL") -> "ExecutionResult":
        """Oracle executemany implementation."""
        cursor.executemany(sql, prepared_params)

        # For executemany, get row count
        row_count = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL") -> "ExecutionResult":
        """Oracle single execution."""
        cursor.execute(sql, prepared_params or {})

        if statement.returns_rows():
            # Extract data immediately for SELECT operations
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            data = cast("list[dict[str, Any]]", [dict(zip(column_names, row)) for row in fetched_data])

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        # For non-SELECT operations, get row count
        row_count = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(cursor, rowcount_override=row_count)

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


class OracleAsyncCursor:
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

    dialect = "oracle"

    def __init__(
        self,
        connection: OracleAsyncConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            statement_config = oracledb_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "OracleAsyncConnection") -> "OracleAsyncCursor":
        return OracleAsyncCursor(connection)

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for Oracle-specific special operations.

        Oracle doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.
        """
        return None

    async def _execute_many(self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL") -> "ExecutionResult":
        """Oracle async executemany implementation."""
        await cursor.executemany(sql, prepared_params)

        # For executemany, get row count
        row_count = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    async def _execute_statement(
        self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL"
    ) -> "ExecutionResult":
        """Oracle async single execution."""
        await cursor.execute(sql, prepared_params or {})

        if statement.returns_rows():
            # Extract data immediately for SELECT operations
            fetched_data = await cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            data = cast("list[dict[str, Any]]", [dict(zip(column_names, row)) for row in fetched_data])

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        # For non-SELECT operations, get row count
        row_count = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(cursor, rowcount_override=row_count)

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
