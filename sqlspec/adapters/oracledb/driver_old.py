# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast

import oracledb
from oracledb import AsyncCursor, Cursor

from sqlspec.adapters.oracledb._types import OracleAsyncConnection, OracleSyncConnection
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.core.result import SQLResult
    from sqlspec.core.statement import SQL
    from sqlspec.driver._common import ExecutionResult


oracledb_statement_config = StatementConfig(
    dialect="oracle",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_COLON,
        supported_parameter_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON},
        type_coercion_map={},
        has_native_list_expansion=False,
        needs_static_script_compilation=True,
    ),
)

__all__ = ("OracleAsyncDriver", "OracleSyncDriver", "oracledb_statement_config")


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
        super().__init__(
            connection=connection,
            statement_config=statement_config or oracledb_statement_config,
            driver_features=driver_features,
        )

    def with_cursor(self, connection: OracleSyncConnection) -> OracleSyncCursor:
        return OracleSyncCursor(connection)

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for Oracle-specific special operations.

        Oracle doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.
        """
        return None

    def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Oracle executemany implementation."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.executemany(sql, prepared_parameters)
        return self.create_execution_result(
            cursor, rowcount_override=cursor.rowcount if cursor.rowcount is not None else 0, is_many_result=True
        )

    def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Oracle single execution."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, prepared_parameters or {})

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            data = cast("list[dict[str, Any]]", [dict(zip(column_names, row)) for row in fetched_data])

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        return self.create_execution_result(
            cursor, rowcount_override=cursor.rowcount if cursor.rowcount is not None else 0
        )

    def begin(self) -> None:
        """Begin a database transaction."""

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.connection.rollback()

    def commit(self) -> None:
        """Commit the current transaction."""
        self.connection.commit()

    def handle_database_exceptions(self) -> "Generator[None, None, None]":
        """Handle Oracle-specific exceptions and wrap them appropriately."""
        return cast("Generator[None, None, None]", self._handle_database_exceptions_impl())

    @contextmanager
    def _handle_database_exceptions_impl(self) -> "Generator[None, None, None]":
        """Implementation of database exception handling without decorator."""
        try:
            yield
        except oracledb.Error as e:
            msg = f"Oracle database error: {e}"
            raise SQLSpecError(msg) from e
        except Exception as e:
            # Handle any other unexpected errors
            if "parse" in str(e).lower() or "syntax" in str(e).lower():
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected error: {e}"
            raise SQLSpecError(msg) from e


class OracleAsyncCursor:
    def __init__(self, connection: "OracleAsyncConnection") -> None:
        self.connection = connection
        self.cursor: Optional[AsyncCursor] = None

    async def __aenter__(self) -> AsyncCursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.cursor:
            self.cursor.close()


class OracleAsyncDriver(AsyncDriverAdapterBase):
    """Oracle Async Driver Adapter. Refactored for new protocol."""

    dialect = "oracle"

    def __init__(
        self,
        connection: OracleAsyncConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config or oracledb_statement_config,
            driver_features=driver_features,
        )

    def with_cursor(self, connection: "OracleAsyncConnection") -> "OracleAsyncCursor":
        return OracleAsyncCursor(connection)

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for Oracle-specific special operations.

        Oracle doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.
        """
        return None

    async def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Oracle async executemany implementation."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.executemany(sql, prepared_parameters)

        return self.create_execution_result(
            cursor, rowcount_override=cursor.rowcount if cursor.rowcount is not None else 0, is_many_result=True
        )

    async def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Oracle async single execution."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.execute(sql, prepared_parameters or {})

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            data = cast("list[dict[str, Any]]", [dict(zip(column_names, row)) for row in fetched_data])

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        return self.create_execution_result(
            cursor, rowcount_override=cursor.rowcount if cursor.rowcount is not None else 0
        )

    async def begin(self) -> None:
        """Begin a database transaction."""

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.connection.rollback()

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self.connection.commit()

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle Oracle-specific exceptions and wrap them appropriately."""
        return self._handle_database_exceptions_impl()

    @asynccontextmanager
    async def _handle_database_exceptions_impl(self) -> Any:
        """Implementation of database exception handling without decorator."""
        try:
            yield
        except oracledb.Error as e:
            msg = f"Oracle database error: {e}"
            raise SQLSpecError(msg) from e
        except Exception as e:
            # Handle any other unexpected errors
            if "parse" in str(e).lower() or "syntax" in str(e).lower():
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected error: {e}"
            raise SQLSpecError(msg) from e
