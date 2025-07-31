# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any, Optional, cast

from oracledb import AsyncCursor, Cursor

from sqlspec.adapters.oracledb._types import OracleAsyncConnection, OracleSyncConnection
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import StatementConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQL

__all__ = ("OracleAsyncDriver", "OracleSyncDriver")

logger = get_logger("adapters.oracledb")


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
        from sqlspec.statement.sql import StatementConfig

        # Set default Oracle-specific configuration
        if statement_config is None:
            from sqlspec.parameters.config import ParameterStyleConfig
            
            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.NAMED_COLON,
                supported_parameter_styles={ParameterStyle.NAMED_COLON},
                type_coercion_map={},  # Oracle specific type mappings
                has_native_list_expansion=False,
                needs_static_script_compilation=True,  # Oracle requires static compilation for scripts
            )
            statement_config = StatementConfig(dialect="oracle", parameter_config=parameter_config)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: OracleSyncConnection) -> OracleSyncCursor:
        return OracleSyncCursor(connection)

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[tuple[Any, Optional[int], Any]]":
        """Hook for Oracle-specific special operations.
        
        Oracle doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.
        """
        return None

    def _execute_many(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """Oracle executemany implementation."""
        cursor.executemany(sql, prepared_params)
        return cursor

    def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """Oracle single execution."""
        cursor.execute(sql, prepared_params or {})
        return cursor

    def _get_selected_data(self, cursor: Cursor) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]
        data = cast("list[dict[str, Any]]", [dict(zip(column_names, row)) for row in fetched_data])
        return data, column_names, len(data)

    def _get_row_count(self, cursor: Cursor) -> int:
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
            from sqlspec.parameters.config import ParameterStyleConfig
            
            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.NAMED_COLON,
                supported_parameter_styles={ParameterStyle.NAMED_COLON},
                type_coercion_map={},  # Oracle specific type mappings
                has_native_list_expansion=False,
                needs_static_script_compilation=True,  # Oracle requires static compilation for scripts
            )
            statement_config = StatementConfig(dialect="oracle", parameter_config=parameter_config)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "OracleAsyncConnection") -> "OracleAsyncCursor":
        return OracleAsyncCursor(connection)

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[tuple[Any, Optional[int], Any]]":
        """Hook for Oracle-specific special operations.
        
        Oracle doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.
        """
        return None

    async def _execute_many(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """Oracle async executemany implementation."""
        await cursor.executemany(sql, prepared_params)
        return cursor

    async def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """Oracle async single execution."""
        await cursor.execute(sql, prepared_params or {})
        return cursor

    async def _get_selected_data(self, cursor: AsyncCursor) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = await cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]
        data = cast("list[dict[str, Any]]", [dict(zip(column_names, row)) for row in fetched_data])
        return data, column_names, len(data)

    def _get_row_count(self, cursor: AsyncCursor) -> int:
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
