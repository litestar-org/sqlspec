# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any, Optional, cast

from oracledb import AsyncCursor, Cursor

from sqlspec.adapters.oracledb._types import OracleAsyncConnection, OracleSyncConnection
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQL, StatementConfig

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

    def __init__(self, connection: OracleSyncConnection, statement_config: "Optional[StatementConfig]" = None, driver_features: "Optional[dict[str, Any]]" = None) -> None:
        from sqlspec.statement.sql import StatementConfig

        # Set default Oracle-specific configuration
        if statement_config is None:
            statement_config = StatementConfig(
                dialect="oracle",
                supported_parameter_styles={ParameterStyle.NAMED_COLON},
                default_parameter_style=ParameterStyle.NAMED_COLON,
                type_coercion_map={
                    # Oracle specific type mappings
                },
                has_native_list_expansion=False,
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: OracleSyncConnection) -> OracleSyncCursor:
        return OracleSyncCursor(connection)

    def _perform_execute(self, cursor: Cursor, statement: "SQL") -> None:
        if statement.is_script:
            # Scripts use STATIC compilation to transpile parameters automatically
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            # Oracle doesn't have executescript - execute statements one by one
            statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    cursor.execute(stmt)
        else:
            # With force_style_conversion=True, always use the default parameter style
            sql, params = self._get_compiled_sql(statement, self.statement_config)

            if statement.is_many:
                # For execute_many, params is already a list of parameter sets
                prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=True)
                cursor.executemany(sql, prepared_params)
            else:
                prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=False)
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

    def __init__(self, connection: OracleAsyncConnection, statement_config: "Optional[StatementConfig]" = None, driver_features: "Optional[dict[str, Any]]" = None) -> None:
        if statement_config is None:
            statement_config = StatementConfig(
                dialect="oracle",
                supported_parameter_styles={ParameterStyle.NAMED_COLON},
                default_parameter_style=ParameterStyle.NAMED_COLON,
                type_coercion_map={
                    # Oracle specific type mappings
                },
                has_native_list_expansion=False,
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "OracleAsyncConnection") -> "OracleAsyncCursor":
        return OracleAsyncCursor(connection)

    async def _perform_execute(self, cursor: AsyncCursor, statement: "SQL") -> None:
        if statement.is_script:
            # Scripts use STATIC compilation to transpile parameters automatically
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            # Oracle doesn't have executescript - execute statements one by one
            statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    await cursor.execute(stmt)
        else:
            # With force_style_conversion=True, always use the default parameter style
            sql, params = self._get_compiled_sql(statement, self.statement_config)

            if statement.is_many:
                # For execute_many, params is already a list of parameter sets
                prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=True)
                await cursor.executemany(sql, prepared_params)
            else:
                prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=False)
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
