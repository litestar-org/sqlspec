import logging
from collections.abc import Iterable, Sequence
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional, TypeVar, Union, cast

import pyarrow as pa
from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor

from sqlspec.base import (
    AsyncDriverAdapterProtocol,
    CommonDriverAttributes,
    SyncDriverAdapterProtocol,
)
from sqlspec.sql.mixins import (
    AsyncArrowMixin,
    ResultConverter,
    SQLTranslatorMixin,
    SyncArrowMixin,
)
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.sql.statement import SQLStatement, Statement, StatementConfig
from sqlspec.typing import StatementParameterType
from sqlspec.utils.sync_tools import ensure_async_

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from sqlspec.sql.filters import StatementFilter

__all__ = ("OracleAsyncConnection", "OracleAsyncDriver", "OracleSyncConnection", "OracleSyncDriver")

OracleSyncConnection = Connection
OracleAsyncConnection = AsyncConnection
ConnectionT = TypeVar("ConnectionT", bound=Union[Connection, AsyncConnection])

logger = logging.getLogger("sqlspec")


class OracleDriverBase(CommonDriverAttributes[ConnectionT], Generic[ConnectionT]):
    """Base class for Oracle drivers with shared functionality."""

    dialect: str = "oracle"

    def _get_placeholder_style(self) -> ParameterStyle:
        """Return the placeholder style for Oracle (:name)."""
        return ParameterStyle.NAMED_COLON


class OracleSyncDriver(
    OracleDriverBase[OracleSyncConnection],
    SyncArrowMixin["OracleSyncConnection"],
    SQLTranslatorMixin["OracleSyncConnection"],
    SyncDriverAdapterProtocol["OracleSyncConnection"],
    ResultConverter,
):
    """Oracle Sync Driver Adapter.

    This driver implements the new unified SQLSpec protocol with the core 3 methods:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations
    - execute_script() - Multi-statement scripts

    Enhanced Features:
    - Full SQLStatement integration with filters and validation
    - Oracle-specific parameter style handling (:name)
    - Comprehensive parameter validation and security checks
    - Support for filter composition (pagination, search, etc.)
    - Native Arrow support via python-oracledb data frames
    - Modern python-oracledb (successor to cx_Oracle)
    """

    connection: "OracleSyncConnection"
    __supports_arrow__: ClassVar[bool] = True  # Oracle supports Arrow via data frames

    def __init__(self, connection: "OracleSyncConnection", statement_config: Optional[StatementConfig] = None) -> None:
        """Initialize the Oracle sync driver adapter."""
        super().__init__(connection=connection, statement_config=statement_config)

    @contextmanager
    def _get_cursor(self, connection: Optional[OracleSyncConnection] = None) -> "Generator[Cursor, None, None]":
        """Get a cursor for the connection."""
        conn_to_use = connection or self.connection
        cursor: Cursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def execute(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[OracleSyncConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> "Union[SelectResult[dict[str, Any]], ExecuteResult[dict[str, Any]]]":
        """Execute a SQL statement and return a StatementResult.

        This is the unified method for all SQL operations (SELECT, INSERT, UPDATE, DELETE).
        Use the StatementResult methods to extract the data you need.

        Args:
            statement: The SQL statement to execute.
            parameters: Parameters for the statement.
            *filters: Statement filters to apply (e.g., pagination, search filters).
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            A StatementResult containing the operation results.

        Example:
            >>> from sqlspec.sql.filters import LimitOffset, SearchFilter
            >>> # Basic query
            >>> result = driver.execute(
            ...     "SELECT * FROM users WHERE id = :id", {"id": 123}
            ... )
            >>> # Query with filters
            >>> result = driver.execute(
            ...     "SELECT * FROM users",
            ...     LimitOffset(limit=10, offset=0),
            ...     SearchFilter(field_name="name", value="John"),
            ... )
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        stmt = SQLStatement(statement, parameters, *filters, dialect=self.dialect, statement_config=config, **kwargs)
        stmt.validate()

        final_sql = stmt.to_sql(placeholder_style=self._get_placeholder_style())
        ordered_params = stmt.get_parameters(style=self._get_placeholder_style())

        # Convert parameters to dict format for Oracle using simplified logic
        oracle_params_dict: dict[str, Any] = {}
        if ordered_params is not None:
            if isinstance(ordered_params, dict):
                oracle_params_dict = ordered_params
            elif isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                # For non-dict iterables, create indexed parameters
                oracle_params_dict = {f"param_{i}": v for i, v in enumerate(ordered_params)}
            else:
                oracle_params_dict = {"param_0": ordered_params}

        with self._get_cursor(conn) as cursor:
            cursor.execute(final_sql, oracle_params_dict)

            if self.returns_rows(stmt.expression):
                raw_data_tuples = cursor.fetchall()
                if not raw_data_tuples:
                    return SelectResult(raw_result=cast("dict[str, Any]", {}), rows=[], column_names=[])
                column_names = [col[0] for col in cursor.description or []]
                rows = [dict(zip(column_names, row)) for row in raw_data_tuples]
                return SelectResult(
                    raw_result=rows[0] if rows else cast("dict[str, Any]", {}),
                    rows=rows,
                    column_names=column_names,
                )

            rows_affected = cursor.rowcount if cursor.rowcount is not None else -1
            operation_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                operation_type = str(stmt.expression.key).upper()

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}),
                rows_affected=rows_affected,
                operation_type=operation_type,
            )

    def execute_many(
        self,
        statement: "Statement",
        parameters: Optional[Sequence[StatementParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional[OracleSyncConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> "ExecuteResult[dict[str, Any]]":
        """Execute a SQL statement with multiple parameter sets.

        Useful for batch INSERT, UPDATE, or DELETE operations.

        Args:
            statement: The SQL statement to execute.
            parameters: Sequence of parameter sets.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            An ExecuteResult containing the batch operation results.

        Example:
            >>> # Batch insert with validation
            >>> driver.execute_many(
            ...     "INSERT INTO users (name, email) VALUES (:name, :email)",
            ...     [
            ...         {"name": "John", "email": "john@example.com"},
            ...         {"name": "Jane", "email": "jane@example.com"},
            ...     ],
            ... )
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        # Create template statement with filters for validation
        template_stmt = SQLStatement(
            statement,
            None,  # No parameters for template
            *filters,
            dialect=self.dialect,
            statement_config=config,
            **kwargs,
        )

        template_stmt.validate()
        final_sql = template_stmt.to_sql(placeholder_style=self._get_placeholder_style())

        # Process parameter sets
        all_oracle_params: list[dict[str, Any]] = []
        param_sequence = parameters if parameters is not None else []

        if param_sequence:
            # Create a building config that skips validation for individual parameter sets
            building_config = replace(config or StatementConfig(), enable_validation=False)

            for param_set in param_sequence:
                item_stmt = SQLStatement(
                    template_stmt.sql,  # Use processed SQL from template
                    param_set,
                    dialect=self.dialect,
                    statement_config=building_config,
                )
                item_params_dict = item_stmt.get_parameters(style=self._get_placeholder_style())

                if isinstance(item_params_dict, dict):
                    all_oracle_params.append(item_params_dict)
                elif item_params_dict is None:
                    all_oracle_params.append({})
                else:
                    logger.warning("execute_many expected dict for NAMED_COLON, got %s", type(item_params_dict))
                    all_oracle_params.append({})

        if not param_sequence:
            return ExecuteResult(raw_result=cast("dict[str, Any]", {}), rows_affected=0, operation_type="EXECUTE")

        with self._get_cursor(conn) as cursor:
            cursor.executemany(final_sql, all_oracle_params)
            raw_rowcount = cursor.rowcount
            rows_affected = -1

            # Oracle rowcount can be a list or int
            if isinstance(raw_rowcount, list):
                rows_affected = sum(raw_rowcount)
            elif isinstance(raw_rowcount, int):
                rows_affected = raw_rowcount
            if rows_affected == -1 and all_oracle_params:
                rows_affected = len(all_oracle_params)

        operation_type = "EXECUTE"
        if template_stmt.expression and hasattr(template_stmt.expression, "key"):
            operation_type = str(template_stmt.expression.key).upper()

        return ExecuteResult(
            raw_result=cast("dict[str, Any]", {}),
            rows_affected=rows_affected,
            operation_type=operation_type,
        )

    def execute_script(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[OracleSyncConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> str:
        """Execute a multi-statement SQL script.

        For script execution, parameters are rendered as static literals directly
        in the SQL rather than using placeholders, since scripts may contain
        multiple statements that don't support parameterization.

        Args:
            statement: The SQL script to execute.
            parameters: Parameters for the script.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            A string with execution results/output.
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        merged_params = parameters
        if kwargs:
            if merged_params is None:
                merged_params = kwargs
            elif isinstance(merged_params, dict):
                merged_params = {**merged_params, **kwargs}

        stmt = SQLStatement(statement, merged_params, *filters, dialect=self.dialect, statement_config=config)
        stmt.validate()
        final_sql = stmt.to_sql(placeholder_style=ParameterStyle.STATIC)

        with self._get_cursor(conn) as cursor:
            cursor.execute(final_sql)
            return "SCRIPT EXECUTED"

    def select_to_arrow(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[OracleSyncConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Execute a SELECT statement and return results as an Apache Arrow Table.

        This method leverages python-oracledb's native data frame support to provide
        high-performance Arrow integration for analytics workloads.

        Args:
            statement: The SQL query to execute.
            parameters: Parameters for the query.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Raises:
            TypeError: If the SQL statement is not a valid query.

        Returns:
            An ArrowResult containing the query results as an Arrow Table.
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        stmt = SQLStatement(statement, parameters, *filters, dialect=self.dialect, statement_config=config, **kwargs)
        stmt.validate()

        if not self.returns_rows(stmt.expression):
            op_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                op_type = str(stmt.expression.key).upper()
            msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
            raise TypeError(msg)

        final_sql = stmt.to_sql(placeholder_style=self._get_placeholder_style())
        ordered_params = stmt.get_parameters(style=self._get_placeholder_style())

        # Convert parameters to dict format for Oracle using simplified logic
        oracle_params_dict: dict[str, Any] = {}
        if ordered_params is not None:
            if isinstance(ordered_params, dict):
                oracle_params_dict = ordered_params
            elif isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                oracle_params_dict = {f"param_{i}": v for i, v in enumerate(ordered_params)}
            else:
                oracle_params_dict = {"param_0": ordered_params}

        with self._get_cursor(conn) as cursor:
            cursor.execute(final_sql, oracle_params_dict)
            rows = cursor.fetchall()
            if not rows:
                return ArrowResult(raw_result=pa.Table.from_arrays([], names=[]))
            column_names = [col[0] for col in cursor.description or []]
            list_of_cols = list(zip(*rows)) if rows else [[] for _ in column_names]
            arrow_table = pa.Table.from_arrays(list_of_cols, names=column_names)
            return ArrowResult(raw_result=arrow_table)

    def _connection(self, connection: Optional[OracleSyncConnection] = None) -> OracleSyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection


class OracleAsyncDriver(
    OracleDriverBase["OracleAsyncConnection"],
    AsyncArrowMixin["OracleAsyncConnection"],
    SQLTranslatorMixin["OracleAsyncConnection"],
    AsyncDriverAdapterProtocol["OracleAsyncConnection"],
    ResultConverter,
):
    """Oracle Async Driver Adapter.

    This driver implements the new unified SQLSpec protocol with the core 3 methods:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations
    - execute_script() - Multi-statement scripts

    Enhanced Features:
    - Full SQLStatement integration with filters and validation
    - Oracle-specific parameter style handling (:name)
    - Comprehensive parameter validation and security checks
    - Support for filter composition (pagination, search, etc.)
    - Native Arrow support via python-oracledb data frames
    - Modern python-oracledb (successor to cx_Oracle)
    """

    connection: "OracleAsyncConnection"
    __supports_arrow__: ClassVar[bool] = True  # Oracle supports Arrow via data frames

    def __init__(self, connection: "OracleAsyncConnection", statement_config: Optional[StatementConfig] = None) -> None:
        """Initialize the Oracle async driver adapter."""
        super().__init__(connection=connection, statement_config=statement_config)

    @asynccontextmanager
    async def _get_cursor(
        self, connection: Optional[OracleAsyncConnection] = None
    ) -> "AsyncGenerator[AsyncCursor, None]":
        """Get an async cursor for the connection."""
        conn_to_use = connection or self.connection
        cursor: AsyncCursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            await ensure_async_(cursor.close)()

    async def execute(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[OracleAsyncConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> "Union[SelectResult[dict[str, Any]], ExecuteResult[dict[str, Any]]]":
        """Execute a SQL statement and return a StatementResult.

        This is the unified method for all SQL operations (SELECT, INSERT, UPDATE, DELETE).
        Use the StatementResult methods to extract the data you need.

        Args:
            statement: The SQL statement to execute.
            parameters: Parameters for the statement.
            *filters: Statement filters to apply (e.g., pagination, search filters).
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            A StatementResult containing the operation results.

        Example:
            >>> from sqlspec.sql.filters import LimitOffset, SearchFilter
            >>> # Basic query
            >>> result = await driver.execute(
            ...     "SELECT * FROM users WHERE id = :id", {"id": 123}
            ... )
            >>> # Query with filters
            >>> result = await driver.execute(
            ...     "SELECT * FROM users",
            ...     LimitOffset(limit=10, offset=0),
            ...     SearchFilter(field_name="name", value="John"),
            ... )
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        stmt = SQLStatement(statement, parameters, *filters, dialect=self.dialect, statement_config=config, **kwargs)
        stmt.validate()

        final_sql = stmt.to_sql(placeholder_style=self._get_placeholder_style())
        ordered_params = stmt.get_parameters(style=self._get_placeholder_style())

        # Convert parameters to dict format for Oracle using simplified logic
        oracle_params_dict: dict[str, Any] = {}
        if ordered_params is not None:
            if isinstance(ordered_params, dict):
                oracle_params_dict = ordered_params
            elif isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                oracle_params_dict = {f"param_{i}": v for i, v in enumerate(ordered_params)}
            else:
                oracle_params_dict = {"param_0": ordered_params}

        async with self._get_cursor(conn) as cursor:
            await cursor.execute(final_sql, oracle_params_dict)

            if self.returns_rows(stmt.expression):
                rows = await cursor.fetchall()
                if not rows:
                    return SelectResult(raw_result=cast("dict[str, Any]", {}), rows=[], column_names=[])
                column_names = [col[0] for col in cursor.description or []]
                dict_rows = [dict(zip(column_names, row)) for row in rows]
                return SelectResult(
                    raw_result=dict_rows[0] if dict_rows else cast("dict[str, Any]", {}),
                    rows=dict_rows,
                    column_names=column_names,
                )

            rows_affected = cursor.rowcount if cursor.rowcount is not None else -1
            operation_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                operation_type = str(stmt.expression.key).upper()

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}),
                rows_affected=rows_affected,
                operation_type=operation_type,
            )

    async def execute_many(
        self,
        statement: "Statement",
        parameters: Optional[Sequence[StatementParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional[OracleAsyncConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> "ExecuteResult[dict[str, Any]]":
        """Execute a SQL statement with multiple parameter sets.

        Useful for batch INSERT, UPDATE, or DELETE operations.

        Args:
            statement: The SQL statement to execute.
            parameters: Sequence of parameter sets.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            An ExecuteResult containing the batch operation results.

        Example:
            >>> # Batch insert with validation
            >>> await driver.execute_many(
            ...     "INSERT INTO users (name, email) VALUES (:name, :email)",
            ...     [
            ...         {"name": "John", "email": "john@example.com"},
            ...         {"name": "Jane", "email": "jane@example.com"},
            ...     ],
            ... )
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        # Create template statement with filters for validation
        template_stmt = SQLStatement(
            statement,
            None,  # No parameters for template
            *filters,
            dialect=self.dialect,
            statement_config=config,
            **kwargs,
        )

        template_stmt.validate()
        final_sql = template_stmt.to_sql(placeholder_style=self._get_placeholder_style())

        # Process parameter sets
        all_oracle_params: list[dict[str, Any]] = []
        param_sequence = parameters if parameters is not None else []

        if param_sequence:
            # Create a building config that skips validation for individual parameter sets
            building_config = replace(config or StatementConfig(), enable_validation=False)

            for param_set in param_sequence:
                item_stmt = SQLStatement(
                    template_stmt.sql,  # Use processed SQL from template
                    param_set,
                    dialect=self.dialect,
                    statement_config=building_config,
                )
                item_params_dict = item_stmt.get_parameters(style=self._get_placeholder_style())

                if isinstance(item_params_dict, dict):
                    all_oracle_params.append(item_params_dict)
                elif item_params_dict is None:
                    all_oracle_params.append({})
                else:
                    logger.warning("execute_many expected dict for NAMED_COLON, got %s", type(item_params_dict))
                    all_oracle_params.append({})

        if not param_sequence:
            return ExecuteResult(raw_result=cast("dict[str, Any]", {}), rows_affected=0, operation_type="EXECUTE")

        async with self._get_cursor(conn) as cursor:
            await cursor.executemany(final_sql, all_oracle_params)
            raw_rowcount = cursor.rowcount
            rows_affected = -1

            # Oracle rowcount can be a list or int
            if isinstance(raw_rowcount, list):
                rows_affected = sum(raw_rowcount)
            elif isinstance(raw_rowcount, int):
                rows_affected = raw_rowcount
            if rows_affected == -1 and all_oracle_params:
                rows_affected = len(all_oracle_params)

        operation_type = "EXECUTE"
        if template_stmt.expression and hasattr(template_stmt.expression, "key"):
            operation_type = str(template_stmt.expression.key).upper()

        return ExecuteResult(
            raw_result=cast("dict[str, Any]", {}),
            rows_affected=rows_affected,
            operation_type=operation_type,
        )

    async def execute_script(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[OracleAsyncConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> str:
        """Execute a multi-statement SQL script.

        For script execution, parameters are rendered as static literals directly
        in the SQL rather than using placeholders, since scripts may contain
        multiple statements that don't support parameterization.

        Args:
            statement: The SQL script to execute.
            parameters: Parameters for the script.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            A string with execution results/output.
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        merged_params = parameters
        if kwargs:
            if merged_params is None:
                merged_params = kwargs
            elif isinstance(merged_params, dict):
                merged_params = {**merged_params, **kwargs}

        stmt = SQLStatement(statement, merged_params, *filters, dialect=self.dialect, statement_config=config)
        stmt.validate()
        final_sql = stmt.to_sql(placeholder_style=ParameterStyle.STATIC)

        async with self._get_cursor(conn) as cursor:
            await cursor.execute(final_sql)
            return "SCRIPT EXECUTED"

    async def select_to_arrow(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[OracleAsyncConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Execute a SELECT statement and return results as an Apache Arrow Table.

        This method leverages python-oracledb's native data frame support to provide
        high-performance Arrow integration for analytics workloads.

        Args:
            statement: The SQL query to execute.
            parameters: Parameters for the query.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Raises:
            TypeError: If the SQL statement is not a valid query.

        Returns:
            An ArrowResult containing the query results as an Arrow Table.
        """
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        stmt = SQLStatement(statement, parameters, *filters, dialect=self.dialect, statement_config=config, **kwargs)
        stmt.validate()

        if not self.returns_rows(stmt.expression):
            op_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                op_type = str(stmt.expression.key).upper()
            msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
            raise TypeError(msg)

        final_sql = stmt.to_sql(placeholder_style=self._get_placeholder_style())
        ordered_params = stmt.get_parameters(style=self._get_placeholder_style())

        # Convert parameters to dict format for Oracle using simplified logic
        oracle_params_dict: dict[str, Any] = {}
        if ordered_params is not None:
            if isinstance(ordered_params, dict):
                oracle_params_dict = ordered_params
            elif isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                oracle_params_dict = {f"param_{i}": v for i, v in enumerate(ordered_params)}
            else:
                oracle_params_dict = {"param_0": ordered_params}

        async with self._get_cursor(conn) as cursor:
            await cursor.execute(final_sql, oracle_params_dict)
            rows = await cursor.fetchall()
            if not rows:
                return ArrowResult(raw_result=pa.Table.from_arrays([], names=[]))
            column_names = [col[0] for col in cursor.description or []]
            list_of_cols = list(zip(*rows)) if rows else [[] for _ in column_names]
            arrow_table = pa.Table.from_arrays(list_of_cols, names=column_names)
            return ArrowResult(raw_result=arrow_table)

    def _connection(self, connection: Optional[OracleAsyncConnection] = None) -> OracleAsyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
