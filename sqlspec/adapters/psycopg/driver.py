# ruff: noqa: PLR6301
import logging
from collections.abc import Iterable, Sequence
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional, TypeVar, Union, cast

from psycopg import AsyncConnection, Connection
from psycopg.rows import DictRow, dict_row

from sqlspec.base import (
    AsyncDriverAdapterProtocol,
    CommonDriverAttributes,
    SyncDriverAdapterProtocol,
)
from sqlspec.sql.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin, SyncArrowMixin
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.result import ExecuteResult, SelectResult
from sqlspec.sql.statement import SQLStatement, Statement, StatementConfig
from sqlspec.typing import StatementParameterType

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from sqlspec.sql.filters import StatementFilter

logger = logging.getLogger("sqlspec")

__all__ = ("PsycopgAsyncConnection", "PsycopgAsyncDriver", "PsycopgSyncConnection", "PsycopgSyncDriver")

PsycopgSyncConnection = Connection[DictRow]
PsycopgAsyncConnection = AsyncConnection[DictRow]
ConnectionT = TypeVar("ConnectionT", bound=Union[Connection[Any], AsyncConnection[Any]])


class PsycopgDriverBase(CommonDriverAttributes[ConnectionT], Generic[ConnectionT]):
    """Base class for Psycopg drivers with shared functionality."""

    dialect: str = "postgres"

    def _get_placeholder_style(self) -> ParameterStyle:
        """Return the placeholder style for Psycopg (pyformat named: %(name)s)."""
        return ParameterStyle.PYFORMAT_NAMED


class PsycopgSyncDriver(
    PsycopgDriverBase[PsycopgSyncConnection],
    SQLTranslatorMixin["PsycopgSyncConnection"],
    SyncDriverAdapterProtocol[PsycopgSyncConnection],
    SyncArrowMixin["PsycopgSyncConnection"],
    ResultConverter,
):
    """Psycopg Sync Driver Adapter.

    This driver implements the new unified SQLSpec protocol with the core 3 methods:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations
    - execute_script() - Multi-statement scripts

    Enhanced Features:
    - Full SQLStatement integration with filters and validation
    - PostgreSQL-specific parameter style handling (%(name)s)
    - Comprehensive parameter validation and security checks
    - Support for filter composition (pagination, search, etc.)
    - Uses modern psycopg 3 with enhanced security
    """

    connection: PsycopgSyncConnection
    __supports_arrow__: ClassVar[bool] = False  # Psycopg doesn't support Arrow natively

    def __init__(self, connection: PsycopgSyncConnection, statement_config: Optional[StatementConfig] = None) -> None:
        """Initialize the Psycopg sync driver adapter."""
        super().__init__(connection=connection, statement_config=statement_config)
        self.connection = connection

    @staticmethod
    @contextmanager
    def _get_cursor(connection: PsycopgSyncConnection) -> "Generator[Any, None, None]":
        cursor = connection.cursor(row_factory=dict_row)
        try:
            yield cursor
        finally:
            cursor.close()

    def execute(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[PsycopgSyncConnection] = None,
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
            ...     "SELECT * FROM users WHERE id = %(id)s", {"id": 123}
            ... )
            >>> # Query with filters
            >>> result = driver.execute(
            ...     "SELECT * FROM users",
            ...     LimitOffset(limit=10, offset=0),
            ...     SearchFilter(field_name="name", value="John"),
            ... )
        """
        conn: PsycopgSyncConnection = self._connection(connection)
        config = statement_config or self.statement_config

        stmt = SQLStatement(statement, parameters, *filters, dialect=self.dialect, statement_config=config, **kwargs)
        stmt.validate()

        final_sql = stmt.to_sql(placeholder_style=self._get_placeholder_style())
        ordered_params = stmt.get_parameters(style=self._get_placeholder_style())

        # Convert parameters to dict format for psycopg using simplified logic
        psycopg_params_dict: dict[str, Any] = {}
        if ordered_params is not None:
            if isinstance(ordered_params, dict):
                psycopg_params_dict = ordered_params
            elif isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                # For non-dict iterables, create indexed parameters
                psycopg_params_dict = {f"param_{i}": v for i, v in enumerate(ordered_params)}
            else:
                psycopg_params_dict = {"param_0": ordered_params}

        with self._get_cursor(conn) as cursor:
            logger.debug("Executing SQL (Psycopg Sync): %s with params: %s", final_sql, psycopg_params_dict)
            cursor.execute(final_sql, psycopg_params_dict)

            if self.returns_rows(stmt.expression):
                fetched_data: list[dict[str, Any]] = cursor.fetchall()
                column_names = [col.name for col in cursor.description or []]
                raw_result_data = fetched_data[0] if fetched_data else cast("dict[str, Any]", {})
                return SelectResult(
                    raw_result=raw_result_data,
                    rows=fetched_data,
                    column_names=column_names,
                )

            operation_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                operation_type = str(stmt.expression.key).upper()

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}),
                rows_affected=cursor.rowcount if cursor.rowcount is not None else -1,
                operation_type=operation_type,
            )

    def execute_many(
        self,
        statement: "Statement",
        parameters: Optional[Sequence[StatementParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional[PsycopgSyncConnection] = None,
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
            ...     "INSERT INTO users (name, email) VALUES (%(name)s, %(email)s)",
            ...     [
            ...         {"name": "John", "email": "john@example.com"},
            ...         {"name": "Jane", "email": "jane@example.com"},
            ...     ],
            ... )
        """
        conn: PsycopgSyncConnection = self._connection(connection)
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
        adapted_parameters_sequence: list[dict[str, Any]] = []
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
                    adapted_parameters_sequence.append(item_params_dict)
                elif item_params_dict is None:
                    adapted_parameters_sequence.append({})
                else:
                    logger.warning("execute_many expected dict for PYFORMAT_NAMED, got %s", type(item_params_dict))
                    adapted_parameters_sequence.append({})

        if not param_sequence:
            return ExecuteResult(raw_result=cast("dict[str, Any]", {}), rows_affected=0, operation_type="EXECUTE")

        with self._get_cursor(conn) as cursor:
            cursor.executemany(final_sql, adapted_parameters_sequence)
            affected_rows = cursor.rowcount if cursor.rowcount is not None else -1

        operation_type = "EXECUTE"
        if template_stmt.expression and hasattr(template_stmt.expression, "key"):
            operation_type = str(template_stmt.expression.key).upper()

        return ExecuteResult(
            raw_result=cast("dict[str, Any]", {}),
            rows_affected=affected_rows,
            operation_type=operation_type,
        )

    def execute_script(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[PsycopgSyncConnection] = None,
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
        conn: PsycopgSyncConnection = self._connection(connection)
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

        logger.debug("Executing script (Psycopg Sync): %s", final_sql)
        with self._get_cursor(conn) as cursor:
            cursor.execute(final_sql)
            current_status = cursor.statusmessage
            return current_status or "SCRIPT EXECUTED"

    def _connection(self, connection: Optional[PsycopgSyncConnection] = None) -> PsycopgSyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection


class PsycopgAsyncDriver(
    PsycopgDriverBase[PsycopgAsyncConnection],
    SQLTranslatorMixin["PsycopgAsyncConnection"],
    AsyncDriverAdapterProtocol[PsycopgAsyncConnection],
    AsyncArrowMixin["PsycopgAsyncConnection"],
    ResultConverter,
):
    """Psycopg Async Driver Adapter.

    This driver implements the new unified SQLSpec protocol with the core 3 methods:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations
    - execute_script() - Multi-statement scripts

    Enhanced Features:
    - Full SQLStatement integration with filters and validation
    - PostgreSQL-specific parameter style handling (%(name)s)
    - Comprehensive parameter validation and security checks
    - Support for filter composition (pagination, search, etc.)
    - Uses modern psycopg 3 with enhanced security
    """

    connection: PsycopgAsyncConnection
    __supports_arrow__: ClassVar[bool] = False  # Psycopg doesn't support Arrow natively

    def __init__(self, connection: PsycopgAsyncConnection, statement_config: Optional[StatementConfig] = None) -> None:
        """Initialize the Psycopg async driver adapter."""
        super().__init__(connection=connection, statement_config=statement_config)
        self.connection = connection

    @staticmethod
    @asynccontextmanager
    async def _get_cursor(connection: PsycopgAsyncConnection) -> "AsyncGenerator[Any, None]":
        """Get an async cursor for the connection."""
        cursor = connection.cursor(row_factory=dict_row)
        try:
            yield cursor
        finally:
            await cursor.close()

    async def execute(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[PsycopgAsyncConnection] = None,
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
            ...     "SELECT * FROM users WHERE id = %(id)s", {"id": 123}
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

        # Convert parameters to dict format for psycopg using simplified logic
        psycopg_params_dict: dict[str, Any] = {}
        if ordered_params is not None:
            if isinstance(ordered_params, dict):
                psycopg_params_dict = ordered_params
            elif isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                # For non-dict iterables, create indexed parameters
                psycopg_params_dict = {f"param_{i}": v for i, v in enumerate(ordered_params)}
            else:
                psycopg_params_dict = {"param_0": ordered_params}

        async with self._get_cursor(conn) as cursor:
            logger.debug("Executing SQL (Psycopg Async): %s with params: %s", final_sql, psycopg_params_dict)
            await cursor.execute(final_sql, psycopg_params_dict)

            if self.returns_rows(stmt.expression):
                fetched_data: list[dict[str, Any]] = await cursor.fetchall()
                column_names = [col.name for col in cursor.description or []]
                raw_result_data = fetched_data[0] if fetched_data else cast("dict[str, Any]", {})
                return SelectResult(
                    raw_result=raw_result_data,
                    rows=fetched_data,
                    column_names=column_names,
                )

            operation_type = "UNKNOWN"
            if stmt.expression and hasattr(stmt.expression, "key"):
                operation_type = str(stmt.expression.key).upper()

            return ExecuteResult(
                raw_result=cast("dict[str, Any]", {}),
                rows_affected=cursor.rowcount if cursor.rowcount is not None else -1,
                operation_type=operation_type,
            )

    async def execute_many(
        self,
        statement: "Statement",
        parameters: Optional[Sequence[StatementParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional[PsycopgAsyncConnection] = None,
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
            ...     "INSERT INTO users (name, email) VALUES (%(name)s, %(email)s)",
            ...     [
            ...         {"name": "John", "email": "john@example.com"},
            ...         {"name": "Jane", "email": "jane@example.com"},
            ...     ],
            ... )
        """
        conn: PsycopgAsyncConnection = self._connection(connection)
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
        adapted_parameters_sequence: list[dict[str, Any]] = []
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
                    adapted_parameters_sequence.append(item_params_dict)
                elif item_params_dict is None:
                    adapted_parameters_sequence.append({})
                else:
                    logger.warning("execute_many expected dict for PYFORMAT_NAMED, got %s", type(item_params_dict))
                    adapted_parameters_sequence.append({})

        if not param_sequence:
            return ExecuteResult(raw_result=cast("dict[str, Any]", {}), rows_affected=0, operation_type="EXECUTE")

        async with self._get_cursor(conn) as cursor:
            await cursor.executemany(final_sql, adapted_parameters_sequence)
            affected_rows = cursor.rowcount if cursor.rowcount is not None else -1

        operation_type = "EXECUTE"
        if template_stmt.expression and hasattr(template_stmt.expression, "key"):
            operation_type = str(template_stmt.expression.key).upper()

        return ExecuteResult(
            raw_result=cast("dict[str, Any]", {}),
            rows_affected=affected_rows,
            operation_type=operation_type,
        )

    async def execute_script(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[PsycopgAsyncConnection] = None,
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
        conn: PsycopgAsyncConnection = self._connection(connection)
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

        logger.debug("Executing script (Psycopg Async): %s", final_sql)
        async with self._get_cursor(conn) as cursor:
            await cursor.execute(final_sql)
            current_status = cursor.statusmessage
            return current_status or "SCRIPT EXECUTED"

    def _connection(self, connection: Optional[PsycopgAsyncConnection] = None) -> PsycopgAsyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
