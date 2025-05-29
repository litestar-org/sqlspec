import datetime
import logging
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import replace
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Optional,
    Union,
    cast,
)

import sqlglot
from google.cloud.bigquery import (
    ArrayQueryParameter,
    Client,
    QueryJob,
    QueryJobConfig,
    ScalarQueryParameter,
)

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.exceptions import (
    ParameterStyleMismatchError,
    SQLSpecError,
)
from sqlspec.sql.mixins import (
    ResultConverter,
    SQLTranslatorMixin,
    SyncArrowMixin,
    SyncParquetMixin,
)
from sqlspec.sql.parameters import ParameterStyle
from sqlspec.sql.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.sql.statement import SQLStatement, Statement, StatementConfig
from sqlspec.typing import StatementParameterType

if TYPE_CHECKING:
    from google.cloud.bigquery.table import Row as BigQueryRow

    from sqlspec.sql.filters import StatementFilter


__all__ = ("BigQueryConnection", "BigQueryDriver")

BigQueryConnection = Client

logger = logging.getLogger("sqlspec")


class BigQueryDriver(
    SyncDriverAdapterProtocol["BigQueryConnection"],
    SyncArrowMixin["BigQueryConnection"],
    SyncParquetMixin["BigQueryConnection"],
    SQLTranslatorMixin["BigQueryConnection"],
    ResultConverter,
):
    """Synchronous BigQuery Driver Adapter.

    This driver implements the new unified SQLSpec protocol with the core 3 methods:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations
    - execute_script() - Multi-statement scripts

    Enhanced Features:
    - Full SQLStatement integration with filters and validation
    - BigQuery-specific parameter style handling (@name)
    - Comprehensive parameter validation and security checks
    - Support for filter composition (pagination, search, etc.)
    - Native Arrow and Parquet support for analytics workloads
    """

    dialect: str = "bigquery"
    connection: "BigQueryConnection"
    __supports_arrow__: ClassVar[bool] = True
    _default_query_job_config: Optional[QueryJobConfig]

    def __init__(
        self, connection: "BigQueryConnection", statement_config: Optional[StatementConfig] = None, **kwargs: Any
    ) -> None:
        super().__init__(connection=connection, statement_config=statement_config)

        default_config_kwarg = kwargs.get("default_query_job_config")
        conn_default_config = getattr(connection, "default_query_job_config", None)

        # Simplified config handling
        if default_config_kwarg is not None and isinstance(default_config_kwarg, QueryJobConfig):
            self._default_query_job_config = default_config_kwarg
        elif conn_default_config is not None and isinstance(conn_default_config, QueryJobConfig):
            self._default_query_job_config = conn_default_config
        else:
            self._default_query_job_config = None

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.NAMED_AT

    @staticmethod
    def _get_bq_param_type(value: Any) -> "tuple[Optional[str], Optional[str]]":
        if isinstance(value, bool):
            return "BOOL", None
        if isinstance(value, int):
            return "INT64", None
        if isinstance(value, float):
            return "FLOAT64", None
        if isinstance(value, Decimal):
            return "BIGNUMERIC", None
        if isinstance(value, str):
            return "STRING", None
        if isinstance(value, bytes):
            return "BYTES", None
        if isinstance(value, datetime.date):
            return "DATE", None
        if isinstance(value, datetime.datetime):
            return "TIMESTAMP" if value.tzinfo else "DATETIME", None
        if isinstance(value, datetime.time):
            return "TIME", None
        if isinstance(value, (list, tuple)):
            if not value:
                msg = "Cannot determine BigQuery ARRAY type for empty sequence. Provide typed empty array or ensure context implies type."
                raise SQLSpecError(msg)
            first_element = value[0]
            element_type, _ = BigQueryDriver._get_bq_param_type(first_element)
            if element_type is None:
                msg = f"Unsupported element type in ARRAY: {type(first_element)}"
                raise SQLSpecError(msg)
            return "ARRAY", element_type
        return None, None

    def _prepare_bq_query_parameters(
        self, params_dict: dict[str, Any]
    ) -> list[Union[ScalarQueryParameter, ArrayQueryParameter]]:
        bq_params: list[Union[ScalarQueryParameter, ArrayQueryParameter]] = []
        if params_dict:
            for name, value in params_dict.items():
                param_name_for_bq = name.lstrip("@")
                param_type, array_element_type = self._get_bq_param_type(value)
                if param_type == "ARRAY" and array_element_type:
                    bq_params.append(ArrayQueryParameter(param_name_for_bq, array_element_type, value))
                elif param_type:
                    bq_params.append(ScalarQueryParameter(param_name_for_bq, param_type, value))
                else:
                    msg = f"Unsupported BigQuery parameter type for value of param '{name}': {type(value)}"
                    raise SQLSpecError(msg)
        return bq_params

    def _run_query_job(
        self,
        sql_str: str,
        bq_query_parameters: Optional[list[Union[ScalarQueryParameter, ArrayQueryParameter]]],
        connection: "Optional[BigQueryConnection]" = None,
        job_config_override: "Optional[QueryJobConfig]" = None,
    ) -> "QueryJob":
        conn = self._connection(connection)

        final_job_config = QueryJobConfig()

        if self._default_query_job_config:
            final_job_config = self._default_query_job_config

        if job_config_override:
            final_job_config = job_config_override

        final_job_config.query_parameters = bq_query_parameters or []
        return conn.query(sql_str, job_config=final_job_config)

    def _rows_to_results(
        self,
        rows_iterator: "Iterator[BigQueryRow]",
    ) -> list[dict[str, Any]]:
        processed_results: list[dict[str, Any]] = [dict(row) for row in rows_iterator]
        return processed_results

    def execute(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[BigQueryConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        job_config: Optional[QueryJobConfig] = None,
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
            job_config: Optional BigQuery job configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            A StatementResult containing the operation results.

        Example:
            >>> from sqlspec.sql.filters import LimitOffset, SearchFilter
            >>> # Basic query
            >>> result = driver.execute(
            ...     "SELECT * FROM users WHERE id = @id", {"id": 123}
            ... )
            >>> # Query with filters
            >>> result = driver.execute(
            ...     "SELECT * FROM users",
            ...     LimitOffset(limit=10, offset=0),
            ...     SearchFilter(field_name="name", value="John"),
            ... )
        """
        stmt_final_expression: Optional[sqlglot.exp.Expression] = None

        # Handle pre-formatted BigQuery parameters
        if (
            isinstance(parameters, (list, tuple))
            and parameters
            and all(isinstance(p, (ScalarQueryParameter, ArrayQueryParameter)) for p in parameters)
        ):
            if not isinstance(statement, str):
                msg = "If providing pre-formatted BigQuery parameters, the SQL argument must be a string."
                raise SQLSpecError(msg)
            if kwargs or filters:
                msg = "Cannot use kwargs or filters when providing pre-formatted BigQuery parameters."
                raise ParameterStyleMismatchError(msg)
            final_sql_str = statement
            bq_query_parameters_list = list(parameters)
            try:
                parsed_temp_expr = sqlglot.parse_one(final_sql_str, dialect=self.dialect)
                stmt_final_expression = parsed_temp_expr
            except Exception:
                stmt_final_expression = None
        else:
            # Process with SQLStatement
            config = statement_config or self.statement_config
            stmt = SQLStatement(
                statement, parameters, *filters, dialect=self.dialect, statement_config=config, **kwargs
            )
            stmt.validate()

            final_sql_str = stmt.to_sql(placeholder_style=self._get_placeholder_style())
            ordered_params = stmt.get_parameters(style=self._get_placeholder_style())

            # Convert parameters to dict format for BigQuery
            params_dict: dict[str, Any] = {}
            if ordered_params is not None:
                if isinstance(ordered_params, dict):
                    params_dict = ordered_params
                elif isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                    params_dict = {f"param_{i}": v for i, v in enumerate(ordered_params)}
                else:
                    params_dict = {"param_0": ordered_params}

            bq_query_parameters_list = self._prepare_bq_query_parameters(params_dict)
            stmt_final_expression = stmt.expression

        query_job = self._run_query_job(
            final_sql_str, bq_query_parameters_list, connection=connection, job_config_override=job_config
        )
        job_result = query_job.result()

        if self.returns_rows(stmt_final_expression):
            rows_list = self._rows_to_results(iter(job_result))
            column_names = [field.name for field in job_result.schema] if job_result.schema else []
            raw_result_data = rows_list[0] if rows_list else cast("dict[str, Any]", {})
            return SelectResult(
                raw_result=raw_result_data,
                rows=rows_list,
                column_names=column_names,
            )

        operation_type = "UNKNOWN"
        if stmt_final_expression and hasattr(stmt_final_expression, "key"):
            operation_type = str(stmt_final_expression.key).upper()

        return ExecuteResult(
            raw_result=cast("dict[str, Any]", {}),
            rows_affected=query_job.num_dml_affected_rows if query_job.num_dml_affected_rows is not None else -1,
            operation_type=operation_type,
        )

    def execute_many(
        self,
        statement: "Statement",
        parameters: Optional[Sequence[StatementParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional[BigQueryConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        job_config: Optional[QueryJobConfig] = None,
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
            job_config: Optional BigQuery job configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            An ExecuteResult containing the batch operation results.

        Example:
            >>> # Batch insert with validation
            >>> driver.execute_many(
            ...     "INSERT INTO users (name, email) VALUES (@name, @email)",
            ...     [
            ...         {"name": "John", "email": "john@example.com"},
            ...         {"name": "Jane", "email": "jane@example.com"},
            ...     ],
            ... )
        """
        if not parameters:
            config = statement_config or self.statement_config
            stmt = SQLStatement(statement, None, *filters, dialect=self.dialect, statement_config=config, **kwargs)
            stmt.validate()
            return ExecuteResult(raw_result=cast("dict[str, Any]", {}), rows_affected=0, operation_type="EXECUTE")

        total_rows_affected = 0
        conn = self._connection(connection)
        config = statement_config or self.statement_config

        # Create template statement
        template_stmt = SQLStatement(statement, None, *filters, dialect=self.dialect, statement_config=config, **kwargs)
        template_stmt.validate()
        processed_sql_template = template_stmt.to_sql(placeholder_style=self._get_placeholder_style())

        for param_set in parameters:
            item_stmt = SQLStatement(
                template_stmt.sql,
                param_set,
                dialect=self.dialect,
                statement_config=replace(config or StatementConfig(), enable_validation=False),
            )
            item_params_dict = item_stmt.get_parameters(style=self._get_placeholder_style())
            if not isinstance(item_params_dict, dict):
                item_params_dict = {}

            bq_item_params = self._prepare_bq_query_parameters(item_params_dict)

            query_job = self._run_query_job(
                processed_sql_template, bq_item_params, connection=conn, job_config_override=job_config
            )
            query_job.result()
            if query_job.num_dml_affected_rows is not None:
                total_rows_affected += query_job.num_dml_affected_rows
            elif not self.returns_rows(template_stmt.expression):
                total_rows_affected += 1

        operation_type = "EXECUTE"
        if template_stmt.expression and hasattr(template_stmt.expression, "key"):
            operation_type = str(template_stmt.expression.key).upper()

        return ExecuteResult(
            raw_result=cast("dict[str, Any]", {}),
            rows_affected=total_rows_affected,
            operation_type=operation_type,
        )

    def execute_script(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[BigQueryConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        job_config: Optional[QueryJobConfig] = None,
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
            job_config: Optional BigQuery job configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            A string with execution results/output.
        """
        config = statement_config or self.statement_config

        merged_params = parameters
        if kwargs:
            if merged_params is None:
                merged_params = kwargs
            elif isinstance(merged_params, dict):
                merged_params = {**merged_params, **kwargs}

        stmt = SQLStatement(statement, merged_params, *filters, dialect=self.dialect, statement_config=config)
        stmt.validate()
        final_sql_script = stmt.to_sql(placeholder_style=ParameterStyle.STATIC)

        query_job = self._run_query_job(final_sql_script, None, connection=connection, job_config_override=job_config)
        query_job.result()
        return f"SCRIPT EXECUTED (Job ID: {query_job.job_id})"

    def select_to_arrow(
        self,
        statement: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[BigQueryConnection] = None,
        statement_config: Optional[StatementConfig] = None,
        job_config: Optional[QueryJobConfig] = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Execute a SELECT statement and return results as an Apache Arrow Table.

        This method leverages BigQuery's native Arrow support for high-performance
        analytics workloads.

        Args:
            statement: The SQL query to execute.
            parameters: Parameters for the query.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            job_config: Optional BigQuery job configuration.
            **kwargs: Additional keyword arguments.

        Raises:
            TypeError: If the SQL statement is not a valid query.

        Returns:
            An ArrowResult containing the query results as an Arrow Table.
        """
        stmt_final_expression: Optional[sqlglot.exp.Expression] = None

        # Handle pre-formatted BigQuery parameters
        if (
            isinstance(parameters, (list, tuple))
            and parameters
            and all(isinstance(p, (ScalarQueryParameter, ArrayQueryParameter)) for p in parameters)
        ):
            if not isinstance(statement, str):
                msg = "If providing pre-formatted BigQuery parameters, the SQL argument must be a string."
                raise SQLSpecError(msg)
            if kwargs or filters:
                msg = "Cannot use kwargs or filters when providing pre-formatted BigQuery parameters."
                raise ParameterStyleMismatchError(msg)
            final_sql_str = statement
            bq_query_parameters_list = list(parameters)
            try:
                parsed_temp_expr = sqlglot.parse_one(final_sql_str, dialect=self.dialect)
                stmt_final_expression = parsed_temp_expr
            except Exception:
                stmt_final_expression = None
            if not self.returns_rows(stmt_final_expression):
                op_type = str(stmt_final_expression.key).upper() if stmt_final_expression else "UNKNOWN"
                msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
                raise TypeError(msg)
        else:
            # Process with SQLStatement
            config = statement_config or self.statement_config
            stmt = SQLStatement(
                statement, parameters, *filters, dialect=self.dialect, statement_config=config, **kwargs
            )
            stmt.validate()

            if not self.returns_rows(stmt.expression):
                op_type = str(stmt.expression.key).upper() if stmt.expression else "UNKNOWN"
                msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
                raise TypeError(msg)

            final_sql_str = stmt.to_sql(placeholder_style=self._get_placeholder_style())
            ordered_params = stmt.get_parameters(style=self._get_placeholder_style())

            # Convert parameters to dict format for BigQuery
            params_dict: dict[str, Any] = {}
            if ordered_params is not None:
                if isinstance(ordered_params, dict):
                    params_dict = ordered_params
                elif isinstance(ordered_params, Iterable) and not isinstance(ordered_params, (str, bytes)):
                    params_dict = {f"param_{i}": v for i, v in enumerate(ordered_params)}
                else:
                    params_dict = {"param_0": ordered_params}

            bq_query_parameters_list = self._prepare_bq_query_parameters(params_dict)

        query_job = self._run_query_job(
            final_sql_str, bq_query_parameters_list, connection=connection, job_config_override=job_config
        )
        arrow_table = query_job.to_arrow()
        return ArrowResult(raw_result=arrow_table)

    def _connection(self, connection: Optional[BigQueryConnection] = None) -> BigQueryConnection:
        return connection or self.connection
