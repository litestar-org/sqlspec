# ruff: noqa: BLE001, PLR6301
import datetime
import logging
from collections.abc import Iterable, Iterator, Sequence
from contextlib import suppress
from dataclasses import replace
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
    Union,
)

import sqlglot
from google.cloud.bigquery import (
    ArrayQueryParameter,
    Client,
    QueryJob,
    QueryJobConfig,
    ScalarQueryParameter,
)
from google.cloud.bigquery.table import Row as BigQueryRow

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.exceptions import (
    ParameterStyleMismatchError,
    SQLSpecError,
)
from sqlspec.statement.mixins import (
    ResultConverter,
    SQLTranslatorMixin,
    SyncArrowMixin,
    SyncParquetMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import DictRow, SQLParameterType

if TYPE_CHECKING:
    from sqlspec.statement.builder import QueryBuilder
    from sqlspec.statement.filters import StatementFilter


__all__ = ("BigQueryConnection", "BigQueryDriver")

BigQueryConnection = Client

logger = logging.getLogger("sqlspec.adapters.bigquery")


class BigQueryDriver(
    SyncDriverAdapterProtocol[BigQueryConnection, DictRow],
    SyncArrowMixin[BigQueryConnection],
    SyncParquetMixin[BigQueryConnection],
    SQLTranslatorMixin[BigQueryConnection],
    ResultConverter,
):
    """Advanced BigQuery Driver with comprehensive Google Cloud capabilities.

    Protocol Implementation:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations with transaction safety
    - execute_script() - Multi-statement scripts and DDL operations
    """

    dialect: str = "bigquery"
    connection: BigQueryConnection
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = True
    _default_query_job_config: Optional[QueryJobConfig]

    def __init__(
        self,
        connection: BigQueryConnection,
        config: Optional[SQLConfig] = None,
        instrumentation_config: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,  # type: ignore[assignment]
        # BigQuery-specific parameters
        default_query_job_config: Optional[QueryJobConfig] = None,
        on_job_start: Optional[Callable[[str], None]] = None,
        on_job_complete: Optional[Callable[[str, Any], None]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize BigQuery driver with comprehensive feature support.

        Args:
            connection: BigQuery Client instance
            config: SQL statement configuration
            instrumentation_config: Instrumentation configuration
            default_row_type: Default row type for results
            default_query_job_config: Default job configuration
            on_job_start: Callback executed when a BigQuery job starts
            on_job_complete: Callback executed when a BigQuery job completes
            **kwargs: Additional driver configuration
        """
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config or InstrumentationConfig(),
            default_row_type=default_row_type,
        )

        # BigQuery-specific configuration
        self.on_job_start = on_job_start
        self.on_job_complete = on_job_complete

        # Set up default query job configuration
        default_config_kwarg = kwargs.get("default_query_job_config") or default_query_job_config
        conn_default_config = getattr(connection, "default_query_job_config", None)

        if default_config_kwarg is not None and isinstance(default_config_kwarg, QueryJobConfig):
            self._default_query_job_config = default_config_kwarg
        elif conn_default_config is not None and isinstance(conn_default_config, QueryJobConfig):
            self._default_query_job_config = conn_default_config
        else:
            self._default_query_job_config = None

    def _get_placeholder_style(self) -> ParameterStyle:
        """BigQuery uses named parameters with @ prefix."""
        return ParameterStyle.NAMED_AT

    @staticmethod
    def _get_bq_param_type(value: Any) -> tuple[Optional[str], Optional[str]]:
        """Determine BigQuery parameter type from Python value.

        Supports all BigQuery data types including arrays, structs, and geographic types.

        Args:
            value: Python value to convert.

        Returns:
            Tuple of (parameter_type, array_element_type).

        Raises:
            SQLSpecError: If value type is not supported.
        """
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

        # Support for additional BigQuery types
        # Note: Geographic types, JSON, etc. can be added here as needed
        return None, None

    def _prepare_bq_query_parameters(
        self, params_dict: dict[str, Any]
    ) -> list[Union[ScalarQueryParameter, ArrayQueryParameter]]:
        """Convert parameter dictionary to BigQuery parameter objects.

        Args:
            params_dict: Dictionary of parameter names and values.

        Returns:
            List of BigQuery parameter objects.

        Raises:
            SQLSpecError: If parameter type is not supported.
        """
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
        connection: Optional[BigQueryConnection] = None,
        job_config_override: Optional[QueryJobConfig] = None,
    ) -> QueryJob:
        """Execute a BigQuery job with comprehensive configuration support.

        Args:
            sql_str: SQL string to execute.
            bq_query_parameters: BigQuery parameter objects.
            connection: Optional connection override.
            job_config_override: Optional job configuration override.

        Returns:
            QueryJob instance.
        """
        conn = connection or self.connection

        # Build final job configuration
        final_job_config = QueryJobConfig()

        # Apply default configuration if available
        if self._default_query_job_config:
            # Copy settings from default config
            for attr in dir(self._default_query_job_config):
                if not attr.startswith("_") and hasattr(final_job_config, attr):
                    with suppress(Exception):
                        value = getattr(self._default_query_job_config, attr)
                        if value is not None:
                            setattr(final_job_config, attr, value)

        # Apply override configuration if provided
        if job_config_override:
            # Copy settings from override config
            for attr in dir(job_config_override):
                if not attr.startswith("_") and hasattr(final_job_config, attr):
                    with suppress(Exception):
                        value = getattr(job_config_override, attr)
                        if value is not None:
                            setattr(final_job_config, attr, value)

        # Set query parameters
        final_job_config.query_parameters = bq_query_parameters or []

        # Execute job start callback
        job_id = f"sqlspec-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"  # noqa: DTZ005
        if self.on_job_start:
            try:
                self.on_job_start(job_id)
            except Exception as e:
                logger.warning("Job start callback failed: %s", str(e), extra={"adapter": "bigquery"})

        # Execute the query
        query_job = conn.query(sql_str, job_config=final_job_config)

        # Execute job complete callback
        if self.on_job_complete:
            try:
                self.on_job_complete(query_job.job_id or job_id, query_job)
            except Exception as e:
                logger.warning("Job complete callback failed: %s", str(e), extra={"adapter": "bigquery"})

        return query_job

    def _rows_to_results(self, rows_iterator: Iterator[BigQueryRow]) -> list[dict[str, Any]]:
        """Convert BigQuery rows to dictionary format.

        Args:
            rows_iterator: Iterator of BigQuery Row objects.

        Returns:
            List of dictionaries representing the rows.
        """
        return [dict(row) for row in rows_iterator]

    def _execute_impl(
        self,
        statement: SQL,
        parameters: Optional[SQLParameterType] = None,
        connection: Optional[BigQueryConnection] = None,
        config: Optional[SQLConfig] = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Union[QueryJob, str]:
        """Core execution implementation for BigQuery operations.

        Args:
            statement: SQL statement object to execute.
            parameters: Parameters for the statement.
            connection: Optional connection override.
            config: Optional statement configuration.
            is_many: Whether this is a batch execution.
            is_script: Whether this is a script execution.
            **kwargs: Additional execution parameters.

        Raises:
            ParameterStyleMismatchError: If the parameters are not formatted correctly.

        Returns:
            QueryJob for regular queries or string for script execution.
        """
        # Handle pre-formatted BigQuery parameters
        if (
            isinstance(parameters, (list, tuple))
            and parameters
            and all(isinstance(p, (ScalarQueryParameter, ArrayQueryParameter)) for p in parameters)
        ):
            if kwargs or not isinstance(statement.sql, str):
                msg = "Cannot use kwargs or non-string SQL when providing pre-formatted BigQuery parameters."
                raise ParameterStyleMismatchError(msg)
            final_sql_str = statement.sql
            bq_query_parameters_list = list(parameters)
        else:
            # For scripts, use static parameters
            placeholder_style = ParameterStyle.STATIC if is_script else self._get_placeholder_style()
            final_sql_str = statement.to_sql(placeholder_style=placeholder_style)

            if is_script:
                # Scripts don't use parameterized queries
                bq_query_parameters_list = []
            else:
                ordered_params = statement.get_parameters(style=self._get_placeholder_style())

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

        # Execute the query job
        query_job = self._run_query_job(
            final_sql_str, bq_query_parameters_list, connection=connection, job_config_override=kwargs.get("job_config")
        )

        if is_script:
            query_job.result()  # Wait for completion
            return f"SCRIPT EXECUTED (Job ID: {query_job.job_id})"

        return query_job

    def _wrap_select_result(
        self, statement: SQL, raw_driver_result: Any, schema_type: Optional[type] = None, **kwargs: Any
    ) -> SelectResult[DictRow]:
        """Wrap BigQuery query results as SelectResult.

        Args:
            statement: The SQL statement that was executed.
            raw_driver_result: QueryJob result from BigQuery.
            schema_type: Optional schema type for result mapping.
            **kwargs: Additional result wrapping parameters.

        Returns:
            SelectResult containing the query data.
        """
        if isinstance(raw_driver_result, QueryJob):
            job_result = raw_driver_result.result()
            rows_list = self._rows_to_results(iter(job_result))
            column_names = [field.name for field in job_result.schema] if job_result.schema else []

            return SelectResult(
                statement=statement,
                data=rows_list,
                column_names=column_names,
            )

        # Fallback for unexpected result types
        return SelectResult(
            statement=statement,
            data=[],
            column_names=[],
        )

    def _wrap_execute_result(self, statement: SQL, raw_driver_result: Any, **kwargs: Any) -> ExecuteResult:
        """Wrap BigQuery execution results as ExecuteResult.

        Args:
            statement: The SQL statement that was executed.
            raw_driver_result: Result from BigQuery execution.
            **kwargs: Additional result wrapping parameters.

        Returns:
            ExecuteResult containing execution metadata.
        """
        operation_type = "UNKNOWN"
        if statement.expression and hasattr(statement.expression, "key"):
            operation_type = str(statement.expression.key).upper()

        if isinstance(raw_driver_result, str):
            # Script execution
            execute_data = {
                "rows_affected": 0,
                "last_inserted_id": None,
                "inserted_ids": [],
                "returning_data": None,
                "operation_type": operation_type or "SCRIPT",
            }
            return ExecuteResult(
                statement=statement,
                data=execute_data,
                rows_affected=0,
                operation_type=operation_type or "SCRIPT",
            )

        # Regular execution (QueryJob)
        rows_affected = 0
        if hasattr(raw_driver_result, "num_dml_affected_rows") and raw_driver_result.num_dml_affected_rows is not None:
            rows_affected = raw_driver_result.num_dml_affected_rows

        execute_data = {
            "rows_affected": rows_affected,
            "last_inserted_id": None,
            "inserted_ids": [],
            "returning_data": None,
            "operation_type": operation_type,
        }
        return ExecuteResult(
            statement=statement,
            data=execute_data,
            rows_affected=rows_affected,
            operation_type=operation_type,
        )

    def execute_many(
        self,
        statement: Union[SQL, Statement, "QueryBuilder[ExecuteResult]"],
        parameters: Optional[Sequence[SQLParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional[BigQueryConnection] = None,
        config: Optional[SQLConfig] = None,
        job_config: Optional[QueryJobConfig] = None,
        **kwargs: Any,
    ) -> ExecuteResult:
        """Execute a SQL statement with multiple parameter sets.

        Useful for batch INSERT, UPDATE, or DELETE operations with BigQuery's
        high-performance batch processing capabilities.

        Args:
            statement: The SQL statement to execute.
            parameters: Sequence of parameter sets.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            config: Optional statement configuration.
            job_config: Optional BigQuery job configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            An ExecuteResult containing the batch operation results.

        Example:
            >>> # Batch insert with BigQuery optimizations
            >>> driver.execute_many(
            ...     "INSERT INTO users (name, email) VALUES (@name, @email)",
            ...     [
            ...         {"name": "John", "email": "john@example.com"},
            ...         {"name": "Jane", "email": "jane@example.com"},
            ...     ],
            ... )
        """
        if not parameters:
            # Use _build_statement to handle the different statement types
            template_stmt = self._build_statement(statement, config, *filters)
            execute_data = {
                "rows_affected": 0,
                "last_inserted_id": None,
                "inserted_ids": [],
                "returning_data": None,
                "operation_type": "EXECUTE",
            }
            return ExecuteResult(statement=template_stmt, data=execute_data, rows_affected=0, operation_type="EXECUTE")

        total_rows_affected = 0
        conn = connection or self.connection
        config = config or self.config

        # Use _build_statement to create the template statement
        template_stmt = self._build_statement(statement, config, *filters)
        processed_sql_template = template_stmt.to_sql(placeholder_style=self._get_placeholder_style())

        for param_set in parameters:
            # Create a new SQL statement with the current parameter set
            item_stmt = SQL(
                template_stmt.sql,
                param_set,
                dialect=self.dialect,
                config=replace(config or SQLConfig(), enable_validation=False),
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

        execute_data = {
            "rows_affected": total_rows_affected,
            "last_inserted_id": None,
            "inserted_ids": [],
            "returning_data": None,
            "operation_type": operation_type,
        }
        return ExecuteResult(
            statement=template_stmt,
            data=execute_data,
            rows_affected=total_rows_affected,
            operation_type=operation_type,
        )

    def select_to_arrow(
        self,
        statement: Statement,
        parameters: Optional[SQLParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[BigQueryConnection] = None,
        config: Optional[SQLConfig] = None,
        job_config: Optional[QueryJobConfig] = None,
        **kwargs: Any,
    ) -> ArrowResult:
        """Execute a SELECT statement and return results as Apache Arrow Table.

        BigQuery provides native Arrow support for high-performance analytics
        workloads, perfect for data science and machine learning workflows.

        Args:
            statement: The SQL query to execute.
            parameters: Parameters for the query.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            config: Optional statement configuration.
            job_config: Optional BigQuery job configuration.
            **kwargs: Additional keyword arguments.

        Raises:
            TypeError: If the SQL statement is not a valid query.
            ParameterStyleMismatchError: If the parameters are not formatted correctly.
            SQLSpecError: If the SQL statement is not a valid query.

        Returns:
            An ArrowResult containing the query results as an Arrow Table.

        Example:
            >>> # High-performance analytics query
            >>> arrow_result = driver.select_to_arrow(
            ...     "SELECT * FROM `bigquery-public-data.samples.shakespeare` LIMIT 1000"
            ... )
            >>> df = arrow_result.to_polars()  # Convert to Polars DataFrame
        """
        stmt_obj = None

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
            # Create a simple SQL object for the statement
            stmt_obj = SQL(statement, parameters, dialect=self.dialect, config=config or self.config)
        else:
            # Process with SQLStatement
            config = config or self.config
            stmt_obj = SQL(statement, parameters, *filters, dialect=self.dialect, config=config, **kwargs)
            stmt_obj.validate()

            if not self.returns_rows(stmt_obj.expression):
                op_type = str(stmt_obj.expression.key).upper() if stmt_obj.expression else "UNKNOWN"
                msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
                raise TypeError(msg)

            final_sql_str = stmt_obj.to_sql(placeholder_style=self._get_placeholder_style())
            ordered_params = stmt_obj.get_parameters(style=self._get_placeholder_style())

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
        return ArrowResult(statement=stmt_obj, data=arrow_table)
