# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import datetime
import logging
from contextlib import contextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional, Union, cast

import sqlglot
import sqlglot.expressions as exp
from google.cloud.bigquery import ArrayQueryParameter, QueryJob, QueryJobConfig, ScalarQueryParameter
from google.cloud.exceptions import GoogleCloudError

from sqlspec.adapters.bigquery._types import BigQueryConnection
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.driver._common import ExecutionResult
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.statement.sql import SQL, StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.result import SQLResult
__all__ = ("BigQueryCursor", "BigQueryDriver", "bigquery_statement_config")


def _get_bq_param_type(value: Any) -> tuple[Optional[str], Optional[str]]:
    """Determine BigQuery parameter type from Python value."""
    if value is None:
        return ("STRING", None)

    value_type = type(value)
    if value_type is datetime.datetime:
        return ("TIMESTAMP" if value.tzinfo else "DATETIME", None)

    type_map = {
        bool: ("BOOL", None),
        int: ("INT64", None),
        float: ("FLOAT64", None),
        Decimal: ("BIGNUMERIC", None),
        str: ("STRING", None),
        bytes: ("BYTES", None),
        datetime.date: ("DATE", None),
        datetime.time: ("TIME", None),
        dict: ("JSON", None),
    }

    if value_type in type_map:
        return type_map[value_type]

    if isinstance(value, (list, tuple)):
        if not value:
            msg = "Cannot determine BigQuery ARRAY type for empty sequence."
            raise SQLSpecError(msg)
        element_type, _ = _get_bq_param_type(value[0])
        if element_type is None:
            msg = f"Unsupported element type in ARRAY: {type(value[0])}"
            raise SQLSpecError(msg)
        return "ARRAY", element_type

    return None, None


def _create_bq_parameters(parameters: Any) -> "list[Union[ArrayQueryParameter, ScalarQueryParameter]]":
    """Create BigQuery QueryParameter objects from parameters."""
    if not parameters:
        return []

    if not isinstance(parameters, dict):
        return []

    bq_parameters: list[Union[ArrayQueryParameter, ScalarQueryParameter]] = []

    for name, value in parameters.items():
        param_name_for_bq = name.lstrip("@")
        actual_value = getattr(value, "value", value)
        param_type, array_element_type = _get_bq_param_type(actual_value)

        if param_type == "ARRAY" and array_element_type:
            bq_parameters.append(ArrayQueryParameter(param_name_for_bq, array_element_type, actual_value))
        elif param_type == "JSON":
            json_str = to_json(actual_value)
            bq_parameters.append(ScalarQueryParameter(param_name_for_bq, "STRING", json_str))
        elif param_type:
            bq_parameters.append(ScalarQueryParameter(param_name_for_bq, param_type, actual_value))
        else:
            msg = f"Unsupported BigQuery parameter type for value of param '{name}': {type(actual_value)}"
            raise SQLSpecError(msg)

    return bq_parameters


bigquery_type_coercion_map = {
    bool: lambda x: x,
    int: lambda x: x,
    float: lambda x: x,
    str: lambda x: x,
    bytes: lambda x: x,
    datetime.datetime: lambda x: x,
    datetime.date: lambda x: x,
    datetime.time: lambda x: x,
    Decimal: lambda x: x,
    dict: to_json,
    list: lambda x: x,
    tuple: list,
    type(None): lambda _: None,
}


bigquery_statement_config = StatementConfig(
    dialect="bigquery",
    enable_parsing=True,
    enable_transformations=True,
    enable_validation=True,
    enable_caching=True,
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_AT,
        supported_parameter_styles={ParameterStyle.NAMED_AT},
        supported_execution_parameter_styles={ParameterStyle.NAMED_AT},
        default_execution_parameter_style=ParameterStyle.NAMED_AT,
        type_coercion_map=bigquery_type_coercion_map,
        has_native_list_expansion=True,
        needs_static_script_compilation=True,
    ),
)


logger = logging.getLogger("sqlspec.adapters.bigquery")


class BigQueryCursor:
    """BigQuery cursor for query execution."""

    def __init__(self, connection: "BigQueryConnection") -> None:
        self.connection = connection
        self.job: Optional[QueryJob] = None


class BigQueryDriver(SyncDriverAdapterBase):
    """BigQuery Driver with proper pipeline integration."""

    dialect: "DialectType" = "bigquery"
    _default_query_job_config: Optional[QueryJobConfig]

    def __init__(
        self,
        connection: BigQueryConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        """Initialize BigQuery driver."""
        super().__init__(
            connection=connection,
            statement_config=statement_config or bigquery_statement_config,
            driver_features=driver_features,
        )
        self._default_query_job_config = (driver_features or {}).get("default_query_job_config")

    @contextmanager
    def with_cursor(self, connection: "BigQueryConnection") -> "Any":
        """Create and return a context manager for cursor acquisition and cleanup."""
        cursor = BigQueryCursor(connection)
        try:
            yield cursor
        finally:
            pass  # No cleanup needed for BigQuery cursors

    def begin(self) -> None:
        """Begin transaction - BigQuery doesn't support transactions."""

    def rollback(self) -> None:
        """Rollback transaction - BigQuery doesn't support transactions."""

    def commit(self) -> None:
        """Commit transaction - BigQuery doesn't support transactions."""

    @staticmethod
    def _copy_job_config_attrs(source_config: QueryJobConfig, target_config: QueryJobConfig) -> None:
        """Copy non-private attributes from source config to target config."""
        for attr in dir(source_config):
            if attr.startswith("_"):
                continue
            value = getattr(source_config, attr)
            if value is not None:
                setattr(target_config, attr, value)

    def _run_query_job(
        self,
        sql_str: str,
        parameters: Any,
        connection: Optional[BigQueryConnection] = None,
        job_config: Optional[QueryJobConfig] = None,
    ) -> QueryJob:
        """Execute a BigQuery job with comprehensive configuration support."""
        conn = connection or self.connection

        final_job_config = QueryJobConfig()

        if self._default_query_job_config:
            self._copy_job_config_attrs(self._default_query_job_config, final_job_config)

        if job_config:
            self._copy_job_config_attrs(job_config, final_job_config)

        # Convert parameters to BigQuery QueryParameter objects
        bq_parameters = _create_bq_parameters(parameters)
        final_job_config.query_parameters = bq_parameters

        return conn.query(sql_str, job_config=final_job_config)

    @staticmethod
    def _rows_to_results(rows_iterator: Any) -> list[dict[str, Any]]:
        """Convert BigQuery rows to dictionary format."""
        return [dict(row) for row in rows_iterator]

    def _try_special_handling(self, cursor: "Any", statement: "SQL") -> "Optional[SQLResult]":
        """Hook for BigQuery-specific special operations."""
        # Let the base class handle script execution via the standard pipeline
        return None

    def _transform_ast_with_literals(self, sql: str, parameters: Any) -> str:
        """Transform SQL AST by replacing placeholders with literal values.

        This approach maintains the single-parse architecture by using proper
        AST transformation instead of string manipulation.
        """
        if not parameters:
            return sql

        # Parse the SQL once
        try:
            ast = sqlglot.parse_one(sql, dialect="bigquery")
        except sqlglot.ParseError:
            # If we can't parse, fall back to original SQL
            return sql

        # Convert parameters to list if needed
        if isinstance(parameters, (list, tuple)):
            param_list = list(parameters)
        elif isinstance(parameters, dict):
            # For named parameters, we need to handle differently
            # For now, return original SQL for dict parameters
            return sql
        else:
            param_list = [parameters]

        # Counter for tracking which parameter we're replacing
        param_index = [0]  # Use list to make it mutable in nested function

        def replace_placeholder(node: exp.Expression) -> exp.Expression:
            """Replace placeholder nodes with literal values."""
            if isinstance(node, (exp.Placeholder, exp.Parameter)) and param_index[0] < len(param_list):
                value = param_list[param_index[0]]
                param_index[0] += 1
                return self._create_literal_node(value)
            return node

        # Transform the AST by replacing placeholders with literals
        transformed_ast = ast.transform(replace_placeholder)

        # Generate SQL from the transformed AST
        return transformed_ast.sql(dialect="bigquery")

    def _create_literal_node(self, value: Any) -> "exp.Expression":
        """Create a SQLGlot literal expression from a Python value."""
        import sqlglot.expressions as exp

        if value is None:
            return exp.Null()
        if isinstance(value, bool):
            return exp.Boolean(this=value)
        if isinstance(value, (int, float)):
            return exp.Literal.number(str(value))
        if isinstance(value, str):
            return exp.Literal.string(value)
        if isinstance(value, (list, tuple)):
            # Create an array literal
            items = [self._create_literal_node(item) for item in value]
            return exp.Array(expressions=items)
        if isinstance(value, dict):
            # For dict, convert to JSON string
            import json

            json_str = json.dumps(value)
            return exp.Literal.string(json_str)
        # Fallback to string representation
        return exp.Literal.string(str(value))

    def _execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """BigQuery execute_many implementation - converts to script execution.

        Uses proper AST transformation to embed literals, then executes as script.
        """
        # Check if we have parameters for execute_many
        if not statement.parameters or not isinstance(statement.parameters, (list, tuple)):
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        parameters_list = statement.parameters
        # Get the original SQL template
        original_sql = statement._raw_sql

        # Build individual statements for each parameter set using AST transformation
        script_statements = []
        for param_set in parameters_list:
            individual_sql = self._transform_ast_with_literals(original_sql, param_set)
            script_statements.append(individual_sql)

        # Generate script and execute it
        script_sql = ";\n".join(script_statements) + ";"

        # Execute the script directly using the connection
        cursor.job = self._run_query_job(script_sql, None, connection=cursor.connection)
        cursor.job.result()  # Wait for completion

        # Return result with row count (BigQuery emulator may report 0)
        return self.create_execution_result(
            cursor, rowcount_override=cursor.job.num_dml_affected_rows or 0, is_many_result=True
        )

    def _execute_statement(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """BigQuery single statement execution."""
        sql = statement.sql
        parameters = statement.parameters
        cursor.job = self._run_query_job(sql, parameters, connection=cursor.connection)

        if statement.returns_rows():
            job_result = cursor.job.result()
            rows_list = self._rows_to_results(iter(job_result))
            column_names = [field.name for field in cursor.job.schema] if cursor.job.schema else []

            return self.create_execution_result(
                cursor,
                selected_data=rows_list,
                column_names=column_names,
                data_row_count=len(rows_list),
                is_select_result=True,
            )

        cursor.job.result()
        return self.create_execution_result(cursor, rowcount_override=cursor.job.num_dml_affected_rows or 0)

    def handle_database_exceptions(self) -> "Generator[None, None, None]":
        """Handle BigQuery-specific exceptions and wrap them appropriately."""
        return cast("Generator[None, None, None]", self._handle_database_exceptions_impl())

    @contextmanager
    def _handle_database_exceptions_impl(self) -> "Generator[None, None, None]":
        """Implementation of database exception handling without decorator."""
        try:
            yield
        except GoogleCloudError as e:
            msg = f"BigQuery database error: {e}"
            raise SQLSpecError(msg) from e
        except Exception as e:
            # Handle any other unexpected errors
            if "parse" in str(e).lower() or "syntax" in str(e).lower():
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected error: {e}"
            raise SQLSpecError(msg) from e
