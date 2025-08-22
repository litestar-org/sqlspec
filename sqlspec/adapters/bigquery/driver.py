"""BigQuery driver implementation.

Provides Google Cloud BigQuery connectivity with parameter style conversion,
type coercion, error handling, and query job management.
"""

import datetime
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional, Union

import sqlglot
import sqlglot.expressions as exp
from google.cloud.bigquery import ArrayQueryParameter, QueryJob, QueryJobConfig, ScalarQueryParameter
from google.cloud.exceptions import GoogleCloudError

from sqlspec.adapters.bigquery._types import BigQueryConnection
from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.driver._common import ExecutionResult
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from sqlspec.core.result import SQLResult
    from sqlspec.core.statement import SQL

logger = logging.getLogger(__name__)

__all__ = ("BigQueryCursor", "BigQueryDriver", "BigQueryExceptionHandler", "bigquery_statement_config")


_BQ_TYPE_MAP: dict[type, tuple[str, Optional[str]]] = {
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


def _get_bq_param_type(value: Any) -> tuple[Optional[str], Optional[str]]:
    """Determine BigQuery parameter type from Python value.

    Args:
        value: Python value to determine BigQuery type for

    Returns:
        Tuple of (parameter_type, array_element_type)
    """
    if value is None:
        return ("STRING", None)

    value_type = type(value)

    if value_type is datetime.datetime:
        return ("TIMESTAMP" if value.tzinfo else "DATETIME", None)

    if value_type in _BQ_TYPE_MAP:
        return _BQ_TYPE_MAP[value_type]

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


_BQ_PARAM_CREATOR_MAP: dict[str, Any] = {
    "ARRAY": lambda name, value, array_type: ArrayQueryParameter(
        name, array_type, [] if value is None else list(value)
    ),
    "JSON": lambda name, value, _: ScalarQueryParameter(name, "STRING", to_json(value)),
    "SCALAR": lambda name, value, param_type: ScalarQueryParameter(name, param_type, value),
}


def _create_bq_parameters(parameters: Any) -> "list[Union[ArrayQueryParameter, ScalarQueryParameter]]":
    """Create BigQuery QueryParameter objects from parameters.

    Args:
        parameters: Dict of named parameters or list of positional parameters

    Returns:
        List of BigQuery QueryParameter objects
    """
    if not parameters:
        return []

    bq_parameters: list[Union[ArrayQueryParameter, ScalarQueryParameter]] = []

    if isinstance(parameters, dict):
        for name, value in parameters.items():
            param_name_for_bq = name.lstrip("@")
            actual_value = getattr(value, "value", value)
            param_type, array_element_type = _get_bq_param_type(actual_value)

            if param_type == "ARRAY" and array_element_type:
                creator = _BQ_PARAM_CREATOR_MAP["ARRAY"]
                bq_parameters.append(creator(param_name_for_bq, actual_value, array_element_type))
            elif param_type == "JSON":
                creator = _BQ_PARAM_CREATOR_MAP["JSON"]
                bq_parameters.append(creator(param_name_for_bq, actual_value, None))
            elif param_type:
                creator = _BQ_PARAM_CREATOR_MAP["SCALAR"]
                bq_parameters.append(creator(param_name_for_bq, actual_value, param_type))
            else:
                msg = f"Unsupported BigQuery parameter type for value of param '{name}': {type(actual_value)}"
                raise SQLSpecError(msg)

    elif isinstance(parameters, (list, tuple)):
        logger.warning("BigQuery received positional parameters instead of named parameters")
        return []

    return bq_parameters


bigquery_type_coercion_map = {
    tuple: list,
    bool: lambda x: x,
    int: lambda x: x,
    float: lambda x: x,
    str: lambda x: x,
    bytes: lambda x: x,
    datetime.datetime: lambda x: x,
    datetime.date: lambda x: x,
    datetime.time: lambda x: x,
    Decimal: lambda x: x,
    dict: lambda x: x,
    list: lambda x: x,
    type(None): lambda _: None,
}


bigquery_statement_config = StatementConfig(
    dialect="bigquery",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_AT,
        supported_parameter_styles={ParameterStyle.NAMED_AT, ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.NAMED_AT,
        supported_execution_parameter_styles={ParameterStyle.NAMED_AT},
        type_coercion_map=bigquery_type_coercion_map,
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
        preserve_original_params_for_many=True,
    ),
    enable_parsing=True,
    enable_validation=True,
    enable_caching=True,
    enable_parameter_type_wrapping=True,
)


class BigQueryCursor:
    """BigQuery cursor with resource management."""

    __slots__ = ("connection", "job")

    def __init__(self, connection: "BigQueryConnection") -> None:
        self.connection = connection
        self.job: Optional[QueryJob] = None

    def __enter__(self) -> "BigQueryConnection":
        return self.connection

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Clean up cursor resources including active QueryJobs."""
        if self.job is not None:
            try:
                # Cancel the job if it's still running to free up resources
                if self.job.state in {"PENDING", "RUNNING"}:
                    self.job.cancel()
                # Clear the job reference
                self.job = None
            except Exception:
                logger.exception("Failed to cancel BigQuery job during cursor cleanup")


class BigQueryExceptionHandler:
    """Custom sync context manager for handling BigQuery database exceptions."""

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            return

        if issubclass(exc_type, GoogleCloudError):
            e = exc_val
            error_msg = str(e).lower()
            if "syntax" in error_msg or "invalid" in error_msg:
                msg = f"BigQuery SQL syntax error: {e}"
                raise SQLParsingError(msg) from e
            if "permission" in error_msg or "access" in error_msg:
                msg = f"BigQuery access error: {e}"
                raise SQLSpecError(msg) from e
            msg = f"BigQuery cloud error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, Exception):
            e = exc_val
            error_msg = str(e).lower()
            if "parse" in error_msg or "syntax" in error_msg:
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected BigQuery operation error: {e}"
            raise SQLSpecError(msg) from e


class BigQueryDriver(SyncDriverAdapterBase):
    """BigQuery driver implementation.

    Provides Google Cloud BigQuery connectivity with parameter style conversion,
    type coercion, error handling, and query job management.
    """

    __slots__ = ("_default_query_job_config",)
    dialect = "bigquery"

    def __init__(
        self,
        connection: BigQueryConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            statement_config = bigquery_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="bigquery",
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._default_query_job_config: Optional[QueryJobConfig] = (driver_features or {}).get(
            "default_query_job_config"
        )

    def with_cursor(self, connection: "BigQueryConnection") -> "BigQueryCursor":
        """Create context manager for cursor management.

        Returns:
            BigQueryCursor: Cursor object for query execution
        """
        return BigQueryCursor(connection)

    def begin(self) -> None:
        """Begin transaction - BigQuery doesn't support transactions."""

    def rollback(self) -> None:
        """Rollback transaction - BigQuery doesn't support transactions."""

    def commit(self) -> None:
        """Commit transaction - BigQuery doesn't support transactions."""

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately."""
        return BigQueryExceptionHandler()

    def _copy_job_config_attrs(self, source_config: QueryJobConfig, target_config: QueryJobConfig) -> None:
        """Copy non-private attributes from source config to target config.

        Args:
            source_config: Configuration to copy attributes from
            target_config: Configuration to copy attributes to
        """
        for attr in dir(source_config):
            if attr.startswith("_"):
                continue
            try:
                value = getattr(source_config, attr)
                if value is not None and not callable(value):
                    setattr(target_config, attr, value)
            except (AttributeError, TypeError):
                continue

    def _run_query_job(
        self,
        sql_str: str,
        parameters: Any,
        connection: Optional[BigQueryConnection] = None,
        job_config: Optional[QueryJobConfig] = None,
    ) -> QueryJob:
        """Execute a BigQuery job with configuration support.

        Args:
            sql_str: SQL string to execute
            parameters: Query parameters
            connection: Optional BigQuery connection override
            job_config: Optional job configuration

        Returns:
            QueryJob object representing the executed job
        """
        conn = connection or self.connection

        final_job_config = QueryJobConfig()

        if self._default_query_job_config:
            self._copy_job_config_attrs(self._default_query_job_config, final_job_config)

        if job_config:
            self._copy_job_config_attrs(job_config, final_job_config)

        bq_parameters = _create_bq_parameters(parameters)
        final_job_config.query_parameters = bq_parameters

        return conn.query(sql_str, job_config=final_job_config)

    @staticmethod
    def _rows_to_results(rows_iterator: Any) -> list[dict[str, Any]]:
        """Convert BigQuery rows to dictionary format.

        Args:
            rows_iterator: BigQuery rows iterator

        Returns:
            List of dictionaries representing the rows
        """
        return [dict(row) for row in rows_iterator]

    def _try_special_handling(self, cursor: "Any", statement: "SQL") -> "Optional[SQLResult]":
        """Hook for BigQuery-specific special operations.

        BigQuery doesn't have complex special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.

        Args:
            cursor: BigQuery cursor object
            statement: SQL statement to analyze

        Returns:
            None - always proceeds with standard execution for BigQuery
        """
        _ = (cursor, statement)
        return None

    def _transform_ast_with_literals(self, sql: str, parameters: Any) -> str:
        """Transform SQL AST by replacing placeholders with literal values.

        Args:
            sql: SQL string to transform
            parameters: Parameters to embed as literals

        Returns:
            Transformed SQL string with literals embedded
        """
        if not parameters:
            return sql

        try:
            ast = sqlglot.parse_one(sql, dialect="bigquery")
        except sqlglot.ParseError:
            return sql

        placeholder_counter = {"index": 0}

        def replace_placeholder(node: exp.Expression) -> exp.Expression:
            """Replace placeholder nodes with literal values."""
            if isinstance(node, exp.Placeholder):
                if isinstance(parameters, (list, tuple)):
                    current_index = placeholder_counter["index"]
                    placeholder_counter["index"] += 1
                    if current_index < len(parameters):
                        return self._create_literal_node(parameters[current_index])
                return node
            if isinstance(node, exp.Parameter):
                param_name = str(node.this) if hasattr(node.this, "__str__") else node.this
                if isinstance(parameters, dict):
                    possible_names = [param_name, f"@{param_name}", f":{param_name}", f"param_{param_name}"]
                    for name in possible_names:
                        if name in parameters:
                            actual_value = getattr(parameters[name], "value", parameters[name])
                            return self._create_literal_node(actual_value)
                    return node
                if isinstance(parameters, (list, tuple)):
                    try:
                        if param_name.startswith("param_"):
                            param_index = int(param_name[6:])
                            if param_index < len(parameters):
                                return self._create_literal_node(parameters[param_index])

                        if param_name.isdigit():
                            param_index = int(param_name)
                            if param_index < len(parameters):
                                return self._create_literal_node(parameters[param_index])
                    except (ValueError, IndexError, AttributeError):
                        pass
                return node
            return node

        transformed_ast = ast.transform(replace_placeholder)

        return transformed_ast.sql(dialect="bigquery")

    def _create_literal_node(self, value: Any) -> "exp.Expression":
        """Create a SQLGlot literal expression from a Python value.

        Args:
            value: Python value to convert to SQLGlot literal

        Returns:
            SQLGlot expression representing the literal value
        """
        if value is None:
            return exp.Null()
        if isinstance(value, bool):
            return exp.Boolean(this=value)
        if isinstance(value, (int, float)):
            return exp.Literal.number(str(value))
        if isinstance(value, str):
            return exp.Literal.string(value)
        if isinstance(value, (list, tuple)):
            items = [self._create_literal_node(item) for item in value]
            return exp.Array(expressions=items)
        if isinstance(value, dict):
            json_str = to_json(value)
            return exp.Literal.string(json_str)

        return exp.Literal.string(str(value))

    def _execute_script(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute SQL script with statement splitting and parameter handling.

        Parameters are embedded as static values for script execution compatibility.

        Args:
            cursor: BigQuery cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with script execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_job = None

        for stmt in statements:
            job = self._run_query_job(stmt, prepared_parameters or {}, connection=cursor)
            job.result()
            last_job = job
            successful_count += 1

        cursor.job = last_job

        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def _execute_many(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """BigQuery execute_many implementation using script-based execution.

        BigQuery doesn't support traditional execute_many with parameter batching.
        Instead, we generate a script with multiple INSERT statements using
        AST transformation to embed literals safely.

        Args:
            cursor: BigQuery cursor object
            statement: SQL statement to execute with multiple parameter sets

        Returns:
            ExecutionResult with batch execution details
        """

        parameters_list = statement.parameters

        if not parameters_list or not isinstance(parameters_list, (list, tuple)):
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        base_sql = statement.sql

        script_statements = []
        for param_set in parameters_list:
            transformed_sql = self._transform_ast_with_literals(base_sql, param_set)
            script_statements.append(transformed_sql)

        script_sql = ";\n".join(script_statements)

        cursor.job = self._run_query_job(script_sql, None, connection=cursor)
        cursor.job.result()

        affected_rows = (
            cursor.job.num_dml_affected_rows if cursor.job.num_dml_affected_rows is not None else len(parameters_list)
        )
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def _execute_statement(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute single SQL statement with BigQuery data handling.

        Args:
            cursor: BigQuery cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with query results and metadata
        """
        sql, parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.job = self._run_query_job(sql, parameters, connection=cursor)

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
        affected_rows = cursor.job.num_dml_affected_rows or 0
        return self.create_execution_result(cursor, rowcount_override=affected_rows)
