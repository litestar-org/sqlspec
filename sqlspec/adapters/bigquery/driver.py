# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import datetime
import logging
from contextlib import contextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional, Union

from google.cloud.bigquery import ArrayQueryParameter, QueryJob, QueryJobConfig, ScalarQueryParameter
from google.cloud.exceptions import GoogleCloudError
from sqlglot import exp

from sqlspec.adapters.bigquery._types import BigQueryConnection
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.driver._common import ExecutionResult
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.statement.sql import SQL, StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.result import SQLResult
__all__ = ("BigQueryCursor", "BigQueryDriver", "BigQueryParameterMapper", "bigquery_statement_config")


class BigQueryParameterMapper:
    def __init__(self, param_mapping: dict[str, Any]) -> None:
        self.param_mapping = param_mapping
        self.placeholder_counter = 0

    def substitute(self, node: Any) -> Any:
        if isinstance(node, exp.Placeholder):
            value = None
            if node.this is None:
                param_key = f"param_{self.placeholder_counter}"
                self.placeholder_counter += 1
                if param_key in self.param_mapping:
                    value = self.param_mapping[param_key]
            elif node.this in self.param_mapping:
                value = self.param_mapping[node.this]

            if value is not None:
                if isinstance(value, (int, float)):
                    return exp.Literal.number(str(value))
                if isinstance(value, bool):
                    return exp.Literal.string("true" if value else "false")
                if value is None:
                    return exp.Null()
                return exp.Literal.string(str(value))
        return node


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


def _bigquery_output_transformer(
    sql: str, parameters: Any
) -> "tuple[str, list[Union[ArrayQueryParameter, ScalarQueryParameter]]]":
    """Transform parameters to BigQuery QueryParameter objects."""
    if not parameters:
        return sql, []

    if not isinstance(parameters, dict):
        return sql, []

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

    return sql, bq_parameters


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
        output_transformer=_bigquery_output_transformer,
        has_native_list_expansion=True,
        needs_static_script_compilation=True,
    ),
)


logger = logging.getLogger("sqlspec.adapters.bigquery")


class BigQueryMockCursor:
    def __init__(self, connection: "BigQueryConnection") -> None:
        self.connection = connection
        self.job: Optional[QueryJob] = None


class BigQueryCursor:
    """Context manager for BigQuery cursor management."""

    def __init__(self, connection: "BigQueryConnection") -> None:
        self.connection = connection
        self.cursor: Optional[BigQueryMockCursor] = None

    def __enter__(self) -> BigQueryMockCursor:
        self.cursor = BigQueryMockCursor(self.connection)
        return self.cursor

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


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

    def with_cursor(self, connection: "BigQueryConnection") -> "BigQueryCursor":
        """Create and return a context manager for cursor acquisition and cleanup."""
        return BigQueryCursor(connection)

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
        bq_query_parameters: "Optional[list[ArrayQueryParameter | ScalarQueryParameter]]",
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

        final_job_config.query_parameters = bq_query_parameters or []

        return conn.query(sql_str, job_config=final_job_config)

    @staticmethod
    def _rows_to_results(rows_iterator: Any) -> list[dict[str, Any]]:
        """Convert BigQuery rows to dictionary format."""
        return [dict(row) for row in rows_iterator]

    def _try_special_handling(self, cursor: "Any", statement: "SQL") -> "Optional[SQLResult]":
        """Hook for BigQuery-specific special operations."""
        # Let the base class handle script execution via the standard pipeline
        return None

    def _execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """BigQuery execute_many implementation - converts to script execution."""
        prepared_parameters = statement.parameters
        if not prepared_parameters:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        script_statements = []

        for param_set in prepared_parameters:
            parsed_expr = statement.expression
            if parsed_expr is None:
                script_statements.append(statement.sql)
                continue

            parsed_expr = parsed_expr.copy()

            param_dict = {}
            if isinstance(param_set, dict):
                for name, value in param_set.items():
                    actual_value = getattr(value, "value", value)
                    param_dict[name.lstrip("@")] = actual_value
            elif isinstance(param_set, (list, tuple)):
                for i, value in enumerate(param_set):
                    actual_value = getattr(value, "value", value)
                    param_dict[f"param_{i}"] = actual_value
            else:
                actual_value = getattr(param_set, "value", param_set)
                param_dict["param_0"] = actual_value

            try:
                substituter = BigQueryParameterMapper(param_dict)
                substituted_expr = parsed_expr.transform(substituter.substitute)
                stmt_sql = substituted_expr.sql(dialect="bigquery")
                script_statements.append(stmt_sql)
            except Exception:
                script_statements.append(statement.sql)

        script_sql = ";\n".join(script_statements) + ";"

        # Create a new SQL statement for the script
        from sqlspec.statement.sql import SQL

        script_statement = SQL(script_sql, statement_config=statement.statement_config)
        return self._execute_script(cursor, script_statement)

    def _execute_statement(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """BigQuery single statement execution."""
        sql = statement.sql
        prepared_parameters = statement.parameters
        cursor.job = self._run_query_job(sql, prepared_parameters, connection=cursor.connection)

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

    def handle_database_exceptions(self) -> "contextmanager[None]":
        """Handle BigQuery-specific exceptions and wrap them appropriately."""
        return contextmanager(self._handle_database_exceptions_impl)()

    def _handle_database_exceptions_impl(self) -> Any:
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
