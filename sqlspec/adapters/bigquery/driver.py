# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false

import datetime
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional, Union

from google.cloud.bigquery import ArrayQueryParameter, QueryJob, QueryJobConfig, ScalarQueryParameter

from sqlspec.adapters.bigquery._types import BigQueryConnection
from sqlspec.adapters.bigquery.pipeline_steps import bigquery_execute_many_to_script_step
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.driver._common import ExecutionResult
from sqlspec.exceptions import SQLSpecError
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.statement.sql import SQL, StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.result import SQLResult


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
    sql: str, params: Any
) -> "tuple[str, list[Union[ArrayQueryParameter, ScalarQueryParameter]]]":
    """Transform parameters to BigQuery QueryParameter objects."""
    if not params:
        return sql, []

    if not isinstance(params, dict):
        # This shouldn't happen with NAMED_AT style, but handle gracefully
        return sql, []

    bq_params: list[Union[ArrayQueryParameter, ScalarQueryParameter]] = []

    for name, value in params.items():
        param_name_for_bq = name.lstrip("@")

        # Handle TypedParameter wrapper
        actual_value = getattr(value, "value", value)

        param_type, array_element_type = _get_bq_param_type(actual_value)

        if param_type == "ARRAY" and array_element_type:
            bq_params.append(ArrayQueryParameter(param_name_for_bq, array_element_type, actual_value))
        elif param_type == "JSON":
            json_str = to_json(actual_value)
            bq_params.append(ScalarQueryParameter(param_name_for_bq, "STRING", json_str))
        elif param_type:
            bq_params.append(ScalarQueryParameter(param_name_for_bq, param_type, actual_value))
        else:
            msg = f"Unsupported BigQuery parameter type for value of param '{name}': {type(actual_value)}"
            raise SQLSpecError(msg)

    return sql, bq_params


# BigQuery type coercion for the built-in pipeline
bigquery_type_coercion_map = {
    # Keep most types as-is since BigQuery handles them well
    bool: lambda x: x,
    int: lambda x: x,
    float: lambda x: x,
    str: lambda x: x,
    bytes: lambda x: x,
    datetime.datetime: lambda x: x,
    datetime.date: lambda x: x,
    datetime.time: lambda x: x,
    Decimal: lambda x: x,
    # Convert dicts to JSON strings for BigQuery
    dict: to_json,
    # Keep lists as-is for BigQuery arrays
    list: lambda x: x,
    tuple: list,  # Convert tuples to lists for BigQuery arrays
    type(None): lambda _: None,
}


bigquery_statement_config = StatementConfig(
    dialect="bigquery",
    # Enhanced pipeline steps for BigQuery-specific processing
    pre_process_steps=[bigquery_execute_many_to_script_step],  # Convert execute_many to script before parameterization
    post_process_steps=None,  # No post-processing needed for BigQuery
    # Core pipeline configuration
    enable_parsing=True,
    enable_transformations=True,
    enable_validation=True,
    enable_caching=True,
    # Parameter processing configuration
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

__all__ = ("BigQueryCursor", "BigQueryDriver", "bigquery_statement_config")

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
        # BigQuery doesn't need cleanup for cursors
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
        # Set default BigQuery-specific configuration
        if statement_config is None:
            statement_config = bigquery_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

        # Get default query job config from driver_features
        features = driver_features or {}
        default_query_job_config = features.get("default_query_job_config")
        self._default_query_job_config = default_query_job_config

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

    def _execute_many(self, cursor: Any, sql: str, prepared_params: Any, statement: SQL) -> ExecutionResult:
        """BigQuery execute_many implementation - should be converted to script by pipeline."""
        # The pipeline step should have converted this to a script execution
        # If we reach here, it means the pipeline step didn't work as expected
        # Fall back to individual execution
        if not prepared_params:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        logger.warning("BigQuery execute_many: Pipeline conversion to script failed, using fallback individual execution")

        total_rows_affected = 0
        last_job = None

        for param_set in prepared_params:
            last_job = self._run_query_job(sql, param_set, connection=cursor.connection)
            last_job.result()

            if last_job.num_dml_affected_rows is not None:
                total_rows_affected += last_job.num_dml_affected_rows

        return self.create_execution_result(last_job, rowcount_override=total_rows_affected, is_many_result=True)

    def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any, statement: SQL) -> ExecutionResult:
        """BigQuery single statement execution."""
        # Trust the pipeline - prepared_params comes from output_transformer as BigQuery QueryParameter objects
        cursor.job = self._run_query_job(sql, prepared_params, connection=cursor.connection)

        if statement.returns_rows():
            # Extract data immediately for SELECT operations
            query_job = cursor.job
            job_result = query_job.result()
            rows_list = self._rows_to_results(iter(job_result))
            column_names = [field.name for field in query_job.schema] if query_job.schema else []

            return self.create_execution_result(
                cursor,
                selected_data=rows_list,
                column_names=column_names,
                data_row_count=len(rows_list),
                is_select_result=True,
            )

        # For non-SELECT operations, get row count
        query_job = cursor.job
        query_job.result()
        row_count = query_job.num_dml_affected_rows or 0
        return self.create_execution_result(cursor, rowcount_override=row_count)
