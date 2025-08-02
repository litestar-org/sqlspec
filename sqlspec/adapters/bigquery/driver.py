# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
import logging
from collections.abc import Iterator
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

from google.cloud.bigquery import ArrayQueryParameter, QueryJob, QueryJobConfig, ScalarQueryParameter
from google.cloud.bigquery.table import Row as BigQueryRow

from sqlspec.adapters.bigquery._types import BigQueryConnection
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.statement.sql import StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL


bigquery_statement_config = StatementConfig(
    dialect="bigquery",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_AT,
        supported_parameter_styles={ParameterStyle.NAMED_AT, ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT},
        execution_parameter_style=ParameterStyle.NAMED_AT,
        type_coercion_map={},
        has_native_list_expansion=True,
        needs_static_script_compilation=True,
    ),
)

__all__ = ("BigQueryCursor", "BigQueryDriver", "bigquery_statement_config")


logger = logging.getLogger("sqlspec.adapters.bigquery")

# Table name parsing constants
FULLY_QUALIFIED_PARTS = 3  # project.dataset.table
DATASET_TABLE_PARTS = 2  # dataset.table
TIMESTAMP_ERROR_MSG_LENGTH = 189  # Length check for timestamp parsing error


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
    """Advanced BigQuery Driver with comprehensive Google Cloud capabilities."""

    dialect: "DialectType" = "bigquery"
    _default_query_job_config: Optional[QueryJobConfig]

    def __init__(
        self,
        connection: BigQueryConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
        default_query_job_config: Optional[QueryJobConfig] = None,
        on_job_start: Optional[Callable[[str], None]] = None,
        on_job_complete: Optional[Callable[[str, Any], None]] = None,
    ) -> None:
        """Initialize BigQuery driver with comprehensive feature support."""
        # Set default BigQuery-specific configuration
        if statement_config is None:
            statement_config = bigquery_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self.on_job_start = on_job_start
        self.on_job_complete = on_job_complete
        conn_default_config = getattr(connection, "default_query_job_config", None)

        if default_query_job_config is not None and isinstance(default_query_job_config, QueryJobConfig):
            self._default_query_job_config = default_query_job_config
        elif conn_default_config is not None and isinstance(conn_default_config, QueryJobConfig):
            self._default_query_job_config = conn_default_config
        else:
            self._default_query_job_config = None

    def _connection(self, connection: "Optional[BigQueryConnection]" = None) -> "BigQueryConnection":
        """Get the connection to use for the operation."""
        return connection or self.connection

    @staticmethod
    def _copy_job_config_attrs(source_config: QueryJobConfig, target_config: QueryJobConfig) -> None:
        """Copy non-private attributes from source config to target config."""
        for attr in dir(source_config):
            if attr.startswith("_"):
                continue
            value = getattr(source_config, attr)
            if value is not None:
                setattr(target_config, attr, value)

    @staticmethod
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
            element_type, _ = BigQueryDriver._get_bq_param_type(value[0])
            if element_type is None:
                msg = f"Unsupported element type in ARRAY: {type(value[0])}"
                raise SQLSpecError(msg)
            return "ARRAY", element_type

        return None, None

    def _prepare_bq_query_parameters(
        self, params_dict: dict[str, Any]
    ) -> list[Union[ScalarQueryParameter, ArrayQueryParameter]]:
        """Convert parameter dictionary to BigQuery parameter objects."""
        bq_params: list[Union[ScalarQueryParameter, ArrayQueryParameter]] = []

        if params_dict:
            for name, value in params_dict.items():
                param_name_for_bq = name.lstrip("@")

                actual_value = getattr(value, "value", value)

                param_type, array_element_type = self._get_bq_param_type(actual_value)

                logger.debug(
                    "Processing parameter %s: value=%r, type=%s, array_element_type=%s",
                    name,
                    actual_value,
                    param_type,
                    array_element_type,
                )

                if param_type == "ARRAY" and array_element_type:
                    bq_params.append(ArrayQueryParameter(param_name_for_bq, array_element_type, actual_value))
                elif param_type == "JSON":
                    json_str = to_json(actual_value)
                    bq_params.append(ScalarQueryParameter(param_name_for_bq, "STRING", json_str))
                elif param_type:
                    bq_params.append(ScalarQueryParameter(param_name_for_bq, param_type, actual_value))
                else:
                    msg = f"Unsupported BigQuery parameter type for value of param '{name}': {type(value)}"
                    raise SQLSpecError(msg)

        return bq_params

    def _run_query_job(
        self,
        sql_str: str,
        bq_query_parameters: Optional[list[Union[ScalarQueryParameter, ArrayQueryParameter]]],
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

        if final_job_config.query_parameters:
            for param in final_job_config.query_parameters:
                param_type = getattr(param, "type_", None) or getattr(param, "array_type", "ARRAY")
                param_value = getattr(param, "value", None) or getattr(param, "values", None)
                logger.debug(
                    "BigQuery parameter: name=%s, type=%s, value=%r (value_type=%s)",
                    param.name,
                    param_type,
                    param_value,
                    type(param_value),
                )
        query_job = conn.query(sql_str, job_config=final_job_config)

        if self.on_job_start and query_job.job_id:
            with contextlib.suppress(Exception):
                self.on_job_start(query_job.job_id)
        if self.on_job_complete and query_job.job_id:
            with contextlib.suppress(Exception):
                self.on_job_complete(query_job.job_id, query_job)

        return query_job

    @staticmethod
    def _rows_to_results(rows_iterator: Iterator[BigQueryRow]) -> list[dict[str, Any]]:
        """Convert BigQuery rows to dictionary format."""
        return [dict(row) for row in rows_iterator]

    def with_cursor(self, connection: "BigQueryConnection") -> "BigQueryCursor":
        """Create and return a context manager for cursor acquisition and cleanup."""
        return BigQueryCursor(connection)

    def begin(self) -> None:
        """Begin transaction - BigQuery doesn't support transactions."""
        # BigQuery doesn't support transactions

    def rollback(self) -> None:
        """Rollback transaction - BigQuery doesn't support transactions."""
        # BigQuery doesn't support transactions

    def commit(self) -> None:
        """Commit transaction - BigQuery doesn't support transactions."""
        # BigQuery doesn't support transactions

    def _try_special_handling(self, cursor: "Any", statement: "SQL") -> "Optional[SQLResult]":
        """Hook for BigQuery-specific special operations.

        BigQuery handles scripts differently by running multiple jobs.
        """
        if statement.is_script:
            sql, params = statement.compile()
            prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=False)
            statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)
            jobs = []

            # Convert params to BigQuery format
            bq_params = self._prepare_bq_query_parameters(self._convert_params_to_dict(prepared_params))

            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    job = self._run_query_job(stmt, bq_params, connection=cursor.connection)
                    jobs.append(job)

            # Store all jobs for result building and calculate script results
            cursor.jobs = jobs
            statement_count = len(statements)
            successful = 0
            total_rows = 0

            # Wait for all jobs to complete and collect results
            for job in jobs:
                job.result()  # Wait for completion
                successful += 1
                total_rows += job.num_dml_affected_rows or 0

            # Create ExecutionResult with script-specific data and build SQLResult directly
            execution_result = self.create_execution_result(
                cursor,
                statement_count=statement_count,
                successful_statements=successful,
                rowcount_override=total_rows,
                is_script_result=True,
                special_data=jobs,  # Store jobs for potential future use
            )
            return self.build_statement_result(statement, execution_result)

        return None

    def _execute_many(self, cursor: "Any", sql: str, prepared_params: Any, statement: "SQL") -> "ExecutionResult":
        """BigQuery doesn't support executemany directly, create script instead."""
        # BigQuery doesn't support executemany directly, create script
        script_parts = []
        all_params: dict[str, Any] = {}
        param_counter = 0

        for param_set in prepared_params:
            if isinstance(param_set, dict):
                param_dict = param_set
            elif isinstance(param_set, (list, tuple)):
                param_dict = {f"param_{i}": val for i, val in enumerate(param_set)}
            else:
                param_dict = {"param_0": param_set}

            param_mapping = {}
            current_sql = sql
            for key, value in param_dict.items():
                new_key = f"p_{param_counter}"
                param_counter += 1
                param_mapping[key] = new_key
                all_params[new_key] = value

            for old_key, new_key in param_mapping.items():
                current_sql = current_sql.replace(f"@{old_key}", f"@{new_key}")

            script_parts.append(current_sql)

        full_script = ";\n".join(script_parts)
        bq_params = self._prepare_bq_query_parameters(all_params)
        cursor.job = self._run_query_job(full_script, bq_params, connection=cursor.connection)

        # Wait for job completion and get row count
        cursor.job.result()
        total_rowcount = cursor.job.num_dml_affected_rows or 0

        return self.create_execution_result(cursor, rowcount_override=total_rowcount, is_many_result=True)

    def _execute_statement(self, cursor: "Any", sql: str, prepared_params: Any, statement: "SQL") -> "ExecutionResult":
        """BigQuery single execution."""
        bq_params = self._prepare_bq_query_parameters(self._convert_params_to_dict(prepared_params))
        cursor.job = self._run_query_job(sql, bq_params, connection=cursor.connection)

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

    def _convert_params_to_dict(self, prepared_params: "Any") -> dict[str, Any]:
        """Convert prepared parameters to a dictionary format for BigQuery."""
        if not prepared_params:
            return {}
        if isinstance(prepared_params, dict):
            return prepared_params
        if isinstance(prepared_params, (list, tuple)):
            return {f"param_{i}": val for i, val in enumerate(prepared_params)}
        return {"param_0": prepared_params}
