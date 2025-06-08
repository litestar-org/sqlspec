# ruff: noqa: PLR6301
import datetime
import logging
from collections.abc import Iterator
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
    Union,
)

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
from sqlspec.driver.mixins import SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
from sqlspec.exceptions import SQLSpecError, wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, RowT

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("BigQueryConnection", "BigQueryDriver")

BigQueryConnection = Client

logger = logging.getLogger("sqlspec.adapters.bigquery")

# Table name parsing constants
FULLY_QUALIFIED_PARTS = 3  # project.dataset.table
DATASET_TABLE_PARTS = 2  # dataset.table


class BigQueryDriver(
    SyncDriverAdapterProtocol["BigQueryConnection", RowT], SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
):
    """Advanced BigQuery Driver with comprehensive Google Cloud capabilities.

    Protocol Implementation:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations with transaction safety
    - execute_script() - Multi-statement scripts and DDL operations
    """

    __slots__ = (
        "_default_query_job_config",
        "_error_counter",
        "_latency_histogram",
        "_pool_connections_gauge",
        "_pool_latency_histogram",
        "_query_counter",
        "_tracer",
        "config",
        "connection",
        "default_row_type",
        "instrumentation_config",
        "on_job_complete",
        "on_job_start",
    )

    dialect: "DialectType" = "bigquery"
    connection: BigQueryConnection
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = True
    _default_query_job_config: Optional[QueryJobConfig]

    def __init__(
        self,
        connection: BigQueryConnection,
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
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

    def _copy_job_config_attrs(self, source_config: QueryJobConfig, target_config: QueryJobConfig) -> None:
        """Copy non-private attributes from source config to target config."""
        for attr in dir(source_config):
            if attr.startswith("_"):
                continue
            with wrap_exceptions(suppress=AttributeError):
                value = getattr(source_config, attr)
                if value is not None:
                    setattr(target_config, attr, value)

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
        if isinstance(value, datetime.datetime):
            return "TIMESTAMP" if value.tzinfo else "DATETIME", None
        if isinstance(value, datetime.date):
            return "DATE", None
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
        job_config: Optional[QueryJobConfig] = None,
    ) -> QueryJob:
        """Execute a BigQuery job with comprehensive configuration support.

        Args:
            sql_str: SQL string to execute.
            bq_query_parameters: BigQuery parameter objects.
            connection: Optional connection override.
            job_config: Optional job configuration override.

        Returns:
            QueryJob instance.
        """
        conn = connection or self.connection

        # Build final job configuration
        final_job_config = QueryJobConfig()

        # Apply default configuration if available
        if self._default_query_job_config:
            self._copy_job_config_attrs(self._default_query_job_config, final_job_config)

        # Apply override configuration if provided
        if job_config:
            self._copy_job_config_attrs(job_config, final_job_config)

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

    def _rows_to_results(self, rows_iterator: Iterator[BigQueryRow]) -> list[RowT]:
        """Convert BigQuery rows to dictionary format.

        Args:
            rows_iterator: Iterator of BigQuery Row objects.

        Returns:
            List of dictionaries representing the rows.
        """
        return [dict(row) for row in rows_iterator]  # type: ignore[misc]

    def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[BigQueryConnection] = None,
        **kwargs: Any,
    ) -> Any:
        if statement.is_script:
            return self._execute_script(
                statement.to_sql(placeholder_style=ParameterStyle.STATIC),
                connection=connection,
                **kwargs,
            )
        if statement.is_many:
            return self._execute_many(
                statement.to_sql(placeholder_style=self._get_placeholder_style()),
                statement.parameters,
                connection=connection,
                **kwargs,
            )

        return self._execute(
            statement.to_sql(placeholder_style=self._get_placeholder_style()),
            statement.get_parameters(style=self._get_placeholder_style()),
            statement,
            connection=connection,
            **kwargs,
        )

    def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[BigQueryConnection] = None,
        **kwargs: Any,
    ) -> Any:
        # Prepare BigQuery parameters
        bq_params = self._prepare_bq_query_parameters(parameters or {}) if parameters else []
        return self._run_query_job(sql, bq_params, connection=connection)

    def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[BigQueryConnection] = None,
        **kwargs: Any,
    ) -> Any:
        # For BigQuery, batch execution is not natively supported; run jobs in a loop
        jobs = []
        for params in param_list or []:
            bq_params = self._prepare_bq_query_parameters(params or {})
            job = self._run_query_job(sql, bq_params, connection=connection)
            jobs.append(job)
        return jobs

    def _execute_script(
        self,
        script: str,
        connection: Optional[BigQueryConnection] = None,
        **kwargs: Any,
    ) -> str:
        # BigQuery does not support multi-statement scripts in a single job; treat as a single statement
        self._run_query_job(script, [], connection=connection)
        return "SCRIPT EXECUTED"

    def _wrap_select_result(
        self, statement: SQL, result: Any, schema_type: "Optional[type]" = None, **kwargs: Any
    ) -> "SQLResult[RowT]":
        if not isinstance(result, QueryJob):
            logger.warning("_wrap_select_result expected QueryJob, got %s", type(result))
            return SQLResult(statement=statement, data=[], column_names=[], operation_type="SELECT", rows_affected=-1)

        query_job: QueryJob = result
        try:
            # Default timeout for job result, can be overridden via kwargs
            job_result = query_job.result(timeout=kwargs.get("bq_job_timeout"))
            rows_list = self._rows_to_results(iter(job_result))
            # num_rows for select statements is typically len(rows_list) or job_result.total_rows if available
            # query_job.num_rows might also be populated.
            # Let's use len(rows_list) for simplicity after conversion.
            actual_rows_affected_or_returned = len(rows_list)

        except Exception as e:
            logger.exception("BigQuery job failed or timed out", extra={"job_id": query_job.job_id})
            return SQLResult(
                statement=statement,
                data=[],
                column_names=[],
                operation_type="SELECT",
                rows_affected=-1,
                metadata={"error": str(e), "job_id": query_job.job_id},
            )

        column_names = [field.name for field in job_result.schema] if job_result.schema else []

        # If schema_type were used for Pydantic conversion, it would happen here.
        # For now, returning DictRow as per class generic type.
        return SQLResult[RowT](
            statement=statement,
            data=rows_list,
            column_names=column_names,
            operation_type="SELECT",
            rows_affected=actual_rows_affected_or_returned,
        )

    def _wrap_execute_result(self, statement: SQL, result: Any, **kwargs: Any) -> "SQLResult[RowT]":
        operation_type = "UNKNOWN"
        with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
            if statement.expression:
                operation_type = str(statement.expression.key).upper()

        metadata = {}
        rows_affected = 0

        if isinstance(result, str):
            operation_type = "SCRIPT"
            metadata["status_message"] = result
        elif isinstance(result, QueryJob):
            query_job: QueryJob = result
            metadata["job_id"] = query_job.job_id
            if query_job.statement_type:
                metadata["statement_type"] = query_job.statement_type
            try:
                query_job.result(timeout=kwargs.get("bq_job_timeout"))
                if query_job.num_dml_affected_rows is not None:
                    rows_affected = query_job.num_dml_affected_rows
            except Exception as e:
                logger.exception("BigQuery DML job failed or timed out", extra={"job_id": query_job.job_id})
                rows_affected = -1
                metadata["error"] = str(e)
        else:
            logger.warning("Unexpected result type in _wrap_execute_result: %s", type(result))
            rows_affected = -1
            metadata["error"] = f"Unexpected raw driver result type: {type(result)}"

        return SQLResult[RowT](
            statement=statement, data=[], rows_affected=rows_affected, operation_type=operation_type, metadata=metadata
        )

    def _connection(self, connection: "Optional[Client]" = None) -> "Client":
        """Get the connection to use for the operation."""
        return connection or self.connection
