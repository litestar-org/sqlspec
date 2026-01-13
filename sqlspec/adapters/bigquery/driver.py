# ruff: noqa: F401
"""BigQuery driver implementation.

Provides Google Cloud BigQuery connectivity with parameter style conversion,
type coercion, error handling, and query job management.
"""

import io
from typing import TYPE_CHECKING, Any, cast

from google.cloud.exceptions import GoogleCloudError

from sqlspec.adapters.bigquery._typing import BigQueryConnection, BigQuerySessionContext
from sqlspec.adapters.bigquery.core import (
    build_dml_rowcount,
    build_inlined_script,
    build_load_job_config,
    build_load_job_telemetry,
    build_retry,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    detect_emulator,
    driver_profile,
    is_simple_insert,
    normalize_script_rowcount,
    run_query_job,
    storage_api_available,
    try_bulk_insert,
)
from sqlspec.adapters.bigquery.data_dictionary import BigQueryDataDictionary
from sqlspec.adapters.bigquery.type_converter import BigQueryOutputConverter
from sqlspec.core import (
    StatementConfig,
    build_arrow_result_from_table,
    build_literal_inlining_transform,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import ExecutionResult, SyncDriverAdapterBase
from sqlspec.exceptions import MissingDependencyError, SQLSpecError, StorageCapabilityError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Callable

    from google.cloud.bigquery import QueryJob, QueryJobConfig
    from typing_extensions import Self

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, ArrowResult, SQLResult, Statement, StatementFilter
    from sqlspec.storage import (
        StorageBridgeJob,
        StorageDestination,
        StorageFormat,
        StorageTelemetry,
        SyncStoragePipeline,
    )
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

logger = get_logger(__name__)

__all__ = ("BigQueryCursor", "BigQueryDriver", "BigQueryExceptionHandler", "BigQuerySessionContext")


class BigQueryCursor:
    """BigQuery cursor with resource management."""

    __slots__ = ("connection", "job")

    def __init__(self, connection: "BigQueryConnection") -> None:
        self.connection = connection
        self.job: QueryJob | None = None

    def __enter__(self) -> "BigQueryConnection":
        return self.connection

    def __exit__(self, *_: Any) -> None:
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
    """Context manager for handling BigQuery API exceptions.

    Maps HTTP status codes and error reasons to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "BigQueryExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        _ = exc_tb
        if exc_type is None:
            return False
        if issubclass(exc_type, GoogleCloudError):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class BigQueryDriver(SyncDriverAdapterBase):
    """BigQuery driver implementation.

    Provides Google Cloud BigQuery connectivity with parameter style conversion,
    type coercion, error handling, and query job management.
    """

    __slots__ = (
        "_data_dictionary",
        "_default_query_job_config",
        "_job_retry",
        "_job_retry_deadline",
        "_json_serializer",
        "_literal_inliner",
        "_type_converter",
        "_using_emulator",
    )
    dialect = "bigquery"

    def __init__(
        self,
        connection: BigQueryConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        features = driver_features or {}

        enable_uuid_conversion = features.get("enable_uuid_conversion", True)
        self._type_converter = BigQueryOutputConverter(enable_uuid_conversion=enable_uuid_conversion)

        if statement_config is None:
            statement_config = default_statement_config.replace(cache_config=get_cache_config())

        parameter_json_serializer = statement_config.parameter_config.json_serializer
        if parameter_json_serializer is None:
            parameter_json_serializer = features.get("json_serializer", to_json)

        self._json_serializer: Callable[[Any], str] = parameter_json_serializer
        self._literal_inliner = build_literal_inlining_transform(json_serializer=self._json_serializer)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._default_query_job_config: QueryJobConfig | None = (driver_features or {}).get("default_query_job_config")
        self._data_dictionary: BigQueryDataDictionary | None = None
        self._using_emulator = detect_emulator(connection)
        self._job_retry_deadline = float(features.get("job_retry_deadline", 60.0))
        self._job_retry = build_retry(self._job_retry_deadline, self._using_emulator)

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def dispatch_execute(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute single SQL statement with BigQuery data handling.

        Args:
            cursor: BigQuery cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with query results and metadata
        """
        sql, parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.job = run_query_job(
            cursor,
            sql,
            parameters,
            default_job_config=self._default_query_job_config,
            job_config=None,
            json_serializer=self._json_serializer,
        )
        job_result = cursor.job.result(job_retry=self._job_retry)
        statement_type = str(cursor.job.statement_type or "").upper()
        is_select_like = (
            statement.returns_rows() or statement_type == "SELECT" or self._should_force_select(statement, cursor)
        )

        if is_select_like:
            rows_list, column_names = collect_rows(job_result, cursor.job.schema)

            return self.create_execution_result(
                cursor,
                selected_data=rows_list,
                column_names=column_names,
                data_row_count=len(rows_list),
                is_select_result=True,
            )

        affected_rows = build_dml_rowcount(cursor.job, 0)
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """BigQuery execute_many with Parquet bulk load optimization.

        Uses Parquet bulk load for INSERT operations (fast path) and falls back
        to literal inlining for UPDATE/DELETE operations.

        Args:
            cursor: BigQuery cursor object
            statement: SQL statement to execute with multiple parameter sets

        Returns:
            ExecutionResult with batch execution details
        """
        compiled_statement, prepared_parameters = self._get_compiled_statement(statement, self.statement_config)
        sql = compiled_statement.compiled_sql
        parsed_expression = compiled_statement.expression

        if not prepared_parameters:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        if isinstance(prepared_parameters, tuple):
            prepared_parameters = list(prepared_parameters)

        if not isinstance(prepared_parameters, list):
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        allow_parse = statement.statement_config.enable_parsing
        if is_simple_insert(sql, parsed_expression, allow_parse=allow_parse):
            rowcount = try_bulk_insert(
                self.connection, sql, prepared_parameters, parsed_expression, allow_parse=allow_parse
            )
            if rowcount is not None:
                return self.create_execution_result(cursor, rowcount_override=rowcount, is_many_result=True)

        script_sql = build_inlined_script(
            sql, prepared_parameters, parsed_expression, allow_parse=allow_parse, literal_inliner=self._literal_inliner
        )
        cursor.job = run_query_job(
            cursor,
            script_sql,
            None,
            default_job_config=self._default_query_job_config,
            job_config=None,
            json_serializer=self._json_serializer,
        )
        cursor.job.result(job_retry=self._job_retry)
        affected_rows = build_dml_rowcount(cursor.job, len(prepared_parameters))
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> ExecutionResult:
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
        last_rowcount = 0

        for stmt in statements:
            job = run_query_job(
                cursor,
                stmt,
                prepared_parameters or {},
                default_job_config=self._default_query_job_config,
                job_config=None,
                json_serializer=self._json_serializer,
            )
            job.result(job_retry=self._job_retry)
            last_job = job
            last_rowcount = normalize_script_rowcount(last_rowcount, job)
            successful_count += 1

        cursor.job = last_job

        return self.create_execution_result(
            cursor,
            statement_count=len(statements),
            successful_statements=successful_count,
            rowcount_override=last_rowcount,
            is_script_result=True,
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # TRANSACTION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────────

    def begin(self) -> None:
        """Begin transaction - BigQuery doesn't support transactions."""

    def commit(self) -> None:
        """Commit transaction - BigQuery doesn't support transactions."""

    def rollback(self) -> None:
        """Rollback transaction - BigQuery doesn't support transactions."""

    def with_cursor(self, connection: "BigQueryConnection") -> "BigQueryCursor":
        """Create context manager for cursor management.

        Returns:
            BigQueryCursor: Cursor object for query execution
        """
        return BigQueryCursor(connection)

    def handle_database_exceptions(self) -> "BigQueryExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately."""
        return BigQueryExceptionHandler()

    # ─────────────────────────────────────────────────────────────────────────────
    # ARROW API METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def select_to_arrow(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        return_format: "ArrowReturnFormat" = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Execute query and return results as Apache Arrow (BigQuery native with Storage API).

        BigQuery provides native Arrow via Storage API (query_job.to_arrow()).
        Requires google-cloud-bigquery-storage package and API enabled.
        Falls back to dict conversion if Storage API not available.

        Args:
            statement: SQL statement, string, or QueryBuilder
            *parameters: Query parameters or filters
            statement_config: Optional statement configuration override
            return_format: "table" for pyarrow.Table (default), "batch" for RecordBatch,
                "batches" for list of RecordBatch, "reader" for RecordBatchReader
            native_only: If True, raise error if Storage API unavailable (default: False)
            batch_size: Batch size hint (for future streaming implementation)
            arrow_schema: Optional pyarrow.Schema for type casting
            **kwargs: Additional keyword arguments

        Returns:
            ArrowResult with native Arrow data (if Storage API available) or converted data

        Raises:
            MissingDependencyError: If pyarrow not installed, or if Storage API not available and native_only=True

        Example:
            >>> # Will use native Arrow if Storage API available, otherwise converts
            >>> result = driver.select_to_arrow(
            ...     "SELECT * FROM dataset.users WHERE age > @age",
            ...     {"age": 18},
            ... )
            >>> df = result.to_pandas()

            >>> # Force native Arrow (raises if Storage API unavailable)
            >>> result = driver.select_to_arrow(
            ...     "SELECT * FROM dataset.users", native_only=True
            ... )
        """
        ensure_pyarrow()

        if not storage_api_available():
            if native_only:
                msg = (
                    "BigQuery native Arrow requires Storage API.\n"
                    "1. Install: pip install google-cloud-bigquery-storage\n"
                    "2. Enable API: https://console.cloud.google.com/apis/library/bigquerystorage.googleapis.com\n"
                    "3. Grant permissions: roles/bigquery.dataViewer"
                )
                raise MissingDependencyError(
                    package="google-cloud-bigquery-storage", install_package="google-cloud-bigquery-storage"
                ) from RuntimeError(msg)

            # Fallback to conversion path
            result: ArrowResult = super().select_to_arrow(
                statement,
                *parameters,
                statement_config=statement_config,
                return_format=return_format,
                native_only=native_only,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
                **kwargs,
            )
            return result

        # Use native path with Storage API
        # Prepare statement
        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)

        # Get compiled SQL and parameters
        sql, driver_params = self._get_compiled_sql(prepared_statement, config)

        with self.handle_database_exceptions():
            query_job = run_query_job(
                self.connection,
                sql,
                driver_params,
                default_job_config=self._default_query_job_config,
                job_config=None,
                json_serializer=self._json_serializer,
            )
            query_job.result()  # Wait for completion

            # Native Arrow via Storage API
            arrow_table = query_job.to_arrow()

            return build_arrow_result_from_table(
                prepared_statement,
                arrow_table,
                return_format=return_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )
        msg = "Unreachable"
        raise RuntimeError(msg)  # pragma: no cover

    # ─────────────────────────────────────────────────────────────────────────────
    # STORAGE API METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def select_to_storage(
        self,
        statement: "Statement | QueryBuilder | SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, object] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Execute a query and persist Arrow results to a storage backend."""

        self._require_capability("arrow_export_enabled")
        arrow_result = self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        sync_pipeline = self._storage_pipeline()
        telemetry_payload = self._write_result_to_storage_sync(
            arrow_result, destination, format_hint=format_hint, pipeline=sync_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data by uploading a temporary Parquet payload to BigQuery."""

        self._require_capability("parquet_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        ensure_pyarrow()

        import pyarrow.parquet as pq

        buffer = io.BytesIO()
        pq.write_table(arrow_table, buffer)
        buffer.seek(0)
        job_config = build_load_job_config("parquet", overwrite)
        job = self.connection.load_table_from_file(buffer, table, job_config=job_config)
        job.result()
        telemetry_payload = build_load_job_telemetry(job, table, format_label="parquet")
        if telemetry:
            telemetry_payload.setdefault("extra", {})
            telemetry_payload["extra"]["arrow_rows"] = telemetry.get("rows_processed")
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload)

    def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load staged artifacts from storage into BigQuery."""

        if file_format != "parquet":
            msg = "BigQuery storage bridge currently supports Parquet ingest only"
            raise StorageCapabilityError(msg, capability="parquet_import_enabled")
        job_config = build_load_job_config(file_format, overwrite)
        job = self.connection.load_table_from_uri(source, table, job_config=job_config)
        job.result()
        telemetry_payload = build_load_job_telemetry(job, table, format_label=file_format)
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload)

    # ─────────────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    @property
    def data_dictionary(self) -> "BigQueryDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = BigQueryDataDictionary()
        return self._data_dictionary

    # ─────────────────────────────────────────────────────────────────────────────
    # PRIVATE / INTERNAL METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        BigQuery does not support transactions.

        Returns:
            False - BigQuery has no transaction support.
        """
        return False


register_driver_profile("bigquery", driver_profile)
