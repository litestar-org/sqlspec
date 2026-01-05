# ruff: noqa: F401
"""BigQuery driver implementation.

Provides Google Cloud BigQuery connectivity with parameter style conversion,
type coercion, error handling, and query job management.
"""

import io
import os
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

import sqlglot
from google.api_core.retry import Retry
from google.cloud.bigquery import LoadJobConfig, QueryJob, QueryJobConfig
from google.cloud.exceptions import GoogleCloudError
from sqlglot import exp

from sqlspec.adapters.bigquery._typing import BigQueryConnection, BigQuerySessionContext
from sqlspec.adapters.bigquery.core import _build_bigquery_profile, _create_bq_parameters
from sqlspec.adapters.bigquery.data_dictionary import BigQuerySyncDataDictionary
from sqlspec.adapters.bigquery.type_converter import BigQueryOutputConverter
from sqlspec.core import (
    StatementConfig,
    build_literal_inlining_transform,
    build_statement_config_from_profile,
    create_arrow_result,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import ExecutionResult, SyncDriverAdapterBase
from sqlspec.exceptions import (
    DatabaseConnectionError,
    DataError,
    MissingDependencyError,
    NotFoundError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    StorageCapabilityError,
    UniqueViolationError,
)
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_guards import has_errors

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, ArrowResult, SQLResult, Statement, StatementFilter
    from sqlspec.driver import SyncDataDictionaryBase
    from sqlspec.storage import (
        StorageBridgeJob,
        StorageDestination,
        StorageFormat,
        StorageTelemetry,
        SyncStoragePipeline,
    )
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

logger = get_logger(__name__)

__all__ = (
    "BigQueryCursor",
    "BigQueryDriver",
    "BigQueryExceptionHandler",
    "BigQuerySessionContext",
    "bigquery_statement_config",
    "build_bigquery_statement_config",
)

HTTP_CONFLICT = 409
HTTP_NOT_FOUND = 404
HTTP_BAD_REQUEST = 400
HTTP_FORBIDDEN = 403
HTTP_SERVER_ERROR = 500


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
            try:
                self._map_bigquery_exception(exc_val)
            except Exception as mapped:
                self.pending_exception = mapped
                return True
        return False

    def _map_bigquery_exception(self, e: Any) -> None:
        """Map BigQuery exception to SQLSpec exception.

        Args:
            e: Google API exception instance
        """
        try:
            status_code = e.code
        except AttributeError:
            status_code = None
        error_msg = str(e).lower()

        if status_code == HTTP_CONFLICT or "already exists" in error_msg:
            self._raise_unique_violation(e, status_code)
        elif status_code == HTTP_NOT_FOUND or "not found" in error_msg:
            self._raise_not_found_error(e, status_code)
        elif status_code == HTTP_BAD_REQUEST:
            self._handle_bad_request(e, status_code, error_msg)
        elif status_code == HTTP_FORBIDDEN:
            self._raise_connection_error(e, status_code)
        elif status_code and status_code >= HTTP_SERVER_ERROR:
            self._raise_operational_error(e, status_code)
        else:
            self._raise_generic_error(e, status_code)

    def _handle_bad_request(self, e: Any, code: "int | None", error_msg: str) -> None:
        """Handle 400 Bad Request errors.

        Args:
            e: Exception instance
            code: HTTP status code
            error_msg: Lowercase error message
        """
        if "syntax" in error_msg or "invalid query" in error_msg:
            self._raise_parsing_error(e, code)
        elif "type" in error_msg or "format" in error_msg:
            self._raise_data_error(e, code)
        else:
            self._raise_generic_error(e, code)

    def _raise_unique_violation(self, e: Any, code: "int | None") -> None:
        code_str = f"[HTTP {code}]" if code else ""
        msg = f"BigQuery resource already exists {code_str}: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_not_found_error(self, e: Any, code: "int | None") -> None:
        code_str = f"[HTTP {code}]" if code else ""
        msg = f"BigQuery resource not found {code_str}: {e}"
        raise NotFoundError(msg) from e

    def _raise_parsing_error(self, e: Any, code: "int | None") -> None:
        code_str = f"[HTTP {code}]" if code else ""
        msg = f"BigQuery query syntax error {code_str}: {e}"
        raise SQLParsingError(msg) from e

    def _raise_data_error(self, e: Any, code: "int | None") -> None:
        code_str = f"[HTTP {code}]" if code else ""
        msg = f"BigQuery data error {code_str}: {e}"
        raise DataError(msg) from e

    def _raise_connection_error(self, e: Any, code: "int | None") -> None:
        code_str = f"[HTTP {code}]" if code else ""
        msg = f"BigQuery permission denied {code_str}: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_operational_error(self, e: Any, code: "int | None") -> None:
        code_str = f"[HTTP {code}]" if code else ""
        msg = f"BigQuery operational error {code_str}: {e}"
        raise OperationalError(msg) from e

    def _raise_generic_error(self, e: Any, code: "int | None") -> None:
        msg = f"BigQuery error [HTTP {code}]: {e}" if code else f"BigQuery error: {e}"
        raise SQLSpecError(msg) from e


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
            cache_config = get_cache_config()
            statement_config = bigquery_statement_config.replace(cache_config=cache_config)

        parameter_json_serializer = statement_config.parameter_config.json_serializer
        if parameter_json_serializer is None:
            parameter_json_serializer = features.get("json_serializer", to_json)

        self._json_serializer: Callable[[Any], str] = parameter_json_serializer
        self._literal_inliner = build_literal_inlining_transform(json_serializer=self._json_serializer)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._default_query_job_config: QueryJobConfig | None = (driver_features or {}).get("default_query_job_config")
        self._data_dictionary: SyncDataDictionaryBase | None = None
        self._using_emulator = self._detect_emulator_endpoint(connection)
        self._job_retry_deadline = float(features.get("job_retry_deadline", 60.0))
        self._job_retry = None if self._using_emulator else self._build_job_retry()

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

    def handle_database_exceptions(self) -> "BigQueryExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately."""
        return BigQueryExceptionHandler()

    @staticmethod
    def _detect_emulator_endpoint(connection: BigQueryConnection) -> bool:
        """Detect whether the BigQuery client targets an emulator endpoint."""

        emulator_host = os.getenv("BIGQUERY_EMULATOR_HOST") or os.getenv("BIGQUERY_EMULATOR_HOST_HTTP")
        if emulator_host:
            return True

        try:
            inner_connection = cast("Any", connection)._connection
        except AttributeError:
            inner_connection = None
        if inner_connection is None:
            return False
        try:
            api_base_url = inner_connection.API_BASE_URL
        except AttributeError:
            api_base_url = ""
        if not api_base_url:
            return False
        return "googleapis.com" not in api_base_url

    def _build_job_retry(self) -> Retry:
        """Build retry policy for job restarts based on error reason codes."""

        return Retry(predicate=self._should_retry_job_exception, deadline=self._job_retry_deadline)

    @staticmethod
    def _should_retry_job_exception(exception: Exception) -> bool:
        """Return True when a BigQuery job exception is safe to retry."""

        if not isinstance(exception, GoogleCloudError):
            return False

        errors = exception.errors if has_errors(exception) and exception.errors is not None else []
        retryable_reasons = {
            "backendError",
            "internalError",
            "jobInternalError",
            "rateLimitExceeded",
            "jobRateLimitExceeded",
        }

        for err in errors:
            if not isinstance(err, dict):
                continue
            reason = err.get("reason")
            message = (err.get("message") or "").lower()
            if reason in retryable_reasons:
                # Emulator sometimes reports invalid DML as jobInternalError; guard with obvious syntax hints
                return not ("nonexistent_column" in message or ("column" in message and "not present" in message))

        return False

    def _should_copy_attribute(self, attr: str, source_config: QueryJobConfig) -> bool:
        """Check if attribute should be copied between job configs.

        Args:
            attr: Attribute name to check.
            source_config: Source configuration object.

        Returns:
            True if attribute should be copied, False otherwise.
        """
        if attr.startswith("_"):
            return False

        try:
            value = source_config.__getattribute__(attr)
            return value is not None and not callable(value)
        except (AttributeError, TypeError):
            return False

    def _copy_job_config_attrs(self, source_config: QueryJobConfig, target_config: QueryJobConfig) -> None:
        """Copy non-private attributes from source config to target config.

        Args:
            source_config: Configuration to copy attributes from.
            target_config: Configuration to copy attributes to.
        """
        for attr in dir(source_config):
            if not self._should_copy_attribute(attr, source_config):
                continue

            try:
                value = source_config.__getattribute__(attr)
                setattr(target_config, attr, value)
            except (AttributeError, TypeError):
                continue

    def _map_source_format(self, file_format: "StorageFormat") -> str:
        if file_format == "parquet":
            return "PARQUET"
        if file_format in {"json", "jsonl"}:
            return "NEWLINE_DELIMITED_JSON"
        msg = f"BigQuery does not support loading '{file_format}' artifacts via the storage bridge"
        raise StorageCapabilityError(msg, capability="parquet_import_enabled")

    def _build_load_job_config(self, file_format: "StorageFormat", overwrite: bool) -> LoadJobConfig:
        job_config = LoadJobConfig()
        job_config.source_format = self._map_source_format(file_format)
        job_config.write_disposition = "WRITE_TRUNCATE" if overwrite else "WRITE_APPEND"
        return job_config

    def _build_load_job_telemetry(self, job: QueryJob, table: str, *, format_label: str) -> "StorageTelemetry":
        try:
            properties = cast("Any", job)._properties
        except AttributeError:
            properties = {}
        load_stats = properties.get("statistics", {}).get("load", {})
        rows_processed = int(load_stats.get("outputRows") or 0)
        bytes_processed = int(load_stats.get("outputBytes") or load_stats.get("inputFileBytes", 0) or 0)
        duration = 0.0
        if job.ended and job.started:
            duration = (job.ended - job.started).total_seconds()
        telemetry: StorageTelemetry = {
            "destination": table,
            "rows_processed": rows_processed,
            "bytes_processed": bytes_processed,
            "duration_s": duration,
            "format": format_label,
        }
        return telemetry

    def _run_query_job(
        self,
        sql_str: str,
        parameters: Any,
        connection: BigQueryConnection | None = None,
        job_config: QueryJobConfig | None = None,
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

        bq_parameters = _create_bq_parameters(parameters, self._json_serializer)
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

    def _inline_literals(self, expression: "sqlglot.Expression", parameters: Any) -> str:
        """Inline literal values into a parsed SQLGlot expression."""

        if not parameters:
            return expression.sql(dialect="bigquery")

        transformed_expression, _ = self._literal_inliner(expression, parameters)
        return cast("str", transformed_expression.sql(dialect="bigquery"))

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
            job.result(job_retry=self._job_retry)
            last_job = job
            successful_count += 1

        cursor.job = last_job

        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def _is_simple_insert_operation(self, sql: str) -> bool:
        """Check if SQL is a simple INSERT VALUES statement.

        Args:
            sql: SQL string to analyze

        Returns:
            True if this is a simple INSERT with VALUES, False otherwise
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect="bigquery")
            if not isinstance(parsed, exp.Insert):
                return False
            return parsed.expression is not None or parsed.find(exp.Values) is not None
        except Exception:
            return False

    def _extract_table_from_insert(self, sql: str) -> str | None:
        """Extract table name from INSERT statement using sqlglot.

        Args:
            sql: INSERT SQL statement

        Returns:
            Fully qualified table name or None if extraction fails
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect="bigquery")
            if isinstance(parsed, exp.Insert):
                table = parsed.find(exp.Table)
                if table:
                    parts = []
                    if table.catalog:
                        parts.append(table.catalog)
                    if table.db:
                        parts.append(table.db)
                    parts.append(table.name)
                    return ".".join(parts)
        except Exception:
            logger.debug("Failed to extract table name from INSERT statement")
        return None

    def _execute_bulk_insert(self, cursor: Any, sql: str, parameters: "list[dict[str, Any]]") -> ExecutionResult | None:
        """Execute INSERT using Parquet bulk load.

        Leverages existing storage bridge infrastructure for optimized bulk inserts.

        Args:
            cursor: BigQuery cursor object
            sql: INSERT SQL statement
            parameters: List of parameter dictionaries

        Returns:
            ExecutionResult if successful, None to fall back to literal inlining
        """
        table_name = self._extract_table_from_insert(sql)
        if not table_name:
            return None

        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            arrow_table = pa.Table.from_pylist(parameters)

            buffer = io.BytesIO()
            pq.write_table(arrow_table, buffer)
            buffer.seek(0)

            job_config = self._build_load_job_config("parquet", overwrite=False)
            job = self.connection.load_table_from_file(buffer, table_name, job_config=job_config)
            job.result()

            return self.create_execution_result(cursor, rowcount_override=len(parameters), is_many_result=True)
        except ImportError:
            logger.debug("pyarrow not available, falling back to literal inlining")
            return None
        except Exception as e:
            logger.debug("Bulk insert failed, falling back to literal inlining: %s", e)
            return None

    def _execute_many_with_inlining(self, cursor: Any, sql: str, parameters: "list[dict[str, Any]]") -> ExecutionResult:
        """Execute many using literal inlining.

        Fallback path for UPDATE/DELETE or when bulk insert unavailable.

        Args:
            cursor: BigQuery cursor object
            sql: SQL statement
            parameters: List of parameter dictionaries

        Returns:
            ExecutionResult with batch execution details
        """
        try:
            parsed_expression = sqlglot.parse_one(sql, dialect="bigquery")
        except sqlglot.ParseError:
            parsed_expression = None

        script_statements = []
        for param_set in parameters:
            if parsed_expression is None:
                script_statements.append(sql)
                continue

            expression_copy = parsed_expression.copy()
            script_statements.append(self._inline_literals(expression_copy, param_set))

        script_sql = ";\n".join(script_statements)

        cursor.job = self._run_query_job(script_sql, None, connection=cursor)
        cursor.job.result(job_retry=self._job_retry)

        affected_rows = (
            cursor.job.num_dml_affected_rows if cursor.job.num_dml_affected_rows is not None else len(parameters)
        )
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def _execute_many(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """BigQuery execute_many with Parquet bulk load optimization.

        Uses Parquet bulk load for INSERT operations (fast path) and falls back
        to literal inlining for UPDATE/DELETE operations.

        Args:
            cursor: BigQuery cursor object
            statement: SQL statement to execute with multiple parameter sets

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters or not isinstance(prepared_parameters, list):
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        if self._is_simple_insert_operation(sql):
            result = self._execute_bulk_insert(cursor, sql, prepared_parameters)
            if result is not None:
                return result

        return self._execute_many_with_inlining(cursor, sql, prepared_parameters)

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
        job_result = cursor.job.result(job_retry=self._job_retry)
        statement_type = str(cursor.job.statement_type or "").upper()
        is_select_like = (
            statement.returns_rows() or statement_type == "SELECT" or self._should_force_select(statement, cursor)
        )

        if is_select_like:
            rows_list = self._rows_to_results(iter(job_result))
            column_names = [field.name for field in cursor.job.schema] if cursor.job.schema else []

            return self.create_execution_result(
                cursor,
                selected_data=rows_list,
                column_names=column_names,
                data_row_count=len(rows_list),
                is_select_result=True,
            )

        affected_rows = cursor.job.num_dml_affected_rows or 0
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        BigQuery does not support transactions.

        Returns:
            False - BigQuery has no transaction support.
        """
        return False

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = BigQuerySyncDataDictionary()
        return self._data_dictionary

    def _storage_api_available(self) -> bool:
        """Check if BigQuery Storage API is available.

        Returns:
            True if Storage API is available and working, False otherwise
        """
        try:
            from google.cloud import bigquery_storage_v1  # pyright: ignore
        except ImportError:
            # Package not installed
            return False
        except Exception:
            # API not enabled or permissions issue
            return False
        else:
            return True

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

        # Check Storage API availability
        if not self._storage_api_available():
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
        import pyarrow as pa

        # Prepare statement
        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)

        # Get compiled SQL and parameters
        sql, driver_params = self._get_compiled_sql(prepared_statement, config)

        # Execute query using existing _run_query_job method
        with self.handle_database_exceptions():
            query_job = self._run_query_job(sql, driver_params)
            query_job.result()  # Wait for completion

            # Native Arrow via Storage API
            arrow_table = query_job.to_arrow()

            # Apply schema casting if requested
            if arrow_schema is not None:
                if not isinstance(arrow_schema, pa.Schema):
                    msg = f"arrow_schema must be a pyarrow.Schema, got {type(arrow_schema).__name__}"
                    raise TypeError(msg)
                arrow_table = arrow_table.cast(arrow_schema)

            if return_format == "batch":
                batches = arrow_table.to_batches(max_chunksize=batch_size)
                arrow_data: Any = batches[0] if batches else pa.RecordBatch.from_pydict({})
            elif return_format == "batches":
                arrow_data = arrow_table.to_batches(max_chunksize=batch_size)
            elif return_format == "reader":
                batches = arrow_table.to_batches(max_chunksize=batch_size)
                arrow_data = pa.RecordBatchReader.from_batches(arrow_table.schema, batches)
            else:
                arrow_data = arrow_table

        # Create ArrowResult
        return create_arrow_result(statement=prepared_statement, data=arrow_data, rows_affected=arrow_table.num_rows)

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
        job_config = self._build_load_job_config("parquet", overwrite)
        job = self.connection.load_table_from_file(buffer, table, job_config=job_config)
        job.result()
        telemetry_payload = self._build_load_job_telemetry(job, table, format_label="parquet")
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
        job_config = self._build_load_job_config(file_format, overwrite)
        job = self.connection.load_table_from_uri(source, table, job_config=job_config)
        job.result()
        telemetry_payload = self._build_load_job_telemetry(job, table, format_label=file_format)
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload)


_BIGQUERY_PROFILE = _build_bigquery_profile()

register_driver_profile("bigquery", _BIGQUERY_PROFILE)


def build_bigquery_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> StatementConfig:
    """Construct the BigQuery statement configuration with optional JSON serializer."""

    serializer = json_serializer or to_json
    return build_statement_config_from_profile(
        _BIGQUERY_PROFILE, statement_overrides={"dialect": "bigquery"}, json_serializer=serializer
    )


bigquery_statement_config = build_bigquery_statement_config()
