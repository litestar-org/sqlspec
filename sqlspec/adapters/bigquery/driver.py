# ruff: noqa: F401
"""BigQuery driver implementation.

Provides Google Cloud BigQuery connectivity with parameter style conversion,
type coercion, error handling, and query job management.
"""

import io
from itertools import chain
from typing import TYPE_CHECKING, Any, cast

from google.cloud.bigquery.retry import POLLING_DEFAULT_VALUE
from google.cloud.exceptions import GoogleCloudError

from sqlspec.adapters.bigquery._typing import BigQueryConnection, BigQueryCursor, BigQuerySessionContext
from sqlspec.adapters.bigquery.core import (
    DEFAULT_REQUEST_TIMEOUT,
    BigQueryStreamSource,
    _run_query_and_wait,
    _uses_local_bigquery_endpoint,
    build_arrow_write_stream_payload,
    build_dml_rowcount,
    build_inlined_script,
    build_load_job_config,
    build_load_job_telemetry,
    build_retry,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    is_simple_insert,
    normalize_script_rowcount,
    resolve_column_names,
    run_query_job,
    storage_api_available,
    try_bulk_insert,
)
from sqlspec.adapters.bigquery.data_dictionary import BigQueryDataDictionary
from sqlspec.core import (
    StatementConfig,
    build_arrow_result_from_reader,
    build_arrow_result_from_table,
    build_literal_inlining_transform,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import BaseSyncExceptionHandler, ExecutionResult, SyncDriverAdapterBase, SyncRowStream
from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from google.api_core.retry import Retry
    from google.cloud import bigquery_storage  # type: ignore[attr-defined, unused-ignore]
    from google.cloud.bigquery import QueryJob, QueryJobConfig

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, ArrowResult, Statement, StatementFilter
    from sqlspec.storage import (
        StorageBridgeJob,
        StorageDestination,
        StorageFormat,
        StorageTelemetry,
        SyncStoragePipeline,
    )
    from sqlspec.typing import ArrowRecordBatch, ArrowRecordBatchReader, ArrowReturnFormat, StatementParameters

__all__ = ("BigQueryCursor", "BigQueryDriver", "BigQueryExceptionHandler", "BigQuerySessionContext")

logger = get_logger(__name__)


class BigQueryExceptionHandler(BaseSyncExceptionHandler):
    """Context manager for handling BigQuery API exceptions.

    Maps HTTP status codes and error reasons to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
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
        "_column_name_cache",
        "_data_dictionary",
        "_default_query_job_config",
        "_enable_storage_write_api",
        "_job_result_kwargs_defaults",
        "_job_result_timeout",
        "_job_retry",
        "_job_retry_deadline",
        "_json_serializer",
        "_literal_inliner",
        "_request_timeout",
        "_use_query_and_wait",
    )
    dialect = "bigquery"

    def __init__(
        self,
        connection: BigQueryConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        features = driver_features or {}

        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        parameter_json_serializer = statement_config.parameter_config.json_serializer
        if parameter_json_serializer is None:
            parameter_json_serializer = features.get("json_serializer", to_json)

        self._json_serializer: Callable[[Any], str] = parameter_json_serializer
        self._literal_inliner = build_literal_inlining_transform(json_serializer=self._json_serializer)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._default_query_job_config: QueryJobConfig | None = (driver_features or {}).get("default_query_job_config")
        self._data_dictionary: BigQueryDataDictionary | None = None
        self._column_name_cache: dict[int, tuple[Any, list[str]]] = {}
        self._job_result_kwargs_defaults = self._build_job_result_kwargs(features)
        self._job_retry_deadline = float(features.get("job_retry_deadline", 60.0))
        self._job_retry: Retry | None = build_retry(self._job_retry_deadline) if self._job_retry_deadline > 0 else None
        self._job_result_timeout: float | object = features.get("job_result_timeout", POLLING_DEFAULT_VALUE)
        self._request_timeout = self._resolve_request_timeout(features)
        self._use_query_and_wait = bool(features.get("use_query_and_wait", False))
        self._enable_storage_write_api = bool(features.get("enable_storage_write_api", False))

    def _resolve_request_timeout(self, features: "dict[str, Any]") -> float:
        timeout = features.get("request_timeout")
        if timeout is None:
            timeout = self._job_result_timeout
        if isinstance(timeout, (int, float)) and not isinstance(timeout, bool):
            return float(timeout)
        return DEFAULT_REQUEST_TIMEOUT

    def _build_job_result_kwargs(self, features: dict[str, Any]) -> dict[str, Any]:
        """Build QueryJob.result keyword arguments for SELECT fetches."""
        job_result_kwargs: dict[str, Any] = {}
        query_page_size = features.get("query_page_size")
        if query_page_size is not None:
            job_result_kwargs["page_size"] = query_page_size
        query_max_results = features.get("query_max_results")
        if query_max_results is not None:
            job_result_kwargs["max_results"] = query_max_results
        return job_result_kwargs

    def _job_request_timeout(self) -> float:
        return self._request_timeout

    def _run_query_job(self, connection: "BigQueryConnection", sql: str, parameters: Any) -> "QueryJob":
        return run_query_job(
            connection,
            sql,
            parameters,
            default_job_config=self._default_query_job_config,
            job_config=None,
            json_serializer=self._json_serializer,
            retry=self._job_retry,
            timeout=self._job_request_timeout(),
            job_retry=self._job_retry,
        )

    def _job_result_kwargs(self) -> dict[str, Any]:
        return dict(self._job_result_kwargs_defaults)

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
        if self._use_query_and_wait:
            row_iterator = _run_query_and_wait(
                cursor,
                sql,
                parameters,
                default_job_config=self._default_query_job_config,
                json_serializer=self._json_serializer,
                retry=self._job_retry,
                wait_timeout=self._job_request_timeout(),
                job_retry=self._job_retry,
            )
            cursor.job = None
            iterator_schema = getattr(row_iterator, "schema", None)
            if statement.returns_rows() or iterator_schema:
                column_names = resolve_column_names(iterator_schema, self._column_name_cache)
                rows_list, _ = collect_rows(row_iterator, iterator_schema, column_names=column_names)

                return self.create_execution_result(
                    cursor,
                    selected_data=rows_list,
                    column_names=column_names,
                    data_row_count=len(rows_list),
                    is_select_result=True,
                    row_format="record",
                )

            affected_rows = build_dml_rowcount(row_iterator, 0)
            return self.create_execution_result(cursor, rowcount_override=affected_rows)

        cursor.job = self._run_query_job(cursor, sql, parameters)
        statement_type = str(cursor.job.statement_type or "").upper()
        is_select_like = (
            statement.returns_rows() or statement_type == "SELECT" or self._should_force_select(statement, cursor)
        )

        if is_select_like:
            job_result = cursor.job.result(
                job_retry=self._job_retry, timeout=self._job_result_timeout, **self._job_result_kwargs()
            )
            job_schema = cursor.job.schema or getattr(job_result, "schema", None)
            column_names = resolve_column_names(job_schema, self._column_name_cache)
            rows_list, _ = collect_rows(job_result, job_schema, column_names=column_names)

            return self.create_execution_result(
                cursor,
                selected_data=rows_list,
                column_names=column_names,
                data_row_count=len(rows_list),
                is_select_result=True,
                row_format="record",
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
                self.connection,
                sql,
                prepared_parameters,
                parsed_expression,
                allow_parse=allow_parse,
                result_timeout=self._job_request_timeout(),
            )
            if rowcount is not None:
                return self.create_execution_result(cursor, rowcount_override=rowcount, is_many_result=True)

        script_sql = build_inlined_script(
            sql, prepared_parameters, parsed_expression, allow_parse=allow_parse, literal_inliner=self._literal_inliner
        )
        cursor.job = self._run_query_job(cursor, script_sql, None)
        cursor.job.result(job_retry=self._job_retry, timeout=self._job_result_timeout)
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
            job = self._run_query_job(cursor, stmt, prepared_parameters or {})
            job.result(job_retry=self._job_retry, timeout=self._job_result_timeout)
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

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native BigQuery row stream backed by page-wise ``RowIterator`` iteration."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        return SyncRowStream(BigQueryStreamSource(self, sql, prepared_parameters, chunk_size))

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
        """
        ensure_pyarrow()

        if return_format in {"reader", "batches"}:
            config = statement_config or self.statement_config
            prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
            sql, driver_params = self._get_compiled_sql(prepared_statement, config)

            exc_handler = self.handle_database_exceptions()
            streaming_result: ArrowResult | None = None
            with exc_handler:
                query_job = self._run_query_job(self.connection, sql, driver_params)
                row_iterator = query_job.result(
                    page_size=batch_size, job_retry=self._job_retry, timeout=self._job_result_timeout
                )
                arrow_batches = row_iterator.to_arrow_iterable(bqstorage_client=self._bqstorage_client_or_none())
                arrow_reader = _bigquery_arrow_reader_from_iterable(arrow_batches)
                if arrow_reader is not None:
                    streaming_result = build_arrow_result_from_reader(
                        prepared_statement,
                        arrow_reader,
                        return_format=return_format,
                        batch_size=batch_size,
                        arrow_schema=arrow_schema,
                    )

            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception from None
            if streaming_result is not None:
                return streaming_result

            return super().select_to_arrow(
                prepared_statement,
                statement_config=config,
                return_format=return_format,
                native_only=native_only,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )

        native_arrow_available = storage_api_available() and not _uses_local_bigquery_endpoint(self.connection)
        if not native_arrow_available:
            if native_only:
                msg = (
                    "BigQuery native Arrow requires Storage API.\n"
                    "1. Install: pip install google-cloud-bigquery-storage\n"
                    "2. Enable API: https://console.cloud.google.com/apis/library/bigquerystorage.googleapis.com\n"
                    "3. Grant permissions: roles/bigquery.dataViewer\n"
                    "4. Use a real BigQuery endpoint instead of the local emulator"
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

        exc_handler = self.handle_database_exceptions()
        arrow_result: ArrowResult | None = None

        with exc_handler:
            query_job = self._run_query_job(self.connection, sql, driver_params)
            query_job.result(
                job_retry=self._job_retry, timeout=self._job_result_timeout, **self._job_result_kwargs()
            )  # Wait for completion

            # Native Arrow via Storage API
            arrow_table = query_job.to_arrow()

            arrow_result = build_arrow_result_from_table(
                prepared_statement,
                arrow_table,
                return_format=return_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )

        if exc_handler.pending_exception is not None:
            raise exc_handler.pending_exception from None

        if arrow_result is None:
            msg = "Unreachable"
            raise RuntimeError(msg)  # pragma: no cover

        return arrow_result

    def _bqstorage_client_or_none(self) -> "bigquery_storage.BigQueryReadClient | None":
        ensure_client = getattr(self.connection, "_ensure_bqstorage_client", None)
        if not callable(ensure_client):
            return None
        try:
            client = ensure_client()
        except Exception:
            return None
        else:
            return cast("bigquery_storage.BigQueryReadClient | None", client)

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
        """Load Arrow data into BigQuery (Parquet load job, or opt-in Storage Write API)."""

        self._require_capability("parquet_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        ensure_pyarrow()

        if self._enable_storage_write_api and not overwrite:
            try:
                telemetry_payload = self._load_arrow_via_storage_write_api(table, arrow_table)
            except ImportError as exc:
                logger.warning("Storage Write API unavailable, falling back to Parquet load job: %s", exc)
            else:
                if telemetry:
                    telemetry_payload.setdefault("extra", {})
                    telemetry_payload["extra"]["arrow_rows"] = telemetry.get("rows_processed")
                self._attach_partition_telemetry(telemetry_payload, partitioner)
                return self._create_storage_job(telemetry_payload)

        import pyarrow.parquet as pq

        buffer = io.BytesIO()
        pq.write_table(arrow_table, buffer)
        buffer.seek(0)
        job_config = build_load_job_config("parquet", overwrite)
        job = self.connection.load_table_from_file(
            buffer, table, job_config=job_config, timeout=self._job_request_timeout()
        )
        job.result(timeout=self._job_request_timeout())
        telemetry_payload = build_load_job_telemetry(job, table, format_label="parquet")
        if telemetry:
            telemetry_payload.setdefault("extra", {})
            telemetry_payload["extra"]["arrow_rows"] = telemetry.get("rows_processed")
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload)

    def _load_arrow_via_storage_write_api(self, table: str, arrow_table: "Any") -> "StorageTelemetry":
        """Ingest an Arrow table via a BigQuery PENDING write stream using native arrow_rows."""
        from google.cloud import bigquery_storage_v1
        from google.cloud.bigquery_storage_v1 import types

        try:
            *prefix, dataset, table_name = table.split(".")
        except ValueError as exc:
            msg = f"Storage Write API requires a dataset-qualified table, got '{table}'"
            raise StorageOperationFailedError(msg) from exc
        project = prefix[-1] if prefix else self.connection.project

        credentials = getattr(self.connection, "_credentials", None)
        client = bigquery_storage_v1.BigQueryWriteClient(credentials=credentials)  # type: ignore[no-untyped-call]
        parent = f"projects/{project}/datasets/{dataset}/tables/{table_name}"
        write_stream = client.create_write_stream(
            parent=parent, write_stream=types.WriteStream(type_=types.WriteStream.Type.PENDING)
        )
        stream_name = write_stream.name

        requests = build_arrow_write_stream_payload(stream_name, arrow_table, types)
        if requests:
            for response in client.append_rows(requests=iter(requests)):
                if response.error.code:
                    msg = f"Storage Write API append failed: {response.error.message}"
                    raise StorageOperationFailedError(msg)
        client.finalize_write_stream(name=stream_name)
        commit = client.batch_commit_write_streams(
            request=types.BatchCommitWriteStreamsRequest(parent=parent, write_streams=[stream_name])
        )
        if getattr(commit, "stream_errors", None):
            msg = f"Storage Write API commit failed: {commit.stream_errors}"
            raise StorageOperationFailedError(msg)

        telemetry_payload = self._build_ingest_telemetry(arrow_table, format_label="arrow-storage-write")
        telemetry_payload["destination"] = table
        return telemetry_payload

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

        job_config = build_load_job_config(file_format, overwrite)
        job = self.connection.load_table_from_uri(
            source, table, job_config=job_config, retry=self._job_retry, timeout=self._job_request_timeout()
        )
        job.result(timeout=self._job_request_timeout())
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

    def collect_rows(self, cursor: Any, fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect BigQuery rows for the direct execution path."""
        schema = cursor.job.schema if cursor.job else None
        column_names = resolve_column_names(schema, self._column_name_cache)
        data, _ = collect_rows(fetched, schema, column_names=column_names)
        return data, column_names, len(data)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve rowcount from BigQuery job for the direct execution path."""
        return build_dml_rowcount(cursor.job, 0) if cursor.job else 0

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        BigQuery does not support transactions.

        Returns:
            False - BigQuery has no transaction support.
        """
        return False


register_driver_profile("bigquery", driver_profile)


def _bigquery_arrow_reader_from_iterable(batches: "Iterable[ArrowRecordBatch]") -> "ArrowRecordBatchReader | None":
    ensure_pyarrow()
    import pyarrow as pa

    iterator = iter(batches)
    try:
        first_batch = next(iterator)
    except StopIteration:
        return None
    return pa.RecordBatchReader.from_batches(first_batch.schema, chain((first_batch,), iterator))
