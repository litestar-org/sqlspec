"""BigQuery driver implementation.

Provides Google Cloud BigQuery connectivity with parameter style conversion,
type coercion, error handling, and query job management.
"""

import io
from collections.abc import Mapping
from itertools import chain
from typing import TYPE_CHECKING, Any, cast

import sqlglot

from sqlspec.adapters.bigquery._typing import BIGQUERY_POLLING_DEFAULT_VALUE as POLLING_DEFAULT_VALUE
from sqlspec.adapters.bigquery._typing import (
    BigQueryConnection,
    BigQueryCursor,
    BigQueryQueryJobConfig,
    BigQuerySessionContext,
    BigQueryStorageWriteModule,
    BigQueryStorageWriteTypes,
    GoogleCloudError,
)
from sqlspec.adapters.bigquery.core import (
    COST_PER_TERABYTE_USD,
    DEFAULT_REQUEST_TIMEOUT,
    BigQueryDryRunResult,
    BigQueryStreamSource,
    _build_export_statement,
    _build_export_uri,
    _resolve_export_format,
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
    ParameterProfile,
    StatementConfig,
    build_arrow_result_from_reader,
    build_arrow_result_from_table,
    build_literal_inlining_transform,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import BaseSyncExceptionHandler, ExecutionResult, SyncDriverAdapterBase, SyncRowStream
from sqlspec.exceptions import ImproperConfigurationError, OperationalError, StorageOperationFailedError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import to_json
from sqlspec.utils.text import split_qualified_identifier

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from sqlspec.adapters.bigquery._typing import BigQueryQueryJob as QueryJob
    from sqlspec.adapters.bigquery._typing import BigQueryQueryJobConfig as QueryJobConfig
    from sqlspec.adapters.bigquery._typing import BigQueryRetry as Retry
    from sqlspec.adapters.bigquery._typing import bigquery_storage_read_module as bigquery_storage
    from sqlspec.adapters.bigquery.core import BigQueryLoadFormat
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, ArrowResult, Statement, StatementFilter
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowRecordBatch, ArrowRecordBatchReader, ArrowReturnFormat, StatementParameters

__all__ = ("BigQueryCursor", "BigQueryDriver", "BigQueryExceptionHandler", "BigQuerySessionContext")

logger = get_logger(__name__)
_DATASET_TABLE_PARTS = 2
_PROJECT_DATASET_TABLE_PARTS = 3


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
        "_in_transaction",
        "_job_result_kwargs_defaults",
        "_job_result_timeout",
        "_job_retry",
        "_job_retry_deadline",
        "_json_serializer",
        "_literal_inliner",
        "_request_timeout",
        "_session_id",
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
        self._use_query_and_wait = bool(features.get("use_query_and_wait", True))
        self._enable_storage_write_api = bool(features.get("enable_storage_write_api", False))
        self._session_id: str | None = None
        self._in_transaction: bool = False

    @property
    def session_id(self) -> str | None:
        """Return the current BigQuery server-side session ID, if active."""
        return self._session_id

    def _can_use_query_and_wait(
        self, statement: "SQL", job_config: "QueryJobConfig | None" = None, sql: str = ""
    ) -> bool:
        """Determine whether statement qualifies for query_and_wait execution."""
        if not self._use_query_and_wait:
            return False
        if not hasattr(self.connection, "query_and_wait"):
            return False
        config = job_config or self._default_query_job_config
        if config is not None:
            if getattr(config, "destination", None) is not None:
                return False
            if getattr(config, "dry_run", False):
                return False
            priority = getattr(config, "priority", None)
            if priority and str(priority).upper() == "BATCH":
                return False
        return "EXPORT DATA OPTIONS" not in sql.upper()

    def dispatch_execute(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute single SQL statement with BigQuery data handling.

        Args:
            cursor: BigQuery cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with query results and metadata
        """
        sql, parameters = self._compiled_sql(statement, self.statement_config)
        statement_job_config = getattr(statement, "job_config", None)
        effective_job_config = statement_job_config or self._default_query_job_config
        if self._can_use_query_and_wait(statement, job_config=effective_job_config, sql=sql):
            row_iterator = _run_query_and_wait(
                cursor,
                sql,
                parameters,
                default_job_config=self._default_query_job_config,
                job_config=statement_job_config,
                json_serializer=self._json_serializer,
                retry=self._job_retry,
                wait_timeout=self._job_request_timeout(),
                job_retry=self._job_retry,
                page_size=self._job_result_kwargs_defaults.get("page_size"),
                max_results=self._job_result_kwargs_defaults.get("max_results"),
                session_id=self._session_id,
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

        cursor.job = self._run_query_job(cursor, sql, parameters, job_config=statement_job_config)
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
        compiled_statement, prepared_parameters = self._compiled_statement(statement, self.statement_config)
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

    def _inline_script_parameters(self, sql: str, parameters: Any) -> str:
        """Inline literal values into a multi-statement script for procedural SQL compatibility."""
        if not parameters:
            return sql
        try:
            expressions = sqlglot.parse(sql, read="bigquery")
            inlined_parts: list[str] = []
            for expr in expressions:
                if expr is not None:
                    transformed, _ = self._literal_inliner(expr.copy(), parameters, ParameterProfile.empty())
                    inlined_parts.append(str(transformed.sql(dialect="bigquery")))
            if inlined_parts:
                return ";\n".join(inlined_parts) + ";"
        except Exception as exc:
            logger.debug("Failed to inline script parameters: %s", exc)
        return sql

    def _extract_script_statement_count(self, job: Any) -> int:
        """Extract total statement count executed by a BigQuery script job."""
        properties = getattr(job, "_properties", {})
        if isinstance(properties, dict):
            stats = properties.get("statistics", {})
            query_stats = stats.get("query", {})
            script_stats = query_stats.get("scriptStatistics", {})
            exec_path = script_stats.get("executionPath", [])
            if exec_path:
                return len(exec_path)
        script_stats = getattr(job, "script_statistics", None)
        if script_stats is not None:
            exec_path = getattr(script_stats, "execution_path", None)
            if exec_path:
                return len(exec_path)
            child_job_ids = getattr(script_stats, "child_job_ids", None)
            if child_job_ids:
                return len(child_job_ids)
        stats = getattr(job, "statistics", None)
        if stats is not None:
            query_stats = getattr(stats, "query", None)
            if query_stats is not None:
                script_statistics = getattr(query_stats, "script_statistics", None) or getattr(
                    query_stats, "scriptStatistics", None
                )
                if script_statistics is not None:
                    exec_path = getattr(script_statistics, "execution_path", None) or getattr(
                        script_statistics, "executionPath", None
                    )
                    if exec_path:
                        return len(exec_path)
        return 1

    def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute SQL script with statement splitting or single procedural query job.

        Args:
            cursor: BigQuery cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with script execution details
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        split_script = bool(
            getattr(statement.statement_config, "split_script_statements", False)
            or self.driver_features.get("split_script_statements", False)
        )

        if split_script:
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

        if prepared_parameters:
            sql = self._inline_script_parameters(sql, prepared_parameters)
            prepared_parameters = None

        cursor.job = self._run_query_job(cursor, sql, prepared_parameters or {})
        cursor.job.result(job_retry=self._job_retry, timeout=self._job_result_timeout)
        rowcount = normalize_script_rowcount(0, cursor.job)
        statement_count = self._extract_script_statement_count(cursor.job)

        return self.create_execution_result(
            cursor,
            statement_count=statement_count,
            successful_statements=statement_count,
            rowcount_override=rowcount,
            is_script_result=True,
        )

    def begin(self) -> None:
        """Begin a multi-statement transaction inside a BigQuery session.

        Raises:
            OperationalError: If a transaction is already active.
        """
        if self._in_transaction:
            msg = "Transaction already in progress"
            raise OperationalError(msg)

        create_session = self._session_id is None
        if hasattr(self.connection, "query"):
            self._run_query_job(self.connection, "BEGIN TRANSACTION;", None, create_session=create_session)
        self._in_transaction = True

    def commit(self) -> None:
        """Commit the active BigQuery multi-statement transaction."""
        if not self._in_transaction:
            return
        if hasattr(self.connection, "query"):
            self._run_query_job(self.connection, "COMMIT TRANSACTION;", None)
        self._in_transaction = False

    def rollback(self) -> None:
        """Rollback the active BigQuery multi-statement transaction."""
        if not self._in_transaction:
            return
        if hasattr(self.connection, "query"):
            self._run_query_job(self.connection, "ROLLBACK TRANSACTION;", None)
        self._in_transaction = False

    def create_savepoint(self, name: str) -> None:
        """Raise because BigQuery does not support savepoints.

        Raises:
            NotImplementedError: Always.
        """
        msg = "BigQuery does not support savepoints."
        raise NotImplementedError(msg)

    def release_savepoint(self, name: str) -> None:
        """Raise because BigQuery does not support savepoints.

        Raises:
            NotImplementedError: Always.
        """
        msg = "BigQuery does not support savepoints."
        raise NotImplementedError(msg)

    def rollback_to_savepoint(self, name: str) -> None:
        """Raise because BigQuery does not support savepoints.

        Raises:
            NotImplementedError: Always.
        """
        msg = "BigQuery does not support savepoints."
        raise NotImplementedError(msg)

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
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        return SyncRowStream(BigQueryStreamSource(self, sql, prepared_parameters, chunk_size))

    def dry_run(
        self,
        statement: "Statement | QueryBuilder | SQL | str",
        *parameters: Any,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> BigQueryDryRunResult:
        """Execute query with dry_run=True to validate and calculate estimated cost without billing.

        Args:
            statement: Query statement to validate.
            *parameters: Query parameters.
            statement_config: Optional statement configuration override.
            **kwargs: Additional statement keyword arguments.

        Returns:
            BigQueryDryRunResult with byte counts, estimated USD cost, and schema.
        """
        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, driver_params = self._compiled_sql(prepared_statement, config)

        job_config = BigQueryQueryJobConfig(dry_run=True, use_query_cache=False)
        if driver_params:
            job_config.query_parameters = driver_params
        if self._default_query_job_config is not None and getattr(self._default_query_job_config, "labels", None):
            job_config.labels = dict(self._default_query_job_config.labels)

        job = self.connection.query(sql, job_config=job_config)

        total_bytes = getattr(job, "total_bytes_processed", 0) or 0
        estimated_cost = (total_bytes / (1024**4)) * COST_PER_TERABYTE_USD

        schema_fields: list[dict[str, Any]] = [
            {
                "name": getattr(field, "name", ""),
                "field_type": getattr(field, "field_type", ""),
                "mode": getattr(field, "mode", "NULLABLE"),
            }
            for field in (getattr(job, "schema", None) or ())
        ]

        referenced_tables: list[str] = []
        raw_tables = getattr(job, "referenced_tables", None)
        if raw_tables:
            for table_ref in raw_tables:
                project = getattr(table_ref, "project", "")
                dataset_id = getattr(table_ref, "dataset_id", "")
                table_id = getattr(table_ref, "table_id", "")
                if project and dataset_id and table_id:
                    referenced_tables.append(f"{project}.{dataset_id}.{table_id}")
                elif dataset_id and table_id:
                    referenced_tables.append(f"{dataset_id}.{table_id}")
                else:
                    referenced_tables.append(str(table_ref))

        statement_type = getattr(job, "statement_type", "SELECT") or "SELECT"

        return {
            "total_bytes_processed": total_bytes,
            "estimated_cost_usd": estimated_cost,
            "schema": schema_fields,
            "referenced_tables": referenced_tables,
            "statement_type": statement_type,
        }

    def handle_database_exceptions(self) -> "BigQueryExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately."""
        return BigQueryExceptionHandler()

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
            MissingDependencyError: If pyarrow is not installed.
        """
        ensure_pyarrow()

        if return_format in {"reader", "batches"}:
            config = statement_config or self.statement_config
            prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
            sql, driver_params = self._compiled_sql(prepared_statement, config)

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
                raise ImproperConfigurationError(msg) from RuntimeError(msg)

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

        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)

        sql, driver_params = self._compiled_sql(prepared_statement, config)

        exc_handler = self.handle_database_exceptions()
        arrow_result: ArrowResult | None = None

        with exc_handler:
            query_job = self._run_query_job(self.connection, sql, driver_params)
            query_job.result(
                job_retry=self._job_retry, timeout=self._job_result_timeout, **self._job_result_kwargs()
            )  # Wait for completion

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
        """Export eligible remote queries natively, or persist client Arrow results.

        Native exports overwrite matching shards but do not remove stale shards.
        Set ``enable_native_storage=False`` to force the client storage writer.
        """

        self._require_capability("arrow_export_enabled")
        export_format = _resolve_export_format(format_hint)
        native_uri = str(destination)
        if (
            self.driver_features.get("enable_native_storage", True)
            and export_format is not None
            and self.storage_pipeline_factory is None
            and not _uses_local_bigquery_endpoint(self.connection)
        ):
            if native_uri.startswith("alias://"):
                native_uri = self._storage_pipeline().resolve_destination(destination).uri
            scheme = native_uri.partition("://")[0]
            connection = self.driver_features.get("native_export_connection")
            if scheme in {"gs", "gcs"} or (scheme in {"s3", "azure"} and connection is not None):
                native_uri = _build_export_uri(native_uri, format_hint)
                config = statement_config or self.statement_config
                prepared = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
                sql, driver_params = self._compiled_sql(prepared, config)
                export_sql = _build_export_statement(sql, native_uri, export_format, connection)
                handler = self.handle_database_exceptions()
                native_telemetry: StorageTelemetry = {
                    "destination": native_uri,
                    "format": format_hint or "parquet",
                    "extra": {"native_export": True},
                }
                span = self.observability.start_storage_span("write", destination=native_uri, format_label=format_hint)
                try:
                    with handler:
                        job = self._run_query_job(self.connection, export_sql, driver_params)
                        job.result(
                            job_retry=self._job_retry, timeout=self._job_result_timeout, **self._job_result_kwargs()
                        )
                        native_telemetry["extra"]["job_id"] = job.job_id
                except Exception as exc:
                    self.observability.end_storage_span(span, error=exc)
                    raise
                if handler.pending_exception is not None:
                    self.observability.end_storage_span(span, error=handler.pending_exception)
                    raise handler.pending_exception from None
                native_telemetry = self.observability.annotate_storage_telemetry(native_telemetry)
                self.observability.end_storage_span(span, telemetry=native_telemetry)
                self._attach_partition_telemetry(native_telemetry, partitioner)
                return self._storage_job(native_telemetry, telemetry)
        arrow_result = self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        sync_pipeline = self._storage_pipeline()
        telemetry_payload = self._write_storage_result(
            arrow_result, destination, format_hint=format_hint, pipeline=sync_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    def load_from_records(
        self,
        table: str,
        records: "Sequence[Mapping[str, Any]] | Sequence[Sequence[Any]]",
        *,
        columns: "list[str] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load in-memory records through BigQuery's newline-delimited JSON load API."""
        rows = _records_to_json_rows(records, columns)
        job_config = build_load_job_config("jsonl", overwrite)
        job = self.connection.load_table_from_json(
            rows, table, job_config=job_config, timeout=self._job_request_timeout()
        )
        job.result(timeout=self._job_request_timeout())
        telemetry_payload = build_load_job_telemetry(job, table, format_label="jsonl")
        return self._storage_job(telemetry_payload)

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
                return self._storage_job(telemetry_payload)

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
        return self._storage_job(telemetry_payload)

    def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "BigQueryLoadFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load staged artifacts from storage into BigQuery."""

        job_config = build_load_job_config(file_format, overwrite)
        gcs_source = _normalize_bigquery_gcs_uri(source)
        if gcs_source is not None:
            job = self.connection.load_table_from_uri(
                gcs_source, table, job_config=job_config, retry=self._job_retry, timeout=self._job_request_timeout()
            )
            source_telemetry: StorageTelemetry | None = None
        else:
            buffer = io.BytesIO()
            for chunk in self._storage_pipeline().stream_read(source):
                buffer.write(chunk)
            source_telemetry = {
                "destination": str(source),
                "bytes_processed": buffer.tell(),
                "rows_processed": 0,
                "format": file_format,
            }
            buffer.seek(0)
            job = self.connection.load_table_from_file(
                buffer, table, job_config=job_config, timeout=self._job_request_timeout()
            )
        job.result(timeout=self._job_request_timeout())
        telemetry_payload = build_load_job_telemetry(job, table, format_label=file_format)
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, source_telemetry)

    @property
    def data_dictionary(self) -> "BigQueryDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = BigQueryDataDictionary()
        return self._data_dictionary

    def collect_rows(self, cursor: Any, fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect BigQuery rows for the direct execution path."""
        schema = cursor.job.schema if cursor.job else None
        column_names = resolve_column_names(schema, self._column_name_cache)
        data, _ = collect_rows(fetched, schema, column_names=column_names)
        return data, column_names, len(data)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve rowcount from BigQuery job for the direct execution path."""
        return build_dml_rowcount(cursor.job, 0) if cursor.job else 0

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

    def _run_query_job(
        self,
        connection: "BigQueryConnection",
        sql: str,
        parameters: Any,
        job_config: "QueryJobConfig | None" = None,
        create_session: bool | None = None,
    ) -> "QueryJob":
        job = run_query_job(
            connection,
            sql,
            parameters,
            default_job_config=self._default_query_job_config,
            job_config=job_config,
            json_serializer=self._json_serializer,
            retry=self._job_retry,
            timeout=self._job_request_timeout(),
            job_retry=self._job_retry,
            session_id=self._session_id,
            create_session=create_session,
        )
        if self._session_id is None:
            session_info = getattr(job, "session_info", None)
            if session_info is not None and getattr(session_info, "session_id", None):
                self._session_id = session_info.session_id
        return job

    def _job_result_kwargs(self) -> dict[str, Any]:
        return dict(self._job_result_kwargs_defaults)

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

    def _load_arrow_via_storage_write_api(
        self, table: str, arrow_table: "Any", *, stream_type: str | None = None
    ) -> "StorageTelemetry":
        """Ingest an Arrow table via BigQuery Storage Write API using native arrow_rows."""
        if BigQueryStorageWriteModule is None or BigQueryStorageWriteTypes is None:
            msg = "google-cloud-bigquery-storage is required for BigQuery Storage Write API ingestion"
            raise ImportError(msg)
        types = BigQueryStorageWriteTypes

        resolved_stream_type = stream_type or self.driver_features.get("storage_write_stream_type", "COMMITTED")
        is_committed = str(resolved_stream_type).upper() == "COMMITTED"

        project, dataset, table_name = _resolve_storage_write_table_path(table, self.connection.project)

        provider = self.driver_features.get("_storage_write_client_provider")
        if provider is not None:
            client = provider(self.connection)
        else:
            credentials = getattr(self.connection, "_credentials", None)
            client = BigQueryStorageWriteModule.BigQueryWriteClient(credentials=credentials)
        parent = f"projects/{project}/datasets/{dataset}/tables/{table_name}"
        write_stream_type_enum = types.WriteStream.Type.COMMITTED if is_committed else types.WriteStream.Type.PENDING
        write_stream = client.create_write_stream(
            parent=parent, write_stream=types.WriteStream(type_=write_stream_type_enum)
        )
        stream_name = write_stream.name

        requests = build_arrow_write_stream_payload(stream_name, arrow_table, types)
        if requests:
            for response in client.append_rows(requests=iter(requests)):
                if response.error.code:
                    msg = f"Storage Write API append failed: {response.error.message}"
                    raise StorageOperationFailedError(msg)

        if not is_committed:
            client.finalize_write_stream(name=stream_name)
            commit = client.batch_commit_write_streams(
                request=types.BatchCommitWriteStreamsRequest(parent=parent, write_streams=[stream_name])
            )
            if getattr(commit, "stream_errors", None):
                msg = f"Storage Write API commit failed: {commit.stream_errors}"
                raise StorageOperationFailedError(msg)

        telemetry_payload = self._ingest_telemetry(arrow_table, format_label="arrow-storage-write")
        telemetry_payload["destination"] = table
        return telemetry_payload

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        Returns:
            True if the connection has an active transaction session.
        """
        return self._in_transaction


def _close_bigquery_cursor(cursor: BigQueryCursor) -> None:
    try:
        if cursor.job is not None:
            if cursor.job.state in {"PENDING", "RUNNING"}:
                cursor.job.cancel()
            cursor.job = None
    except Exception:
        logger.exception("Failed to cancel BigQuery job during cursor cleanup")


def _resolve_storage_write_table_path(table: str, default_project: str) -> tuple[str, str, str]:
    parts = split_qualified_identifier(table, quote_chars="`", allow_bracket_quotes=False)
    if len(parts) == 1 and "." in parts[0]:
        parts = tuple(part for part in parts[0].split(".") if part)
    if len(parts) == _DATASET_TABLE_PARTS:
        dataset, table_name = parts
        return default_project, dataset, table_name
    if len(parts) == _PROJECT_DATASET_TABLE_PARTS:
        project, dataset, table_name = parts
        return project, dataset, table_name
    msg = f"Storage Write API requires a dataset-qualified table, got '{table}'"
    raise StorageOperationFailedError(msg)


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


def _records_to_json_rows(
    records: "Sequence[Mapping[str, Any]] | Sequence[Sequence[Any]]", columns: "list[str] | None"
) -> "list[dict[str, Any]]":
    materialized = records if isinstance(records, list) else list(records)
    if not materialized:
        msg = "load_from_records requires at least one record."
        raise ImproperConfigurationError(msg)

    first = materialized[0]
    if isinstance(first, Mapping):
        if columns is None:
            resolved = list(first.keys())
            expected = set(resolved)
            can_reuse = True
            for record in materialized:
                if not isinstance(record, Mapping):
                    msg = "load_from_records mapping records must all be mappings."
                    raise ImproperConfigurationError(msg)
                record_keys = list(record.keys())
                if set(record_keys) != expected:
                    msg = "load_from_records mapping records must all share the same keys."
                    raise ImproperConfigurationError(msg)
                if type(record) is not dict or record_keys != resolved:
                    can_reuse = False
            validated_records = cast("list[Mapping[str, Any]]", materialized)
            if can_reuse:
                return cast("list[dict[str, Any]]", validated_records)
            return [{column: record[column] for column in resolved} for record in validated_records]

        resolved = columns
        expected = set(resolved)
        rows: list[dict[str, Any]] = []
        for record in materialized:
            if not isinstance(record, Mapping):
                msg = "load_from_records mapping records must all be mappings."
                raise ImproperConfigurationError(msg)
            if set(record.keys()) != expected:
                msg = "load_from_records mapping records must all share the same keys."
                raise ImproperConfigurationError(msg)
            rows.append({column: record[column] for column in resolved})
        return rows

    if columns is None:
        msg = "load_from_records requires columns when records are positional sequences."
        raise ImproperConfigurationError(msg)

    rows = []
    for record in materialized:
        values = list(record)
        if len(values) != len(columns):
            msg = "load_from_records positional records must match the number of columns."
            raise ImproperConfigurationError(msg)
        rows.append(dict(zip(columns, values, strict=True)))
    return rows


def _normalize_bigquery_gcs_uri(source: "StorageDestination") -> str | None:
    if not isinstance(source, str):
        return None
    if source.startswith("gs://"):
        return source
    if source.startswith("gcs://"):
        return f"gs://{source.removeprefix('gcs://')}"
    return None
