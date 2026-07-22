"""BigQuery adapter compiled helpers."""

import contextlib
import datetime
import importlib
import io
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, cast
from urllib.parse import urlparse

import sqlglot
from sqlglot import exp

from sqlspec.core import (
    DriverParameterProfile,
    ParameterProfile,
    ParameterStyle,
    StatementConfig,
    build_null_pruning_transform,
    build_statement_config_from_profile,
)
from sqlspec.exceptions import (
    DataError,
    NotFoundError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    StorageCapabilityError,
    StorageOperationFailedError,
    UniqueViolationError,
)
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_converters import build_uuid_coercions
from sqlspec.utils.type_guards import has_errors, has_value_attribute

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping
    from typing import Literal

    from google.api_core.retry import Retry
    from google.cloud.bigquery import LoadJobConfig, QueryJob, QueryJobConfig

    from sqlspec.adapters.bigquery._typing import BigQueryConnection, BigQueryParam
    from sqlspec.driver._common import SyncExceptionHandler
    from sqlspec.storage import StorageTelemetry
    from sqlspec.typing import StatementParameters

    BigQueryLoadFormat = Literal["jsonl", "json", "parquet", "arrow-ipc", "csv", "avro", "orc"]


__all__ = (
    "BigQueryStreamSource",
    "apply_driver_features",
    "build_arrow_write_stream_payload",
    "build_dml_rowcount",
    "build_inlined_script",
    "build_load_job_config",
    "build_load_job_telemetry",
    "build_profile",
    "build_retry",
    "build_statement_config",
    "collect_rows",
    "copy_job_config",
    "create_mapped_exception",
    "create_parameters",
    "default_statement_config",
    "driver_profile",
    "extract_insert_table",
    "is_simple_insert",
    "normalize_script_rowcount",
    "resolve_column_names",
    "run_query_job",
    "storage_api_available",
    "try_bulk_insert",
)

HTTP_CONFLICT = 409
HTTP_NOT_FOUND = 404
HTTP_BAD_REQUEST = 400
HTTP_FORBIDDEN = 403
HTTP_SERVER_ERROR = 500
COLUMN_CACHE_MAX_SIZE = 256

DEFAULT_REQUEST_TIMEOUT = 120.0
"""Fallback per-request HTTP timeout (seconds) for job API calls.

``google-cloud-bigquery`` defaults the transport timeout to ``None``, which
waits on the socket indefinitely when a server accepts the request but never
responds. Real BigQuery answers ``jobs.insert`` immediately, so a finite bound
only converts hung servers (notably emulators that execute jobs synchronously
inside the HTTP handler) into fast, retryable errors.
"""
LOCAL_BIGQUERY_ENDPOINT_HOSTS = frozenset({"localhost", "127.0.0.1", "0.0.0.0", "::1"})  # noqa: S104
BQ_STORAGE_WRITE_MAX_APPEND_REQUEST_BYTES = 20_000_000


def storage_api_available() -> bool:
    """Return True when the BigQuery Storage API client can be imported."""
    try:
        importlib.import_module("google.cloud.bigquery_storage_v1")
    except Exception:
        return False
    return True


_BIGQUERY_MODULE: Any | None = None
_BQ_TYPE_MAP: "dict[type, tuple[str, str | None]]" = {
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


def try_bulk_insert(
    connection: "BigQueryConnection",
    sql: str,
    parameters: "list[dict[str, Any]]",
    expression: "exp.Expr | None" = None,
    *,
    allow_parse: bool = True,
    result_timeout: float | None = None,
) -> "int | None":
    """Attempt bulk insert via Parquet load.

    Args:
        connection: BigQuery connection instance.
        sql: INSERT SQL statement.
        parameters: Parameter dictionaries for the insert.
        expression: Optional parsed expression to reuse.
        allow_parse: Whether to parse SQL when expression is unavailable.
        result_timeout: Timeout forwarded to the load job request and result wait.

    Returns:
        Inserted row count if bulk insert succeeds, otherwise None.
    """
    if _uses_local_bigquery_endpoint(connection):
        return None

    table_name = extract_insert_table(sql, expression, allow_parse=allow_parse)
    if not table_name:
        return None

    if _has_synthetic_positional_keys(parameters):
        return None

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        arrow_table = pa.Table.from_pylist(parameters)

        buffer = io.BytesIO()
        pq.write_table(arrow_table, buffer)
        buffer.seek(0)

        job_config = build_load_job_config("parquet", overwrite=False)
        job = connection.load_table_from_file(buffer, table_name, job_config=job_config, timeout=result_timeout)
        job.result(timeout=result_timeout)
        return len(parameters)
    except ImportError:
        logger.debug("pyarrow not available, falling back to literal inlining")
        return None
    except Exception as exc:
        logger.debug("Bulk insert failed, falling back to literal inlining: %s", exc)
        return None


_MAX_INLINED_INSERT_ROWS = 500


def build_inlined_script(
    sql: str,
    parameters: "list[dict[str, Any]]",
    expression: "exp.Expr | None" = None,
    *,
    allow_parse: bool = True,
    literal_inliner: "Callable[[Any, Any, ParameterProfile], tuple[Any, Any]]",
) -> str:
    """Build a BigQuery script with literal inlining.

    Simple INSERT ... VALUES batches collapse into chunked multi-row INSERT
    statements; other statements emit one inlined statement per parameter set.

    Args:
        sql: SQL statement to inline.
        parameters: Parameter dictionaries to inline.
        expression: Optional parsed expression to reuse.
        allow_parse: Whether to parse SQL when expression is unavailable.
        literal_inliner: Callable used to inline literal values.

    Returns:
        Script SQL with inlined parameters.
    """
    parsed_expression = expression
    if parsed_expression is None and allow_parse:
        try:
            parsed_expression = sqlglot.parse_one(sql, dialect="bigquery")
        except sqlglot.ParseError:
            parsed_expression = None

    if parsed_expression is None:
        return ";\n".join([sql] * len(parameters))

    multi_row_script = _build_multi_row_insert_script(parsed_expression, parameters, literal_inliner)
    if multi_row_script is not None:
        return multi_row_script

    script_statements: list[str] = []
    for param_set in parameters:
        expression_copy: exp.Expr = parsed_expression.copy()
        script_statements.append(_inline_bigquery_literals(expression_copy, param_set, literal_inliner))
    return ";\n".join(script_statements)


logger = get_logger("sqlspec.adapters.bigquery.core")


def create_parameters(parameters: Any, json_serializer: "Callable[[Any], str]") -> "list[BigQueryParam]":
    """Create BigQuery QueryParameter objects from parameters.

    Args:
        parameters: Dict of named parameters or list of positional parameters
        json_serializer: Function to serialize dict/list to JSON string

    Returns:
        List of BigQuery QueryParameter objects
    """
    if not parameters:
        return []

    bq_parameters: list[BigQueryParam] = []

    if isinstance(parameters, dict):
        for name, value in parameters.items():
            param_name_for_bq = name.lstrip("@")
            actual_value = value.value if has_value_attribute(value) else value
            param_type, array_element_type = _query_parameter_type(actual_value)

            if param_type == "ARRAY" and array_element_type:
                bq_parameters.append(_create_array_parameter(param_name_for_bq, actual_value, array_element_type))
            elif param_type == "JSON":
                bq_parameters.append(_create_json_parameter(param_name_for_bq, actual_value, json_serializer))
            elif param_type:
                bq_parameters.append(_create_scalar_parameter(param_name_for_bq, actual_value, param_type))
            else:
                msg = f"Unsupported BigQuery parameter type for value of param '{name}': {type(actual_value)}"
                raise SQLSpecError(msg)

    elif isinstance(parameters, (list, tuple)):
        msg = "BigQuery driver requires named parameters; positional parameters are not supported"
        raise SQLSpecError(msg)

    return bq_parameters


def build_retry(deadline: float) -> "Retry":
    """Build retry policy for job restarts based on error reason codes."""
    from google.api_core.retry import Retry

    return Retry(predicate=_should_retry_bigquery_job, deadline=deadline)


_COPY_JOB_FIELDS: tuple[str, ...] = (
    "allow_large_results",
    "clustering_fields",
    "connection_properties",
    "create_disposition",
    "create_session",
    "default_dataset",
    "destination",
    "destination_encryption_configuration",
    "dry_run",
    "flatten_results",
    "job_timeout_ms",
    "labels",
    "max_slots",
    "maximum_billing_tier",
    "maximum_bytes_billed",
    "priority",
    "range_partitioning",
    "reservation",
    "schema_update_options",
    "script_options",
    "time_partitioning",
    "udf_resources",
    "use_legacy_sql",
    "use_query_cache",
    "write_disposition",
    "write_incremental_results",
)


def copy_job_config(source_config: "QueryJobConfig", target_config: "QueryJobConfig") -> None:
    """Copy known job config fields from source to target."""
    for attr in _COPY_JOB_FIELDS:
        _copy_job_config_field(source_config, target_config, attr)


def run_query_job(
    connection: "BigQueryConnection",
    sql: str,
    parameters: Any,
    *,
    default_job_config: "QueryJobConfig | None",
    job_config: "QueryJobConfig | None",
    json_serializer: "Callable[[Any], str]",
    retry: "Retry | None" = None,
    timeout: float | None = None,
    job_retry: "Retry | None" = None,
    api_method: str | None = None,
    timestamp_precision: Any | None = None,
    job_id: str | None = None,
    job_id_prefix: str | None = None,
) -> "QueryJob":
    """Execute a BigQuery query job with merged configuration.

    Args:
        connection: BigQuery connection instance.
        sql: SQL string to execute.
        parameters: Prepared parameters payload.
        default_job_config: Default job configuration to merge.
        job_config: Optional job configuration override.
        json_serializer: JSON serializer for parameter values.
        retry: Retry policy for the API request that starts the query job.
            ``None`` disables API retries instead of using the client default.
        timeout: Per-request HTTP timeout for the API request that starts the
            query job. ``None`` waits on the transport indefinitely.
        job_retry: Retry policy attached to the returned query job. ``None``
            disables job retries and the client's built-in ``jobs.insert``
            retry wrapper (which carries a fixed 600s deadline).
        api_method: Optional query API method override.
        timestamp_precision: Optional timestamp precision override.
        job_id: Explicit BigQuery job ID.
        job_id_prefix: Prefix used by BigQuery to generate a job ID when
            ``job_id`` is not provided.

    Returns:
        QueryJob object representing the executed job.
    """
    from google.cloud.bigquery import QueryJobConfig

    final_job_config = QueryJobConfig()
    if default_job_config:
        copy_job_config(default_job_config, final_job_config)
    if job_config:
        copy_job_config(job_config, final_job_config)
    final_job_config.query_parameters = create_parameters(parameters, json_serializer)

    query_kwargs: dict[str, Any] = {
        "job_config": final_job_config,
        "retry": retry,
        "timeout": timeout,
        "job_retry": job_retry,
    }
    if api_method is not None:
        query_kwargs["api_method"] = api_method
    if timestamp_precision is not None:
        query_kwargs["timestamp_precision"] = timestamp_precision
    if job_id is not None:
        query_kwargs["job_id"] = job_id
    elif job_id_prefix is not None:
        query_kwargs["job_id_prefix"] = job_id_prefix
    return connection.query(sql, **query_kwargs)


def build_load_job_config(file_format: "BigQueryLoadFormat", overwrite: bool) -> "LoadJobConfig":
    from google.cloud.bigquery import LoadJobConfig

    job_config = LoadJobConfig()
    job_config.source_format = _map_bigquery_source_format(file_format)
    job_config.write_disposition = "WRITE_TRUNCATE" if overwrite else "WRITE_APPEND"
    return job_config


def build_arrow_write_stream_payload(
    stream_name: str,
    arrow_table: Any,
    types: Any,
    *,
    max_request_bytes: int = BQ_STORAGE_WRITE_MAX_APPEND_REQUEST_BYTES,
) -> "list[Any]":
    """Build PENDING-stream AppendRowsRequest payloads from an Arrow table (native arrow_rows)."""
    schema_bytes = arrow_table.schema.serialize().to_pybytes()
    requests: list[Any] = []
    for batch in arrow_table.to_batches():
        _append_sized_arrow_requests(
            requests, stream_name, batch, schema_bytes, types, max_request_bytes=max_request_bytes
        )
    return requests


def build_load_job_telemetry(job: "QueryJob", table: str, *, format_label: str) -> "StorageTelemetry":
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


def is_simple_insert(sql: str, expression: "exp.Expr | None" = None, *, allow_parse: bool = True) -> bool:
    """Check if SQL is a simple INSERT VALUES statement.

    Args:
        sql: SQL string to inspect.
        expression: Optional pre-parsed expression to reuse.
        allow_parse: When False, skip parsing and return False if expression is missing.
    """
    if expression is None and not allow_parse:
        return False
    try:
        parsed = expression or sqlglot.parse_one(sql, dialect="bigquery")
        if not isinstance(parsed, exp.Insert):
            return False
        return parsed.expression is not None or parsed.find(exp.Values) is not None
    except Exception:
        return False


def extract_insert_table(sql: str, expression: "exp.Expr | None" = None, *, allow_parse: bool = True) -> str | None:
    """Extract table name from INSERT statement using sqlglot.

    Args:
        sql: SQL string to inspect.
        expression: Optional pre-parsed expression to reuse.
        allow_parse: When False, skip parsing and return None if expression is missing.
    """
    if expression is None and not allow_parse:
        return None
    try:
        parsed = expression or sqlglot.parse_one(sql, dialect="bigquery")
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


def resolve_column_names(schema: Any | None, cache: "dict[int, tuple[Any, list[str]]]") -> list[str]:
    """Resolve and cache BigQuery schema column names.

    Args:
        schema: BigQuery schema object.
        cache: Driver-local cache keyed by ``id(schema)``.

    Returns:
        Resolved column names.
    """
    if not schema:
        return []

    cache_key = id(schema)
    cached = cache.get(cache_key)
    if cached is not None and cached[0] is schema:
        return cached[1]

    column_names = [field.name for field in schema]
    if len(cache) >= COLUMN_CACHE_MAX_SIZE:
        cache.pop(next(iter(cache)))
    cache[cache_key] = (schema, column_names)
    return column_names


class BigQueryStreamSource:
    """Compiled chunk source streaming dict rows from a BigQuery ``RowIterator`` page by page."""

    __slots__ = ("_chunk_size", "_driver", "_job", "_pages", "_parameters", "_sql")

    def __init__(
        self, driver: "_BigQueryStreamDriver", sql: str, parameters: "StatementParameters", chunk_size: int
    ) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._job: QueryJob | None = None
        self._pages: Iterator[Iterable[_BigQueryRow]] | None = None

    def start(self) -> None:
        from google.cloud.bigquery.retry import DEFAULT_RETRY

        handler = self._driver.handle_database_exceptions()
        with handler:
            page_size = None if _uses_local_bigquery_endpoint(self._driver.connection) else self._chunk_size
            deadline = self._driver._job_retry_deadline
            page_retry = DEFAULT_RETRY.with_timeout(deadline) if deadline > 0 else None
            job = self._driver._run_query_job(self._driver.connection, self._sql, self._parameters)
            row_iterator = job.result(
                page_size=page_size,
                retry=page_retry,
                job_retry=self._driver._job_retry,
                timeout=self._driver._job_result_timeout,
            )
            self._job = job
            self._pages = row_iterator.pages
        self._driver._check_pending_exception(handler)

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        pages = self._pages
        if pages is None:
            return []
        while True:
            handler = self._driver.handle_database_exceptions()
            page: Iterable[_BigQueryRow] | None = None
            with handler:
                page = next(pages, None)
            self._driver._check_pending_exception(handler)
            if page is None:
                return []
            rows = [dict(cast("Mapping[str, Any]", row)) for row in page]
            if rows:
                return rows

    def close(self, error: bool = False) -> None:
        job = self._job
        self._job = None
        self._pages = None
        if job is not None and getattr(job, "state", None) in {"PENDING", "RUNNING"}:
            with contextlib.suppress(Exception):
                job.cancel()


def collect_rows(
    job_result: Any,
    schema: Any | None,
    *,
    column_names: "list[str] | None" = None,
    column_name_cache: "dict[int, tuple[Any, list[str]]] | None" = None,
) -> "tuple[list[Any], list[str]]":
    """Collect BigQuery rows and schema into structured lists.

    Returns raw BigQuery Row objects without copying to dicts.
    Lazy dict materialization is handled by SQLResult when needed.

    Args:
        job_result: BigQuery job result iterator.
        schema: BigQuery schema object (or None).
        column_names: Optional precomputed column names.
        column_name_cache: Optional cache used when column names are not precomputed.

    Returns:
        Tuple of (rows_list, column_names).
    """
    rows_list = job_result if isinstance(job_result, list) else list(iter(job_result))
    if column_names is not None:
        resolved_column_names = column_names
    elif column_name_cache is not None:
        resolved_column_names = resolve_column_names(schema, column_name_cache)
    else:
        resolved_column_names = [field.name for field in schema] if schema else []
    return rows_list, resolved_column_names


def build_dml_rowcount(job: Any, fallback: int) -> int:
    """Resolve affected rowcount for BigQuery DML jobs.

    Args:
        job: BigQuery job object with optional num_dml_affected_rows.
        fallback: Fallback rowcount when job does not expose metadata.

    Returns:
        Resolved rowcount.
    """
    try:
        rowcount = job.num_dml_affected_rows
    except AttributeError:
        return fallback
    if rowcount is None:
        return fallback
    if isinstance(rowcount, int):
        return rowcount
    return fallback


def normalize_script_rowcount(previous: int, job: Any) -> int:
    """Normalize BigQuery script rowcount from the latest job metadata.

    Args:
        previous: Previously recorded rowcount value.
        job: BigQuery job with optional num_dml_affected_rows metadata.

    Returns:
        Updated rowcount value.
    """
    return build_dml_rowcount(job, previous)


def build_profile() -> "DriverParameterProfile":
    """Create the BigQuery driver parameter profile."""

    return DriverParameterProfile(
        name="BigQuery",
        default_style=ParameterStyle.NAMED_AT,
        supported_styles={ParameterStyle.NAMED_AT},
        default_execution_style=ParameterStyle.NAMED_AT,
        supported_execution_styles={ParameterStyle.NAMED_AT},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions={
            int: _identity,
            float: _identity,
            bytes: _identity,
            datetime.datetime: _identity,
            datetime.date: _identity,
            datetime.time: _identity,
            Decimal: _identity,
            dict: _identity,
            list: _identity,
            type(None): _return_none,
            **build_uuid_coercions(),
        },
        default_ast_transformer=build_null_pruning_transform(dialect="bigquery"),
        extras={"json_tuple_strategy": "tuple", "type_coercion_overrides": {list: _identity, tuple: _tuple_to_list}},
        default_dialect="bigquery",
    )


def build_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> StatementConfig:
    """Construct the BigQuery statement configuration with optional JSON serializer."""
    serializer = json_serializer or to_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "bigquery"}, json_serializer=serializer
    )


def apply_driver_features(
    statement_config: StatementConfig, driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply BigQuery driver feature defaults and extract core options."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    features.setdefault("enable_uuid_conversion", True)
    features.setdefault("json_serializer", to_json)
    if not features["enable_uuid_conversion"]:
        type_coercion_map = dict(statement_config.parameter_config.type_coercion_map)
        for uuid_type in build_uuid_coercions():
            type_coercion_map.pop(uuid_type, None)
        parameter_config = statement_config.parameter_config.replace(type_coercion_map=type_coercion_map)
        statement_config = statement_config.replace(parameter_config=parameter_config)
    return statement_config, features


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> SQLSpecError:
    """Map BigQuery exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Mapping priority:
        1. HTTP status codes from BigQuery API
        2. Message pattern matching for specific errors
        3. Default SQLSpecError fallback

    Mapped Statuses:
        * UniqueViolationError: HTTP 409 (Conflict) or "already exists" in message
        * NotFoundError: HTTP 404 (Not Found) or "not found" in message
        * QueryTimeoutError: "timeout", "deadline exceeded", or "cancelled" in message
        * SQLParsingError / DataError / SQLSpecError: HTTP 400 (Bad Request)
        * PermissionDeniedError: HTTP 403 (Forbidden) or "access denied" / "permission denied" in message
        * OperationalError: HTTP 500+ (Server error)

    Args:
        error: The BigQuery exception to map
        logger: Optional logger accepted for adapter signature parity.

    Returns:
        A SQLSpec exception that wraps the original error
    """
    del logger
    try:
        status_code = error.code
    except AttributeError:
        status_code = None
    error_msg = str(error).lower()

    if status_code == HTTP_CONFLICT or "already exists" in error_msg:
        return _create_bigquery_error(error, status_code, UniqueViolationError, "resource already exists")

    if status_code == HTTP_NOT_FOUND or "not found" in error_msg:
        return _create_bigquery_error(error, status_code, NotFoundError, "resource not found")

    if "timeout" in error_msg or "deadline exceeded" in error_msg or "cancelled" in error_msg:
        return _create_bigquery_error(error, status_code, QueryTimeoutError, "query timeout or cancelled")

    if status_code == HTTP_BAD_REQUEST:
        if "syntax" in error_msg or "invalid query" in error_msg:
            return _create_bigquery_error(error, status_code, SQLParsingError, "query syntax error")
        if "type" in error_msg or "format" in error_msg:
            return _create_bigquery_error(error, status_code, DataError, "data error")
        return _create_bigquery_error(error, status_code, SQLSpecError, "error")

    if status_code == HTTP_FORBIDDEN or "access denied" in error_msg or "permission denied" in error_msg:
        return _create_bigquery_error(error, status_code, PermissionDeniedError, "permission denied")

    if status_code and status_code >= HTTP_SERVER_ERROR:
        return _create_bigquery_error(error, status_code, OperationalError, "operational error")

    return _create_bigquery_error(error, status_code, SQLSpecError, "error")


class _BigQueryRow(Protocol):
    def items(self) -> "Iterable[tuple[str, object]]": ...


class _BigQueryStreamDriver(Protocol):
    connection: "BigQueryConnection"
    _job_retry: "Retry | None"
    _job_retry_deadline: float
    _job_result_timeout: float | object

    def handle_database_exceptions(self) -> "SyncExceptionHandler": ...

    def _run_query_job(
        self, connection: "BigQueryConnection", sql: str, parameters: "StatementParameters"
    ) -> "QueryJob": ...

    def _check_pending_exception(self, exc_handler: "SyncExceptionHandler") -> None: ...


def _has_synthetic_positional_keys(parameters: "list[dict[str, Any]]") -> bool:
    """Return True when the first parameter mapping is keyed only by synthetic positional names.

    Synthetic names are ``param_<int>`` or bare digit strings produced by qmark/positional
    parameter expansion. They do not correspond to the target table's columns, so a Parquet
    column-load cannot be built from them.
    """
    if not parameters:
        return False
    first = parameters[0]
    if not isinstance(first, dict) or not first:
        return False
    return all(
        isinstance(key, str) and (key.isdigit() or (key.startswith("param_") and key[6:].isdigit())) for key in first
    )


def _uses_local_bigquery_endpoint(connection: "BigQueryConnection") -> bool:
    """Return True when a BigQuery client points at a local emulator endpoint."""
    bq_conn = cast("object", getattr(connection, "_connection", None))
    api_base_url = cast("str | None", getattr(bq_conn, "API_BASE_URL", None)) if bq_conn is not None else None
    if not isinstance(api_base_url, str):
        return False
    try:
        hostname = urlparse(api_base_url).hostname
    except ValueError:
        return False
    return hostname in LOCAL_BIGQUERY_ENDPOINT_HOSTS


def _build_multi_row_insert_script(
    expression: "exp.Expr",
    parameters: "list[dict[str, Any]]",
    literal_inliner: "Callable[[Any, Any, ParameterProfile], tuple[Any, Any]]",
) -> "str | None":
    """Collapse a single-tuple INSERT ... VALUES batch into chunked multi-row statements.

    Args:
        expression: Parsed statement expression.
        parameters: Parameter dictionaries to inline.
        literal_inliner: Callable used to inline literal values.

    Returns:
        Script SQL with one multi-row INSERT per chunk, or None when the statement
        shape does not support multi-row collapsing.
    """
    if not isinstance(expression, exp.Insert):
        return None
    values = expression.args.get("expression")
    if not isinstance(values, exp.Values):
        return None
    value_tuples = values.expressions
    if len(value_tuples) != 1 or not isinstance(value_tuples[0], exp.Tuple):
        return None

    template_tuple = value_tuples[0]
    inlined_tuples: list[exp.Expr] = []
    for param_set in parameters:
        tuple_copy: exp.Expr = template_tuple.copy()
        if param_set:
            transformed, _ = literal_inliner(tuple_copy, param_set, ParameterProfile.empty())
            if not isinstance(transformed, exp.Tuple):
                return None
            tuple_copy = transformed
        inlined_tuples.append(tuple_copy)

    statements: list[str] = []
    for start in range(0, len(inlined_tuples), _MAX_INLINED_INSERT_ROWS):
        chunk = inlined_tuples[start : start + _MAX_INLINED_INSERT_ROWS]
        statement = expression.copy()
        statement_values = statement.args.get("expression")
        if not isinstance(statement_values, exp.Values):
            return None
        statement_values.set("expressions", chunk)
        statements.append(str(statement.sql(dialect="bigquery")))
    return ";\n".join(statements)


def _create_array_parameter(name: str, value: Any, array_type: str) -> "BigQueryParam":
    """Create BigQuery ARRAY parameter.

    Args:
        name: Parameter name.
        value: Array value (converted to list, empty list if None).
        array_type: BigQuery array element type.

    Returns:
        ArrayQueryParameter instance.
    """
    bigquery = _load_bigquery_module()
    return cast("BigQueryParam", bigquery.ArrayQueryParameter(name, array_type, [] if value is None else list(value)))


def _create_json_parameter(name: str, value: Any, json_serializer: "Callable[[Any], str]") -> "BigQueryParam":
    """Create BigQuery JSON parameter as STRING type.

    Args:
        name: Parameter name.
        value: JSON-serializable value.
        json_serializer: Function to serialize to JSON string.

    Returns:
        ScalarQueryParameter with STRING type.
    """
    bigquery = _load_bigquery_module()
    return cast("BigQueryParam", bigquery.ScalarQueryParameter(name, "STRING", json_serializer(value)))


def _create_scalar_parameter(name: str, value: Any, param_type: str) -> "BigQueryParam":
    """Create BigQuery scalar parameter.

    Args:
        name: Parameter name.
        value: Scalar value.
        param_type: BigQuery parameter type (INT64, FLOAT64, etc.).

    Returns:
        ScalarQueryParameter instance.
    """
    bigquery = _load_bigquery_module()
    return cast("BigQueryParam", bigquery.ScalarQueryParameter(name, param_type, value))


def _load_bigquery_module() -> Any:
    global _BIGQUERY_MODULE
    if _BIGQUERY_MODULE is None:
        from google.cloud import bigquery

        _BIGQUERY_MODULE = bigquery
    return _BIGQUERY_MODULE


def _query_parameter_type(value: Any) -> "tuple[str | None, str | None]":
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
        element_type, _ = _query_parameter_type(value[0])
        if element_type is None:
            msg = f"Unsupported element type in ARRAY: {type(value[0])}"
            raise SQLSpecError(msg)
        return "ARRAY", element_type

    return None, None


def _inline_bigquery_literals(
    expression: "exp.Expr", parameters: Any, inliner: "Callable[[Any, Any, ParameterProfile], tuple[Any, Any]]"
) -> str:
    """Inline literal values into a parsed SQLGlot expression."""
    if not parameters:
        return str(expression.sql(dialect="bigquery"))

    transformed_expression, _ = inliner(expression, parameters, ParameterProfile.empty())
    return str(transformed_expression.sql(dialect="bigquery"))


def _should_retry_bigquery_job(exception: Exception) -> bool:
    """Return True when a BigQuery job exception is safe to retry."""
    from google.cloud.exceptions import GoogleCloudError

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
            return not ("nonexistent_column" in message or ("column" in message and "not present" in message))

    return False


def _copy_job_config_field(source_config: "QueryJobConfig", target_config: "QueryJobConfig", attr: str) -> None:
    try:
        value = getattr(source_config, attr)
    except (AttributeError, TypeError):
        return
    if value is not None:
        setattr(target_config, attr, value)


def _run_query_and_wait(
    connection: "BigQueryConnection",
    sql: str,
    parameters: Any,
    *,
    default_job_config: "QueryJobConfig | None",
    json_serializer: "Callable[[Any], str]",
    retry: "Retry | None" = None,
    wait_timeout: float | None = None,
    job_retry: "Retry | None" = None,
    page_size: int | None = None,
    max_results: int | None = None,
) -> Any:
    """Execute a BigQuery query via query_and_wait and return the row iterator."""
    from google.cloud.bigquery import QueryJobConfig

    final_job_config = QueryJobConfig()
    if default_job_config:
        copy_job_config(default_job_config, final_job_config)
    final_job_config.query_parameters = create_parameters(parameters, json_serializer)

    query_kwargs: dict[str, Any] = {"job_config": final_job_config}
    if retry is not None:
        query_kwargs["retry"] = retry
    if wait_timeout is not None:
        query_kwargs["api_timeout"] = wait_timeout
        query_kwargs["wait_timeout"] = wait_timeout
    if job_retry is not None:
        query_kwargs["job_retry"] = job_retry
    if page_size is not None:
        query_kwargs["page_size"] = page_size
    if max_results is not None:
        query_kwargs["max_results"] = max_results
    return connection.query_and_wait(sql, **query_kwargs)


def _append_sized_arrow_requests(
    requests: "list[Any]", stream_name: str, batch: Any, schema_bytes: bytes, types: Any, *, max_request_bytes: int
) -> None:
    pending = [batch]
    while pending:
        current = pending.pop(0)
        include_schema = not requests
        request = _build_arrow_append_request(
            stream_name, current, schema_bytes, types, include_stream=include_schema, include_schema=include_schema
        )
        if _append_request_size(request) < max_request_bytes:
            requests.append(request)
            continue
        if current.num_rows <= 1:
            msg = "BigQuery Storage Write API row exceeds the maximum AppendRowsRequest size."
            raise StorageOperationFailedError(msg)
        left_count = current.num_rows // 2
        pending.insert(0, current.slice(left_count, current.num_rows - left_count))
        pending.insert(0, current.slice(0, left_count))


def _build_arrow_append_request(
    stream_name: str, batch: Any, schema_bytes: bytes, types: Any, *, include_stream: bool, include_schema: bool
) -> Any:
    arrow_data = types.AppendRowsRequest.ArrowData(
        rows=types.ArrowRecordBatch(serialized_record_batch=batch.serialize().to_pybytes(), row_count=batch.num_rows)
    )
    if include_schema:
        arrow_data.writer_schema = types.ArrowSchema(serialized_schema=schema_bytes)
    request = types.AppendRowsRequest(arrow_rows=arrow_data)
    if include_stream:
        request.write_stream = stream_name
    return request


def _append_request_size(request: Any) -> int:
    try:
        return int(request._pb.ByteSize())
    except AttributeError:
        arrow_rows = request.arrow_rows
        size = len(arrow_rows.rows.serialized_record_batch)
        size += len(getattr(arrow_rows.writer_schema, "serialized_schema", b"") or b"")
        size += len(getattr(request, "write_stream", "").encode())
        return size


def _map_bigquery_source_format(file_format: "BigQueryLoadFormat | str") -> str:
    if file_format == "parquet":
        return "PARQUET"
    if file_format in {"json", "jsonl"}:
        return "NEWLINE_DELIMITED_JSON"
    if file_format == "csv":
        return "CSV"
    if file_format == "avro":
        return "AVRO"
    if file_format == "orc":
        return "ORC"
    msg = f"BigQuery does not support loading '{file_format}' artifacts via the storage bridge"
    raise StorageCapabilityError(msg, capability="parquet_import_enabled")


def _identity(value: Any) -> Any:
    return value


def _tuple_to_list(value: Any) -> Any:
    if isinstance(value, tuple):
        return list(value)
    return value


def _return_none(_: Any) -> None:
    return None


def _create_bigquery_error(
    error: Any, code: "int | None", error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a SQLSpec exception from a BigQuery error.

    Args:
        error: The original BigQuery exception
        code: HTTP status code
        error_class: The SQLSpec exception class to instantiate
        description: Human-readable description of the error type

    Returns:
        A new SQLSpec exception instance with the original as its cause
    """
    code_str = f"[HTTP {code}]" if code else ""
    msg = f"BigQuery {description} {code_str}: {error}" if code_str else f"BigQuery {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


driver_profile = build_profile()

default_statement_config = build_statement_config()
