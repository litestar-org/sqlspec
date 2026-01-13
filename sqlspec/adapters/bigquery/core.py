"""BigQuery adapter compiled helpers."""

import datetime
import importlib
import io
import os
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

import sqlglot
from google.api_core.retry import Retry
from google.cloud.bigquery import LoadJobConfig, QueryJob, QueryJobConfig
from google.cloud.exceptions import GoogleCloudError
from sqlglot import exp

from sqlspec.core import (
    DriverParameterProfile,
    ParameterProfile,
    ParameterStyle,
    StatementConfig,
    build_statement_config_from_profile,
)
from sqlspec.exceptions import (
    DatabaseConnectionError,
    DataError,
    NotFoundError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    StorageCapabilityError,
    UniqueViolationError,
)
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_guards import has_errors, has_value_attribute

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.adapters.bigquery._typing import BigQueryConnection, BigQueryParam
    from sqlspec.storage import StorageFormat, StorageTelemetry

__all__ = (
    "apply_driver_features",
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
    "detect_emulator",
    "driver_profile",
    "extract_insert_table",
    "is_simple_insert",
    "normalize_script_rowcount",
    "run_query_job",
    "storage_api_available",
    "try_bulk_insert",
)

HTTP_CONFLICT = 409
HTTP_NOT_FOUND = 404
HTTP_BAD_REQUEST = 400
HTTP_FORBIDDEN = 403
HTTP_SERVER_ERROR = 500


def _identity(value: Any) -> Any:
    return value


def _tuple_to_list(value: Any) -> Any:
    if isinstance(value, tuple):
        return list(value)
    return value


def _return_none(_: Any) -> None:
    return None


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
    expression: "exp.Expression | None" = None,
    *,
    allow_parse: bool = True,
) -> "int | None":
    """Attempt bulk insert via Parquet load.

    Args:
        connection: BigQuery connection instance.
        sql: INSERT SQL statement.
        parameters: Parameter dictionaries for the insert.
        expression: Optional parsed expression to reuse.
        allow_parse: Whether to parse SQL when expression is unavailable.

    Returns:
        Inserted row count if bulk insert succeeds, otherwise None.
    """
    table_name = extract_insert_table(sql, expression, allow_parse=allow_parse)
    if not table_name:
        return None

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        arrow_table = pa.Table.from_pylist(parameters)

        buffer = io.BytesIO()
        pq.write_table(arrow_table, buffer)
        buffer.seek(0)

        job_config = build_load_job_config("parquet", overwrite=False)
        job = connection.load_table_from_file(buffer, table_name, job_config=job_config)
        job.result()
        return len(parameters)
    except ImportError:
        logger.debug("pyarrow not available, falling back to literal inlining")
        return None
    except Exception as exc:
        logger.debug("Bulk insert failed, falling back to literal inlining: %s", exc)
        return None


def build_inlined_script(
    sql: str,
    parameters: "list[dict[str, Any]]",
    expression: "exp.Expression | None" = None,
    *,
    allow_parse: bool = True,
    literal_inliner: "Callable[[Any, Any, ParameterProfile], tuple[Any, Any]]",
) -> str:
    """Build a BigQuery script with literal inlining.

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

    script_statements: list[str] = []
    for param_set in parameters:
        expression_copy = parsed_expression.copy()
        script_statements.append(_inline_bigquery_literals(expression_copy, param_set, literal_inliner))
    return ";\n".join(script_statements)


def _create_array_parameter(name: str, value: Any, array_type: str) -> "BigQueryParam":
    """Create BigQuery ARRAY parameter.

    Args:
        name: Parameter name.
        value: Array value (converted to list, empty list if None).
        array_type: BigQuery array element type.

    Returns:
        ArrayQueryParameter instance.
    """
    bigquery = _get_bigquery_module()
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
    bigquery = _get_bigquery_module()
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
    bigquery = _get_bigquery_module()
    return cast("BigQueryParam", bigquery.ScalarQueryParameter(name, param_type, value))


def _get_bigquery_module() -> Any:
    global _BIGQUERY_MODULE
    if _BIGQUERY_MODULE is None:
        from google.cloud import bigquery

        _BIGQUERY_MODULE = bigquery
    return _BIGQUERY_MODULE


logger = get_logger("sqlspec.adapters.bigquery.core")


def _get_bq_param_type(value: Any) -> "tuple[str | None, str | None]":
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
            param_type, array_element_type = _get_bq_param_type(actual_value)

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
        msg = "BigQuery driver requires named parameters (e.g., @name); positional parameters are not supported"
        raise SQLSpecError(msg)

    return bq_parameters


def _inline_bigquery_literals(
    expression: "exp.Expression", parameters: Any, inliner: "Callable[[Any, Any, ParameterProfile], tuple[Any, Any]]"
) -> str:
    """Inline literal values into a parsed SQLGlot expression."""
    if not parameters:
        return str(expression.sql(dialect="bigquery"))

    transformed_expression, _ = inliner(expression, parameters, ParameterProfile.empty())
    return str(transformed_expression.sql(dialect="bigquery"))


def detect_emulator(connection: "BigQueryConnection") -> bool:
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


def _should_retry_bigquery_job(exception: Exception) -> bool:
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
            return not ("nonexistent_column" in message or ("column" in message and "not present" in message))

    return False


def build_retry(deadline: float, using_emulator: bool) -> "Retry | None":
    """Build retry policy for job restarts based on error reason codes."""
    if using_emulator:
        return None
    return Retry(predicate=_should_retry_bigquery_job, deadline=deadline)


def _should_copy_job_attribute(attr: str, source_config: QueryJobConfig) -> bool:
    if attr.startswith("_"):
        return False

    try:
        value = source_config.__getattribute__(attr)
        return value is not None and not callable(value)
    except (AttributeError, TypeError):
        return False


def copy_job_config(source_config: QueryJobConfig, target_config: QueryJobConfig) -> None:
    """Copy non-private attributes from source config to target config."""
    for attr in dir(source_config):
        if not _should_copy_job_attribute(attr, source_config):
            continue

        try:
            value = source_config.__getattribute__(attr)
            setattr(target_config, attr, value)
        except (AttributeError, TypeError):
            continue


def run_query_job(
    connection: "BigQueryConnection",
    sql: str,
    parameters: Any,
    *,
    default_job_config: QueryJobConfig | None,
    job_config: QueryJobConfig | None,
    json_serializer: "Callable[[Any], str]",
) -> QueryJob:
    """Execute a BigQuery query job with merged configuration.

    Args:
        connection: BigQuery connection instance.
        sql: SQL string to execute.
        parameters: Prepared parameters payload.
        default_job_config: Default job configuration to merge.
        job_config: Optional job configuration override.
        json_serializer: JSON serializer for parameter values.

    Returns:
        QueryJob object representing the executed job.
    """
    final_job_config = QueryJobConfig()
    if default_job_config:
        copy_job_config(default_job_config, final_job_config)
    if job_config:
        copy_job_config(job_config, final_job_config)
    final_job_config.query_parameters = create_parameters(parameters, json_serializer)
    return connection.query(sql, job_config=final_job_config)


def build_load_job_config(file_format: "StorageFormat", overwrite: bool) -> "LoadJobConfig":
    job_config = LoadJobConfig()
    job_config.source_format = _map_bigquery_source_format(file_format)
    job_config.write_disposition = "WRITE_TRUNCATE" if overwrite else "WRITE_APPEND"
    return job_config


def build_load_job_telemetry(job: QueryJob, table: str, *, format_label: str) -> "StorageTelemetry":
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


def is_simple_insert(sql: str, expression: "exp.Expression | None" = None, *, allow_parse: bool = True) -> bool:
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


def extract_insert_table(
    sql: str, expression: "exp.Expression | None" = None, *, allow_parse: bool = True
) -> str | None:
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


def _map_bigquery_source_format(file_format: "StorageFormat") -> str:
    if file_format == "parquet":
        return "PARQUET"
    if file_format in {"json", "jsonl"}:
        return "NEWLINE_DELIMITED_JSON"
    msg = f"BigQuery does not support loading '{file_format}' artifacts via the storage bridge"
    raise StorageCapabilityError(msg, capability="parquet_import_enabled")


def _rows_to_results(rows_iterator: Any) -> "list[dict[str, Any]]":
    """Convert BigQuery rows to dictionary format.

    Args:
        rows_iterator: BigQuery rows iterator.

    Returns:
        List of dictionaries representing the rows.
    """
    return [dict(row) for row in rows_iterator]


def collect_rows(job_result: Any, schema: Any | None) -> "tuple[list[dict[str, Any]], list[str]]":
    """Collect BigQuery rows and schema into structured lists.

    Args:
        job_result: BigQuery job result iterator.
        schema: BigQuery schema object (or None).

    Returns:
        Tuple of (rows_list, column_names).
    """
    rows_list = _rows_to_results(iter(job_result))
    column_names = [field.name for field in schema] if schema else []
    return rows_list, column_names


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
        supported_styles={ParameterStyle.NAMED_AT, ParameterStyle.QMARK},
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
        },
        extras={"json_tuple_strategy": "tuple", "type_coercion_overrides": {list: _identity, tuple: _tuple_to_list}},
        default_dialect="bigquery",
    )


driver_profile = build_profile()


def build_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> StatementConfig:
    """Construct the BigQuery statement configuration with optional JSON serializer."""
    serializer = json_serializer or to_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "bigquery"}, json_serializer=serializer
    )


default_statement_config = build_statement_config()


def _normalize_bigquery_driver_features(
    driver_features: "Mapping[str, Any] | None",
) -> "tuple[dict[str, Any], Callable[[Any], str] | None, Callable[[Any], None] | None, Any | None]":
    """Normalize driver feature defaults and extract core options."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    user_connection_hook = features.pop("on_connection_create", None)
    features.setdefault("enable_uuid_conversion", True)
    serializer = features.setdefault("json_serializer", to_json)
    connection_instance = features.get("connection_instance")
    if connection_instance is not None:
        features.pop("connection_instance", None)

    return (
        features,
        cast("Callable[[Any], str] | None", serializer),
        cast("Callable[[Any], None] | None", user_connection_hook),
        connection_instance,
    )


def apply_driver_features(
    driver_features: "Mapping[str, Any] | None",
) -> "tuple[dict[str, Any], Callable[[Any], str] | None, Callable[[Any], None] | None, Any | None]":
    """Apply BigQuery driver feature defaults and extract core options."""
    return _normalize_bigquery_driver_features(driver_features)


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


def create_mapped_exception(error: Any) -> SQLSpecError:
    """Map BigQuery exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Args:
        error: The BigQuery exception to map

    Returns:
        A SQLSpec exception that wraps the original error
    """
    try:
        status_code = error.code
    except AttributeError:
        status_code = None
    error_msg = str(error).lower()

    if status_code == HTTP_CONFLICT or "already exists" in error_msg:
        return _create_bigquery_error(error, status_code, UniqueViolationError, "resource already exists")
    if status_code == HTTP_NOT_FOUND or "not found" in error_msg:
        return _create_bigquery_error(error, status_code, NotFoundError, "resource not found")
    if status_code == HTTP_BAD_REQUEST:
        if "syntax" in error_msg or "invalid query" in error_msg:
            return _create_bigquery_error(error, status_code, SQLParsingError, "query syntax error")
        if "type" in error_msg or "format" in error_msg:
            return _create_bigquery_error(error, status_code, DataError, "data error")
        return _create_bigquery_error(error, status_code, SQLSpecError, "error")
    if status_code == HTTP_FORBIDDEN:
        return _create_bigquery_error(error, status_code, DatabaseConnectionError, "permission denied")
    if status_code and status_code >= HTTP_SERVER_ERROR:
        return _create_bigquery_error(error, status_code, OperationalError, "operational error")
    return _create_bigquery_error(error, status_code, SQLSpecError, "error")
