"""BigQuery driver implementation.

Provides Google Cloud BigQuery connectivity with parameter style conversion,
type coercion, error handling, and query job management.
"""

import datetime
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import sqlglot
import sqlglot.expressions as exp
from google.cloud.bigquery import ArrayQueryParameter, QueryJob, QueryJobConfig, ScalarQueryParameter
from google.cloud.exceptions import GoogleCloudError

from sqlspec.adapters.bigquery._types import BigQueryConnection
from sqlspec.adapters.bigquery.type_converter import BigQueryTypeConverter
from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig, create_arrow_result, get_cache_config
from sqlspec.driver import ExecutionResult, SyncDriverAdapterBase
from sqlspec.exceptions import (
    DatabaseConnectionError,
    DataError,
    NotFoundError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, ArrowResult, SQLResult, Statement, StatementFilter
    from sqlspec.driver import SyncDataDictionaryBase
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

logger = logging.getLogger(__name__)

__all__ = ("BigQueryCursor", "BigQueryDriver", "BigQueryExceptionHandler", "bigquery_statement_config")

HTTP_CONFLICT = 409
HTTP_NOT_FOUND = 404
HTTP_BAD_REQUEST = 400
HTTP_FORBIDDEN = 403
HTTP_SERVER_ERROR = 500


_default_type_converter = BigQueryTypeConverter()


_BQ_TYPE_MAP: dict[type, tuple[str, str | None]] = {
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


def _create_array_parameter(name: str, value: Any, array_type: str) -> ArrayQueryParameter:
    """Create BigQuery ARRAY parameter.

    Args:
        name: Parameter name.
        value: Array value (converted to list, empty list if None).
        array_type: BigQuery array element type.

    Returns:
        ArrayQueryParameter instance.
    """
    return ArrayQueryParameter(name, array_type, [] if value is None else list(value))


def _create_json_parameter(name: str, value: Any, json_serializer: "Callable[[Any], str]") -> ScalarQueryParameter:
    """Create BigQuery JSON parameter as STRING type.

    Args:
        name: Parameter name.
        value: JSON-serializable value.
        json_serializer: Function to serialize to JSON string.

    Returns:
        ScalarQueryParameter with STRING type.
    """
    return ScalarQueryParameter(name, "STRING", json_serializer(value))


def _create_scalar_parameter(name: str, value: Any, param_type: str) -> ScalarQueryParameter:
    """Create BigQuery scalar parameter.

    Args:
        name: Parameter name.
        value: Scalar value.
        param_type: BigQuery parameter type (INT64, FLOAT64, etc.).

    Returns:
        ScalarQueryParameter instance.
    """
    return ScalarQueryParameter(name, param_type, value)


def _create_literal_node(value: Any, json_serializer: "Callable[[Any], str]") -> "exp.Expression":
    """Create a SQLGlot literal expression from a Python value.

    Args:
        value: Python value to convert to SQLGlot literal.
        json_serializer: Function to serialize dict/list to JSON string.

    Returns:
        SQLGlot expression representing the literal value.
    """
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.Boolean(this=value)
    if isinstance(value, (int, float)):
        return exp.Literal.number(str(value))
    if isinstance(value, str):
        return exp.Literal.string(value)
    if isinstance(value, (list, tuple)):
        items = [_create_literal_node(item, json_serializer) for item in value]
        return exp.Array(expressions=items)
    if isinstance(value, dict):
        json_str = json_serializer(value)
        return exp.Literal.string(json_str)

    return exp.Literal.string(str(value))


def _replace_placeholder_node(
    node: "exp.Expression",
    parameters: Any,
    placeholder_counter: dict[str, int],
    json_serializer: "Callable[[Any], str]",
) -> "exp.Expression":
    """Replace placeholder or parameter nodes with literal values.

    Handles both positional placeholders (?) and named parameters (@name, :name).
    Converts values to SQLGlot literal expressions for safe embedding in SQL.

    Args:
        node: SQLGlot expression node to check and potentially replace.
        parameters: Parameter values (dict, list, or tuple).
        placeholder_counter: Mutable counter dict for positional placeholders.
        json_serializer: Function to serialize dict/list to JSON string.

    Returns:
        Literal expression if replacement made, otherwise original node.
    """
    if isinstance(node, exp.Placeholder):
        if isinstance(parameters, (list, tuple)):
            current_index = placeholder_counter["index"]
            placeholder_counter["index"] += 1
            if current_index < len(parameters):
                return _create_literal_node(parameters[current_index], json_serializer)
        return node

    if isinstance(node, exp.Parameter):
        param_name = str(node.this) if hasattr(node.this, "__str__") else node.this

        if isinstance(parameters, dict):
            possible_names = [param_name, f"@{param_name}", f":{param_name}", f"param_{param_name}"]
            for name in possible_names:
                if name in parameters:
                    actual_value = getattr(parameters[name], "value", parameters[name])
                    return _create_literal_node(actual_value, json_serializer)
            return node

        if isinstance(parameters, (list, tuple)):
            try:
                if param_name.startswith("param_"):
                    param_index = int(param_name[6:])
                    if param_index < len(parameters):
                        return _create_literal_node(parameters[param_index], json_serializer)

                if param_name.isdigit():
                    param_index = int(param_name)
                    if param_index < len(parameters):
                        return _create_literal_node(parameters[param_index], json_serializer)
            except (ValueError, IndexError, AttributeError):
                pass
        return node

    return node


def _get_bq_param_type(value: Any) -> tuple[str | None, str | None]:
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


def _get_bq_param_creator_map(json_serializer: "Callable[[Any], str]") -> dict[str, Any]:
    """Get BigQuery parameter creator map with configurable JSON serializer.

    Args:
        json_serializer: Function to serialize dict/list to JSON string.

    Returns:
        Dictionary mapping parameter types to creator functions.
    """
    return {
        "ARRAY": _create_array_parameter,
        "JSON": lambda name, value, _: _create_json_parameter(name, value, json_serializer),
        "SCALAR": _create_scalar_parameter,
    }


def _create_bq_parameters(
    parameters: Any, json_serializer: "Callable[[Any], str]"
) -> "list[ArrayQueryParameter | ScalarQueryParameter]":
    """Create BigQuery QueryParameter objects from parameters.

    Args:
        parameters: Dict of named parameters or list of positional parameters
        json_serializer: Function to serialize dict/list to JSON string

    Returns:
        List of BigQuery QueryParameter objects
    """
    if not parameters:
        return []

    bq_parameters: list[ArrayQueryParameter | ScalarQueryParameter] = []
    param_creator_map = _get_bq_param_creator_map(json_serializer)

    if isinstance(parameters, dict):
        for name, value in parameters.items():
            param_name_for_bq = name.lstrip("@")
            actual_value = getattr(value, "value", value)
            param_type, array_element_type = _get_bq_param_type(actual_value)

            if param_type == "ARRAY" and array_element_type:
                creator = param_creator_map["ARRAY"]
                bq_parameters.append(creator(param_name_for_bq, actual_value, array_element_type))
            elif param_type == "JSON":
                creator = param_creator_map["JSON"]
                bq_parameters.append(creator(param_name_for_bq, actual_value, None))
            elif param_type:
                creator = param_creator_map["SCALAR"]
                bq_parameters.append(creator(param_name_for_bq, actual_value, param_type))
            else:
                msg = f"Unsupported BigQuery parameter type for value of param '{name}': {type(actual_value)}"
                raise SQLSpecError(msg)

    elif isinstance(parameters, (list, tuple)):
        logger.warning("BigQuery received positional parameters instead of named parameters")
        return []

    return bq_parameters


def _get_bigquery_type_coercion_map(type_converter: BigQueryTypeConverter) -> dict[type, Any]:
    """Get BigQuery type coercion map with configurable type converter.

    Args:
        type_converter: BigQuery type converter instance

    Returns:
        Type coercion map for BigQuery
    """
    return {
        tuple: list,
        bool: lambda x: x,
        int: lambda x: x,
        float: lambda x: x,
        str: type_converter.convert_if_detected,
        bytes: lambda x: x,
        datetime.datetime: lambda x: x,
        datetime.date: lambda x: x,
        datetime.time: lambda x: x,
        Decimal: lambda x: x,
        dict: lambda x: x,
        list: lambda x: x,
        type(None): lambda _: None,
    }


bigquery_type_coercion_map = _get_bigquery_type_coercion_map(_default_type_converter)


bigquery_statement_config = StatementConfig(
    dialect="bigquery",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_AT,
        supported_parameter_styles={ParameterStyle.NAMED_AT, ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.NAMED_AT,
        supported_execution_parameter_styles={ParameterStyle.NAMED_AT},
        type_coercion_map=bigquery_type_coercion_map,
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
        preserve_original_params_for_many=True,
    ),
    enable_parsing=True,
    enable_validation=True,
    enable_caching=True,
    enable_parameter_type_wrapping=True,
)


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
    """

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = exc_tb
        if exc_type is None:
            return
        if issubclass(exc_type, GoogleCloudError):
            self._map_bigquery_exception(exc_val)

    def _map_bigquery_exception(self, e: Any) -> None:
        """Map BigQuery exception to SQLSpec exception.

        Args:
            e: Google API exception instance
        """
        status_code = getattr(e, "code", None)
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

    __slots__ = ("_data_dictionary", "_default_query_job_config", "_json_serializer", "_type_converter")
    dialect = "bigquery"

    def __init__(
        self,
        connection: BigQueryConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        features = driver_features or {}

        json_serializer = features.get("json_serializer")
        if json_serializer is None:
            json_serializer = to_json

        self._json_serializer: Callable[[Any], str] = json_serializer

        enable_uuid_conversion = features.get("enable_uuid_conversion", True)
        self._type_converter = BigQueryTypeConverter(enable_uuid_conversion=enable_uuid_conversion)

        if statement_config is None:
            cache_config = get_cache_config()
            statement_config = bigquery_statement_config.replace(cache_config=cache_config)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._default_query_job_config: QueryJobConfig | None = (driver_features or {}).get("default_query_job_config")
        self._data_dictionary: SyncDataDictionaryBase | None = None

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

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately."""
        return BigQueryExceptionHandler()

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
            value = getattr(source_config, attr)
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
                value = getattr(source_config, attr)
                setattr(target_config, attr, value)
            except (AttributeError, TypeError):
                continue

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

    def _try_special_handling(self, cursor: "Any", statement: "SQL") -> "SQLResult | None":
        """Hook for BigQuery-specific special operations.

        BigQuery doesn't have complex special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.

        Args:
            cursor: BigQuery cursor object
            statement: SQL statement to analyze

        Returns:
            None - always proceeds with standard execution for BigQuery
        """
        _ = (cursor, statement)
        return None

    def _transform_ast_with_literals(self, sql: str, parameters: Any) -> str:
        """Transform SQL AST by replacing placeholders with literal values.

        Used for BigQuery script execution and execute_many operations where
        parameter binding is not supported. Safely embeds values as SQL literals.

        Args:
            sql: SQL string to transform.
            parameters: Parameters to embed as literals.

        Returns:
            Transformed SQL string with literals embedded.
        """
        if not parameters:
            return sql

        try:
            ast = sqlglot.parse_one(sql, dialect="bigquery")
        except sqlglot.ParseError:
            return sql

        placeholder_counter = {"index": 0}

        transformed_ast = ast.transform(
            lambda node: _replace_placeholder_node(node, parameters, placeholder_counter, self._json_serializer)
        )

        return transformed_ast.sql(dialect="bigquery")

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
            job.result()
            last_job = job
            successful_count += 1

        cursor.job = last_job

        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def _execute_many(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """BigQuery execute_many implementation using script-based execution.

        BigQuery doesn't support traditional execute_many with parameter batching.
        Instead, we generate a script with multiple INSERT statements using
        AST transformation to embed literals safely.

        Args:
            cursor: BigQuery cursor object
            statement: SQL statement to execute with multiple parameter sets

        Returns:
            ExecutionResult with batch execution details
        """

        parameters_list = statement.parameters

        if not parameters_list or not isinstance(parameters_list, (list, tuple)):
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        base_sql = statement.sql

        script_statements = []
        for param_set in parameters_list:
            transformed_sql = self._transform_ast_with_literals(base_sql, param_set)
            script_statements.append(transformed_sql)

        script_sql = ";\n".join(script_statements)

        cursor.job = self._run_query_job(script_sql, None, connection=cursor)
        cursor.job.result()

        affected_rows = (
            cursor.job.num_dml_affected_rows if cursor.job.num_dml_affected_rows is not None else len(parameters_list)
        )
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

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
        affected_rows = cursor.job.num_dml_affected_rows or 0
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            from sqlspec.adapters.bigquery.data_dictionary import BigQuerySyncDataDictionary

            self._data_dictionary = BigQuerySyncDataDictionary()
        return self._data_dictionary

    def _storage_api_available(self) -> bool:
        """Check if BigQuery Storage API is available.

        Returns:
            True if Storage API is available and working, False otherwise
        """
        try:
            from google.cloud import bigquery_storage_v1  # type: ignore[attr-defined]

            # Try to create client (will fail if API not enabled or credentials missing)
            _ = bigquery_storage_v1.BigQueryReadClient()
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
            return_format: "table" for pyarrow.Table (default), "batch" for RecordBatch
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
        from sqlspec.utils.module_loader import ensure_pyarrow

        ensure_pyarrow()

        # Check Storage API availability
        if not self._storage_api_available():
            if native_only:
                from sqlspec.exceptions import MissingDependencyError

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
                arrow_table = arrow_table.cast(arrow_schema)

            # Convert to batch if requested
            if return_format == "batch":
                batches = arrow_table.to_batches()
                arrow_data: Any = batches[0] if batches else pa.RecordBatch.from_pydict({})
            else:
                arrow_data = arrow_table

        # Create ArrowResult
        return create_arrow_result(statement=prepared_statement, data=arrow_data, rows_affected=arrow_data.num_rows)
