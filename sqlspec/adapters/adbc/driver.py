"""ADBC driver implementation for Arrow Database Connectivity.

Provides database connectivity through ADBC with support for multiple
database dialects, parameter style conversion, and transaction management.
"""

import contextlib
import datetime
import decimal
from typing import TYPE_CHECKING, Any, Literal, cast

from sqlspec.adapters.adbc.data_dictionary import AdbcDataDictionary
from sqlspec.adapters.adbc.type_converter import ADBCTypeConverter
from sqlspec.core import (
    SQL,
    DriverParameterProfile,
    ParameterStyle,
    StatementConfig,
    build_null_pruning_transform,
    build_statement_config_from_profile,
    create_arrow_result,
    get_cache_config,
    get_driver_profile,
    register_driver_profile,
)
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.typing import Empty
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from adbc_driver_manager.dbapi import Cursor

    from sqlspec.adapters.adbc._types import AdbcConnection
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, SQLResult, Statement, StatementFilter
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._sync import SyncDataDictionaryBase
    from sqlspec.storage import (
        StorageBridgeJob,
        StorageDestination,
        StorageFormat,
        StorageTelemetry,
        SyncStoragePipeline,
    )
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

__all__ = ("AdbcCursor", "AdbcDriver", "AdbcExceptionHandler", "get_adbc_statement_config")

logger = get_logger("adapters.adbc")

DIALECT_PATTERNS = {
    "postgres": ["postgres", "postgresql"],
    "bigquery": ["bigquery"],
    "sqlite": ["sqlite", "flight", "flightsql"],
    "duckdb": ["duckdb"],
    "mysql": ["mysql"],
    "snowflake": ["snowflake"],
}

DIALECT_PARAMETER_STYLES = {
    "postgres": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "postgresql": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "bigquery": (ParameterStyle.NAMED_AT, [ParameterStyle.NAMED_AT]),
    "sqlite": (ParameterStyle.QMARK, [ParameterStyle.QMARK]),
    "duckdb": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR]),
    "mysql": (ParameterStyle.POSITIONAL_PYFORMAT, [ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT]),
    "snowflake": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC]),
}


def _identity(value: Any) -> Any:
    return value


def _convert_array_for_postgres_adbc(value: Any) -> Any:
    """Convert array values for PostgreSQL compatibility."""

    if isinstance(value, tuple):
        return list(value)
    return value


class AdbcCursor:
    """Context manager for cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AdbcConnection") -> None:
        self.connection = connection
        self.cursor: Cursor | None = None

    def __enter__(self) -> "Cursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()  # type: ignore[no-untyped-call]


class AdbcExceptionHandler:
    """Context manager for handling ADBC database exceptions.

    ADBC propagates underlying database errors. Exception mapping
    depends on the specific ADBC driver being used.
    """

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = exc_tb
        if exc_type is None:
            return
        self._map_adbc_exception(exc_val)

    def _map_adbc_exception(self, e: Any) -> None:
        """Map ADBC exception to SQLSpec exception.

        ADBC drivers may expose SQLSTATE codes or driver-specific codes.

        Args:
            e: ADBC exception instance
        """
        sqlstate = getattr(e, "sqlstate", None)

        if sqlstate:
            self._map_sqlstate_exception(e, sqlstate)
        else:
            self._map_message_based_exception(e)

    def _map_sqlstate_exception(self, e: Any, sqlstate: str) -> None:
        """Map SQLSTATE code to exception.

        Args:
            e: Exception instance
            sqlstate: SQLSTATE error code
        """
        if sqlstate == "23505":
            self._raise_unique_violation(e)
        elif sqlstate == "23503":
            self._raise_foreign_key_violation(e)
        elif sqlstate == "23502":
            self._raise_not_null_violation(e)
        elif sqlstate == "23514":
            self._raise_check_violation(e)
        elif sqlstate.startswith("23"):
            self._raise_integrity_error(e)
        elif sqlstate.startswith("42"):
            self._raise_parsing_error(e)
        elif sqlstate.startswith("08"):
            self._raise_connection_error(e)
        elif sqlstate.startswith("40"):
            self._raise_transaction_error(e)
        elif sqlstate.startswith("22"):
            self._raise_data_error(e)
        else:
            self._raise_generic_error(e)

    def _map_message_based_exception(self, e: Any) -> None:
        """Map exception using message-based detection.

        Args:
            e: Exception instance
        """
        error_msg = str(e).lower()

        if "unique" in error_msg or "duplicate" in error_msg:
            self._raise_unique_violation(e)
        elif "foreign key" in error_msg:
            self._raise_foreign_key_violation(e)
        elif "not null" in error_msg or "null value" in error_msg:
            self._raise_not_null_violation(e)
        elif "check constraint" in error_msg:
            self._raise_check_violation(e)
        elif "constraint" in error_msg:
            self._raise_integrity_error(e)
        elif "syntax" in error_msg:
            self._raise_parsing_error(e)
        elif "connection" in error_msg or "connect" in error_msg:
            self._raise_connection_error(e)
        else:
            self._raise_generic_error(e)

    def _raise_unique_violation(self, e: Any) -> None:
        msg = f"ADBC unique constraint violation: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any) -> None:
        msg = f"ADBC foreign key constraint violation: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any) -> None:
        msg = f"ADBC not-null constraint violation: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_check_violation(self, e: Any) -> None:
        msg = f"ADBC check constraint violation: {e}"
        raise CheckViolationError(msg) from e

    def _raise_integrity_error(self, e: Any) -> None:
        msg = f"ADBC integrity constraint violation: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any) -> None:
        msg = f"ADBC SQL parsing error: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any) -> None:
        msg = f"ADBC connection error: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_transaction_error(self, e: Any) -> None:
        msg = f"ADBC transaction error: {e}"
        raise TransactionError(msg) from e

    def _raise_data_error(self, e: Any) -> None:
        msg = f"ADBC data error: {e}"
        raise DataError(msg) from e

    def _raise_generic_error(self, e: Any) -> None:
        msg = f"ADBC database error: {e}"
        raise SQLSpecError(msg) from e


class AdbcDriver(SyncDriverAdapterBase):
    """ADBC driver for Arrow Database Connectivity.

    Provides database connectivity through ADBC with support for multiple
    database dialects, parameter style conversion, and transaction management.
    """

    __slots__ = ("_data_dictionary", "_detected_dialect", "dialect")

    def __init__(
        self,
        connection: "AdbcConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        self._detected_dialect = self._get_dialect(connection)

        if statement_config is None:
            cache_config = get_cache_config()
            base_config = get_adbc_statement_config(self._detected_dialect)
            statement_config = base_config.replace(
                enable_caching=cache_config.compiled_cache_enabled, enable_parsing=True, enable_validation=True
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self.dialect = statement_config.dialect
        self._data_dictionary: SyncDataDictionaryBase | None = None

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Detect database dialect from connection information.

        Args:
            connection: ADBC connection

        Returns:
            Detected dialect name (defaults to 'postgres')
        """
        try:
            driver_info = connection.adbc_get_info()
            vendor_name = driver_info.get("vendor_name", "").lower()
            driver_name = driver_info.get("driver_name", "").lower()

            for dialect, patterns in DIALECT_PATTERNS.items():
                if any(pattern in vendor_name or pattern in driver_name for pattern in patterns):
                    logger.debug("Dialect detected: %s (from %s/%s)", dialect, vendor_name, driver_name)
                    return dialect
        except Exception as e:
            logger.debug("Dialect detection failed: %s", e)

        logger.warning("Could not determine dialect from driver info. Defaulting to 'postgres'.")
        return "postgres"

    def _handle_postgres_rollback(self, cursor: "Cursor") -> None:
        """Execute rollback for PostgreSQL after transaction failure.

        Args:
            cursor: Database cursor
        """
        if self.dialect == "postgres":
            with contextlib.suppress(Exception):
                cursor.execute("ROLLBACK")
                logger.debug("PostgreSQL rollback executed after transaction failure")

    def _handle_postgres_empty_parameters(self, parameters: Any) -> Any:
        """Process empty parameters for PostgreSQL compatibility.

        Args:
            parameters: Parameter values

        Returns:
            Processed parameters
        """
        if self.dialect == "postgres" and isinstance(parameters, dict) and not parameters:
            return None
        return parameters

    def prepare_driver_parameters(
        self,
        parameters: Any,
        statement_config: "StatementConfig",
        is_many: bool = False,
        prepared_statement: Any | None = None,
    ) -> Any:
        """Prepare parameters with cast-aware type coercion for ADBC.

        For PostgreSQL, applies cast-aware parameter processing using metadata from the compiled statement.
        This allows proper handling of JSONB casts and other type conversions.
        Respects driver_features['enable_cast_detection'] configuration.

        Args:
            parameters: Parameters in any format
            statement_config: Statement configuration
            is_many: Whether this is for execute_many operation
            prepared_statement: Prepared statement containing the original SQL statement

        Returns:
            Parameters with cast-aware type coercion applied
        """
        enable_cast_detection = self.driver_features.get("enable_cast_detection", True)

        if enable_cast_detection and prepared_statement and self.dialect in {"postgres", "postgresql"} and not is_many:
            parameter_casts = self._get_parameter_casts(prepared_statement)
            postgres_compatible = self._handle_postgres_empty_parameters(parameters)
            return self._prepare_parameters_with_casts(postgres_compatible, parameter_casts, statement_config)

        return super().prepare_driver_parameters(parameters, statement_config, is_many, prepared_statement)

    def _get_parameter_casts(self, statement: SQL) -> "dict[int, str]":
        """Get parameter cast metadata from compiled statement.

        Args:
            statement: SQL statement with compiled metadata

        Returns:
            Dict mapping parameter positions to cast types
        """

        processed_state = statement.get_processed_state()
        if processed_state is not Empty:
            return processed_state.parameter_casts or {}
        return {}

    def _prepare_parameters_with_casts(
        self, parameters: Any, parameter_casts: "dict[int, str]", statement_config: "StatementConfig"
    ) -> Any:
        """Prepare parameters with cast-aware type coercion.

        Uses type coercion map for non-dict types and dialect-aware dict handling.
        Respects driver_features configuration for JSON serialization backend.

        Args:
            parameters: Parameter values (list, tuple, or scalar)
            parameter_casts: Mapping of parameter positions to cast types
            statement_config: Statement configuration for type coercion

        Returns:
            Parameters with cast-aware type coercion applied
        """
        json_encoder = statement_config.parameter_config.json_serializer or self.driver_features.get(
            "json_serializer", to_json
        )

        if isinstance(parameters, (list, tuple)):
            result: list[Any] = []
            for idx, param in enumerate(parameters, start=1):  # pyright: ignore
                cast_type = parameter_casts.get(idx, "").upper()
                if cast_type in {"JSON", "JSONB", "TYPE.JSON", "TYPE.JSONB"}:
                    if isinstance(param, dict):
                        result.append(json_encoder(param))
                    else:
                        result.append(param)
                elif isinstance(param, dict):
                    result.append(ADBCTypeConverter(self.dialect).convert_dict(param))  # type: ignore[arg-type]
                else:
                    if statement_config.parameter_config.type_coercion_map:
                        for type_check, converter in statement_config.parameter_config.type_coercion_map.items():
                            if type_check is not dict and isinstance(param, type_check):
                                param = converter(param)
                                break
                    result.append(param)
            return tuple(result) if isinstance(parameters, tuple) else result
        return parameters

    def with_cursor(self, connection: "AdbcConnection") -> "AdbcCursor":
        """Create context manager for cursor.

        Args:
            connection: Database connection

        Returns:
            Cursor context manager
        """
        return AdbcCursor(connection)

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately.

        Returns:
            Exception handler context manager
        """
        return AdbcExceptionHandler()

    def _try_special_handling(self, cursor: "Cursor", statement: SQL) -> "SQLResult | None":
        """Handle special operations.

        Args:
            cursor: Database cursor
            statement: SQL statement to analyze

        Returns:
            SQLResult if special operation was handled, None for standard execution
        """
        _ = (cursor, statement)
        return None

    def _execute_many(self, cursor: "Cursor", statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            Execution result with row counts
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        parameter_casts = self._get_parameter_casts(statement)

        try:
            if not prepared_parameters:
                cursor._rowcount = 0  # pyright: ignore[reportPrivateUsage]
                row_count = 0
            elif isinstance(prepared_parameters, (list, tuple)) and prepared_parameters:
                processed_params = []
                for param_set in prepared_parameters:
                    postgres_compatible = self._handle_postgres_empty_parameters(param_set)

                    if self.dialect in {"postgres", "postgresql"}:
                        # For postgres, always use cast-aware parameter preparation
                        formatted_params = self._prepare_parameters_with_casts(
                            postgres_compatible, parameter_casts, self.statement_config
                        )
                    else:
                        formatted_params = self.prepare_driver_parameters(
                            postgres_compatible, self.statement_config, is_many=False
                        )
                    processed_params.append(formatted_params)

                cursor.executemany(sql, processed_params)
                row_count = cursor.rowcount if cursor.rowcount is not None else -1
            else:
                cursor.executemany(sql, prepared_parameters)
                row_count = cursor.rowcount if cursor.rowcount is not None else -1

        except Exception:
            self._handle_postgres_rollback(cursor)
            raise

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: "Cursor", statement: SQL) -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            Execution result with data or row count
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        parameter_casts = self._get_parameter_casts(statement)

        try:
            postgres_compatible_params = self._handle_postgres_empty_parameters(prepared_parameters)

            if self.dialect in {"postgres", "postgresql"}:
                formatted_params = self._prepare_parameters_with_casts(
                    postgres_compatible_params, parameter_casts, self.statement_config
                )
                cursor.execute(sql, parameters=formatted_params)
            else:
                cursor.execute(sql, parameters=postgres_compatible_params)

        except Exception:
            self._handle_postgres_rollback(cursor)
            raise

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            if fetched_data and isinstance(fetched_data[0], tuple):
                dict_data: list[dict[Any, Any]] = [dict(zip(column_names, row, strict=False)) for row in fetched_data]
            else:
                dict_data = fetched_data  # type: ignore[assignment]

            return self.create_execution_result(
                cursor,
                selected_data=cast("list[dict[str, Any]]", dict_data),
                column_names=column_names,
                data_row_count=len(dict_data),
                is_select_result=True,
            )

        row_count = cursor.rowcount if cursor.rowcount is not None else -1
        return self.create_execution_result(cursor, rowcount_override=row_count)

    def _execute_script(self, cursor: "Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script containing multiple statements.

        Args:
            cursor: Database cursor
            statement: SQL script to execute

        Returns:
            Execution result with statement counts
        """
        if statement.is_script:
            sql = statement.raw_sql
            prepared_parameters: list[Any] = []
        else:
            sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_rowcount = 0

        try:
            for stmt in statements:
                if prepared_parameters:
                    postgres_compatible_params = self._handle_postgres_empty_parameters(prepared_parameters)
                    cursor.execute(stmt, parameters=postgres_compatible_params)
                else:
                    cursor.execute(stmt)
                successful_count += 1
                if cursor.rowcount is not None:
                    last_rowcount = cursor.rowcount
        except Exception:
            self._handle_postgres_rollback(cursor)
            logger.exception("Script execution failed")
            raise

        return self.create_execution_result(
            cursor,
            statement_count=len(statements),
            successful_statements=successful_count,
            rowcount_override=last_rowcount,
            is_script_result=True,
        )

    def begin(self) -> None:
        """Begin database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("BEGIN")
        except Exception as e:
            msg = f"Failed to begin transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("ROLLBACK")
        except Exception as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("COMMIT")
        except Exception as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = AdbcDataDictionary()
        return self._data_dictionary

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
        """Execute query and return results as Apache Arrow (ADBC native path).

        ADBC provides zero-copy Arrow support via cursor.fetch_arrow_table().
        This is 5-10x faster than the conversion path for large datasets.

        Args:
            statement: SQL statement, string, or QueryBuilder
            *parameters: Query parameters or filters
            statement_config: Optional statement configuration override
            return_format: "table" for pyarrow.Table (default), "batch" for RecordBatch
            native_only: Ignored for ADBC (always uses native path)
            batch_size: Batch size hint (for future streaming implementation)
            arrow_schema: Optional pyarrow.Schema for type casting
            **kwargs: Additional keyword arguments

        Returns:
            ArrowResult with native Arrow data

        Example:
            >>> result = driver.select_to_arrow(
            ...     "SELECT * FROM users WHERE age > $1", 18
            ... )
            >>> df = result.to_pandas()  # Fast zero-copy conversion
        """
        ensure_pyarrow()

        import pyarrow as pa

        # Prepare statement
        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)

        # Use ADBC cursor for native Arrow
        with self.with_cursor(self.connection) as cursor, self.handle_database_exceptions():
            if cursor is None:
                msg = "Failed to create cursor"
                raise DatabaseConnectionError(msg)

            # Get compiled SQL and parameters
            sql, driver_params = self._get_compiled_sql(prepared_statement, config)

            # Execute query
            cursor.execute(sql, driver_params or ())

            # Fetch as Arrow table (zero-copy!)
            arrow_table = cursor.fetch_arrow_table()

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

    def select_to_storage(
        self,
        statement: "Statement | QueryBuilder | SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, Any] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Stream query results to storage via the Arrow fast path."""

        _ = kwargs
        self._require_capability("arrow_export_enabled")
        arrow_result = self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        sync_pipeline: SyncStoragePipeline = cast("SyncStoragePipeline", self._storage_pipeline())
        telemetry_payload = arrow_result.write_to_storage_sync(
            destination,
            format_hint=format_hint,
            pipeline=sync_pipeline,
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, Any] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Ingest an Arrow payload directly through the ADBC cursor."""

        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        ingest_mode: Literal["append", "create", "replace", "create_append"]
        ingest_mode = "replace" if overwrite else "create_append"
        with self.with_cursor(self.connection) as cursor, self.handle_database_exceptions():
            cursor.adbc_ingest(table, arrow_table, mode=ingest_mode)
        telemetry_payload = self._build_ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, Any] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Read an artifact from storage and ingest it via ADBC."""

        arrow_table, inbound = self._read_arrow_from_storage_sync(source, file_format=file_format)
        return self.load_from_arrow(
            table,
            arrow_table,
            partitioner=partitioner,
            overwrite=overwrite,
            telemetry=inbound,
        )


def get_type_coercion_map(dialect: str) -> "dict[type, Any]":
    """Return dialect-aware type coercion mapping for Arrow parameter handling."""

    return {
        datetime.datetime: lambda x: x,
        datetime.date: lambda x: x,
        datetime.time: lambda x: x,
        decimal.Decimal: float,
        bool: lambda x: x,
        int: lambda x: x,
        float: lambda x: x,
        bytes: lambda x: x,
        tuple: _convert_array_for_postgres_adbc,
        list: _convert_array_for_postgres_adbc,
        dict: lambda x: x,
    }


def _build_adbc_profile() -> DriverParameterProfile:
    """Create the ADBC driver parameter profile."""

    return DriverParameterProfile(
        name="ADBC",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions={
            datetime.datetime: _identity,
            datetime.date: _identity,
            datetime.time: _identity,
            decimal.Decimal: float,
            bool: _identity,
            int: _identity,
            float: _identity,
            bytes: _identity,
            tuple: _convert_array_for_postgres_adbc,
            list: _convert_array_for_postgres_adbc,
            dict: _identity,
        },
        extras={
            "type_coercion_overrides": {list: _convert_array_for_postgres_adbc, tuple: _convert_array_for_postgres_adbc}
        },
    )


_ADBC_PROFILE = _build_adbc_profile()

register_driver_profile("adbc", _ADBC_PROFILE)


def get_adbc_statement_config(detected_dialect: str) -> StatementConfig:
    """Create statement configuration for the specified dialect."""
    default_style, supported_styles = DIALECT_PARAMETER_STYLES.get(
        detected_dialect, (ParameterStyle.QMARK, [ParameterStyle.QMARK])
    )

    type_map = get_type_coercion_map(detected_dialect)

    sqlglot_dialect = "postgres" if detected_dialect == "postgresql" else detected_dialect

    parameter_overrides: dict[str, Any] = {
        "default_parameter_style": default_style,
        "supported_parameter_styles": set(supported_styles),
        "default_execution_parameter_style": default_style,
        "supported_execution_parameter_styles": set(supported_styles),
        "type_coercion_map": type_map,
    }

    if detected_dialect in {"postgres", "postgresql"}:
        parameter_overrides["ast_transformer"] = build_null_pruning_transform(dialect=sqlglot_dialect)

    return build_statement_config_from_profile(
        get_driver_profile("adbc"),
        parameter_overrides=parameter_overrides,
        statement_overrides={"dialect": sqlglot_dialect},
        json_serializer=to_json,
    )
