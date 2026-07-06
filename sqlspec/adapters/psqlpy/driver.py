"""Psqlpy driver implementation for PostgreSQL connectivity.

Provides parameter style conversion, type coercion, error handling,
and transaction management.
"""

import inspect
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.psqlpy._typing import PsqlpyCursor, PsqlpyDatabaseError, PsqlpyError, PsqlpySessionContext
from sqlspec.adapters.psqlpy.core import (
    PsqlpyStreamSource,
    build_insert_statement,
    coerce_numeric_for_write,
    coerce_records_for_execute_many,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    encode_records_for_binary_copy,
    extract_rows_affected,
    format_execute_many_parameters,
    format_table_identifier,
    get_parameter_casts,
    prepare_parameters_with_casts,
    split_schema_and_table,
)
from sqlspec.adapters.psqlpy.data_dictionary import PsqlpyDataDictionary
from sqlspec.core import SQL, StatementConfig, get_cache_config, register_driver_profile
from sqlspec.driver import AsyncDriverAdapterBase, AsyncRowStream, BaseAsyncExceptionHandler
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.text import normalize_identifier, quote_identifier

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlspec.adapters.psqlpy._typing import PsqlpyConnection
    from sqlspec.core import ArrowResult, SQLResult
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = ("PsqlpyCursor", "PsqlpyDriver", "PsqlpyExceptionHandler", "PsqlpySessionContext")

logger = get_logger("sqlspec.adapters.psqlpy")


class PsqlpyExceptionHandler(BaseAsyncExceptionHandler):
    """Async context manager for handling psqlpy database exceptions.

    Maps PostgreSQL SQLSTATE error codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __aexit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False

        if issubclass(exc_type, (PsqlpyDatabaseError, PsqlpyError)):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class PsqlpyDriver(AsyncDriverAdapterBase):
    """PostgreSQL driver implementation using psqlpy.

    Provides parameter style conversion, type coercion, error handling,
    and transaction management.
    """

    __slots__ = ("_data_dictionary",)
    dialect = "postgres"

    def __init__(
        self,
        connection: "PsqlpyConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: PsqlpyDataDictionary | None = None

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    async def dispatch_execute(self, cursor: "PsqlpyConnection", statement: SQL) -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: Psqlpy connection object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with execution metadata
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        params = cast("Sequence[Any] | Mapping[str, Any] | None", prepared_parameters) or []

        if statement.returns_rows():
            query_result = await cursor.fetch(sql, params)
            dict_rows, column_names = collect_rows(query_result)

            return self.create_execution_result(
                cursor,
                selected_data=dict_rows,
                column_names=column_names,
                data_row_count=len(dict_rows),
                is_select_result=True,
                row_format="dict",
            )

        result = await cursor.execute(sql, params)
        rows_affected = extract_rows_affected(result)

        return self.create_execution_result(cursor, rowcount_override=rows_affected)

    async def dispatch_execute_many(self, cursor: "PsqlpyConnection", statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: Psqlpy connection object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution metadata
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        formatted_parameters = format_execute_many_parameters(prepared_parameters, coerce_numeric=False)

        await cursor.execute_many(sql, formatted_parameters)

        rows_affected = len(formatted_parameters)

        return self.create_execution_result(cursor, rowcount_override=rows_affected, is_many_result=True)

    async def dispatch_execute_script(self, cursor: "PsqlpyConnection", statement: SQL) -> "ExecutionResult":
        """Execute SQL script with statement splitting.

        Args:
            cursor: Psqlpy connection object
            statement: SQL statement with script content

        Returns:
            ExecutionResult with script execution metadata
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        prepared_parameters = cast("Sequence[Any] | Mapping[str, Any] | None", prepared_parameters)
        statement_config = statement.statement_config
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
        if len(statements) > 1 and prepared_parameters:
            msg = (
                "Parameterized multi-statement scripts are not supported; use execute_many or individual execute calls"
            )
            raise SQLSpecError(msg)

        successful_count = 0
        last_result = None
        params = prepared_parameters or []

        for stmt in statements:
            last_result = await cursor.execute(stmt, params)
            successful_count += 1

        return self.create_execution_result(
            last_result, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def _execute_cache_hit(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: Any
    ) -> "SQLResult":
        """Execute cached psqlpy queries with cast-aware parameter preparation."""
        prepared_params = self.prepare_driver_parameters(params, self.statement_config, prepared_statement=cached)
        direct_statement = self._cached_statement(
            sql,
            params,
            cached,
            cast("tuple[Any, ...] | list[Any] | dict[str, Any]", prepared_params),
            params_are_simple=True,
            compiled_sql=cached.compiled_sql,
        )
        return await self._execute_cached_statement(direct_statement)

    # ─────────────────────────────────────────────────────────────────────────────
    # TRANSACTION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────────

    async def begin(self) -> None:
        """Begin a database transaction."""

        try:
            await self.connection.execute("BEGIN")
        except PsqlpyDatabaseError as e:
            msg = f"Failed to begin psqlpy transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction."""

        try:
            await self.connection.execute("COMMIT")
        except PsqlpyDatabaseError as e:
            msg = f"Failed to commit psqlpy transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction."""

        try:
            await self.connection.execute("ROLLBACK")
        except PsqlpyDatabaseError as e:
            msg = f"Failed to rollback psqlpy transaction: {e}"
            raise SQLSpecError(msg) from e

    async def set_migration_session_schema(self, schema: str) -> None:
        """Set the PostgreSQL search path for migration SQL."""
        normalized_schema = normalize_identifier(schema, "postgres")
        quoted_schema = quote_identifier(normalized_schema)
        await self.connection.execute(f'SET LOCAL search_path TO {quoted_schema}, "$user", public')

    async def set_migration_non_transactional_schema(self, schema: str) -> None:
        """Set the PostgreSQL search path for non-transactional migration SQL."""
        normalized_schema = normalize_identifier(schema, "postgres")
        quoted_schema = quote_identifier(normalized_schema)
        await self.connection.execute(f'SET search_path TO {quoted_schema}, "$user", public')

    async def reset_migration_session_schema(self) -> None:
        """Reset the PostgreSQL search path after non-transactional migration SQL."""
        await self.connection.execute("RESET search_path")

    async def has_schema(self, schema: str) -> bool:
        """Return whether a PostgreSQL schema exists."""
        normalized_schema = normalize_identifier(schema, "postgres")
        rows = await self.connection.fetch(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = $1", [normalized_schema]
        )
        data, _ = collect_rows(rows)
        return bool(data)

    def with_cursor(self, connection: "PsqlpyConnection") -> "PsqlpyCursor":
        """Create context manager for psqlpy cursor.

        Args:
            connection: Psqlpy connection object

        Returns:
            PsqlpyCursor context manager
        """
        return PsqlpyCursor(connection)

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "AsyncRowStream[dict[str, Any]] | None":
        """Return a native psqlpy row stream backed by a server-side cursor in a transaction."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        params = cast("Sequence[Any] | Mapping[str, Any] | None", prepared_parameters) or []
        return AsyncRowStream(PsqlpyStreamSource(self, sql, params, chunk_size))

    def handle_database_exceptions(self) -> "PsqlpyExceptionHandler":
        """Handle database-specific exceptions.

        Returns:
            Exception handler context manager
        """
        return PsqlpyExceptionHandler()

    # ─────────────────────────────────────────────────────────────────────────────
    # STORAGE API METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    async def select_to_storage(
        self,
        statement: "SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: Any,
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, object] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Execute a query and stream Arrow results to a storage backend."""

        self._require_capability("arrow_export_enabled")
        arrow_result = await self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        async_pipeline = self._storage_pipeline()
        telemetry_payload = await self._write_storage_result(
            arrow_result, destination, format_hint=format_hint, pipeline=async_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    async def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow-formatted data into PostgreSQL via psqlpy binary COPY."""

        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            qualified = format_table_identifier(table)
            exc_handler = self.handle_database_exceptions()
            async with exc_handler, self.with_cursor(self.connection) as cursor:
                await cursor.execute(f"TRUNCATE TABLE {qualified}")
            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception from None

        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            schema_name, table_name = split_schema_and_table(table)
            exc_handler = self.handle_database_exceptions()
            async with exc_handler, self.with_cursor(self.connection) as cursor:
                copy_kwargs: dict[str, Any] = {"columns": columns}
                if schema_name:
                    copy_kwargs["schema_name"] = schema_name
                try:
                    copy_payload = encode_records_for_binary_copy(records)
                    copy_operation = cursor.binary_copy_to_table(copy_payload, table_name, **copy_kwargs)
                    if inspect.isawaitable(copy_operation):
                        await copy_operation
                except (TypeError, PsqlpyDatabaseError) as exc:
                    logger.debug("Binary COPY not available for psqlpy; falling back to INSERT statements: %s", exc)
                    insert_sql = build_insert_statement(table, columns)
                    formatted_records = coerce_records_for_execute_many(records)
                    try:
                        insert_operation = cursor.execute_many(insert_sql, formatted_records)
                        if inspect.isawaitable(insert_operation):
                            await insert_operation
                    except (PsqlpyDatabaseError, PsqlpyError) as fallback_exc:
                        if "PyJSON must be dict, list, or tuple" not in str(fallback_exc):
                            raise
                        formatted_records = coerce_records_for_execute_many(records, parse_json_text=True)
                        insert_operation = cursor.execute_many(insert_sql, formatted_records)
                        if inspect.isawaitable(insert_operation):
                            await insert_operation
            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception from None

        telemetry_payload = self._ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load staged artifacts from storage using the storage bridge pipeline."""

        arrow_table, inbound = await self._read_storage_arrow(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def prepare_driver_parameters(
        self,
        parameters: Any,
        statement_config: "StatementConfig",
        is_many: bool = False,
        prepared_statement: Any | None = None,
    ) -> Any:
        """Prepare parameters with cast-aware type coercion for psqlpy.

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
            parameter_casts = get_parameter_casts(prepared_statement)
            prepared = prepare_parameters_with_casts(parameters, parameter_casts, statement_config)
        else:
            prepared = super().prepare_driver_parameters(parameters, statement_config, is_many, prepared_statement)

        if not is_many:
            operation_type = getattr(prepared_statement, "operation_type", None)
            if operation_type != "SELECT":
                prepared = coerce_numeric_for_write(prepared)
        elif getattr(prepared_statement, "operation_type", None) != "SELECT":
            prepared = coerce_numeric_for_write(prepared)

        if not is_many and isinstance(prepared, list):
            prepared = tuple(prepared)

        return prepared

    @property
    def data_dictionary(self) -> "PsqlpyDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = PsqlpyDataDictionary()
        return self._data_dictionary

    # ─────────────────────────────────────────────────────────────────────────────
    # PRIVATE/INTERNAL METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def collect_rows(self, cursor: "PsqlpyConnection", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect psqlpy rows for the direct execution path.

        The ``fetched`` argument may be a psqlpy query result or a plain list.
        """
        dict_rows, column_names = collect_rows(fetched)
        return dict_rows, column_names, len(dict_rows)

    def resolve_rowcount(self, cursor: "PsqlpyConnection") -> int:
        """Resolve rowcount from psqlpy result for the direct execution path."""
        return extract_rows_affected(cursor)

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return bool(self.connection.in_transaction())


register_driver_profile("psqlpy", driver_profile)
