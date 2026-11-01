"""Psqlpy driver implementation for PostgreSQL connectivity.

Provides parameter style conversion, type coercion, error handling,
and transaction management.
"""

from contextlib import suppress
from itertools import chain
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal, cast

from sqlspec.adapters.psqlpy._typing import (
    PsqlpyConnectionExecuteError,
    PsqlpyCursor,
    PsqlpyDatabaseError,
    PsqlpyError,
    PsqlpySessionContext,
)
from sqlspec.adapters.psqlpy.core import (
    _DML_COUNT_COLUMN,
    PsqlpyStreamSource,
    _dml_count_query,
    coerce_json_columns,
    coerce_numeric_for_write,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    extract_rows_affected,
    format_execute_many_parameters,
    format_table_identifier,
    get_parameter_casts,
    prepare_parameters_with_casts,
    split_schema_and_table,
)
from sqlspec.adapters.psqlpy.data_dictionary import PsqlpyDataDictionary
from sqlspec.core import SQL, StatementConfig, get_cache_config, register_driver_profile
from sqlspec.core.sqlcommenter import split_sqlcomment
from sqlspec.driver import AsyncDriverAdapterBase, AsyncRowStream, BaseAsyncExceptionHandler
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.serializers import from_json
from sqlspec.utils.text import normalize_identifier, quote_identifier

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from sqlspec.adapters.psqlpy._typing import PsqlpyConnection
    from sqlspec.core import ArrowResult, SQLResult
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = ("PsqlpyCursor", "PsqlpyDriver", "PsqlpyExceptionHandler", "PsqlpySessionContext")

_CAST_DIALECTS = frozenset({"postgres", "postgresql", "pgvector", "paradedb", "pg_textsearch"})


class PsqlpyExceptionHandler(BaseAsyncExceptionHandler):
    """Async context manager for handling psqlpy database exceptions.

    Maps PostgreSQL SQLSTATE error codes to SQLSpec exceptions stored on
    ``pending_exception``.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False

        if issubclass(exc_type, (PsqlpyDatabaseError, PsqlpyConnectionExecuteError, PsqlpyError)):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class PsqlpyDriver(AsyncDriverAdapterBase):
    """PostgreSQL driver implementation using psqlpy.

    Provides parameter style conversion, type coercion, error handling,
    and transaction management.
    """

    __slots__ = ("_data_dictionary", "_transaction_active")
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
        self._transaction_active = False

    @property
    def data_dictionary(self) -> "PsqlpyDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = PsqlpyDataDictionary()
        return self._data_dictionary

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
            query_result = (
                await cursor.fetch(sql, params, prepared=False)
                if self._is_transaction_pooler_mode()
                else await cursor.fetch(sql, params)
            )
            dict_rows, column_names = collect_rows(query_result)

            return self.create_execution_result(
                cursor,
                selected_data=dict_rows,
                column_names=column_names,
                data_row_count=len(dict_rows),
                is_select_result=True,
                row_format="dict",
            )

        if statement.operation_type in {"INSERT", "UPDATE", "DELETE"}:
            base_sql, comment_suffix = split_sqlcomment(sql)
            count_sql = _dml_count_query(base_sql)
            if count_sql is not None:
                wire_count_sql = f"{count_sql} {comment_suffix}" if comment_suffix else count_sql
                count_result = (
                    await cursor.fetch(wire_count_sql, params, prepared=False)
                    if self._is_transaction_pooler_mode()
                    else await cursor.fetch(wire_count_sql, params)
                )
                count_rows, _ = collect_rows(count_result)
                if len(count_rows) != 1 or set(count_rows[0]) != {_DML_COUNT_COLUMN}:
                    msg = "psqlpy DML row count query returned an invalid result"
                    raise SQLSpecError(msg)
                rows_affected = count_rows[0][_DML_COUNT_COLUMN]
                if type(rows_affected) is not int or rows_affected < 0:
                    msg = "psqlpy DML row count query returned an invalid count"
                    raise SQLSpecError(msg)
                return self.create_execution_result(cursor, rowcount_override=rows_affected)

        result = (
            await cursor.execute(sql, params, prepared=False)
            if self._is_transaction_pooler_mode()
            else await cursor.execute(sql, params)
        )
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

        if self._is_transaction_pooler_mode():
            await cursor.execute_many(sql, formatted_parameters, prepared=False)
        else:
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

        statement_count = len(statements)
        successful_count = 0
        params = prepared_parameters or []
        is_single = len(statements) == 1
        runtime = self._observability
        has_observers = runtime is not None and runtime.has_statement_observers
        use_prepared = not self._is_transaction_pooler_mode()

        for idx, stmt in enumerate(statements):
            stmt_params = params if (is_single or "$" in stmt) else []
            stmt_started = perf_counter() if has_observers else 0.0
            self._pending_statement_profile = None
            await cursor.execute(stmt, stmt_params, prepared=use_prepared)
            if has_observers:
                stmt_duration = perf_counter() - stmt_started
                stmt_profile = self._pending_statement_profile
                self._pending_statement_profile = None
                self._emit_script_statement_event(
                    sql=stmt,
                    parameters=stmt_params,
                    operation="SCRIPT",
                    rows_affected=None,
                    duration_s=stmt_duration,
                    started_at=stmt_started,
                    profile=stmt_profile,
                    script_ordinal=idx,
                    script_statement_count=statement_count,
                )
            successful_count += 1

        self._pending_statement_profile = None
        return self.create_execution_result(
            cursor, statement_count=statement_count, successful_statements=successful_count, is_script_result=True
        )

    async def begin(self) -> None:
        """Begin a database transaction."""
        if self._transaction_active:
            return

        try:
            await self.connection.execute("BEGIN")
        except (PsqlpyDatabaseError, PsqlpyConnectionExecuteError, PsqlpyError, Exception) as e:
            raise create_mapped_exception(e) from e
        try:
            if self._should_set_local_columnar():
                await self.connection.execute("SET LOCAL google_columnar_engine.enable_columnar_scan = 'on'")
        except Exception as e:
            with suppress(Exception):
                await self.connection.execute("ROLLBACK")
            raise create_mapped_exception(e) from e
        self._transaction_active = True

    async def commit(self) -> None:
        """Commit the current transaction."""
        try:
            await self.connection.execute("COMMIT")
        except (PsqlpyDatabaseError, PsqlpyConnectionExecuteError, PsqlpyError, Exception) as e:
            raise create_mapped_exception(e) from e
        finally:
            self._transaction_active = False

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            await self.connection.execute("ROLLBACK")
        except (PsqlpyDatabaseError, PsqlpyConnectionExecuteError, PsqlpyError, Exception) as e:
            raise create_mapped_exception(e) from e
        finally:
            self._transaction_active = False

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
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow-formatted data into PostgreSQL via psqlpy binary COPY."""

        self._require_capability("arrow_import_enabled")
        if stage_table or conflict_keys or drop_indexes:
            return await self._execute_staged_load_async(
                table,
                source,
                raw_loader=self.load_from_arrow,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=drop_indexes,
                concurrent_indexes=concurrent_indexes,
                type_casts=type_casts,
            )
        if overwrite:
            qualified = format_table_identifier(table)
            exc_handler = self.handle_database_exceptions()
            async with exc_handler, self.with_cursor(self.connection) as cursor:
                await cursor.execute(f"TRUNCATE TABLE {qualified}")
            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception from None

        columns, batch_iter = self._coerce_arrow_batches(source)
        first_batch = next((b for b in batch_iter if b.num_rows > 0), None)
        metrics: dict[str, int] = {"rows_processed": 0, "bytes_processed": 0}
        if first_batch is not None:
            all_batches = chain((first_batch,), (b for b in batch_iter if b.num_rows > 0))
            schema_name, table_name = split_schema_and_table(table)
            use_binary_wire = bool(self.driver_features.get("enable_pgcopy_binary", False))
            exc_handler = self.handle_database_exceptions()
            async with exc_handler, self.with_cursor(self.connection) as cursor:
                copy_kwargs: dict[str, Any] = {"columns": columns}
                if schema_name:
                    copy_kwargs["schema_name"] = schema_name
                if use_binary_wire and hasattr(cursor, "binary_copy_to_table"):
                    for batch in all_batches:
                        metrics["rows_processed"] += int(batch.num_rows)
                        metrics["bytes_processed"] += int(batch.nbytes)
                        payload = self._encode_arrow_batch_pg_binary(
                            batch, columns=columns, include_header=True, include_trailer=True
                        )
                        await cursor.binary_copy_to_table(payload, table_name, **copy_kwargs)
                else:
                    json_columns = await self._resolve_json_columns(schema_name, table_name)
                    deserializer = cast(
                        "Callable[[str], Any]", self.driver_features.get("json_deserializer", from_json)
                    )
                    for batch in all_batches:
                        metrics["rows_processed"] += int(batch.num_rows)
                        metrics["bytes_processed"] += int(batch.nbytes)
                        records = self._arrow_batch_to_rows(batch, columns)
                        if records:
                            await cursor.copy_records_to_table(
                                table_name,
                                coerce_json_columns(records, columns, json_columns, deserializer=deserializer),
                                **copy_kwargs,
                            )
            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception from None

        telemetry_payload = self._ingest_telemetry_from_batches(metrics["rows_processed"], metrics["bytes_processed"])
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
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Load staged artifacts from storage using the storage bridge pipeline."""

        return await super().load_from_storage(
            table, source, file_format=file_format, partitioner=partitioner, overwrite=overwrite, **kwargs
        )

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
        active_dialect = statement_config.dialect or self.dialect

        prepared: Any
        if enable_cast_detection and prepared_statement and active_dialect in _CAST_DIALECTS:
            parameter_casts = get_parameter_casts(prepared_statement)
            if is_many:
                if isinstance(parameters, (list, tuple)):
                    prepared = [
                        prepare_parameters_with_casts(param_set, parameter_casts, statement_config)
                        for param_set in parameters
                    ]
                else:
                    prepared = super().prepare_driver_parameters(
                        parameters, statement_config, is_many, prepared_statement
                    )
            else:
                prepared = prepare_parameters_with_casts(parameters, parameter_casts, statement_config)
        else:
            prepared = super().prepare_driver_parameters(parameters, statement_config, is_many, prepared_statement)

        if getattr(prepared_statement, "operation_type", None) != "SELECT":
            prepared = coerce_numeric_for_write(prepared)

        if not is_many and isinstance(prepared, list):
            prepared = tuple(prepared)

        return prepared

    def collect_rows(self, cursor: "PsqlpyConnection", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect psqlpy rows for the direct execution path.

        The ``fetched`` argument may be a psqlpy query result or a plain list.
        """
        dict_rows, column_names = collect_rows(fetched)
        return dict_rows, column_names, len(dict_rows)

    def resolve_rowcount(self, cursor: "PsqlpyConnection") -> int:
        """Resolve rowcount from psqlpy result for the direct execution path."""
        return extract_rows_affected(cursor)

    async def _resolve_json_columns(self, schema_name: "str | None", table_name: str) -> "set[str]":
        """Return the destination columns the database reports as json or jsonb.

        Args:
            schema_name: Destination schema, or None for the current search path.
            table_name: Destination table name.

        The relation is resolved the way the server would resolve it in a query,
        so a table shadowed on the search path contributes no columns, and only
        the one relation it names is inspected. A column typed as a domain is
        read through to the type the domain is built on.

        Returns:
            Names of columns typed json or jsonb.
        """
        qualified = quote_identifier(table_name)
        if schema_name is not None:
            qualified = f"{quote_identifier(schema_name)}.{qualified}"
        rows = await self.connection.fetch(
            "SELECT a.attname AS column_name "
            "FROM pg_attribute a "
            "JOIN pg_type t ON t.oid = a.atttypid "
            "LEFT JOIN pg_type b ON b.oid = t.typbasetype "
            "WHERE a.attrelid = to_regclass($1) "
            "AND a.attnum > 0 AND NOT a.attisdropped "
            "AND COALESCE(b.typname, t.typname) IN ('json', 'jsonb')",
            [qualified],
        )
        data, _ = collect_rows(rows)
        return {str(row["column_name"]) for row in data}

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

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        psqlpy's ``in_transaction()`` is a coroutine, so its truth value cannot be read
        synchronously; the state is tracked via a flag toggled in begin/commit/rollback.
        """
        return self._transaction_active

    def _is_transaction_pooler_mode(self) -> bool:
        pooler_mode = self.driver_features.get("pooler_mode", "session")
        return pooler_mode in {"transaction", "statement"} or bool(
            self.driver_features.get("pgbouncer") or self.driver_features.get("pgbouncer_mode")
        )

    def _should_set_local_columnar(self) -> bool:
        return bool(self.driver_features.get("enable_alloydb_columnar", False)) and self._is_transaction_pooler_mode()


register_driver_profile("psqlpy", driver_profile)
