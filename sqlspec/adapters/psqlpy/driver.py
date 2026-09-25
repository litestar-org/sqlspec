"""Psqlpy driver implementation for PostgreSQL connectivity.

Provides parameter style conversion, type coercion, error handling,
and transaction management.
"""

import contextlib
from time import perf_counter
from typing import TYPE_CHECKING, Any, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.psqlpy._typing import PsqlpyCursor, PsqlpyDatabaseError, PsqlpyError, PsqlpySessionContext
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
    records_to_arrow_table,
    split_schema_and_table,
)
from sqlspec.adapters.psqlpy.data_dictionary import PsqlpyDataDictionary
from sqlspec.core import (
    SQL,
    StackResult,
    StatementConfig,
    create_arrow_result,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.core.stack import StatementStack
from sqlspec.driver import AsyncDriverAdapterBase, AsyncRowStream, BaseAsyncExceptionHandler
from sqlspec.driver._common import validate_savepoint_name
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.schema import to_value_type
from sqlspec.utils.text import normalize_identifier, quote_identifier

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlspec.adapters.psqlpy._typing import PsqlpyConnection
    from sqlspec.core import ArrowResult, SQLResult
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = ("PsqlpyCursor", "PsqlpyDriver", "PsqlpyExceptionHandler", "PsqlpySessionContext")


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


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class PsqlpyDriver(AsyncDriverAdapterBase):
    """PostgreSQL driver implementation using psqlpy.

    Provides parameter style conversion, type coercion, error handling,
    and transaction management.
    """

    _data_dictionary: PsqlpyDataDictionary | None
    _transaction_active: bool
    _json_columns_cache: dict[tuple[str | None, str], set[str]]

    __slots__ = ("_data_dictionary", "_json_columns_cache", "_transaction_active")
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
        self._data_dictionary = None
        self._transaction_active = False
        self._json_columns_cache = {}

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

        if statement.operation_type in {"INSERT", "UPDATE", "DELETE"}:
            if "returning" in sql.lower():
                query_result = await cursor.fetch(sql, params)
                dict_rows, column_names = collect_rows(query_result)
                rows_affected = len(dict_rows)
                return self.create_execution_result(
                    cursor,
                    selected_data=dict_rows,
                    column_names=column_names,
                    data_row_count=rows_affected,
                    rowcount_override=rows_affected,
                    is_select_result=statement.returns_rows(),
                )
            count_sql = _dml_count_query(sql)
            if count_sql is not None:
                count_result = await cursor.fetch(count_sql, params)
                count_rows, _ = collect_rows(count_result)
                if len(count_rows) != 1 or set(count_rows[0]) != {_DML_COUNT_COLUMN}:
                    msg = "psqlpy DML row count query returned an invalid result"
                    raise SQLSpecError(msg)
                rows_affected = count_rows[0][_DML_COUNT_COLUMN]
                if type(rows_affected) is not int or rows_affected < 0:
                    msg = "psqlpy DML row count query returned an invalid count"
                    raise SQLSpecError(msg)
                return self.create_execution_result(cursor, rowcount_override=rows_affected)

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
        """Execute SQL script with statement splitting or batch execution.

        Args:
            cursor: Psqlpy connection object
            statement: SQL statement with script content

        Returns:
            ExecutionResult with script execution metadata
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        prepared_parameters = cast("Sequence[Any] | Mapping[str, Any] | None", prepared_parameters)
        statement_config = statement.statement_config

        if not prepared_parameters and hasattr(cursor, "execute_batch"):
            statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
            exc_handler = self.handle_database_exceptions()
            async with exc_handler:
                await cursor.execute_batch(sql)
            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception from None
            return self.create_execution_result(
                cursor, statement_count=len(statements), successful_statements=len(statements), is_script_result=True
            )

        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_result = None
        params = prepared_parameters or []

        for stmt in statements:
            last_result = await cursor.execute(stmt, params)
            successful_count += 1

        return self.create_execution_result(
            last_result, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def begin(self) -> None:
        """Begin a database transaction."""

        try:
            await self.connection.execute("BEGIN")
        except PsqlpyDatabaseError as e:
            msg = f"Failed to begin psqlpy transaction: {e}"
            raise SQLSpecError(msg) from e
        self._transaction_active = True

    async def commit(self) -> None:
        """Commit the current transaction."""

        try:
            await self.connection.execute("COMMIT")
        except PsqlpyDatabaseError as e:
            msg = f"Failed to commit psqlpy transaction: {e}"
            raise SQLSpecError(msg) from e
        finally:
            self._transaction_active = False

    async def rollback(self) -> None:
        """Rollback the current transaction."""

        try:
            await self.connection.execute("ROLLBACK")
        except PsqlpyDatabaseError as e:
            msg = f"Failed to rollback psqlpy transaction: {e}"
            raise SQLSpecError(msg) from e
        finally:
            self._transaction_active = False

    async def savepoint(self, name: str) -> None:
        """Create a savepoint within the current transaction."""
        validate_savepoint_name(name)
        quoted_name = quote_identifier(name)
        await self.connection.execute(f"SAVEPOINT {quoted_name}")

    async def release_savepoint(self, name: str) -> None:
        """Release a savepoint within the current transaction."""
        validate_savepoint_name(name)
        quoted_name = quote_identifier(name)
        await self.connection.execute(f"RELEASE SAVEPOINT {quoted_name}")

    async def rollback_savepoint(self, name: str) -> None:
        """Rollback to a savepoint within the current transaction."""
        validate_savepoint_name(name)
        quoted_name = quote_identifier(name)
        await self.connection.execute(f"ROLLBACK TO SAVEPOINT {quoted_name}")

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
        cache_key = (schema_name, table_name)
        if cache_key in self._json_columns_cache:
            return self._json_columns_cache[cache_key]

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
        result = {str(row["column_name"]) for row in data}
        self._json_columns_cache[cache_key] = result
        return result

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

    async def execute_stack(
        self, stack: "StatementStack", *, continue_on_error: bool = False
    ) -> "tuple[StackResult, ...]":
        """Execute a StatementStack using psqlpy pipelining when available."""
        if not isinstance(stack, StatementStack) or not stack or self.stack_native_disabled or continue_on_error:
            return await super().execute_stack(stack, continue_on_error=continue_on_error)

        queries: list[tuple[str, list[Any] | None]] = []
        prepared_operations: list[tuple[Any, Any]] = []

        for operation in stack.operations:
            kwargs = dict(operation.keyword_arguments) if operation.keyword_arguments else {}
            config = kwargs.pop("statement_config", None) or self.statement_config
            sql_statement = self.prepare_statement(
                operation.statement, operation.arguments, statement_config=config, kwargs=kwargs
            )
            if sql_statement.is_script or sql_statement.is_many:
                return await super().execute_stack(stack, continue_on_error=continue_on_error)
            sql, params = self._compiled_sql(sql_statement, config)
            p_list = list(params) if isinstance(params, (list, tuple)) else None
            queries.append((sql, p_list))
            prepared_operations.append((operation, sql_statement))

        transaction = self.connection.transaction()
        needs_commit = False
        if not self._connection_in_transaction():
            await transaction.begin()
            needs_commit = True

        results: list[StackResult] = []
        try:
            query_results = await transaction.pipeline(queries)
            if needs_commit:
                await transaction.commit()
            for (_op, stmt), q_res in zip(prepared_operations, query_results, strict=False):
                rows, column_names = collect_rows(q_res)
                exec_result = self.create_execution_result(
                    self.connection,
                    selected_data=rows,
                    column_names=column_names,
                    data_row_count=len(rows),
                    is_select_result=stmt.returns_rows(),
                )
                sql_result = self.build_statement_result(stmt, exec_result)
                results.append(StackResult(result=sql_result))
        except Exception as exc:
            if needs_commit:
                with contextlib.suppress(Exception):
                    await transaction.rollback()
            msg = f"Pipelined stack execution failed: {exc}"
            raise SQLSpecError(msg) from exc

        return tuple(results)

    async def select_to_arrow(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: "StatementConfig | None" = None,
        return_format: str = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Execute a query and return results formatted as Apache Arrow."""
        import pyarrow as pa

        config = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, prepared_parameters = self._compiled_sql(sql_statement, config)
        params = cast("Sequence[Any] | Mapping[str, Any] | None", prepared_parameters) or []

        start_time = perf_counter()
        query_result: Any = None
        exc_handler = self.handle_database_exceptions()
        async with exc_handler, self.with_cursor(self.connection) as cursor:
            query_result = await cursor.fetch(sql, params)
        if exc_handler.pending_exception is not None:
            raise exc_handler.pending_exception from None
        execution_time = perf_counter() - start_time

        records = query_result.records() if hasattr(query_result, "records") else query_result.result()
        columns = list(records[0].keys()) if records and hasattr(records[0], "keys") else []

        table = records_to_arrow_table(records, columns, schema=arrow_schema)

        if return_format == "table":
            data: Any = table
        elif return_format == "batch":
            batches = table.to_batches()
            data = batches[0] if batches else pa.RecordBatch.from_arrays([], schema=table.schema)
        elif return_format == "batches":
            data = table.to_batches(max_chunksize=batch_size) if batch_size else table.to_batches()
        elif return_format == "reader":
            data = table.to_reader(max_chunksize=batch_size)
        else:
            data = table

        return create_arrow_result(
            statement=sql_statement,
            data=data,
            rows_affected=len(records),
            execution_time=execution_time,
            metadata={"columns": columns},
        )

    async def select_one_or_none(
        self,
        statement: Any,
        /,
        *parameters: Any,
        schema_type: Any = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query returning at most one row using fetch_row fast-path."""
        config = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, prepared_parameters = self._compiled_sql(sql_statement, config)
        params = cast("Sequence[Any] | Mapping[str, Any] | None", prepared_parameters) or []

        single_result: Any = None
        exc_handler = self.handle_database_exceptions()
        async with exc_handler, self.with_cursor(self.connection) as cursor:
            single_result = await cursor.fetch_row(sql, params)
        if exc_handler.pending_exception is not None:
            raise exc_handler.pending_exception from None

        if single_result is None:
            return None
        row_dict = single_result.result() if hasattr(single_result, "result") else dict(cast("Any", single_result))
        if not row_dict:
            return None
        if schema_type is not None:
            return self.to_schema(row_dict, schema_type=schema_type)
        return row_dict

    async def select_value(
        self,
        statement: Any,
        /,
        *parameters: Any,
        value_type: Any = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query returning a scalar value using fetch_val fast-path."""
        config = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, prepared_parameters = self._compiled_sql(sql_statement, config)
        params = cast("Sequence[Any] | Mapping[str, Any] | None", prepared_parameters) or []

        val: Any = None
        exc_handler = self.handle_database_exceptions()
        async with exc_handler, self.with_cursor(self.connection) as cursor:
            val = await cursor.fetch_val(sql, params)
        if exc_handler.pending_exception is not None:
            raise exc_handler.pending_exception from None

        if val is None:
            return None
        if value_type is not None:
            return to_value_type(val, value_type)
        return val

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

    async def load_from_records(
        self,
        table: str,
        records: "Sequence[Mapping[str, Any]] | Sequence[Sequence[Any]]",
        *,
        columns: "list[str] | None" = None,
        overwrite: bool = False,
        partitioner: "dict[str, object] | None" = None,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Python records into PostgreSQL via psqlpy binary COPY."""
        self._require_capability("arrow_import_enabled")
        if overwrite:
            qualified = format_table_identifier(table)
            exc_handler = self.handle_database_exceptions()
            async with exc_handler, self.with_cursor(self.connection) as cursor:
                await cursor.execute(f"TRUNCATE TABLE {qualified}")
            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception from None

        if not records:
            empty_payload: StorageTelemetry = {"destination": table, "rows_processed": 0, "bytes_processed": 0}
            self._attach_partition_telemetry(empty_payload, partitioner)
            return self._storage_job(empty_payload, telemetry)

        schema_name, table_name = split_schema_and_table(table)
        first_record = records[0]
        from collections.abc import Mapping as MappingABC

        if columns is None:
            if isinstance(first_record, MappingABC):
                resolved_columns = list(first_record.keys())
            else:
                msg = "columns must be provided when records are sequences"
                raise SQLSpecError(msg)
        else:
            resolved_columns = columns

        if isinstance(first_record, MappingABC):
            row_tuples = [
                tuple(r.get(col) for col in resolved_columns) for r in cast("Sequence[Mapping[str, Any]]", records)
            ]
        else:
            row_tuples = [tuple(r) for r in cast("Sequence[Sequence[Any]]", records)]

        json_columns = await self._resolve_json_columns(schema_name, table_name)
        coerced_records = coerce_json_columns(row_tuples, resolved_columns, json_columns)

        copy_kwargs: dict[str, Any] = {"columns": resolved_columns}
        if schema_name:
            copy_kwargs["schema_name"] = schema_name

        exc_handler = self.handle_database_exceptions()
        async with exc_handler, self.with_cursor(self.connection) as cursor:
            await cursor.copy_records_to_table(table_name, coerced_records, **copy_kwargs)
        if exc_handler.pending_exception is not None:
            raise exc_handler.pending_exception from None

        telemetry_payload: StorageTelemetry = {
            "destination": table,
            "rows_processed": len(records),
            "bytes_processed": 0,
        }
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
                json_columns = await self._resolve_json_columns(schema_name, table_name)
                await cursor.copy_records_to_table(
                    table_name, coerce_json_columns(records, columns, json_columns), **copy_kwargs
                )
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

    def collect_rows(self, cursor: "PsqlpyConnection", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect psqlpy rows for the direct execution path.

        The ``fetched`` argument may be a psqlpy query result or a plain list.
        """
        dict_rows, column_names = collect_rows(fetched)
        return dict_rows, column_names, len(dict_rows)

    def resolve_rowcount(self, cursor: "PsqlpyConnection") -> int:
        """Resolve rowcount from psqlpy result for the direct execution path."""
        return extract_rows_affected(cursor)

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


register_driver_profile("psqlpy", driver_profile)
