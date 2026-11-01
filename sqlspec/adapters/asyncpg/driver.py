"""AsyncPG PostgreSQL driver implementation for async PostgreSQL operations."""

import re
from collections.abc import Mapping
from contextlib import asynccontextmanager, nullcontext, suppress
from io import BytesIO
from itertools import chain
from typing import TYPE_CHECKING, Any, Literal, cast

from sqlglot import exp, parse_one
from sqlglot.errors import ParseError

from sqlspec.adapters.asyncpg._typing import AsyncpgCursor, AsyncpgPostgresError, AsyncpgSessionContext
from sqlspec.adapters.asyncpg._typing import asyncpg_module as asyncpg
from sqlspec.adapters.asyncpg.core import (
    AsyncpgStreamSource,
    NormalizedStackOperation,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    parse_status,
    resolve_many_rowcount,
)
from sqlspec.adapters.asyncpg.data_dictionary import AsyncpgDataDictionary
from sqlspec.core import (
    SQL,
    DMLResult,
    StackResult,
    StatementStack,
    get_cache_config,
    is_copy_from_operation,
    is_copy_operation,
    register_driver_profile,
)
from sqlspec.driver import (
    AsyncDriverAdapterBase,
    AsyncRowStream,
    BaseAsyncExceptionHandler,
    StackExecutionObserver,
    describe_stack_statement,
    validate_savepoint_name,
)
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError, StackExecutionError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.text import normalize_identifier, quote_identifier
from sqlspec.utils.type_guards import has_sqlstate
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from sqlspec.adapters.asyncpg._typing import AsyncpgConnection
    from sqlspec.core import ArrowResult, SQLResult, StatementConfig
    from sqlspec.driver import CachedQuery, ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry


__all__ = ("AsyncpgCursor", "AsyncpgDriver", "AsyncpgExceptionHandler", "AsyncpgSessionContext")

_COPY_FROM_STDIN_RE: re.Pattern[str] = re.compile(
    r'COPY\s+(?:ONLY\s+)?((?:"(?:[^"]|"")+"|\w+)(?:\s*\.\s*(?:"(?:[^"]|"")+"|\w+))?)(?:\s*\([^)]*\))?\s+FROM\s+STDIN',
    re.IGNORECASE,
)

logger = get_logger("sqlspec.adapters.asyncpg")


class AsyncpgExceptionHandler(BaseAsyncExceptionHandler):
    """Async context manager for handling AsyncPG database exceptions.

    Maps PostgreSQL SQLSTATE error codes to SQLSpec exceptions stored on
    ``pending_exception``.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:

        _ = exc_type
        if (
            isinstance(exc_val, (AsyncpgPostgresError, TimeoutError, OSError))
            or has_sqlstate(exc_val)
            or (asyncpg is not None and isinstance(exc_val, getattr(asyncpg, "InterfaceError", ())))
        ):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class AsyncpgDriver(AsyncDriverAdapterBase):
    """AsyncPG PostgreSQL driver for async database operations.

    Supports COPY operations, numeric parameter style handling, PostgreSQL
    exception handling, transaction management, SQL statement compilation
    and caching, and parameter processing with type coercion.
    """

    __slots__ = ("_data_dictionary", "_transaction")
    dialect = "postgres"

    def __init__(
        self,
        connection: "AsyncpgConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: AsyncpgDataDictionary | None = None
        self._transaction: Any = None

    @property
    def data_dictionary(self) -> "AsyncpgDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = AsyncpgDataDictionary()
        return self._data_dictionary

    async def dispatch_execute(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Handles both SELECT queries and non-SELECT operations.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with statement execution details
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        params: tuple[Any, ...] = cast("tuple[Any, ...]", prepared_parameters) if prepared_parameters else ()

        timeout = self._timeout_value(statement.statement_config)
        if statement.returns_rows():
            records = (
                await cursor.fetch(sql, *params, timeout=timeout)
                if timeout is not None
                else await cursor.fetch(sql, *params)
            )
            data, column_names = collect_rows(records)

            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=len(data),
                is_select_result=True,
                row_format="record",
            )

        result = (
            await cursor.execute(sql, *params, timeout=timeout)
            if timeout is not None
            else await cursor.execute(sql, *params)
        )
        affected_rows = parse_status(result)

        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    async def dispatch_execute_many(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using AsyncPG's executemany.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        timeout = self._timeout_value(statement.statement_config)

        if prepared_parameters:
            parameter_sets = cast("list[Sequence[object]]", prepared_parameters)
            if timeout is not None:
                await cursor.executemany(sql, parameter_sets, timeout=timeout)
            else:
                await cursor.executemany(sql, parameter_sets)
            affected_rows = resolve_many_rowcount(parameter_sets)
        else:
            affected_rows = 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def _execute_cache_hit_with_cursor(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        wire_sql = self._format_wire_sql(cached.compiled_sql)
        timeout = self._timeout_value(self.statement_config)
        args = (
            params if isinstance(params, (tuple, list)) else tuple(params.values()) if isinstance(params, dict) else ()
        )
        if cached.operation_profile.returns_rows:
            records = (
                await self.connection.fetch(wire_sql, *args, timeout=timeout)
                if timeout is not None
                else await self.connection.fetch(wire_sql, *args)
            )
            data, column_names = collect_rows(records)
            return self.build_cached_select_result(
                sql,
                params,
                cached,
                selected_data=data,
                column_names=column_names,
                data_row_count=len(data),
                row_format="record",
                compiled_sql=wire_sql,
            )
        status = (
            await self.connection.execute(wire_sql, *args, timeout=timeout)
            if timeout is not None
            else await self.connection.execute(wire_sql, *args)
        )
        affected_rows = parse_status(status)
        return DMLResult(cached.operation_type, affected_rows)

    async def dispatch_execute_script(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement containing multiple statements

        Returns:
            ExecutionResult with script execution details
        """
        return await super().dispatch_execute_script(cursor, statement)

    async def dispatch_special_handling(self, cursor: "AsyncpgConnection", statement: "SQL") -> "SQLResult | None":
        """Handle PostgreSQL COPY operations and other special cases.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement to analyze

        Returns:
            SQLResult if special operation was handled, None for standard execution
        """
        if is_copy_operation(statement.operation_type):
            await self._handle_copy_operation(cursor, statement)
            return self.build_statement_result(statement, self.create_execution_result(cursor))

        return None

    async def begin(self) -> None:
        """Begin a database transaction.

        Uses asyncpg's own transaction handle so the driver and the connection
        agree on transaction state. A second call is a no-op rather than a
        savepoint, matching the other adapters and leaving nested
        ``transaction()`` blocks to the savepoint handling in the driver base.

        asyncpg claims the connection for a transaction before it issues BEGIN
        and does not release the claim when BEGIN fails, which would turn the
        next attempt into a savepoint whose commit releases rather than commits.
        The claim is therefore released here.
        """
        if self._transaction is not None:
            if self._connection_in_transaction():
                return
            self._transaction = None
        transaction = self.connection.transaction()
        try:
            await transaction.start()
        except Exception as e:
            self._release_failed_transaction_claim(transaction)
            raise create_mapped_exception(e) from e
        try:
            if self._should_set_local_columnar():
                await self.connection.execute("SET LOCAL google_columnar_engine.enable_columnar_scan = 'on'")
        except Exception as e:
            with suppress(Exception):
                await transaction.rollback()
            raise create_mapped_exception(e) from e
        self._transaction = transaction

    async def commit(self) -> None:
        """Commit the current transaction."""
        transaction = self._transaction
        self._transaction = None
        try:
            if transaction is not None:
                await transaction.commit()
            elif self._connection_in_transaction():
                await self.connection.execute("COMMIT")
        except Exception as e:
            raise create_mapped_exception(e) from e

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        transaction = self._transaction
        self._transaction = None
        try:
            if transaction is not None:
                await transaction.rollback()
            elif self._connection_in_transaction():
                await self.connection.execute("ROLLBACK")
        except Exception as e:
            raise create_mapped_exception(e) from e

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
        return bool(
            await self.connection.fetchval(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = $1", normalized_schema
            )
        )

    def with_cursor(self, connection: "AsyncpgConnection") -> "AsyncpgCursor":
        """Create context manager for AsyncPG cursor."""
        return AsyncpgCursor(connection)

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "AsyncRowStream[dict[str, Any]] | None":
        """Return a native asyncpg row stream backed by a cursor in a stream-owned transaction."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        params: tuple[Any, ...] = cast("tuple[Any, ...]", prepared_parameters) if prepared_parameters else ()
        timeout = self._timeout_value(statement.statement_config)
        return AsyncRowStream(AsyncpgStreamSource(self, sql, params, chunk_size, timeout))

    def handle_database_exceptions(self) -> "AsyncpgExceptionHandler":
        """Handle database exceptions with PostgreSQL error codes."""
        return AsyncpgExceptionHandler()

    async def execute_stack(
        self, stack: "StatementStack", *, continue_on_error: bool = False
    ) -> "tuple[StackResult, ...]":
        """Execute a StatementStack sequentially, reusing native prepared statements."""

        if not isinstance(stack, StatementStack) or not stack or self.stack_native_disabled:
            return await super().execute_stack(stack, continue_on_error=continue_on_error)

        return await self._execute_stack_native(stack, continue_on_error=continue_on_error)

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
        """Execute a query and write the results to storage through the storage pipeline.

        Results are encoded client-side and uploaded. PostgreSQL offers no native
        object-store destination for ``COPY``, and asyncpg's COPY helpers use the
        ``STDIN``/``STDOUT`` stream form.
        """

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
        """Load Arrow data into a PostgreSQL table via COPY."""
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
        table_name, schema_name, quoted_target = self._copy_target(table)
        if overwrite:
            try:
                await self.connection.execute(f"TRUNCATE TABLE {quoted_target}")
            except Exception as exc:
                mapped = create_mapped_exception(exc)
                msg = f"Failed to truncate table '{table}': {exc}"
                raise type(mapped)(msg) from exc
        columns, batch_iter = self._coerce_arrow_batches(source)
        first_batch = next((b for b in batch_iter if b.num_rows > 0), None)
        metrics: dict[str, int] = {"rows_processed": 0, "bytes_processed": 0}
        if first_batch is not None:
            all_batches = chain((first_batch,), (b for b in batch_iter if b.num_rows > 0))
            use_binary = bool(self.driver_features.get("enable_pgcopy_binary", False))
            try:
                if use_binary:
                    async_chunks = self._iter_pgcopy_binary_chunks_async(all_batches, columns=columns, metrics=metrics)
                    await self.connection.copy_to_table(
                        table_name, source=async_chunks, columns=columns, schema_name=schema_name, format="binary"
                    )
                else:
                    for batch in all_batches:
                        metrics["rows_processed"] += int(batch.num_rows)
                        metrics["bytes_processed"] += int(batch.nbytes)
                        records = self._arrow_batch_to_rows(batch, columns)
                        if records:
                            await self.connection.copy_records_to_table(
                                table_name, records=records, columns=columns, schema_name=schema_name
                            )
            except Exception as exc:
                raise create_mapped_exception(exc) from exc
        telemetry_payload = self._ingest_telemetry_from_batches(metrics["rows_processed"], metrics["bytes_processed"])
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    async def load_from_records(
        self,
        table: str,
        records: "Sequence[Mapping[str, Any]] | Sequence[Sequence[Any]]",
        *,
        columns: "list[str] | None" = None,
        overwrite: bool = False,
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load mapping or positional records directly with binary COPY."""
        if stage_table or conflict_keys or drop_indexes:
            return await super().load_from_records(
                table,
                records,
                columns=columns,
                overwrite=overwrite,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=drop_indexes,
                concurrent_indexes=concurrent_indexes,
                type_casts=type_casts,
            )
        self._require_capability("arrow_import_enabled")
        materialized = list(records)
        if not materialized:
            if overwrite:
                table_name, schema_name, quoted_target = self._copy_target(table)
                try:
                    await self.connection.execute(f"TRUNCATE TABLE {quoted_target}")
                except Exception as exc:
                    mapped = create_mapped_exception(exc)
                    msg = f"Failed to truncate table '{table}': {exc}"
                    raise type(mapped)(msg) from exc
                empty_telemetry: StorageTelemetry = {
                    "bytes_processed": 0,
                    "destination": table,
                    "format": "records",
                    "rows_processed": 0,
                }
                return self._storage_job(empty_telemetry)
            msg = "load_from_records requires at least one record."
            raise ImproperConfigurationError(msg)

        first = materialized[0]
        if isinstance(first, Mapping):
            resolved_columns = columns if columns is not None else list(first.keys())
            expected_keys = set(resolved_columns)
            copy_rows: list[tuple[Any, ...]] = []
            for record in materialized:
                if not isinstance(record, Mapping):
                    msg = "load_from_records mapping records must all be mappings."
                    raise ImproperConfigurationError(msg)
                if set(record.keys()) != expected_keys:
                    msg = "load_from_records mapping records must all share the same keys."
                    raise ImproperConfigurationError(msg)
                copy_rows.append(tuple(record[column] for column in resolved_columns))
        else:
            if columns is None:
                msg = "load_from_records requires columns when records are positional sequences."
                raise ImproperConfigurationError(msg)
            resolved_columns = columns
            copy_rows = []
            for record in materialized:
                if isinstance(record, Mapping):
                    msg = "load_from_records positional records must all have the same shape."
                    raise ImproperConfigurationError(msg)
                row = tuple(record)
                if len(row) != len(resolved_columns):
                    msg = "load_from_records positional records must match the number of columns."
                    raise ImproperConfigurationError(msg)
                copy_rows.append(row)

        table_name, schema_name, quoted_target = self._copy_target(table)
        if overwrite:
            try:
                await self.connection.execute(f"TRUNCATE TABLE {quoted_target}")
            except Exception as exc:
                mapped = create_mapped_exception(exc)
                msg = f"Failed to truncate table '{table}': {exc}"
                raise type(mapped)(msg) from exc
        try:
            await self.connection.copy_records_to_table(
                table_name, records=copy_rows, columns=resolved_columns, schema_name=schema_name
            )
        except Exception as exc:
            raise create_mapped_exception(exc) from exc
        telemetry_payload: StorageTelemetry = {
            "bytes_processed": 0,
            "destination": table,
            "format": "records",
            "rows_processed": len(copy_rows),
        }
        return self._storage_job(telemetry_payload)

    def _timeout_value(self, config: "StatementConfig") -> float | None:
        for options in (config.execution_args, self.statement_config.execution_args):
            if options:
                for key in ("timeout", "command_timeout"):
                    if key in options:
                        val = options[key]
                        return float(val) if val is not None else None
        return None

    def _timeout_arguments(self, config: "StatementConfig") -> "dict[str, Any]":
        timeout = self._timeout_value(config)
        return {"timeout": timeout} if timeout is not None else {}

    def _release_failed_transaction_claim(self, transaction: Any) -> None:
        """Clear the connection's transaction claim left by a failed start."""
        with suppress(Exception):
            if getattr(self.connection, "_top_xact", None) is transaction:
                self.connection._top_xact = None

    @staticmethod
    def _copy_target(table: str) -> "tuple[str, str | None, str]":
        try:
            parsed = parse_one(table, read="postgres", into=exp.Table)
        except ParseError as exc:
            msg = "AsyncPG COPY targets must be unqualified or schema-qualified table names."
            raise ImproperConfigurationError(msg) from exc
        schema_expression = parsed.args.get("db")
        schema_identifier = schema_expression if isinstance(schema_expression, exp.Identifier) else None
        if (
            parsed.catalog
            or not isinstance(parsed.this, exp.Identifier)
            or not parsed.this.name
            or parsed.this.name == "*"
            or (schema_expression is not None and schema_identifier is None)
            or (schema_identifier is not None and not schema_identifier.name)
        ):
            msg = "AsyncPG COPY targets must be unqualified or schema-qualified table names."
            raise ImproperConfigurationError(msg)
        table_identifier = parsed.this
        table_name = (
            table_identifier.name
            if table_identifier.quoted
            else normalize_identifier(table_identifier.name, "postgres")
        )
        schema_name = None
        if schema_identifier is not None:
            schema_name = (
                schema_identifier.name
                if schema_identifier.quoted
                else normalize_identifier(schema_identifier.name, "postgres")
            )
        quoted_target = quote_identifier(table_name)
        if schema_name is not None:
            quoted_target = f"{quote_identifier(schema_name)}.{quoted_target}"
        return table_name, schema_name, quoted_target

    async def _execute_stack_native(
        self, stack: "StatementStack", *, continue_on_error: bool
    ) -> "tuple[StackResult, ...]":
        results: list[StackResult] = []

        caller_in_tx = self._connection_in_transaction()
        transaction_cm = None
        if not continue_on_error and not caller_in_tx:
            transaction_cm = self.connection.transaction()

        with StackExecutionObserver(self, stack, continue_on_error, native_pipeline=False) as observer:
            if transaction_cm is not None:
                async with transaction_cm:
                    await self._run_stack_operations(
                        stack, continue_on_error, observer, results, caller_in_tx=caller_in_tx
                    )
            else:
                await self._run_stack_operations(stack, continue_on_error, observer, results, caller_in_tx=caller_in_tx)

        return tuple(results)

    async def _run_stack_operations(
        self,
        stack: "StatementStack",
        continue_on_error: bool,
        observer: "StackExecutionObserver",
        results: "list[StackResult]",
        *,
        caller_in_tx: bool = False,
    ) -> None:
        """Run operations for statement stack execution.

        Extracted from _execute_stack_native to avoid closure compilation issues.
        """
        for index, operation in enumerate(stack.operations):
            try:
                scope = self._stack_savepoint() if continue_on_error and caller_in_tx else nullcontext()
                async with scope:
                    normalized: NormalizedStackOperation | None = None
                    if operation.method == "execute":
                        kwargs = dict(operation.keyword_arguments) if operation.keyword_arguments else {}
                        statement_config = kwargs.pop("statement_config", None)
                        config = statement_config or self.statement_config

                        sql_statement = self.prepare_statement(
                            operation.statement, operation.arguments, statement_config=config, kwargs=kwargs
                        )
                        if not sql_statement.is_script and not sql_statement.is_many:
                            sql_text, prepared_parameters = self._compiled_sql(sql_statement, config)
                            prepared_parameters = cast("tuple[Any, ...] | dict[str, Any] | None", prepared_parameters)
                            normalized = NormalizedStackOperation(
                                operation=operation,
                                statement=sql_statement,
                                sql=sql_text,
                                parameters=prepared_parameters,
                            )

                    if normalized is not None:
                        stack_result = await self._execute_stack_operation_prepared(normalized)
                    else:
                        result = await self._execute_stack_operation(operation)
                        stack_result = StackResult(result=result)
            except Exception as exc:
                stack_error = StackExecutionError(
                    index,
                    describe_stack_statement(operation.statement),
                    exc,
                    adapter=type(self).__name__,
                    mode="continue-on-error" if continue_on_error else "fail-fast",
                )
                if continue_on_error:
                    if not caller_in_tx:
                        await self._rollback_failed_stack()
                    observer.record_operation_error(stack_error)
                    results.append(StackResult.from_error(stack_error))
                    continue
                raise stack_error from exc

            results.append(stack_result)
            if continue_on_error and not caller_in_tx:
                await self._commit_stack_success()

    @asynccontextmanager
    async def _stack_savepoint(self) -> "AsyncIterator[None]":
        name = validate_savepoint_name(f"sqlspec_stack_{uuid4().hex}")
        await self.connection.execute(f"SAVEPOINT {name}")
        try:
            yield
        except BaseException:
            await self.connection.execute(f"ROLLBACK TO SAVEPOINT {name}")
            raise
        finally:
            await self.connection.execute(f"RELEASE SAVEPOINT {name}")

    async def _execute_stack_operation_prepared(self, normalized: "NormalizedStackOperation") -> StackResult:
        result = await self._execute_cached_statement(normalized.statement)
        if not self._is_transaction_pooler_mode():
            result.metadata["prepared_statement"] = True
        return StackResult.from_sql_result(result)

    def _is_transaction_pooler_mode(self) -> bool:
        pooler_mode = self.driver_features.get("pooler_mode", "session")
        return pooler_mode in {"transaction", "statement"}

    def _should_set_local_columnar(self) -> bool:
        return bool(self.driver_features.get("enable_alloydb_columnar", False)) and self._is_transaction_pooler_mode()

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return bool(self.connection.is_in_transaction())

    async def _handle_copy_operation(self, cursor: "AsyncpgConnection", statement: "SQL") -> None:
        """Handle PostgreSQL COPY operations.

        Supports COPY FROM STDIN data import and falls through to cursor.execute
        for COPY TO STDOUT and file-based COPY variants.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement with COPY operation
        """

        execution_args = statement.statement_config.execution_args
        metadata: dict[str, Any] = dict(execution_args) if execution_args else {}
        timeout_args = self._timeout_arguments(statement.statement_config)
        if statement.is_processed:
            sql_text = statement.get_processed_state().compiled_sql
        else:
            sql_text, _ = self._compiled_sql(statement, statement.statement_config)
        copy_data = metadata.get("postgres_copy_data")

        if copy_data is not None and is_copy_from_operation(statement.operation_type):
            if isinstance(copy_data, dict):
                data_str = (
                    str(next(iter(copy_data.values())))
                    if len(copy_data) == 1
                    else "\n".join(str(value) for value in copy_data.values())
                )
            elif isinstance(copy_data, (list, tuple)):
                data_str = str(copy_data[0]) if len(copy_data) == 1 else "\n".join(str(value) for value in copy_data)
            else:
                data_str = str(copy_data)

            data_io = BytesIO(data_str.encode("utf-8"))
            raw_table = cast("str | None", metadata.get("postgres_copy_table"))
            if raw_table is None:
                raw_table = _extract_copy_from_stdin_table(sql_text)

            if raw_table is None:
                msg = "COPY FROM STDIN requires a table name or postgres_copy_table execution argument"
                raise SQLSpecError(msg)

            table_name, schema_name, _ = self._copy_target(raw_table)
            copy_kwargs: dict[str, Any] = {"source": data_io, **timeout_args}
            if schema_name is not None:
                copy_kwargs["schema_name"] = schema_name
            if "postgres_copy_columns" in metadata:
                copy_kwargs["columns"] = metadata["postgres_copy_columns"]
            if "postgres_copy_format" in metadata:
                copy_kwargs["format"] = metadata["postgres_copy_format"]
            if "postgres_copy_delimiter" in metadata:
                copy_kwargs["delimiter"] = metadata["postgres_copy_delimiter"]

            await cursor.copy_to_table(table_name, **copy_kwargs)
            return

        await cursor.execute(sql_text, **timeout_args)


def _extract_copy_from_stdin_table(sql_text: str) -> "str | None":
    with suppress(Exception):
        parsed = parse_one(sql_text, read="postgres")
        if isinstance(parsed, exp.Copy):
            target = parsed.this
            if isinstance(target, exp.Schema):
                target = target.this
            if isinstance(target, exp.Table):
                return target.sql(dialect="postgres")
    match = _COPY_FROM_STDIN_RE.search(sql_text)
    if match:
        return match.group(1)
    return None


register_driver_profile("asyncpg", driver_profile)
