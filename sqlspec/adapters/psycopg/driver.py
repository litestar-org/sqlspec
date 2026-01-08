"""PostgreSQL psycopg driver implementation."""

from contextlib import AsyncExitStack, ExitStack
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import psycopg

from sqlspec.adapters.psycopg._typing import (
    PsycopgAsyncConnection,
    PsycopgAsyncSessionContext,
    PsycopgSyncConnection,
    PsycopgSyncSessionContext,
)
from sqlspec.adapters.psycopg.core import (
    build_copy_from_command,
    build_psycopg_async_pipeline_execution_result,
    build_psycopg_pipeline_execution_result,
    build_psycopg_profile,
    build_psycopg_statement_config,
    build_truncate_command,
    collect_psycopg_rows,
    normalize_psycopg_rowcount,
    psycopg_pipeline_supported,
    psycopg_statement_config,
    raise_psycopg_exception,
)
from sqlspec.adapters.psycopg.data_dictionary import PsycopgAsyncDataDictionary, PsycopgSyncDataDictionary
from sqlspec.core import (
    SQL,
    SQLResult,
    StackOperation,
    StackResult,
    StatementConfig,
    StatementStack,
    get_cache_config,
    is_copy_from_operation,
    is_copy_operation,
    is_copy_to_operation,
    register_driver_profile,
)
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.driver._common import StackExecutionObserver, describe_stack_statement
from sqlspec.exceptions import SQLSpecError, StackExecutionError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import is_readable

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg._typing import PsycopgPipelineDriver
    from sqlspec.core import ArrowResult
    from sqlspec.driver._common import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry


__all__ = (
    "PsycopgAsyncCursor",
    "PsycopgAsyncDriver",
    "PsycopgAsyncExceptionHandler",
    "PsycopgAsyncSessionContext",
    "PsycopgSyncCursor",
    "PsycopgSyncDriver",
    "PsycopgSyncExceptionHandler",
    "PsycopgSyncSessionContext",
    "build_psycopg_statement_config",
    "psycopg_statement_config",
)

logger = get_logger("adapters.psycopg")


class _PreparedStackOperation(NamedTuple):
    """Precompiled stack operation metadata for psycopg pipeline execution."""

    operation_index: int
    operation: "StackOperation"
    statement: "SQL"
    sql: str
    parameters: "tuple[Any, ...] | dict[str, Any] | None"


class _PipelineCursorEntry(NamedTuple):
    """Cursor pending result data for psycopg pipeline execution."""

    prepared: "_PreparedStackOperation"
    cursor: Any


class PsycopgPipelineMixin:
    """Shared helpers for psycopg sync/async pipeline execution."""

    __slots__ = ()

    def _prepare_pipeline_operations(self, stack: "StatementStack") -> "list[_PreparedStackOperation] | None":
        prepared: list[_PreparedStackOperation] = []
        for index, operation in enumerate(stack.operations):
            if operation.method != "execute":
                return None

            kwargs = dict(operation.keyword_arguments) if operation.keyword_arguments else {}
            statement_config = kwargs.pop("statement_config", None)
            driver = cast("PsycopgPipelineDriver", self)
            config = statement_config or driver.statement_config

            sql_statement = driver.prepare_statement(
                operation.statement, operation.arguments, statement_config=config, kwargs=kwargs
            )

            if sql_statement.is_script or sql_statement.is_many:
                return None

            sql_text, prepared_parameters = driver._get_compiled_sql(  # pyright: ignore[reportPrivateUsage]
                sql_statement, config
            )
            prepared.append(
                _PreparedStackOperation(
                    operation_index=index,
                    operation=operation,
                    statement=sql_statement,
                    sql=sql_text,
                    parameters=prepared_parameters,
                )
            )
        return prepared


TRANSACTION_STATUS_IDLE = 0
TRANSACTION_STATUS_ACTIVE = 1
TRANSACTION_STATUS_INTRANS = 2
TRANSACTION_STATUS_INERROR = 3
TRANSACTION_STATUS_UNKNOWN = 4


class PsycopgSyncCursor:
    """Context manager for PostgreSQL psycopg cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: PsycopgSyncConnection) -> None:
        self.connection = connection
        self.cursor: Any | None = None

    def __enter__(self) -> Any:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            self.cursor.close()


class PsycopgSyncExceptionHandler:
    """Context manager for handling PostgreSQL psycopg database exceptions.

    Maps PostgreSQL SQLSTATE error codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "PsycopgSyncExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, psycopg.Error):
            try:
                raise_psycopg_exception(exc_val)
            except Exception as mapped:
                self.pending_exception = mapped
                return True
        return False


class PsycopgSyncDriver(PsycopgPipelineMixin, SyncDriverAdapterBase):
    """PostgreSQL psycopg synchronous driver.

    Provides synchronous database operations for PostgreSQL using psycopg3.
    Supports SQL statement execution with parameter binding, transaction
    management, result processing with column metadata, parameter style
    conversion, PostgreSQL arrays and JSON handling, COPY operations for
    bulk data transfer, and PostgreSQL-specific error handling.
    """

    __slots__ = ("_data_dictionary",)
    dialect = "postgres"

    def __init__(
        self,
        connection: PsycopgSyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            default_config = psycopg_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="postgres",
            )
            statement_config = default_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: PsycopgSyncDataDictionary | None = None

    def with_cursor(self, connection: PsycopgSyncConnection) -> PsycopgSyncCursor:
        """Create context manager for PostgreSQL cursor."""
        return PsycopgSyncCursor(connection)

    def begin(self) -> None:
        """Begin a database transaction on the current connection."""
        try:
            if self.connection.autocommit:
                self.connection.autocommit = False
        except Exception as e:
            msg = f"Failed to begin transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback the current transaction on the current connection."""
        try:
            self.connection.rollback()
        except Exception as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit the current transaction on the current connection."""
        try:
            self.connection.commit()
        except Exception as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    def handle_database_exceptions(self) -> "PsycopgSyncExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately."""
        return PsycopgSyncExceptionHandler()

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
        """Hook for PostgreSQL-specific special operations.

        Args:
            cursor: Psycopg cursor object
            statement: SQL statement to analyze

        Returns:
            SQLResult if special handling was applied, None otherwise
        """

        if not is_copy_operation(statement.operation_type):
            return None

        sql, _ = self._get_compiled_sql(statement, statement.statement_config)
        operation_type = statement.operation_type
        copy_data = statement.parameters
        if isinstance(copy_data, list) and len(copy_data) == 1:
            copy_data = copy_data[0]

        if is_copy_from_operation(operation_type):
            if isinstance(copy_data, (str, bytes)):
                data_to_write = copy_data
            elif is_readable(copy_data):
                data_to_write = copy_data.read()
            else:
                data_to_write = str(copy_data)

            if isinstance(data_to_write, str):
                data_to_write = data_to_write.encode()

            with cursor.copy(sql) as copy_ctx:
                copy_ctx.write(data_to_write)

            rows_affected = max(cursor.rowcount, 0)

            return SQLResult(
                data=None, rows_affected=rows_affected, statement=statement, metadata={"copy_operation": "FROM_STDIN"}
            )

        if is_copy_to_operation(operation_type):
            output_data: list[str] = []
            with cursor.copy(sql) as copy_ctx:
                output_data.extend(row.decode() if isinstance(row, bytes) else str(row) for row in copy_ctx)

            exported_data = "".join(output_data)

            return SQLResult(
                data=[{"copy_output": exported_data}],
                rows_affected=0,
                statement=statement,
                metadata={"copy_operation": "TO_STDOUT"},
            )

        cursor.execute(sql)
        rows_affected = max(cursor.rowcount, 0)

        return SQLResult(
            data=None, rows_affected=rows_affected, statement=statement, metadata={"copy_operation": "FILE"}
        )

    def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with multiple statements.

        Args:
            cursor: Database cursor
            statement: SQL statement containing multiple commands

        Returns:
            ExecutionResult with script execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            if prepared_parameters:
                cursor.execute(stmt, prepared_parameters)
            else:
                cursor.execute(stmt)
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def execute_stack(self, stack: "StatementStack", *, continue_on_error: bool = False) -> "tuple[StackResult, ...]":
        """Execute a StatementStack using psycopg pipeline mode when supported."""

        if (
            not isinstance(stack, StatementStack)
            or not stack
            or self.stack_native_disabled
            or not psycopg_pipeline_supported()
            or continue_on_error
        ):
            return super().execute_stack(stack, continue_on_error=continue_on_error)

        prepared_ops = self._prepare_pipeline_operations(stack)
        if prepared_ops is None:
            return super().execute_stack(stack, continue_on_error=continue_on_error)

        return self._execute_stack_pipeline(stack, prepared_ops)

    def _execute_stack_pipeline(
        self, stack: "StatementStack", prepared_ops: "list[_PreparedStackOperation]"
    ) -> "tuple[StackResult, ...]":
        results: list[StackResult] = []
        started_transaction = False

        with StackExecutionObserver(self, stack, continue_on_error=False, native_pipeline=True):
            try:
                if not self._connection_in_transaction():
                    self.begin()
                    started_transaction = True

                with ExitStack() as resource_stack:
                    pipeline = resource_stack.enter_context(self.connection.pipeline())
                    pending: list[_PipelineCursorEntry] = []

                    for prepared in prepared_ops:
                        exception_ctx = self.handle_database_exceptions()
                        resource_stack.enter_context(exception_ctx)
                        cursor = resource_stack.enter_context(self.with_cursor(self.connection))

                        try:
                            if prepared.parameters:
                                cursor.execute(prepared.sql, prepared.parameters)
                            else:
                                cursor.execute(prepared.sql)
                        except Exception as exc:
                            stack_error = StackExecutionError(
                                prepared.operation_index,
                                describe_stack_statement(prepared.operation.statement),
                                exc,
                                adapter=type(self).__name__,
                                mode="fail-fast",
                            )
                            raise stack_error from exc

                        pending.append(_PipelineCursorEntry(prepared=prepared, cursor=cursor))

                    pipeline.sync()
                    for entry in pending:
                        statement = entry.prepared.statement
                        cursor = entry.cursor

                        execution_result = build_psycopg_pipeline_execution_result(statement, cursor)
                        sql_result = self.build_statement_result(statement, execution_result)
                        results.append(StackResult.from_sql_result(sql_result))

                if started_transaction:
                    self.commit()
            except Exception:
                if started_transaction:
                    try:
                        self.rollback()
                    except Exception as rollback_error:  # pragma: no cover - diagnostics only
                        logger.debug("Rollback after psycopg pipeline failure failed: %s", rollback_error)
                raise

        return tuple(results)

    def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: Database cursor
            statement: SQL statement with parameter list

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        cursor.executemany(sql, prepared_parameters)

        affected_rows = normalize_psycopg_rowcount(cursor)

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            ExecutionResult with statement execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if prepared_parameters:
            cursor.execute(sql, prepared_parameters)
        else:
            cursor.execute(sql)

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            fetched_data, column_names = collect_psycopg_rows(
                cast("list[Any] | None", fetched_data), cursor.description
            )

            return self.create_execution_result(
                cursor,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
            )

        affected_rows = normalize_psycopg_rowcount(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    def select_to_storage(
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
        """Execute a query and stream Arrow results to storage (sync)."""

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
        """Load Arrow data into PostgreSQL using COPY."""

        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            truncate_sql = build_truncate_command(table)
            with self.with_cursor(self.connection) as cursor, self.handle_database_exceptions():
                cursor.execute(truncate_sql)
        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            copy_sql = build_copy_from_command(table, columns)
            with ExitStack() as stack:
                stack.enter_context(self.handle_database_exceptions())
                cursor = stack.enter_context(self.with_cursor(self.connection))
                copy_ctx = stack.enter_context(cursor.copy(copy_sql))
                for record in records:
                    copy_ctx.write_row(record)
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
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load staged artifacts into PostgreSQL via COPY."""

        arrow_table, inbound = self._read_arrow_from_storage_sync(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return bool(self.connection.info.transaction_status != TRANSACTION_STATUS_IDLE)

    @property
    def data_dictionary(self) -> "PsycopgSyncDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = PsycopgSyncDataDictionary()
        return self._data_dictionary


class PsycopgAsyncCursor:
    """Async context manager for PostgreSQL psycopg cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "PsycopgAsyncConnection") -> None:
        self.connection = connection
        self.cursor: Any | None = None

    async def __aenter__(self) -> Any:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)
        if self.cursor is not None:
            await self.cursor.close()


class PsycopgAsyncExceptionHandler:
    """Async context manager for handling PostgreSQL psycopg database exceptions.

    Maps PostgreSQL SQLSTATE error codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __aexit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> "PsycopgAsyncExceptionHandler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, psycopg.Error):
            try:
                raise_psycopg_exception(exc_val)
            except Exception as mapped:
                self.pending_exception = mapped
                return True
        return False


class PsycopgAsyncDriver(PsycopgPipelineMixin, AsyncDriverAdapterBase):
    """PostgreSQL psycopg asynchronous driver.

    Provides asynchronous database operations for PostgreSQL using psycopg3.
    Supports async SQL statement execution with parameter binding, async
    transaction management, async result processing with column metadata,
    parameter style conversion, PostgreSQL arrays and JSON handling, COPY
    operations for bulk data transfer, PostgreSQL-specific error handling,
    and async pub/sub support.
    """

    __slots__ = ("_data_dictionary",)
    dialect = "postgres"

    def __init__(
        self,
        connection: "PsycopgAsyncConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            default_config = psycopg_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="postgres",
            )
            statement_config = default_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: PsycopgAsyncDataDictionary | None = None

    def with_cursor(self, connection: "PsycopgAsyncConnection") -> "PsycopgAsyncCursor":
        """Create async context manager for PostgreSQL cursor."""
        return PsycopgAsyncCursor(connection)

    async def begin(self) -> None:
        """Begin a database transaction on the current connection."""
        try:
            try:
                autocommit_flag = self.connection.autocommit
            except AttributeError:
                autocommit_flag = None
            if isinstance(autocommit_flag, bool) and not autocommit_flag:
                return
            await self.connection.set_autocommit(False)
        except Exception as e:
            msg = f"Failed to begin transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction on the current connection."""
        try:
            await self.connection.rollback()
        except Exception as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction on the current connection."""
        try:
            await self.connection.commit()
        except Exception as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    def handle_database_exceptions(self) -> "PsycopgAsyncExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately."""
        return PsycopgAsyncExceptionHandler()

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
        """Hook for PostgreSQL-specific special operations.

        Args:
            cursor: Psycopg async cursor object
            statement: SQL statement to analyze

        Returns:
            SQLResult if special handling was applied, None otherwise
        """

        if not is_copy_operation(statement.operation_type):
            return None

        sql, _ = self._get_compiled_sql(statement, statement.statement_config)
        sql_upper = sql.upper()
        operation_type = statement.operation_type
        copy_data = statement.parameters
        if isinstance(copy_data, list) and len(copy_data) == 1:
            copy_data = copy_data[0]

        if is_copy_from_operation(operation_type) and "FROM STDIN" in sql_upper:
            if isinstance(copy_data, (str, bytes)):
                data_to_write = copy_data
            elif is_readable(copy_data):
                data_to_write = copy_data.read()
            else:
                data_to_write = str(copy_data)

            if isinstance(data_to_write, str):
                data_to_write = data_to_write.encode()

            async with cursor.copy(sql) as copy_ctx:
                await copy_ctx.write(data_to_write)

            rows_affected = max(cursor.rowcount, 0)

            return SQLResult(
                data=None, rows_affected=rows_affected, statement=statement, metadata={"copy_operation": "FROM_STDIN"}
            )

        if is_copy_to_operation(operation_type) and "TO STDOUT" in sql_upper:
            output_data: list[str] = []
            async with cursor.copy(sql) as copy_ctx:
                output_data.extend([row.decode() if isinstance(row, bytes) else str(row) async for row in copy_ctx])

            exported_data = "".join(output_data)

            return SQLResult(
                data=[{"copy_output": exported_data}],
                rows_affected=0,
                statement=statement,
                metadata={"copy_operation": "TO_STDOUT"},
            )

        await cursor.execute(sql)
        rows_affected = max(cursor.rowcount, 0)

        return SQLResult(
            data=None, rows_affected=rows_affected, statement=statement, metadata={"copy_operation": "FILE"}
        )

    async def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with multiple statements (async).

        Args:
            cursor: Database cursor
            statement: SQL statement containing multiple commands

        Returns:
            ExecutionResult with script execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            if prepared_parameters:
                await cursor.execute(stmt, prepared_parameters)
            else:
                await cursor.execute(stmt)
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def execute_stack(
        self, stack: "StatementStack", *, continue_on_error: bool = False
    ) -> "tuple[StackResult, ...]":
        """Execute a StatementStack using psycopg async pipeline when supported."""

        if (
            not isinstance(stack, StatementStack)
            or not stack
            or self.stack_native_disabled
            or not psycopg_pipeline_supported()
            or continue_on_error
        ):
            return await super().execute_stack(stack, continue_on_error=continue_on_error)

        prepared_ops = self._prepare_pipeline_operations(stack)
        if prepared_ops is None:
            return await super().execute_stack(stack, continue_on_error=continue_on_error)

        return await self._execute_stack_pipeline(stack, prepared_ops)

    async def _execute_stack_pipeline(
        self, stack: "StatementStack", prepared_ops: "list[_PreparedStackOperation]"
    ) -> "tuple[StackResult, ...]":
        results: list[StackResult] = []
        started_transaction = False

        with StackExecutionObserver(self, stack, continue_on_error=False, native_pipeline=True):
            try:
                if not self._connection_in_transaction():
                    await self.begin()
                    started_transaction = True

                async with AsyncExitStack() as resource_stack:
                    pipeline = await resource_stack.enter_async_context(self.connection.pipeline())
                    pending: list[_PipelineCursorEntry] = []

                    for prepared in prepared_ops:
                        exception_ctx = self.handle_database_exceptions()
                        await resource_stack.enter_async_context(exception_ctx)
                        cursor = await resource_stack.enter_async_context(self.with_cursor(self.connection))

                        try:
                            if prepared.parameters:
                                await cursor.execute(prepared.sql, prepared.parameters)
                            else:
                                await cursor.execute(prepared.sql)
                        except Exception as exc:
                            stack_error = StackExecutionError(
                                prepared.operation_index,
                                describe_stack_statement(prepared.operation.statement),
                                exc,
                                adapter=type(self).__name__,
                                mode="fail-fast",
                            )
                            raise stack_error from exc

                        pending.append(_PipelineCursorEntry(prepared=prepared, cursor=cursor))

                    await pipeline.sync()
                    for entry in pending:
                        statement = entry.prepared.statement
                        cursor = entry.cursor

                        execution_result = await build_psycopg_async_pipeline_execution_result(statement, cursor)
                        sql_result = self.build_statement_result(statement, execution_result)
                        results.append(StackResult.from_sql_result(sql_result))

                if started_transaction:
                    await self.commit()
            except Exception:
                if started_transaction:
                    try:
                        await self.rollback()
                    except Exception as rollback_error:  # pragma: no cover - diagnostics only
                        logger.debug("Rollback after psycopg pipeline failure failed: %s", rollback_error)
                raise

        return tuple(results)

    async def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets (async).

        Args:
            cursor: Database cursor
            statement: SQL statement with parameter list

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        await cursor.executemany(sql, prepared_parameters)

        affected_rows = normalize_psycopg_rowcount(cursor)

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement (async).

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            ExecutionResult with statement execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if prepared_parameters:
            await cursor.execute(sql, prepared_parameters)
        else:
            await cursor.execute(sql)

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            fetched_data, column_names = collect_psycopg_rows(
                cast("list[Any] | None", fetched_data), cursor.description
            )

            return self.create_execution_result(
                cursor,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
            )

        affected_rows = normalize_psycopg_rowcount(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

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
        """Execute a query and stream Arrow data to storage asynchronously."""

        self._require_capability("arrow_export_enabled")
        arrow_result = await self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        async_pipeline = self._storage_pipeline()
        telemetry_payload = await self._write_result_to_storage_async(
            arrow_result, destination, format_hint=format_hint, pipeline=async_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    async def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into PostgreSQL asynchronously via COPY."""

        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            truncate_sql = build_truncate_command(table)
            async with self.with_cursor(self.connection) as cursor, self.handle_database_exceptions():
                await cursor.execute(truncate_sql)
        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            copy_sql = build_copy_from_command(table, columns)
            async with AsyncExitStack() as stack:
                await stack.enter_async_context(self.handle_database_exceptions())
                cursor = await stack.enter_async_context(self.with_cursor(self.connection))
                copy_ctx = await stack.enter_async_context(cursor.copy(copy_sql))
                for record in records:
                    await copy_ctx.write_row(record)
        telemetry_payload = self._build_ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load staged artifacts asynchronously."""

        arrow_table, inbound = await self._read_arrow_from_storage_async(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return bool(self.connection.info.transaction_status != TRANSACTION_STATUS_IDLE)

    @property
    def data_dictionary(self) -> "PsycopgAsyncDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = PsycopgAsyncDataDictionary()
        return self._data_dictionary


_PSYCOPG_PROFILE = build_psycopg_profile()

register_driver_profile("psycopg", _PSYCOPG_PROFILE)
