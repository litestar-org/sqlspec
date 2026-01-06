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
    build_psycopg_profile,
    build_truncate_command,
    psycopg_pipeline_supported,
)
from sqlspec.adapters.psycopg.data_dictionary import PostgresAsyncDataDictionary, PostgresSyncDataDictionary
from sqlspec.core import (
    SQL,
    ParameterStyleConfig,
    SQLResult,
    StackOperation,
    StackResult,
    StatementConfig,
    StatementStack,
    build_statement_config_from_profile,
    get_cache_config,
    is_copy_from_operation,
    is_copy_operation,
    is_copy_to_operation,
    register_driver_profile,
)
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.driver._common import StackExecutionObserver, describe_stack_statement
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    StackExecutionError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_converters import build_json_list_converter, build_json_tuple_converter
from sqlspec.utils.type_guards import has_sqlstate, is_readable

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.psycopg._typing import PsycopgPipelineDriver
    from sqlspec.core import ArrowResult
    from sqlspec.driver._async import AsyncDataDictionaryBase
    from sqlspec.driver._common import ExecutionResult
    from sqlspec.driver._sync import SyncDataDictionaryBase
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
            normalized = self._normalize_stack_operation_for_pipeline(index, operation)
            if normalized is None:
                return None
            prepared.append(normalized)
        return prepared

    def _normalize_stack_operation_for_pipeline(
        self, index: int, operation: "StackOperation"
    ) -> "_PreparedStackOperation | None":
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
        return _PreparedStackOperation(
            operation_index=index,
            operation=operation,
            statement=sql_statement,
            sql=sql_text,
            parameters=prepared_parameters,
        )


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
                self._map_postgres_exception(exc_val)
            except Exception as mapped:
                self.pending_exception = mapped
                return True
        return False

    def _map_postgres_exception(self, e: Any) -> None:
        """Map PostgreSQL exception to SQLSpec exception.

        Args:
            e: psycopg.Error instance

        Raises:
            Specific SQLSpec exception based on SQLSTATE code
        """
        error_code = e.sqlstate if has_sqlstate(e) and e.sqlstate is not None else None

        if not error_code:
            self._raise_generic_error(e, None)
            return

        if error_code == "23505":
            self._raise_unique_violation(e, error_code)
        elif error_code == "23503":
            self._raise_foreign_key_violation(e, error_code)
        elif error_code == "23502":
            self._raise_not_null_violation(e, error_code)
        elif error_code == "23514":
            self._raise_check_violation(e, error_code)
        elif error_code.startswith("23"):
            self._raise_integrity_error(e, error_code)
        elif error_code.startswith("42"):
            self._raise_parsing_error(e, error_code)
        elif error_code.startswith("08"):
            self._raise_connection_error(e, error_code)
        elif error_code.startswith("40"):
            self._raise_transaction_error(e, error_code)
        elif error_code.startswith("22"):
            self._raise_data_error(e, error_code)
        elif error_code.startswith(("53", "54", "55", "57", "58")):
            self._raise_operational_error(e, error_code)
        else:
            self._raise_generic_error(e, error_code)

    def _raise_unique_violation(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL unique constraint violation [{code}]: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL foreign key constraint violation [{code}]: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL not-null constraint violation [{code}]: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_check_violation(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL check constraint violation [{code}]: {e}"
        raise CheckViolationError(msg) from e

    def _raise_integrity_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL integrity constraint violation [{code}]: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL SQL syntax error [{code}]: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL connection error [{code}]: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_transaction_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL transaction error [{code}]: {e}"
        raise TransactionError(msg) from e

    def _raise_data_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL data error [{code}]: {e}"
        raise DataError(msg) from e

    def _raise_operational_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL operational error [{code}]: {e}"
        raise OperationalError(msg) from e

    def _raise_generic_error(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL database error [{code}]: {e}" if code else f"PostgreSQL database error: {e}"
        raise SQLSpecError(msg) from e


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
        self._data_dictionary: SyncDataDictionaryBase | None = None

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

    def _handle_transaction_error_cleanup(self) -> None:
        """Handle transaction cleanup after database errors."""
        try:
            status = self.connection.info.transaction_status
            if status == TRANSACTION_STATUS_INERROR:
                logger.debug("Connection in aborted transaction state, performing rollback")
                self.connection.rollback()
        except Exception as cleanup_error:
            logger.warning("Failed to cleanup transaction state: %s", cleanup_error)

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
        """Hook for PostgreSQL-specific special operations.

        Args:
            cursor: Psycopg cursor object
            statement: SQL statement to analyze

        Returns:
            SQLResult if special handling was applied, None otherwise
        """

        if is_copy_operation(statement.operation_type):
            return self._handle_copy_operation(cursor, statement)

        return None

    def _handle_copy_operation(self, cursor: Any, statement: "SQL") -> "SQLResult":
        """Handle PostgreSQL COPY operations using copy_expert.

        Args:
            cursor: Psycopg cursor object
            statement: SQL statement with COPY operation

        Returns:
            SQLResult with COPY operation results
        """

        sql, _ = statement.compile()
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

                    results.extend(self._build_pipeline_stack_result(entry) for entry in pending)

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

    def _build_pipeline_stack_result(self, entry: "_PipelineCursorEntry") -> StackResult:
        statement = entry.prepared.statement
        cursor = entry.cursor

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
            execution_result = self.create_execution_result(
                cursor,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
            )
        else:
            affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            execution_result = self.create_execution_result(cursor, rowcount_override=affected_rows)

        sql_result = self.build_statement_result(statement, execution_result)
        return StackResult.from_sql_result(sql_result)

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

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

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
            column_names = [col.name for col in cursor.description or []]

            return self.create_execution_result(
                cursor,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
            )

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
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
            self._truncate_table_sync(table)
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

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = PostgresSyncDataDictionary()
        return self._data_dictionary

    def _truncate_table_sync(self, table: str) -> None:
        truncate_sql = build_truncate_command(table)
        with self.with_cursor(self.connection) as cursor, self.handle_database_exceptions():
            cursor.execute(truncate_sql)

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return bool(self.connection.info.transaction_status != TRANSACTION_STATUS_IDLE)


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
                self._map_postgres_exception(exc_val)
            except Exception as mapped:
                self.pending_exception = mapped
                return True
        return False

    def _map_postgres_exception(self, e: Any) -> None:
        """Map PostgreSQL exception to SQLSpec exception.

        Args:
            e: psycopg.Error instance

        Raises:
            Specific SQLSpec exception based on SQLSTATE code
        """
        error_code = e.sqlstate if has_sqlstate(e) and e.sqlstate is not None else None

        if not error_code:
            self._raise_generic_error(e, None)
            return

        if error_code == "23505":
            self._raise_unique_violation(e, error_code)
        elif error_code == "23503":
            self._raise_foreign_key_violation(e, error_code)
        elif error_code == "23502":
            self._raise_not_null_violation(e, error_code)
        elif error_code == "23514":
            self._raise_check_violation(e, error_code)
        elif error_code.startswith("23"):
            self._raise_integrity_error(e, error_code)
        elif error_code.startswith("42"):
            self._raise_parsing_error(e, error_code)
        elif error_code.startswith("08"):
            self._raise_connection_error(e, error_code)
        elif error_code.startswith("40"):
            self._raise_transaction_error(e, error_code)
        elif error_code.startswith("22"):
            self._raise_data_error(e, error_code)
        elif error_code.startswith(("53", "54", "55", "57", "58")):
            self._raise_operational_error(e, error_code)
        else:
            self._raise_generic_error(e, error_code)

    def _raise_unique_violation(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL unique constraint violation [{code}]: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL foreign key constraint violation [{code}]: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL not-null constraint violation [{code}]: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_check_violation(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL check constraint violation [{code}]: {e}"
        raise CheckViolationError(msg) from e

    def _raise_integrity_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL integrity constraint violation [{code}]: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL SQL syntax error [{code}]: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL connection error [{code}]: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_transaction_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL transaction error [{code}]: {e}"
        raise TransactionError(msg) from e

    def _raise_data_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL data error [{code}]: {e}"
        raise DataError(msg) from e

    def _raise_operational_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL operational error [{code}]: {e}"
        raise OperationalError(msg) from e

    def _raise_generic_error(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL database error [{code}]: {e}" if code else f"PostgreSQL database error: {e}"
        raise SQLSpecError(msg) from e


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
        self._data_dictionary: AsyncDataDictionaryBase | None = None

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

    async def _handle_transaction_error_cleanup_async(self) -> None:
        """Handle async transaction cleanup after database errors."""
        try:
            status = self.connection.info.transaction_status
            if status == TRANSACTION_STATUS_INERROR:
                logger.debug("Connection in aborted transaction state, performing async rollback")
                await self.connection.rollback()
        except Exception as cleanup_error:
            logger.warning("Failed to cleanup transaction state: %s", cleanup_error)

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
        """Hook for PostgreSQL-specific special operations.

        Args:
            cursor: Psycopg async cursor object
            statement: SQL statement to analyze

        Returns:
            SQLResult if special handling was applied, None otherwise
        """

        if is_copy_operation(statement.operation_type):
            return await self._handle_copy_operation_async(cursor, statement)

        return None

    async def _handle_copy_operation_async(self, cursor: Any, statement: "SQL") -> "SQLResult":
        """Handle PostgreSQL COPY operations (async).

        Args:
            cursor: Psycopg async cursor object
            statement: SQL statement with COPY operation

        Returns:
            SQLResult with COPY operation results
        """

        sql, _ = statement.compile()
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

                    results.extend([await self._build_pipeline_stack_result_async(entry) for entry in pending])

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

    async def _build_pipeline_stack_result_async(self, entry: "_PipelineCursorEntry") -> StackResult:
        statement = entry.prepared.statement
        cursor = entry.cursor

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
            execution_result = self.create_execution_result(
                cursor,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
            )
        else:
            affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            execution_result = self.create_execution_result(cursor, rowcount_override=affected_rows)

        sql_result = self.build_statement_result(statement, execution_result)
        return StackResult.from_sql_result(sql_result)

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

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

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
            column_names = [col.name for col in cursor.description or []]

            return self.create_execution_result(
                cursor,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
            )

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
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
            await self._truncate_table_async(table)
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

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = PostgresAsyncDataDictionary()
        return self._data_dictionary

    async def _truncate_table_async(self, table: str) -> None:
        truncate_sql = build_truncate_command(table)
        async with self.with_cursor(self.connection) as cursor, self.handle_database_exceptions():
            await cursor.execute(truncate_sql)

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return bool(self.connection.info.transaction_status != TRANSACTION_STATUS_IDLE)


_PSYCOPG_PROFILE = build_psycopg_profile()

register_driver_profile("psycopg", _PSYCOPG_PROFILE)


def _create_psycopg_parameter_config(serializer: "Callable[[Any], str]") -> "ParameterStyleConfig":
    """Construct parameter configuration with shared JSON serializer support."""

    base_config = build_statement_config_from_profile(_PSYCOPG_PROFILE, json_serializer=serializer).parameter_config

    updated_type_map = dict(base_config.type_coercion_map)
    updated_type_map[list] = build_json_list_converter(serializer)
    updated_type_map[tuple] = build_json_tuple_converter(serializer)

    return base_config.replace(type_coercion_map=updated_type_map)


def build_psycopg_statement_config(*, json_serializer: "Callable[[Any], str]" = to_json) -> "StatementConfig":
    """Construct the psycopg statement configuration with optional JSON codecs."""

    parameter_config = _create_psycopg_parameter_config(json_serializer)
    base_config = build_statement_config_from_profile(_PSYCOPG_PROFILE, json_serializer=json_serializer)
    return base_config.replace(parameter_config=parameter_config)


psycopg_statement_config = build_psycopg_statement_config()
