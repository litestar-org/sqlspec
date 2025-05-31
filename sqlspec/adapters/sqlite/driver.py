# ruff: noqa: PLR6301
import contextlib
import logging
import sqlite3
from collections.abc import (
    Iterator,
    Sequence,  # Iterable needed for isinstance check
)
from contextlib import contextmanager
from typing import Any, ClassVar, Optional, cast

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.statement.mixins import ResultConverter
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ExecuteResult, SelectResult
from sqlspec.statement.sql import (
    SQL,
    SQLConfig,
)
from sqlspec.typing import ModelDTOT, SQLParameterType

__all__ = ("SqliteConnection", "SqliteDriver")

logger = logging.getLogger("sqlspec")

SqliteConnection = sqlite3.Connection


class SqliteDriver(
    SyncDriverAdapterProtocol[SqliteConnection, dict[str, Any]],  # DefaultRowT is dict
    ResultConverter,
):
    """SQLite Sync Driver Adapter.
    Refactored to align with the new enhanced driver architecture.
    """

    __supports_arrow__: ClassVar[bool] = False
    dialect: str = "sqlite"

    def __init__(
        self,
        connection: SqliteConnection,
        config: Optional[SQLConfig] = None,
        instrumentation_config: Optional[InstrumentationConfig] = None,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            # default_row_type for CommonDriverAttributes is dict[str, Any] by protocol default
        )
        # Ensure connection.row_factory is set for dictionary-like row access if desired.
        # This is a common practice for SQLite to make rows behave like dicts.
        if not isinstance(connection.row_factory, type(sqlite3.Row)):
            connection.row_factory = sqlite3.Row

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.QMARK

    @staticmethod
    @contextmanager
    def _with_cursor(connection: SqliteConnection) -> Iterator[sqlite3.Cursor]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    def _execute_impl(
        self,
        statement: SQL,  # Prepared SQL object from _build_statement
        parameters: Optional[SQLParameterType] = None,  # Passed from protocol's execute methods
        connection: Optional[SqliteConnection] = None,
        config: Optional[SQLConfig] = None,  # Passed from protocol's execute methods
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,  # Consumed by _execute_impl directly if needed, or ignored
    ) -> Any:  # Returns raw driver result (e.g., cursor or script output string)
        conn = self._connection(connection)
        # Config from statement or overridden by the config arg to _execute_impl
        # effective_config = config or statement.config # Not explicitly used, but statement uses its own config

        final_sql: str
        final_driver_params: Any = None

        if is_script:
            # Parameters for scripts are expected to be baked into the statement by `to_sql`
            # when placeholder_style is STATIC. The `SQL` object should be created with these parameters.
            final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
            # final_driver_params remains None
        else:
            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
            if is_many:
                batch_params: list[tuple[Any, ...]] = []
                if parameters is not None and isinstance(parameters, Sequence):
                    for param_set in parameters:
                        if isinstance(param_set, (list, tuple)):
                            batch_params.append(tuple(param_set))
                        else:
                            batch_params.append((param_set,))
                final_driver_params = batch_params
            else:
                # For single execute, `statement` (SQL object) already has its parameters processed
                # by `_build_statement` using the `parameters` from the public `execute` call.
                # `statement.get_parameters()` will retrieve these in the correct format.
                retrieved_params = statement.get_parameters(style=self._get_placeholder_style())
                if isinstance(retrieved_params, tuple):
                    single_params_tuple = retrieved_params
                elif isinstance(retrieved_params, list):  # Should be tuple for qmark, but handle list
                    single_params_tuple = tuple(retrieved_params)
                elif retrieved_params is None:
                    single_params_tuple: tuple[Any, ...] = ()  # type: ignore[no-redef]
                else:  # Should be a sequence for qmark
                    single_params_tuple = (retrieved_params,)  # Ensure it's a tuple if single value
                final_driver_params = single_params_tuple

        with self._with_cursor(conn) as cursor:
            if is_script:
                cursor.executescript(final_sql)
                return "SCRIPT EXECUTED"

            if is_many:
                # We ensured final_driver_params is list[tuple[Any,...]] or empty list
                cursor.executemany(
                    final_sql,
                    cast("list[tuple[Any, ...]]", final_driver_params if final_driver_params is not None else []),
                )
            else:
                # We ensured final_driver_params is tuple[Any,...]
                cursor.execute(
                    final_sql, cast("tuple[Any, ...]", final_driver_params if final_driver_params is not None else ())
                )

            return cursor  # Return cursor for wrapper methods to process

    def _wrap_select_result(
        self,
        statement: SQL,  # Renamed to avoid conflict with 'statement' module
        raw_driver_result: Any,  # sqlite3.Cursor
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,  # Consumed
    ) -> SelectResult[Any]:
        cursor = cast("sqlite3.Cursor", raw_driver_result)
        fetched_rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description or []]

        # sqlite3.Row objects are dict-like and can be converted to dict
        rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in fetched_rows]

        # For now, always return SelectResult with dict rows since that's what the rows field expects
        # Schema conversion can be handled by the caller or the result system needs redesign
        return SelectResult(
            raw_result=fetched_rows,  # Store the original fetched rows
            rows=rows_as_dicts,
            column_names=column_names,
        )

    def _wrap_execute_result(
        self,
        statement: SQL,  # Renamed to avoid conflict
        raw_driver_result: Any,  # sqlite3.Cursor
        **kwargs: Any,  # Consumed
    ) -> ExecuteResult[Any]:
        cursor = cast("sqlite3.Cursor", raw_driver_result)
        rowcount = cursor.rowcount

        operation_type = "UNKNOWN"
        if statement.expression and hasattr(statement.expression, "key"):
            operation_type = str(statement.expression.key).upper()

        return ExecuteResult(
            raw_result=None,  # Or potentially the cursor itself if useful, but typically not for DML
            rows_affected=rowcount,
            operation_type=operation_type,
            last_inserted_id=cursor.lastrowid,
        )
