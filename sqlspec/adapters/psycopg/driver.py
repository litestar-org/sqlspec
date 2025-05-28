import logging
from collections.abc import Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar, Union, cast

import sqlglot  # Ensure sqlglot itself is imported
from psycopg import AsyncConnection, Connection
from psycopg import sql as psycopg_sql
from psycopg.rows import DictRow, dict_row

from sqlspec.base import (
    AsyncDriverAdapterProtocol,
    CommonDriverAttributes,
    StatementResultType,
    SyncDriverAdapterProtocol,
)
from sqlspec.exceptions import SQLParsingError
from sqlspec.sql.result import ExecuteResult, SelectResult
from sqlspec.sql.statement import SQLStatement
from sqlspec.typing import StatementParameterType

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from psycopg.sql import LiteralString
    from sqlglot.expressions import Expression as SQLGlotExpression

    from sqlspec.sql.filters import StatementFilter
    from sqlspec.sql.statement import Statement

logger = logging.getLogger("sqlspec")

__all__ = ("PsycopgAsyncConnection", "PsycopgAsyncDriver", "PsycopgSyncConnection", "PsycopgSyncDriver")

# Restore these crucial type definitions
PsycopgSyncConnection = Connection[DictRow]
PsycopgAsyncConnection = AsyncConnection[DictRow]
ConnectionT = TypeVar("ConnectionT", bound=Union[Connection[Any], AsyncConnection[Any]])

# NAMED_PARAM_REGEX is no longer used with the sqlglot dialect output approach.


class PsycopgDriverBase(CommonDriverAttributes[ConnectionT], Generic[ConnectionT]):
    dialect: str = "postgres"

    def _process_sql_params(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, Union[list[Any], dict[str, Any]]]":
        sql_input_for_super: Union[str, SQLGlotExpression]
        if isinstance(sql, SQLStatement):
            if not isinstance(sql.expression, sqlglot.Expression):
                # This should ideally be caught earlier or SQLStatement ensures .expression is always valid post-init
                msg = f"SQLStatement instance passed to PsycopgDriverBase has an invalid .expression attribute. Type: {type(sql.expression)}"
                raise SQLParsingError(msg)
            sql_input_for_super = sql.expression
        elif isinstance(sql, (str, sqlglot.Expression)):
            sql_input_for_super = sql
        else:
            # This provides more specific type feedback to the user/developer.
            msg = f"Unexpected type for 'sql' argument: {type(sql)}. Expected str, sqlglot.Expression, or SQLStatement."
            raise TypeError(msg)

        # super()._process_sql_params() (which calls SQLStatement.process())
        # should now return SQL string with psycopg-compatible placeholders (%s or %(name)s)
        # and the corresponding parameters list/dict because SQLStatement now handles this.
        processed_sql_str, processed_params = super()._process_sql_params(
            sql_input_for_super, parameters, *filters, **kwargs
        )

        # No further transformation of placeholders should be needed here for psycopg.
        logger.debug(f"Psycopg SQL (from SQLStatement): {processed_sql_str}, Params: {processed_params}")
        return processed_sql_str, processed_params


class PsycopgSyncDriver(
    PsycopgDriverBase[PsycopgSyncConnection],
    SyncDriverAdapterProtocol[PsycopgSyncConnection],
):
    """Psycopg Sync Driver Adapter."""

    connection: PsycopgSyncConnection

    def __init__(self, connection: PsycopgSyncConnection, **kwargs: Any) -> None:
        super().__init__(connection=connection)
        self.connection = connection

    @staticmethod
    @contextmanager
    def _with_cursor(connection: PsycopgSyncConnection) -> "Generator[Any, None, None]":
        cursor = connection.cursor(row_factory=dict_row)
        try:
            yield cursor
        finally:
            cursor.close()

    def execute(
        self,
        sql: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> StatementResultType:
        conn: PsycopgSyncConnection = self._connection(connection)
        pyformat_sql, psycopg_params = self._process_sql_params(sql, parameters, *filters, **kwargs)

        cursor = conn.cursor(row_factory=dict_row)
        try:
            logger.debug("Executing SQL (Psycopg Sync): %s with params: %s", pyformat_sql, psycopg_params)
            cursor.execute(psycopg_sql.SQL(pyformat_sql), psycopg_params)  # type: ignore[arg-type]

            if cursor.description:
                fetched_data: list[dict[str, Any]] = cursor.fetchall()
                column_names = [col.name for col in cursor.description]
                return SelectResult(
                    raw_result=fetched_data,
                    rows=fetched_data,
                    column_names=column_names,
                    rows_affected=cursor.rowcount if cursor.rowcount != -1 else len(fetched_data),
                    metadata={"dialect": self.dialect, "status_message": cursor.statusmessage},
                )
            return ExecuteResult(
                raw_result=None,
                rows_affected=cursor.rowcount,
                metadata={"dialect": self.dialect, "status_message": cursor.statusmessage},
            )
        finally:
            cursor.close()

    def execute_many(
        self,
        sql: "Statement",
        parameters: Optional[Sequence[StatementParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> StatementResultType:
        conn: PsycopgSyncConnection = self._connection(connection)

        if not parameters:
            logger.debug("execute_many called with no parameters for SQL: %s", sql)
            pyformat_sql_template, _ = self._process_sql_params(sql, None, *filters, **kwargs)
            logger.debug("Validated empty-parameter SQL for execute_many (Psycopg Sync): %s", pyformat_sql_template)
            return ExecuteResult(raw_result=None, rows_affected=0, metadata={"dialect": self.dialect})

        pyformat_sql_template, _ = self._process_sql_params(sql, parameters[0], *filters, **kwargs)
        adapted_parameters_sequence: list[Union[list[Any], dict[str, Any]]] = []
        for param_set in parameters:
            _, adapted_params = self._process_sql_params(sql, param_set, *filters, **kwargs)
            adapted_parameters_sequence.append(adapted_params)
        affected_rows: int = -1
        status_message: Optional[str] = None
        cursor = conn.cursor()
        try:
            cursor.executemany(psycopg_sql.SQL(pyformat_sql_template), adapted_parameters_sequence)  # type: ignore[arg-type]
            affected_rows = cursor.rowcount
            status_message = cursor.statusmessage
        finally:
            cursor.close()

        return ExecuteResult(
            raw_result=None,
            rows_affected=affected_rows,
            metadata={"dialect": self.dialect, "status_message": status_message},
        )

    def execute_script(
        self,
        sql: "Statement",
        parameters: Optional[StatementParameterType] = None,
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> str:
        conn: PsycopgSyncConnection = self._connection(connection)
        pyformat_sql, psycopg_params = self._process_sql_params(sql, parameters, **kwargs)

        logger.debug("Executing script (Psycopg Sync): %s with params: %s", pyformat_sql, psycopg_params)

        status_message_agg: list[str] = []
        cursor = conn.cursor(row_factory=dict_row)
        try:
            cursor.execute(psycopg_sql.SQL(cast("LiteralString", pyformat_sql)), psycopg_params)
            current_status = cursor.statusmessage
            while current_status:
                status_message_agg.append(current_status)
                current_status = cursor.statusmessage if cursor.nextset() else None
        finally:
            cursor.close()

        return "; ".join(s for s in status_message_agg if s) if status_message_agg else "DONE"


class PsycopgAsyncDriver(
    PsycopgDriverBase[PsycopgAsyncConnection],
    AsyncDriverAdapterProtocol[PsycopgAsyncConnection],
):
    """Psycopg Async Driver Adapter."""

    connection: PsycopgAsyncConnection

    def __init__(self, connection: PsycopgAsyncConnection) -> None:
        super().__init__(connection=connection)
        self.connection = connection

    @staticmethod
    @asynccontextmanager
    async def _with_cursor(connection: PsycopgAsyncConnection) -> "AsyncGenerator[Any, None]":
        cursor = connection.cursor(row_factory=dict_row)
        try:
            yield cursor
        finally:
            await cursor.close()

    async def execute(
        self,
        sql: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> StatementResultType:
        connection = self._connection(connection)
        sql, binds = self._process_sql_params(sql, parameters, *filters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, binds)  # type: ignore[arg-type]

            if cursor.description:
                fetched_data: list[dict[str, Any]] = await cursor.fetchall()
                column_names = [col.name for col in cursor.description]
                return SelectResult(
                    raw_result=fetched_data,
                    rows=fetched_data,
                    column_names=column_names,
                    rows_affected=cursor.rowcount if cursor.rowcount != -1 else len(fetched_data),
                    metadata={"dialect": self.dialect, "status_message": cursor.statusmessage},
                )
            return ExecuteResult(
                raw_result=None,
                rows_affected=cursor.rowcount,
                metadata={"dialect": self.dialect, "status_message": cursor.statusmessage},
            )

    async def execute_many(
        self,
        sql: "Statement",
        parameters: Optional[Sequence[StatementParameterType]] = None,
        *filters: "StatementFilter",
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> StatementResultType:
        conn: PsycopgAsyncConnection = self._connection(connection)

        if not parameters:
            logger.debug("execute_many called with no parameters for SQL: %s", sql)
            pyformat_sql_template, _ = self._process_sql_params(sql, None, *filters, **kwargs)
            logger.debug("Validated empty-parameter SQL for execute_many (Psycopg Async): %s", pyformat_sql_template)
            return ExecuteResult(raw_result=None, rows_affected=0, metadata={"dialect": self.dialect})

        pyformat_sql_template, _ = self._process_sql_params(sql, parameters[0], *filters, **kwargs)

        adapted_parameters_sequence: list[Union[list[Any], dict[str, Any]]] = []
        for param_set in parameters:
            _, adapted_params = self._process_sql_params(sql, param_set, *filters, **kwargs)
            adapted_parameters_sequence.append(adapted_params)
        affected_rows: int = -1
        status_message: Optional[str] = None
        async with self._with_cursor(conn) as cursor:
            await cursor.executemany(psycopg_sql.SQL(pyformat_sql_template), adapted_parameters_sequence)  # type: ignore[arg-type]
            affected_rows = cursor.rowcount
            status_message = cursor.statusmessage

        return ExecuteResult(
            raw_result=None,
            rows_affected=affected_rows,
            metadata={"dialect": self.dialect, "status_message": status_message},
        )

    async def execute_script(
        self,
        sql: "Statement",
        parameters: Optional[StatementParameterType] = None,
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> str:
        conn: PsycopgAsyncConnection = self._connection(connection)
        pyformat_sql, psycopg_params = self._process_sql_params(sql, parameters, **kwargs)

        logger.debug("Executing script (Psycopg Async): %s with params: %s", pyformat_sql, psycopg_params)
        status_message_agg: list[str] = []
        async with self._with_cursor(conn) as cursor:
            await cursor.execute(psycopg_sql.SQL(pyformat_sql), psycopg_params)  # type: ignore[arg-type]
            current_status = cursor.statusmessage
            while current_status:
                status_message_agg.append(current_status)
                break

        return "; ".join(s for s in status_message_agg if s) if status_message_agg else "DONE"
