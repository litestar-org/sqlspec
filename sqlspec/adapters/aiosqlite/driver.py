import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

import aiosqlite

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.exceptions import RiskLevel
from sqlspec.sql.result import ExecuteResult, SelectResult

if TYPE_CHECKING:
    from sqlspec.exceptions import RiskLevel
    from sqlspec.sql.filters import StatementFilter
    from sqlspec.sql.statement import Statement
    from sqlspec.typing import StatementParameterType

__all__ = ("AiosqliteConnection", "AiosqliteDriver")
AiosqliteConnection = aiosqlite.Connection

logger = logging.getLogger("sqlspec")


class AiosqliteDriver(AsyncDriverAdapterProtocol[AiosqliteConnection]):
    """SQLite Async Driver Adapter."""

    dialect: str = "sqlite"

    def __init__(self, connection: AiosqliteConnection, **kwargs: Any) -> None:
        super().__init__(connection, **kwargs)

    async def execute(
        self,
        sql: "Statement",
        parameters: Optional["StatementParameterType"] = None,
        *filters: "StatementFilter",
        connection: Optional[AiosqliteConnection] = None,
        **kwargs: Any,
    ) -> "Union[SelectResult[Any], ExecuteResult[Any]]":
        conn: AiosqliteConnection = self._connection(connection)
        conn.row_factory = aiosqlite.Row

        processed_sql, processed_params, _ = self._process_sql_params(sql, parameters, *filters, **kwargs)
        db_params = processed_params if processed_params is not None else []

        cursor: Optional[aiosqlite.Cursor] = None
        try:
            cursor = await conn.cursor()
            logger.debug("Executing SQL (aiosqlite): %s with params: %s", processed_sql, db_params)
            await cursor.execute(processed_sql, db_params)

            if cursor.description:
                raw_rows: list[aiosqlite.Row] = await cursor.fetchall()  # type: ignore[assignment]
                column_names = [d[0] for d in cursor.description]
                dict_rows = [dict(row) for row in raw_rows]
                return SelectResult(
                    raw_result=raw_rows,
                    rows=dict_rows,
                    column_names=column_names,
                    rows_affected=len(dict_rows),
                    metadata={"dialect": self.dialect},
                )
            affected_rows = cursor.rowcount
            last_id = cursor.lastrowid
            await conn.commit()
            return ExecuteResult(
                raw_result=None,
                rows_affected=affected_rows,
                last_inserted_id=last_id,
                metadata={"dialect": self.dialect},
            )
        finally:
            if cursor:
                await cursor.close()

    async def execute_many(
        self,
        sql: "Statement",
        parameters: Optional[Sequence["StatementParameterType"]] = None,
        *filters: "StatementFilter",
        connection: Optional[AiosqliteConnection] = None,
        **kwargs: Any,
    ) -> "ExecuteResult[Any]":
        conn: AiosqliteConnection = self._connection(connection)

        if not parameters:
            logger.debug("execute_many called with no parameters for SQL: %s", sql)
            processed_sql_template, _, _ = self._process_sql_params(sql, None, *filters, **kwargs)
            logger.debug("Validated empty-parameter SQL for execute_many (aiosqlite): %s", processed_sql_template)
            return ExecuteResult(raw_result=None, rows_affected=0, metadata={"dialect": self.dialect})

        processed_sql_template, first_adapted_params, _ = self._process_sql_params(
            sql, parameters[0], *filters, **kwargs
        )

        adapted_parameters_sequence: list[Union[list[Any], dict[str, Any]]] = []

        def adapt_param_set(params_for_adapter: Any) -> Union[list[Any], dict[str, Any]]:
            if params_for_adapter is None:
                return []
            return params_for_adapter

        adapted_parameters_sequence.append(adapt_param_set(first_adapted_params))
        for i, param_set in enumerate(parameters):
            if i == 0:
                continue
            _, adapted_params_for_set, _ = self._process_sql_params(sql, param_set, *filters, **kwargs)
            adapted_parameters_sequence.append(adapt_param_set(adapted_params_for_set))

        logger.debug(
            "Executing SQL (aiosqlite) many: %s with %s param sets.",
            processed_sql_template,
            len(adapted_parameters_sequence),
        )

        affected_rows: int = -1
        cursor: Optional[aiosqlite.Cursor] = None
        try:
            cursor = await conn.cursor()
            await cursor.executemany(processed_sql_template, adapted_parameters_sequence)
            affected_rows = cursor.rowcount
            await conn.commit()
        finally:
            if cursor:
                await cursor.close()

        return ExecuteResult(raw_result=None, rows_affected=affected_rows, metadata={"dialect": self.dialect})

    async def execute_script(
        self,
        sql: "Statement",
        parameters: Optional["StatementParameterType"] = None,
        connection: Optional["AiosqliteConnection"] = None,
        risk_level: Optional["RiskLevel"] = None,
        **kwargs: Any,
    ) -> str:
        conn: AiosqliteConnection = self._connection(connection)

        processed_sql, processed_params, _ = self._process_sql_params(sql, parameters, **kwargs)

        if (
            processed_params
            and not (isinstance(processed_params, (list, tuple)) and not processed_params)
            and not (isinstance(processed_params, dict) and not processed_params)
        ):
            logger.warning(
                "aiosqlite.execute_script: Parameters were processed but aiosqlite.executescript does not use them. SQL: %s",
                processed_sql,
            )

        logger.debug("Executing script (aiosqlite): %s", processed_sql)
        cursor: Optional[aiosqlite.Cursor] = None
        try:
            cursor = await conn.cursor()
            await cursor.executescript(processed_sql)
            await conn.commit()
        finally:
            if cursor:
                await cursor.close()
        return "DONE"
