import logging
import re
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

import aiosqlite

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin
from sqlspec.statement import PARAM_REGEX
from sqlspec.utils.text import bind_parameters

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("AiosqliteConnection", "AiosqliteDriver")
AiosqliteConnection = aiosqlite.Connection

logger = logging.getLogger("sqlspec")

# Regex to find '?' placeholders, skipping those inside quotes or SQL comments
QMARK_REGEX = re.compile(
    r"""(?P<dquote>"[^"]*") | # Double-quoted strings
         (?P<squote>'[^']*') | # Single-quoted strings
         (?P<comment>--[^\n]*|/\*.*?\*/) | # SQL comments (single/multi-line)
         (?P<qmark>\?) # The question mark placeholder
      """,
    re.VERBOSE | re.DOTALL,
)


class AiosqliteDriver(
    SQLTranslatorMixin["AiosqliteConnection"],
    AsyncDriverAdapterProtocol["AiosqliteConnection"],
):
    """SQLite Async Driver Adapter."""

    connection: "AiosqliteConnection"
    dialect: str = "sqlite"

    def __init__(self, connection: "AiosqliteConnection") -> None:
        self.connection = connection

    @staticmethod
    async def _cursor(connection: "AiosqliteConnection", *args: Any, **kwargs: Any) -> "aiosqlite.Cursor":
        return await connection.cursor(*args, **kwargs)

    @asynccontextmanager
    async def _with_cursor(self, connection: "AiosqliteConnection") -> "AsyncGenerator[aiosqlite.Cursor, None]":
        cursor = await self._cursor(connection)
        try:
            yield cursor
        finally:
            await cursor.close()

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        # 1. Merge parameters and kwargs
        merged_params: Optional[Union[dict[str, Any], list[Any], tuple[Any, ...]]] = None

        if kwargs:
            if isinstance(parameters, dict):
                merged_params = {**parameters, **kwargs}
            elif parameters is not None:
                msg = "Cannot mix positional parameters with keyword arguments for aiosqlite driver."
                raise ParameterStyleMismatchError(msg)
            else:
                merged_params = kwargs
        elif parameters is not None:
            merged_params = parameters  # type: ignore

        # Use bind_parameters for named parameters
        if isinstance(merged_params, dict):
            final_sql, final_params = bind_parameters(sql, merged_params, dialect="sqlite")
            return final_sql, final_params

        # Case 2: Sequence parameters - pass through
        if isinstance(merged_params, (list, tuple)):
            return sql, merged_params
        # Case 3: Scalar parameter - wrap in tuple
        if merged_params is not None:
            return sql, (merged_params,)

        # Case 0: No parameters provided
        # Basic validation for placeholders
        has_placeholders = False
        for match in PARAM_REGEX.finditer(sql):
            if not (match.group("dquote") or match.group("squote") or match.group("comment")) and match.group(
                "var_name"
            ):
                has_placeholders = True
                break
        if not has_placeholders:
            # Check for ? style placeholders
            for match in re.finditer(
                r"(\"(?:[^\"]|\"\")*\")|(\'(?:[^\']|\'\')*\')|(--.*?\n)|(\/\*.*?\*\/)|(\?)", sql, re.DOTALL
            ):
                if match.group(5):
                    has_placeholders = True
                    break

        if has_placeholders:
            msg = f"aiosqlite: SQL contains parameter placeholders, but no parameters were provided. SQL: {sql}"
            raise SQLParsingError(msg)
        return sql, None

    # --- Public API Methods --- #
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AiosqliteConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        results = await cursor.fetchall()
        if not results:
            return []

        # Get column names
        column_names = [column[0] for column in cursor.description]

        if schema_type is None:
            return [dict(zip(column_names, row)) for row in results]
        return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AiosqliteConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        result = self.check_not_found(result)

        # Get column names
        column_names = [column[0] for column in cursor.description]

        if schema_type is None:
            return dict(zip(column_names, result))
        return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AiosqliteConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        if result is None:
            return None

        # Get column names
        column_names = [column[0] for column in cursor.description]

        if schema_type is None:
            return dict(zip(column_names, result))
        return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AiosqliteConnection"] = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        result = self.check_not_found(result)

        # Return first value from the row
        result_value = result[0]
        if schema_type is None:
            return result_value
        return cast("T", schema_type(result_value))  # type: ignore[call-arg]

    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AiosqliteConnection"] = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        if result is None:
            return None

        # Return first value from the row
        result_value = result[0]
        if schema_type is None:
            return result_value
        return cast("T", schema_type(result_value))  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AiosqliteConnection"] = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        await connection.commit()
        return cursor.rowcount

    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AiosqliteConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        await connection.commit()
        result = await cursor.fetchone()
        result = self.check_not_found(result)

        # Get column names
        column_names = [column[0] for column in cursor.description]

        if schema_type is None:
            return dict(zip(column_names, result))
        return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    async def execute_script(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AiosqliteConnection"] = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the script
        await connection.executescript(sql)
        await connection.commit()
        return "Script executed successfully."

    def _connection(self, connection: Optional["AiosqliteConnection"] = None) -> "AiosqliteConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
