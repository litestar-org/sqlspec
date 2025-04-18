from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from psycopg.rows import dict_row

from sqlspec.base import PARAM_REGEX, AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from psycopg import AsyncConnection, Connection

    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("PsycopgAsyncDriver", "PsycopgSyncDriver")


class PsycopgSyncDriver(SyncDriverAdapterProtocol["Connection"]):
    """Psycopg Sync Driver Adapter."""

    connection: "Connection"
    param_style: str = "%s"

    def __init__(self, connection: "Connection") -> None:
        self.connection = connection

    @staticmethod
    @contextmanager
    def _with_cursor(connection: "Connection") -> "Generator[Any, None, None]":
        cursor = connection.cursor(row_factory=dict_row)
        try:
            yield cursor
        finally:
            cursor.close()

    def _process_sql_params(
        self, sql: str, parameters: "Optional[StatementParameterType]" = None
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL query and parameters for DB-API execution.

        Converts named parameters (:name) to positional parameters (%s)
        if the input parameters are a dictionary.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).

        Returns:
            A tuple containing the processed SQL string and the processed parameters
            (always a tuple or None if the input was a dictionary, otherwise the original type).

        Raises:
            ValueError: If a named parameter in the SQL is not found in the dictionary
                        or if a parameter in the dictionary is not used in the SQL.
        """
        if not isinstance(parameters, dict) or not parameters:
            # If parameters are not a dict, or empty dict, assume positional/no params
            # Let the underlying driver handle tuples/lists directly
            return sql, parameters

        processed_sql = ""
        processed_params_list: list[Any] = []
        last_end = 0
        found_params: set[str] = set()

        for match in PARAM_REGEX.finditer(sql):
            if match.group("dquote") is not None or match.group("squote") is not None:
                # Skip placeholders within quotes
                continue

            var_name = match.group("var_name")
            if var_name is None:  # Should not happen with the regex, but safeguard
                continue

            if var_name not in parameters:
                msg = f"Named parameter ':{var_name}' found in SQL but not provided in parameters dictionary."
                raise ValueError(msg)

            # Append segment before the placeholder + the driver's positional placeholder
            processed_sql += sql[last_end : match.start("var_name") - 1] + "%s"
            processed_params_list.append(parameters[var_name])
            found_params.add(var_name)
            last_end = match.end("var_name")

        # Append the rest of the SQL string
        processed_sql += sql[last_end:]

        # Check if all provided parameters were used
        unused_params = set(parameters.keys()) - found_params
        if unused_params:
            msg = f"Parameters provided but not found in SQL: {unused_params}"
            # Depending on desired strictness, this could be a warning or an error
            # For now, let's raise an error for clarity
            raise ValueError(msg)

        return processed_sql, tuple(processed_params_list)

    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            results = cursor.fetchall()
            if not results:
                return []

            if schema_type is not None:
                return [cast("ModelDTOT", schema_type(**row)) for row in results]  # pyright: ignore[reportUnknownArgumentType]
            return [cast("dict[str,Any]", row) for row in results]  # pyright: ignore[reportUnknownArgumentType]

    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            row = cursor.fetchone()
            row = self.check_not_found(row)
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**cast("dict[str,Any]", row)))
            return cast("dict[str,Any]", row)

    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            row = cursor.fetchone()
            if row is None:
                return None
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**cast("dict[str,Any]", row)))
            return cast("dict[str,Any]", row)

    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            row = cursor.fetchone()
            row = self.check_not_found(row)
            val = next(iter(row))
            if schema_type is not None:
                return schema_type(val)  # type: ignore[call-arg]
            return val

    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            row = cursor.fetchone()
            if row is None:
                return None
            val = next(iter(row))
            if schema_type is not None:
                return schema_type(val)  # type: ignore[call-arg]
            return val

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            return cursor.rowcount if hasattr(cursor, "rowcount") else -1

    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            result = cursor.fetchone()

            if result is None:
                return None

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**result))  # pyright: ignore[reportUnknownArgumentType]
            return cast("dict[str, Any]", result)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            return str(cursor.rowcount)

    def execute_script_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Execute a script and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            result = cursor.fetchone()

            if result is None:
                return None

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**result))  # pyright: ignore[reportUnknownArgumentType]
            return cast("dict[str, Any]", result)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]


class PsycopgAsyncDriver(AsyncDriverAdapterProtocol["AsyncConnection"]):
    """Psycopg Async Driver Adapter."""

    connection: "AsyncConnection"
    param_style: str = "%s"

    def __init__(self, connection: "AsyncConnection") -> None:
        self.connection = connection

    @staticmethod
    @asynccontextmanager
    async def _with_cursor(connection: "AsyncConnection") -> "AsyncGenerator[Any, None]":
        cursor = connection.cursor(row_factory=dict_row)
        try:
            yield cursor
        finally:
            await cursor.close()

    def _process_sql_params(
        self, sql: str, parameters: "Optional[StatementParameterType]" = None
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL query and parameters for DB-API execution.

        Converts named parameters (:name) to positional parameters (%s)
        if the input parameters are a dictionary.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).

        Returns:
            A tuple containing the processed SQL string and the processed parameters
            (always a tuple or None if the input was a dictionary, otherwise the original type).

        Raises:
            ValueError: If a named parameter in the SQL is not found in the dictionary
                        or if a parameter in the dictionary is not used in the SQL.
        """
        if not isinstance(parameters, dict) or not parameters:
            # If parameters are not a dict, or empty dict, assume positional/no params
            # Let the underlying driver handle tuples/lists directly
            return sql, parameters

        processed_sql = ""
        processed_params_list: list[Any] = []
        last_end = 0
        found_params: set[str] = set()

        for match in PARAM_REGEX.finditer(sql):
            if match.group("dquote") is not None or match.group("squote") is not None:
                # Skip placeholders within quotes
                continue

            var_name = match.group("var_name")
            if var_name is None:  # Should not happen with the regex, but safeguard
                continue

            if var_name not in parameters:
                msg = f"Named parameter ':{var_name}' found in SQL but not provided in parameters dictionary."
                raise ValueError(msg)

            # Append segment before the placeholder + the driver's positional placeholder
            processed_sql += sql[last_end : match.start("var_name") - 1] + "%s"
            processed_params_list.append(parameters[var_name])
            found_params.add(var_name)
            last_end = match.end("var_name")

        # Append the rest of the SQL string
        processed_sql += sql[last_end:]

        # Check if all provided parameters were used
        unused_params = set(parameters.keys()) - found_params
        if unused_params:
            msg = f"Parameters provided but not found in SQL: {unused_params}"
            # Depending on desired strictness, this could be a warning or an error
            # For now, let's raise an error for clarity
            raise ValueError(msg)

        return processed_sql, tuple(processed_params_list)

    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        results: list[Union[ModelDTOT, dict[str, Any]]] = []

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            results = await cursor.fetchall()
            if not results:
                return []
            if schema_type is not None:
                return [cast("ModelDTOT", schema_type(**cast("dict[str,Any]", row))) for row in results]  # pyright: ignore[reportUnknownArgumentType]
            return [cast("dict[str,Any]", row) for row in results]  # pyright: ignore[reportUnknownArgumentType]

    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            row = await cursor.fetchone()
            row = self.check_not_found(row)
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**cast("dict[str,Any]", row)))
            return cast("dict[str,Any]", row)

    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            row = await cursor.fetchone()
            if row is None:
                return None
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**cast("dict[str,Any]", row)))
            return cast("dict[str,Any]", row)

    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            row = await cursor.fetchone()
            row = self.check_not_found(row)
            val = next(iter(row))
            if schema_type is not None:
                return schema_type(val)  # type: ignore[call-arg]
            return val

    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            row = await cursor.fetchone()
            if row is None:
                return None
            val = next(iter(row))
            if schema_type is not None:
                return schema_type(val)  # type: ignore[call-arg]
            return val

    async def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            try:
                rowcount = int(cursor.rowcount)
            except (TypeError, ValueError):
                rowcount = -1
            return rowcount

    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()

            if result is None:
                return None

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**result))  # pyright: ignore[reportUnknownArgumentType]
            return cast("dict[str, Any]", result)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            return str(cursor.rowcount)

    async def execute_script_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Execute a script and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()

            if result is None:
                return None

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**result))  # pyright: ignore[reportUnknownArgumentType]
            return cast("dict[str, Any]", result)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
