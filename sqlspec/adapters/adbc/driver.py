import contextlib
import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.base import SyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("AdbcDriver",)


# Regex to find :param or %(param)s style placeholders, skipping those inside quotes
PARAM_REGEX = re.compile(
    r"""
    (?P<dquote>"([^"]|\\")*") | # Double-quoted strings
    (?P<squote>'([^']|\\')*') | # Single-quoted strings
    : (?P<var_name_colon>[a-zA-Z_][a-zA-Z0-9_]*) | # :var_name
    % \( (?P<var_name_perc>[a-zA-Z_][a-zA-Z0-9_]*) \) s # %(var_name)s
    """,
    re.VERBOSE,
)


class AdbcDriver(SyncDriverAdapterProtocol["Connection"]):
    """ADBC Sync Driver Adapter."""

    connection: Connection

    def __init__(self, connection: "Connection") -> None:
        """Initialize the ADBC driver adapter."""
        self.connection = connection
        # Potentially introspect connection.paramstyle here if needed in the future
        # For now, assume 'qmark' based on typical ADBC DBAPI behavior

    @staticmethod
    def _cursor(connection: "Connection", *args: Any, **kwargs: Any) -> "Cursor":
        return connection.cursor(*args, **kwargs)

    @contextmanager
    def _with_cursor(self, connection: "Connection") -> Generator["Cursor", None, None]:
        cursor = self._cursor(connection)
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()  # type: ignore[no-untyped-call]

    def _process_sql_params(
        self, sql: str, parameters: "Optional[StatementParameterType]" = None
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL query and parameters for DB-API execution.

        Converts named parameters (:name or %(name)s) to positional parameters specified by `self.param_style`
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
            return self._process_sql_statement(sql), parameters

        processed_sql = ""
        processed_params_list: list[Any] = []
        last_end = 0
        found_params: set[str] = set()

        for match in PARAM_REGEX.finditer(sql):
            if match.group("dquote") is not None or match.group("squote") is not None:
                # Skip placeholders within quotes
                continue

            # Get name from whichever group matched
            var_name = match.group("var_name_colon") or match.group("var_name_perc")

            if var_name is None:  # Should not happen with the new regex structure
                continue

            if var_name not in parameters:
                placeholder = match.group(0)  # Get the full matched placeholder
                msg = f"Named parameter '{placeholder}' found in SQL but not provided in parameters dictionary."
                raise ValueError(msg)

            # Append segment before the placeholder
            processed_sql += sql[last_end : match.start()]
            # Append the driver's positional placeholder
            processed_sql += self.param_style
            processed_params_list.append(parameters[var_name])
            found_params.add(var_name)
            last_end = match.end()

        # Append the rest of the SQL string
        processed_sql += sql[last_end:]

        # Check if all provided parameters were used
        unused_params = set(parameters.keys()) - found_params
        if unused_params:
            msg = f"Parameters provided but not found in SQL: {unused_params}"
            # Depending on desired strictness, this could be a warning or an error
            # For now, let's raise an error for clarity
            raise ValueError(msg)

        return self._process_sql_statement(processed_sql), tuple(processed_params_list)

    def select(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            results = cursor.fetchall()  # pyright: ignore
            if not results:
                return []

            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
            return [dict(zip(column_names, row)) for row in results]  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    def select_one(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportUnknownArgumentType]
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is None:
                return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            return schema_type(**dict(zip(column_names, result)))  # type: ignore[return-value]

    def select_one_or_none(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is None:
                return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            return schema_type(**dict(zip(column_names, result)))  # type: ignore[return-value]

    def select_value(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportUnknownArgumentType]
            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownVariableType]
            return schema_type(result[0])  # type: ignore[call-arg]

    def select_value_or_none(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if result is None:
                return None
            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownVariableType]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount if hasattr(cursor, "rowcount") else -1

    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        column_names: list[str] = []

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if len(result) == 0:  # pyright: ignore[reportUnknownArgumentType]
                return None
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result[0]))))  # pyright: ignore[reportUnknownArgumentType]
            return dict(zip(column_names, result[0]))  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]

    def execute_script(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cast("str", cursor.statusmessage) if hasattr(cursor, "statusmessage") else "DONE"  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]

    def execute_script_returning(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Execute a script and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        column_names: list[str] = []

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if len(result) == 0:  # pyright: ignore[reportUnknownArgumentType]
                return None
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result[0]))))  # pyright: ignore[reportUnknownArgumentType]
            return dict(zip(column_names, result[0]))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
