from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.base import SyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from collections.abc import Generator

    from duckdb import DuckDBPyConnection

    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("DuckDBDriver",)


class DuckDBDriver(SyncDriverAdapterProtocol["DuckDBPyConnection"]):
    """DuckDB Sync Driver Adapter."""

    connection: "DuckDBPyConnection"
    use_cursor: bool = True
    # param_style is inherited from CommonDriverAttributes

    def __init__(self, connection: "DuckDBPyConnection", use_cursor: bool = True) -> None:
        self.connection = connection
        self.use_cursor = use_cursor

    # --- Helper Methods --- #
    def _cursor(self, connection: "DuckDBPyConnection") -> "DuckDBPyConnection":
        if self.use_cursor:
            # Ignore lack of type hint on cursor()
            return connection.cursor()
        return connection

    @contextmanager
    def _with_cursor(self, connection: "DuckDBPyConnection") -> "Generator[DuckDBPyConnection, None, None]":
        if self.use_cursor:
            cursor = self._cursor(connection)
            try:
                yield cursor
            finally:
                cursor.close()
        else:
            yield connection  # Yield the connection directly

    # --- Public API Methods (Original Implementation + _process_sql_params) --- #

    def select(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["DuckDBPyConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            results = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not results:
                return []

            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]  # pyright: ignore[reportUnknownArgumentType]
            return [dict(zip(column_names, row)) for row in results]  # pyright: ignore[reportUnknownArgumentType]

    def select_one(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["DuckDBPyConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  #  pyright: ignore

            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    def select_one_or_none(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["DuckDBPyConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if result is None:
                return None

            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[DuckDBPyConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Union[T, Any]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            result = self.check_not_found(result)  #  pyright: ignore
            if schema_type is None:
                return result[0]  # pyright: ignore
            return schema_type(result[0])  # type: ignore[call-arg]

    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[DuckDBPyConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if result is None:
                return None
            if schema_type is None:
                return result[0]  # pyright: ignore
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["DuckDBPyConnection"] = None,
    ) -> int:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return getattr(cursor, "rowcount", -1)  # pyright: ignore[reportUnknownMemberType]

    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["DuckDBPyConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not result:
                return None  # pyright: ignore[reportUnknownArgumentType]
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result[0]))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result[0]))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    def _process_sql_params(
        self, sql: str, parameters: "Optional[StatementParameterType]" = None
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL query and parameters for DB-API execution.

        Converts named parameters (:name) to positional parameters (?) for DuckDB.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).

        Returns:
            A tuple containing the processed SQL string and the processed parameters.
        """
        if not isinstance(parameters, dict) or not parameters:
            # If parameters are not a dict, or empty dict, assume positional/no params
            # Let the underlying driver handle tuples/lists directly
            return sql, parameters

        # Convert named parameters to positional parameters
        processed_sql = sql
        processed_params: list[Any] = []
        for key, value in parameters.items():
            # Replace :key with ? in the SQL
            processed_sql = processed_sql.replace(f":{key}", "?")
            processed_params.append(value)

        return processed_sql, tuple(processed_params)

    def execute_script(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["DuckDBPyConnection"] = None,
    ) -> str:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cast("str", getattr(cursor, "statusmessage", "DONE"))  # pyright: ignore[reportUnknownMemberType]
