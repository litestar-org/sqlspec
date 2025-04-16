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
    results_as_dict: bool = True

    def __init__(self, connection: "DuckDBPyConnection", use_cursor: bool = True, results_as_dict: bool = True) -> None:
        self.connection = connection
        self.use_cursor = use_cursor
        self.results_as_dict = results_as_dict

    def _cursor(self, connection: "DuckDBPyConnection") -> "DuckDBPyConnection":
        if self.use_cursor:
            return connection.cursor()
        return connection

    @contextmanager
    def _with_cursor(self, connection: "DuckDBPyConnection") -> "Generator[DuckDBPyConnection, None, None]":
        cursor = self._cursor(connection)
        try:
            yield cursor
        finally:
            if self.use_cursor:
                cursor.close()

    def select(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[DuckDBPyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Generator[Union[ModelDTOT, dict[str, Any]], None, None]":
        """Fetch data from the database.

        Yields:
            Row data as either model instances or dictionaries.
        """
        column_names: list[str] = []
        connection = connection if connection is not None else self.connection
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)

            if schema_type is None:
                first = True
                for row in cursor.fetchall():
                    if first:  # get column names on the fly
                        column_names = [c[0] for c in cursor.description or []]
                        first = False
                    if self.results_as_dict:  # pragma: no cover
                        # strict=False: requires 3.10
                        yield dict(zip(column_names, row))
                    else:
                        yield row
            else:  # pragma: no cover
                first = True
                for row in cursor.fetchall():
                    if first:
                        column_names = [c[0] for c in cursor.description or []]
                        first = False
                    yield cast("ModelDTOT", dict(zip(column_names, row)))

    def select_one(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[DuckDBPyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        column_names: list[str] = []
        connection = connection if connection is not None else self.connection
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            # DuckDB's fetchone returns a tuple of values or None
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if result is None:
                return None
            if schema_type is None and self.results_as_dict:
                column_names = [c[0] for c in cursor.description or []]
                return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
            if schema_type is not None:
                column_names = [c[0] for c in cursor.description or []]
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            return result  # pyright: ignore[reportUnknownReturnType, reportUnknownVariableType]

    def select_value(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[DuckDBPyConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = connection if connection is not None else self.connection
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            # DuckDB's fetchone returns a tuple of values or None
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if result is None:
                return None
            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownReturnType, reportUnknownVariableType]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[DuckDBPyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[int, Any, ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Insert, update, or delete data from the database.

        Returns:
            Row count if not returning data, otherwise the first row of results.
        """
        column_names: list[str] = []
        connection = connection if connection is not None else self.connection
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            if returning is False:
                return cursor.rowcount if hasattr(cursor, "rowcount") else -1
            result = cursor.fetchall()
            if len(result) == 0:
                return None
            if schema_type:
                column_names = [c[0] for c in cursor.description or []]
                return schema_type(**dict(zip(column_names, result[0])))
            if self.results_as_dict:
                column_names = [c[0] for c in cursor.description or []]
                return dict(zip(column_names, result[0]))
            return result[0]

    def execute_script(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[DuckDBPyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[Any, ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Execute a script.

        Returns:
            The number of rows affected by the script.
        """
        column_names: list[str] = []
        connection = connection if connection is not None else self.connection
        with self._with_cursor(connection) as cursor:
            if returning is False:
                cursor.execute(sql, parameters)
                # DuckDB doesn't have a statusmessage attribute, so we return a default value
                return "DONE"
            result = cursor.fetchall()
            if len(result) == 0:
                return None
            if schema_type:
                column_names = [c[0] for c in cursor.description or []]
                return schema_type(**dict(zip(column_names, result[0])))
            if self.results_as_dict:
                column_names = [c[0] for c in cursor.description or []]
                return dict(zip(column_names, result[0]))
            return result[0]
