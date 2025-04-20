from contextlib import contextmanager
from sqlite3 import Connection, Cursor
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.base import SyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("SqliteDriver",)


class SqliteDriver(SyncDriverAdapterProtocol["Connection"]):
    """SQLite Sync Driver Adapter."""

    connection: "Connection"
    dialect: str = "sqlite"

    def __init__(self, connection: "Connection") -> None:
        self.connection = connection

    @staticmethod
    def _cursor(connection: "Connection", *args: Any, **kwargs: Any) -> Cursor:
        return connection.cursor(*args, **kwargs)  # type: ignore[no-any-return]

    @contextmanager
    def _with_cursor(self, connection: "Connection") -> "Generator[Cursor, None, None]":
        cursor = self._cursor(connection)
        try:
            yield cursor
        finally:
            cursor.close()

    def select(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            if not parameters:
                cursor.execute(sql)  # pyright: ignore[reportUnknownMemberType]
            else:
                cursor.execute(sql, parameters)
            results = cursor.fetchall()
            if not results:
                return []
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is not None:
                return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]  # pyright: ignore[reportUnknownArgumentType]
            return [dict(zip(column_names, row)) for row in results]  # pyright: ignore[reportUnknownArgumentType]

    def select_one(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            if not parameters:
                cursor.execute(sql)  # pyright: ignore[reportUnknownMemberType]
            else:
                cursor.execute(sql, parameters)
            result = cursor.fetchone()
            result = self.check_not_found(result)
            column_names = [c[0] for c in cursor.description or []]
            if schema_type is None:
                return dict(zip(column_names, result))
            return schema_type(**dict(zip(column_names, result)))  # type: ignore[return-value]

    def select_one_or_none(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            if not parameters:
                cursor.execute(sql)  # pyright: ignore[reportUnknownMemberType]
            else:
                cursor.execute(sql, parameters)
            result = cursor.fetchone()
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]
            if schema_type is None:
                return dict(zip(column_names, result))
            return schema_type(**dict(zip(column_names, result)))  # type: ignore[return-value]

    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            if not parameters:
                cursor.execute(sql)  # pyright: ignore[reportUnknownMemberType]
            else:
                cursor.execute(sql, parameters)
            result = cursor.fetchone()
            result = self.check_not_found(result)
            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # type: ignore[call-arg]

    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            if not parameters:
                cursor.execute(sql)  # pyright: ignore[reportUnknownMemberType]
            else:
                cursor.execute(sql, parameters)
            result = cursor.fetchone()
            if result is None:
                return None
            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            if not parameters:
                cursor.execute(sql)  # pyright: ignore[reportUnknownMemberType]
            else:
                cursor.execute(sql, parameters)
            return cursor.rowcount if hasattr(cursor, "rowcount") else -1

    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            if not parameters:
                cursor.execute(sql)  # pyright: ignore[reportUnknownMemberType]
            else:
                cursor.execute(sql, parameters)
            result = cursor.fetchall()
            if len(result) == 0:
                return None

            # Get column names from cursor description
            column_names = [c[0] for c in cursor.description or []]

            # Get the first row's values - ensure we're getting the actual values
            row_values = result[0]

            # Debug print to see what we're getting

            # Create dictionary mapping column names to values
            result_dict = {}
            for i, col_name in enumerate(column_names):
                result_dict[col_name] = row_values[i]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**result_dict))
            return result_dict

    def execute_script(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # The _process_sql_params handles parameter formatting for the dialect.
        with self._with_cursor(connection) as cursor:
            if not parameters:
                cursor.execute(sql)  # pyright: ignore[reportUnknownMemberType]
            else:
                cursor.execute(sql, parameters)

        return cast("str", cursor.statusmessage) if hasattr(cursor, "statusmessage") else "DONE"  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]

    def execute_script_returning(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Execute a script and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            if not parameters:
                cursor.execute(sql)  # pyright: ignore[reportUnknownMemberType]
            else:
                cursor.execute(sql, parameters)
            result = cursor.fetchall()
            if len(result) == 0:
                return None
            column_names = [c[0] for c in cursor.description or []]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result[0]))))
            return dict(zip(column_names, result[0]))
