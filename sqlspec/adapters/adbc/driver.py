from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.base import SyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("AdbcDriver",)


class AdbcDriver(SyncDriverAdapterProtocol["Connection"]):
    """ADBC Sync Driver Adapter."""

    connection: Connection
    results_as_dict: bool = True

    def __init__(self, connection: "Connection", results_as_dict: bool = True) -> None:
        self.connection = connection
        self.results_as_dict = results_as_dict

    @staticmethod
    def _cursor(connection: "Connection", *args: Any, **kwargs: Any) -> "Cursor":
        return connection.cursor(*args, **kwargs)

    @contextmanager
    def _with_cursor(self, connection: "Connection") -> Generator["Cursor", None, None]:
        cursor = self._cursor(connection)
        try:
            yield cursor
        finally:
            cursor.close()  # type: ignore[no-untyped-call]

    def select(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Generator[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]], None, None]":
        """Fetch data from the database.

        Yields:
            Row data as either model instances or dictionaries.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        column_names: list[str] = []
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]

            if schema_type is None:
                first = True
                for row in cursor.fetchall():  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                    if first:  # get column names on the fly
                        column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                        first = False
                    if self.results_as_dict:  # pragma: no cover
                        # strict=False: requires 3.10
                        yield dict(zip(column_names, row))  # pyright: ignore[reportUnknownArgumentType]
                    else:
                        yield row
            else:  # pragma: no cover
                first = True
                for row in cursor.fetchall():  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                    if first:
                        column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                        first = False
                    yield cast("ModelDTOT", schema_type(**dict(zip(column_names, row))))  # pyright: ignore[reportUnknownArgumentType]

    def select_one(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        column_names: list[str] = []
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if result is None:
                return None
            if schema_type is None and self.results_as_dict:
                column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            if schema_type is not None:
                column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                return schema_type(**dict(zip(column_names, result)))  # type: ignore[return-value]
            return result  # pyright: ignore[reportUnknownVariableType]

    def select_value(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
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
        parameters: "StatementParameterType",
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[int, Any,ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Insert, update, or delete data from the database.

        Returns:
            Row count if not returning data, otherwise the first row of results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        column_names: list[str] = []
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            if returning is False:
                return cursor.rowcount if hasattr(cursor, "rowcount") else -1
            result = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if len(result) == 0:  # pyright: ignore[reportUnknownArgumentType]
                return None
            if schema_type:
                column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                return schema_type(**dict(zip(column_names, result[0])))  # pyright: ignore[reportUnknownArgumentType]
            if self.results_as_dict:
                column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                return dict(zip(column_names, result[0]))  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
            return result[0]  # pyright: ignore[reportUnknownVariableType]

    def execute_script(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[Any,ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Execute a script.

        Returns:
            The number of rows affected by the script.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        column_names: list[str] = []
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            if returning is False:
                return cast("str", cursor.statusmessage) if hasattr(cursor, "statusmessage") else "DONE"  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]
            result = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if len(result) == 0:  # pyright: ignore[reportUnknownArgumentType]
                return None
            if schema_type:
                column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                return schema_type(**dict(zip(column_names, result[0])))  # pyright: ignore[reportUnknownArgumentType]
            if self.results_as_dict:
                column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                return dict(zip(column_names, result[0]))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            return result[0]  # pyright: ignore[reportUnknownVariableType]
