from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.base import SyncArrowBulkOperationsMixin, SyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from collections.abc import Generator

    from duckdb import DuckDBPyConnection

    from sqlspec.typing import ArrowTable, ModelDTOT, StatementParameterType

__all__ = ("DuckDBDriver",)


class DuckDBDriver(SyncArrowBulkOperationsMixin["DuckDBPyConnection"], SyncDriverAdapterProtocol["DuckDBPyConnection"]):
    """DuckDB Sync Driver Adapter."""

    connection: "DuckDBPyConnection"
    use_cursor: bool = True
    dialect: str = "duckdb"

    def __init__(self, connection: "DuckDBPyConnection", use_cursor: bool = True) -> None:
        self.connection = connection
        self.use_cursor = use_cursor

    # --- Helper Methods --- #
    def _cursor(self, connection: "DuckDBPyConnection") -> "DuckDBPyConnection":
        if self.use_cursor:
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
            yield connection

    # --- Public API Methods --- #

    def select(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["DuckDBPyConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
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
        *,
        connection: Optional["DuckDBPyConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore

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
        *,
        connection: Optional["DuckDBPyConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
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
        *,
        connection: "Optional[DuckDBPyConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore
            if schema_type is None:
                return result[0]  # pyright: ignore
            return schema_type(result[0])  # type: ignore[call-arg]

    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBPyConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
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
        *,
        connection: Optional["DuckDBPyConnection"] = None,
        **kwargs: Any,
    ) -> int:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return getattr(cursor, "rowcount", -1)  # pyright: ignore[reportUnknownMemberType]

    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["DuckDBPyConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
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

    def execute_script(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["DuckDBPyConnection"] = None,
        **kwargs: Any,
    ) -> str:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cast("str", getattr(cursor, "statusmessage", "DONE"))  # pyright: ignore[reportUnknownMemberType]

    # --- Arrow Bulk Operations ---

    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBPyConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cast("ArrowTable", cursor.fetch_arrow_table())  # pyright: ignore[reportUnknownMemberType]
