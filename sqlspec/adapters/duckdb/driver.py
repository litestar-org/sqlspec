import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from duckdb import DuckDBPyConnection

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin, SyncArrowBulkOperationsMixin
from sqlspec.statement import PARAM_REGEX, QMARK_REGEX
from sqlspec.typing import ArrowTable, StatementParameterType
from sqlspec.utils.text import bind_parameters

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from sqlspec.typing import ArrowTable, ModelDTOT, StatementParameterType, T

__all__ = ("DuckDBConnection", "DuckDBDriver")

logger = logging.getLogger("sqlspec")

DuckDBConnection = DuckDBPyConnection


class DuckDBDriver(
    SyncArrowBulkOperationsMixin["DuckDBConnection"],
    SQLTranslatorMixin["DuckDBConnection"],
    SyncDriverAdapterProtocol["DuckDBConnection"],
):
    """DuckDB Sync Driver Adapter."""

    connection: "DuckDBConnection"
    use_cursor: bool = True
    dialect: str = "duckdb"

    def __init__(self, connection: "DuckDBConnection", use_cursor: bool = True) -> None:
        self.connection = connection
        self.use_cursor = use_cursor

    def _cursor(self, connection: "DuckDBConnection") -> "DuckDBConnection":
        if self.use_cursor:
            return connection.cursor()
        return connection

    @contextmanager
    def _with_cursor(self, connection: "DuckDBConnection") -> "Generator[DuckDBConnection, None, None]":
        if self.use_cursor:
            cursor = self._cursor(connection)
            try:
                yield cursor
            finally:
                cursor.close()
        else:
            yield connection

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL and parameters for DuckDB.

        DuckDB supports both named (:name) and positional (?) parameters.
        This method merges parameters and validates them.

        Args:
            sql: The SQL statement to process.
            parameters: The parameters to process.
            **kwargs: Additional keyword arguments.

        Raises:
            ParameterStyleMismatchError: If positional parameters are mixed with keyword arguments.
            SQLParsingError: If SQL contains parameter placeholders, but no parameters were provided.

        Returns:
            A tuple of the processed SQL and parameters.
        """
        # 1. Merge parameters and kwargs
        merged_params: Optional[Union[dict[str, Any], list[Any], tuple[Any, ...]]] = None

        if kwargs:
            if isinstance(parameters, dict):
                merged_params = {**parameters, **kwargs}
            elif parameters is not None:
                msg = "Cannot mix positional parameters with keyword arguments for DuckDB driver."
                raise ParameterStyleMismatchError(msg)
            else:
                merged_params = kwargs
        elif parameters is not None:
            merged_params = parameters

        # Use bind_parameters for named parameters
        if isinstance(merged_params, dict):
            final_sql, final_params = bind_parameters(sql, merged_params, dialect="duckdb")
            return final_sql, final_params

        # Case 2: Sequence parameters - pass through
        if isinstance(merged_params, (list, tuple)):
            return sql, merged_params
        # Case 3: Scalar parameter - wrap in tuple
        if merged_params is not None:
            return sql, (merged_params,)  # type: ignore[unreachable]

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
            for match in QMARK_REGEX.finditer(sql):
                if match.group("qmark"):
                    has_placeholders = True
                    break

        if has_placeholders:
            msg = f"duckdb: SQL contains parameter placeholders, but no parameters were provided. SQL: {sql}"
            raise SQLParsingError(msg)
        return sql, None

    # --- Public API Methods --- #
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            results = cursor.fetchall()
            if not results:
                return []
            column_names = [column[0] for column in cursor.description or []]
            if schema_type is None:
                return [dict(zip(column_names, row)) for row in results]
            return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            result = cursor.fetchone()
            result = self.check_not_found(result)
            column_names = [column[0] for column in cursor.description or []]
            if schema_type is None:
                return dict(zip(column_names, result))
            return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            result = cursor.fetchone()
            if result is None:
                return None
            column_names = [column[0] for column in cursor.description or []]
            if schema_type is None:
                return dict(zip(column_names, result))
            return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            result = cursor.fetchone()
            result = self.check_not_found(result)
            result_value = result[0]
            if schema_type is None:
                return result_value
            return schema_type(result_value)  # type: ignore[call-arg]

    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            result = cursor.fetchone()
            if result is None:
                return None
            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: Any,
    ) -> int:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            params = [] if parameters is None else parameters
            cursor.execute(sql, params)
            return getattr(cursor, "rowcount", -1)

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            params = [] if parameters is None else parameters
            cursor.execute(sql, params)
            result = cursor.fetchall()
            result = self.check_not_found(result)
            column_names = [col[0] for col in cursor.description or []]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result[0]))))
            return dict(zip(column_names, result[0]))

    def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: Any,
    ) -> str:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            params = [] if parameters is None else parameters
            cursor.execute(sql, params)
            return cast("str", getattr(cursor, "statusmessage", "DONE"))

    # --- Arrow Bulk Operations ---

    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            params = [] if parameters is None else parameters
            cursor.execute(sql, params)
            return cast("ArrowTable", cursor.fetch_arrow_table())

    def _connection(self, connection: "Optional[DuckDBConnection]" = None) -> "DuckDBConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
