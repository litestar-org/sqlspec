import contextlib
import logging
import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.base import SyncArrowBulkOperationsMixin, SyncDriverAdapterProtocol, T
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.statement import SQLStatement
from sqlspec.typing import ArrowTable, StatementParameterType

if TYPE_CHECKING:
    from sqlspec.typing import ArrowTable, ModelDTOT, StatementParameterType

__all__ = ("AdbcDriver",)

logger = logging.getLogger("sqlspec")


PARAM_REGEX = re.compile(
    r"""(?<![:\w\$]) # Avoid matching ::, \:, etc. and other vendor prefixes
    (?:
        (?P<dquote>"(?:[^"]|"")*") |     # Double-quoted strings
        (?P<squote>'(?:[^']|'')*') |     # Single-quoted strings
        (?P<comment>--.*?\n|\/\*.*?\*\/) | # SQL comments
        (?P<lead>[:\$])(?P<var_name>[a-zA-Z_][a-zA-Z0-9_]*) # :name or $name identifier
    )
    """,
    re.VERBOSE | re.DOTALL,
)


class AdbcDriver(SyncArrowBulkOperationsMixin["Connection"], SyncDriverAdapterProtocol["Connection"]):
    """ADBC Sync Driver Adapter."""

    connection: Connection
    __supports_arrow__: ClassVar[bool] = True

    def __init__(self, connection: "Connection") -> None:
        """Initialize the ADBC driver adapter."""
        self.connection = connection
        self.dialect = self._get_dialect(connection)

    @staticmethod
    def _get_dialect(connection: "Connection") -> str:  # noqa: PLR0911
        """Get the database dialect based on the driver name.

        Args:
            connection: The ADBC connection object.

        Returns:
            The database dialect.
        """
        driver_name = connection.adbc_get_info()["vendor_name"].lower()
        if "postgres" in driver_name:
            return "postgres"
        if "bigquery" in driver_name:
            return "bigquery"
        if "sqlite" in driver_name:
            return "sqlite"
        if "duckdb" in driver_name:
            return "duckdb"
        if "mysql" in driver_name:
            return "mysql"
        if "snowflake" in driver_name:
            return "snowflake"
        return "postgres"  # default to postgresql dialect

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
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        # Determine effective parameter type *before* calling SQLStatement
        merged_params_type = dict if kwargs else type(parameters)

        # If ADBC + sqlite/duckdb + dictionary params, handle conversion manually
        if self.dialect in {"sqlite", "duckdb"} and merged_params_type is dict:
            logger.debug(
                "ADBC/%s with dict params; bypassing SQLStatement conversion, manually converting to '?' positional.",
                self.dialect,
            )

            # Combine parameters and kwargs into the actual dictionary to use
            parameter_dict = {}  # type: ignore[var-annotated]
            if isinstance(parameters, dict):
                parameter_dict.update(parameters)
            if kwargs:
                parameter_dict.update(kwargs)

            # Define regex locally to find :name or $name

            processed_sql_parts: list[str] = []
            ordered_params = []
            last_end = 0
            found_params_regex: list[str] = []

            for match in PARAM_REGEX.finditer(sql):  # Use original sql
                if match.group("dquote") or match.group("squote") or match.group("comment"):
                    continue

                if match.group("var_name"):
                    var_name = match.group("var_name")
                    leading_char = match.group("lead")  # : or $
                    found_params_regex.append(var_name)
                    # Use match span directly for replacement
                    start = match.start()
                    end = match.end()

                    if var_name not in parameter_dict:
                        msg = f"Named parameter '{leading_char}{var_name}' found in SQL but not provided. SQL: {sql}"
                        raise SQLParsingError(msg)

                    processed_sql_parts.extend((sql[last_end:start], "?"))  # Force ? style
                    ordered_params.append(parameter_dict[var_name])
                    last_end = end

            processed_sql_parts.append(sql[last_end:])

            if not found_params_regex and parameter_dict:
                msg = f"ADBC/{self.dialect}: Dict params provided, but no :name or $name placeholders found. SQL: {sql}"
                raise ParameterStyleMismatchError(msg)

            # Key validation
            provided_keys = set(parameter_dict.keys())
            missing_keys = set(found_params_regex) - provided_keys
            if missing_keys:
                msg = (
                    f"Named parameters found in SQL ({found_params_regex}) but not provided: {missing_keys}. SQL: {sql}"
                )
                raise SQLParsingError(msg)
            extra_keys = provided_keys - set(found_params_regex)
            if extra_keys:
                logger.debug("Extra parameters provided for ADBC/%s: %s", self.dialect, extra_keys)
                # Allow extra keys

            final_sql = "".join(processed_sql_parts)
            final_params = tuple(ordered_params)
            return final_sql, final_params
        # For all other cases (other dialects, or non-dict params for sqlite/duckdb),
        # use the standard SQLStatement processing.
        stmt = SQLStatement(sql=sql, parameters=parameters, dialect=self.dialect, kwargs=kwargs or None)
        return stmt.process()

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
        *,
        connection: Optional["Connection"] = None,
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
        *,
        connection: Optional["Connection"] = None,
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
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
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
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if not result:
                return None

            first_row = result[0]

            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            result_dict = dict(zip(column_names, first_row))

            if schema_type is None:
                return result_dict
            return cast("ModelDTOT", schema_type(**result_dict))

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
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cast("str", cursor.statusmessage) if hasattr(cursor, "statusmessage") else "DONE"  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]

    # --- Arrow Bulk Operations ---

    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Connection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":
        """Execute a SQL query and return results as an Apache Arrow Table.

        Returns:
            The results of the query as an Apache Arrow Table.
        """
        conn = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(conn) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cast("ArrowTable", cursor.fetch_arrow_table())  # pyright: ignore[reportUnknownMemberType]
