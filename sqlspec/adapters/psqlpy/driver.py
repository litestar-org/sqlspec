# ruff: noqa: PLR0915, PLR0914, PLR0912, C901
"""Psqlpy Driver Implementation."""

import logging
import re
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from psqlpy.exceptions import RustPSQLDriverPyBaseError

from sqlspec.base import AsyncDriverAdapterProtocol, T
from sqlspec.exceptions import SQLParsingError
from sqlspec.statement import PARAM_REGEX, SQLStatement

if TYPE_CHECKING:
    from psqlpy import Connection, QueryResult

    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("PsqlpyDriver",)


# Regex to find '?' placeholders, skipping those inside quotes or SQL comments
QMARK_REGEX = re.compile(
    r"""(?P<dquote>"[^"]*") | # Double-quoted strings
         (?P<squote>\'[^\']*\') | # Single-quoted strings
         (?P<comment>--[^\n]*|/\*.*?\*/) | # SQL comments (single/multi-line)
         (?P<qmark>\?) # The question mark placeholder
      """,
    re.VERBOSE | re.DOTALL,
)
logger = logging.getLogger("sqlspec")


class PsqlpyDriver(AsyncDriverAdapterProtocol["Connection"]):
    """Psqlpy Postgres Driver Adapter."""

    connection: "Connection"
    dialect: str = "postgres"

    def __init__(self, connection: "Connection") -> None:
        self.connection = connection

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL and parameters for psqlpy.

        psqlpy uses $1, $2 style parameters natively.
        This method converts '?' (tuple/list) and ':name' (dict) styles to $n.
        It relies on SQLStatement for initial parameter validation and merging.
        """
        stmt = SQLStatement(sql=sql, parameters=parameters, dialect=self.dialect, kwargs=kwargs or None)
        sql, parameters = stmt.process()

        # Case 1: Parameters are a dictionary
        if isinstance(parameters, dict):
            processed_sql_parts: list[str] = []
            ordered_params = []
            last_end = 0
            param_index = 1
            found_params_regex: list[str] = []

            for match in PARAM_REGEX.finditer(sql):
                if match.group("dquote") or match.group("squote") or match.group("comment"):
                    continue

                if match.group("var_name"):  # Finds :var_name
                    var_name = match.group("var_name")
                    found_params_regex.append(var_name)
                    start = match.start("var_name") - 1
                    end = match.end("var_name")

                    if var_name not in parameters:
                        msg = f"Named parameter ':{var_name}' missing from parameters. SQL: {sql}"
                        raise SQLParsingError(msg)

                    processed_sql_parts.extend((sql[last_end:start], f"${param_index}"))
                    ordered_params.append(parameters[var_name])
                    last_end = end
                    param_index += 1

            processed_sql_parts.append(sql[last_end:])
            final_sql = "".join(processed_sql_parts)

            if not found_params_regex and parameters:
                logger.warning(
                    "Dict params provided (%s), but no :name placeholders found. SQL: %s",
                    list(parameters.keys()),
                    sql,
                )
                return sql, ()

            provided_keys = set(parameters.keys())
            found_keys = set(found_params_regex)
            unused_keys = provided_keys - found_keys
            if unused_keys:
                logger.warning("Unused parameters provided: %s. SQL: %s", unused_keys, sql)

            return final_sql, tuple(ordered_params)

        # Case 2: Parameters are a sequence/scalar
        if isinstance(parameters, (list, tuple)):
            sequence_processed_parts: list[str] = []
            param_index = 1
            last_end = 0
            qmark_found = False

            for match in QMARK_REGEX.finditer(sql):
                if match.group("dquote") or match.group("squote") or match.group("comment"):
                    continue

                if match.group("qmark"):
                    qmark_found = True
                    start = match.start("qmark")
                    end = match.end("qmark")
                    sequence_processed_parts.extend((sql[last_end:start], f"${param_index}"))
                    last_end = end
                    param_index += 1

            sequence_processed_parts.append(sql[last_end:])
            final_sql = "".join(sequence_processed_parts)

            if parameters and not qmark_found:
                logger.warning("Sequence parameters provided, but no '?' placeholders found. SQL: %s", sql)
                return sql, parameters

            expected_params = param_index - 1
            actual_params = len(parameters)
            if expected_params != actual_params:
                msg = f"Parameter count mismatch: Expected {expected_params}, got {actual_params}. SQL: {final_sql}"
                raise SQLParsingError(msg)

            return final_sql, parameters

        # Case 3: Parameters are None
        if PARAM_REGEX.search(sql) or QMARK_REGEX.search(sql):
            # Perform a simpler check if any placeholders might exist if no params are given
            for match in PARAM_REGEX.finditer(sql):
                if not (match.group("dquote") or match.group("squote") or match.group("comment")) and match.group(
                    "var_name"
                ):
                    msg = f"SQL contains named parameters (:name) but no parameters provided. SQL: {sql}"
                    raise SQLParsingError(msg)
            for match in QMARK_REGEX.finditer(sql):
                if not (match.group("dquote") or match.group("squote") or match.group("comment")) and match.group(
                    "qmark"
                ):
                    msg = f"SQL contains positional parameters (?) but no parameters provided. SQL: {sql}"
                    raise SQLParsingError(msg)

        return sql, ()

    async def select(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []  # psqlpy expects a list/tuple

        results: QueryResult = await connection.fetch(sql, parameters=parameters)

        if schema_type is None:
            return cast("list[dict[str, Any]]", results.result())  # type: ignore[return-value]
        return results.as_class(as_class=schema_type)

    async def select_one(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        result = await connection.fetch(sql, parameters=parameters)

        if schema_type is None:
            result = cast("list[dict[str, Any]]", result.result())  # type: ignore[assignment]
            return cast("dict[str, Any]", result[0])  # type: ignore[index]
        return result.as_class(as_class=schema_type)[0]

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        result = await connection.fetch(sql, parameters=parameters)
        if schema_type is None:
            result = cast("list[dict[str, Any]]", result.result())  # type: ignore[assignment]
            if len(result) == 0:  # type: ignore[arg-type]
                return None
            return cast("dict[str, Any]", result[0])  # type: ignore[index]
        result = cast("list[ModelDTOT]", result.as_class(as_class=schema_type))  # type: ignore[assignment]
        if len(result) == 0:  # type: ignore[arg-type]
            return None
        return cast("ModelDTOT", result[0])  # type: ignore[index]

    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        value = await connection.fetch_val(sql, parameters=parameters)

        if schema_type is None:
            return value
        return schema_type(value)  # type: ignore[call-arg]

    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []
        try:
            value = await connection.fetch_val(sql, parameters=parameters)
        except RustPSQLDriverPyBaseError:
            return None

        if value is None:
            return None
        if schema_type is None:
            return value
        return schema_type(value)  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        **kwargs: Any,
    ) -> int:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        await connection.execute(sql, parameters=parameters)
        # For INSERT/UPDATE/DELETE, psqlpy returns an empty list but the operation succeeded
        # if no error was raised
        return 1

    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        result = await connection.execute(sql, parameters=parameters)
        if schema_type is None:
            result = result.result()  # type: ignore[assignment]
            if len(result) == 0:  # type: ignore[arg-type]
                return None
            return cast("dict[str, Any]", result[0])  # type: ignore[index]
        result = result.as_class(as_class=schema_type)  # type: ignore[assignment]
        if len(result) == 0:  # type: ignore[arg-type]
            return None
        return cast("ModelDTOT", result[0])  # type: ignore[index]

    async def execute_script(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["Connection"] = None,
        **kwargs: Any,
    ) -> str:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        await connection.execute(sql, parameters=parameters)
        return sql

    def _connection(self, connection: Optional["Connection"] = None) -> "Connection":
        """Get the connection to use.

        Args:
            connection: Optional connection to use. If not provided, use the default connection.

        Returns:
            The connection to use.
        """
        return connection or self.connection
