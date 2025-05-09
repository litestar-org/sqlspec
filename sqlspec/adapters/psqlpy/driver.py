# ruff: noqa: PLR0915, PLR0912, C901, PLR6301
"""Psqlpy Driver Implementation."""

import logging
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from psqlpy import Connection, QueryResult
from psqlpy.exceptions import RustPSQLDriverPyBaseError

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin
from sqlspec.statement import PARAM_REGEX, QMARK_REGEX
from sqlspec.utils.text import bind_parameters

if TYPE_CHECKING:
    from collections.abc import Sequence

    from psqlpy import QueryResult

    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("PsqlpyConnection", "PsqlpyDriver")


PsqlpyConnection = Connection
logger = logging.getLogger("sqlspec")


class PsqlpyDriver(
    SQLTranslatorMixin["PsqlpyConnection"],
    AsyncDriverAdapterProtocol["PsqlpyConnection"],
):
    """Psqlpy Postgres Driver Adapter."""

    connection: "PsqlpyConnection"
    dialect: str = "postgres"

    def __init__(self, connection: "PsqlpyConnection") -> None:
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
        merged_params: Optional[Union[dict[str, Any], Sequence[Any]]] = None

        if kwargs:
            if isinstance(parameters, dict):
                merged_params = {**parameters, **kwargs}
            elif parameters is not None:
                msg = "Cannot mix positional parameters with keyword arguments for psqlpy driver."
                raise ParameterStyleMismatchError(msg)
            else:
                merged_params = kwargs
        elif parameters is not None:
            merged_params = parameters

        # Use bind_parameters for named parameters
        if isinstance(merged_params, dict):
            final_sql, _ = bind_parameters(sql, merged_params, dialect="postgres")
            # psqlpy expects positional parameters, so convert dict to tuple in order of appearance
            # We'll use regex to find order for now
            param_names = []
            for match in PARAM_REGEX.finditer(sql):
                if match.group("dquote") or match.group("squote") or match.group("comment"):
                    continue
                if match.group("var_name"):
                    param_names.append(match.group("var_name"))
            ordered_params = tuple(merged_params[name] for name in param_names)
            # Replace :name with $1, $2, ...
            for idx, name in enumerate(param_names, 1):
                final_sql = final_sql.replace(f":{name}", f"${idx}")
            return final_sql, ordered_params

        # Case b: Sequence or scalar parameters (? style)
        if isinstance(merged_params, (list, tuple)):
            sequence_processed_parts: list[str] = []
            param_index = 1
            last_end = 0
            qmark_count = 0

            for match in QMARK_REGEX.finditer(sql):
                if match.group("dquote") or match.group("squote") or match.group("comment"):
                    continue

                if match.group("qmark"):
                    qmark_count += 1
                    start = match.start("qmark")
                    end = match.end("qmark")
                    sequence_processed_parts.extend((sql[last_end:start], f"${param_index}"))
                    last_end = end
                    param_index += 1

            sequence_processed_parts.append(sql[last_end:])
            final_sql = "".join(sequence_processed_parts)

            # Validation
            if not qmark_count and merged_params:
                msg = f"psqlpy: Sequence parameters provided, but no '?' placeholders found in SQL: {sql}"
                raise ParameterStyleMismatchError(msg)

            actual_count = len(merged_params)
            if qmark_count != actual_count:
                msg = f"psqlpy: Parameter count mismatch. SQL expects {qmark_count} positional parameters ('?'), but {actual_count} were provided. SQL: {sql}"
                raise SQLParsingError(msg)

            return final_sql, merged_params
        # Case c: Scalar
        # Convert to a one-element tuple
        if merged_params is not None:
            scalar_param_tuple = (merged_params,)
            sequence_processed_parts = []
            param_index = 1
            last_end = 0
            qmark_count = 0

            for match in QMARK_REGEX.finditer(sql):
                if match.group("dquote") or match.group("squote") or match.group("comment"):
                    continue

                if match.group("qmark"):
                    qmark_count += 1
                    start = match.start("qmark")
                    end = match.end("qmark")
                    sequence_processed_parts.extend((sql[last_end:start], f"${param_index}"))
                    last_end = end
                    param_index += 1

            sequence_processed_parts.append(sql[last_end:])
            final_sql = "".join(sequence_processed_parts)

            # Validation - for scalar, we expect exactly one placeholder
            if qmark_count != 1:
                msg = f"psqlpy: Parameter count mismatch. SQL expects 1 positional parameter ('?') for scalar input, but found {qmark_count}. SQL: {sql}"
                raise SQLParsingError(msg)

            return final_sql, scalar_param_tuple

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
            # Check for ? style placeholders
            for match in QMARK_REGEX.finditer(sql):
                if not (match.group("dquote") or match.group("squote") or match.group("comment")) and match.group(
                    "qmark"
                ):
                    has_placeholders = True
                    break

        if has_placeholders:
            msg = f"psqlpy: SQL contains parameter placeholders, but no parameters were provided. SQL: {sql}"
            raise SQLParsingError(msg)
        return sql, ()

    # --- Public API Methods --- #
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []  # psqlpy expects a list/tuple

        results: QueryResult = await connection.fetch(sql, parameters=parameters)

        if schema_type is None:
            return cast("list[dict[str, Any]]", results.result())
        return results.as_class(as_class=schema_type)

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
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

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
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

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
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

    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
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
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        **kwargs: Any,
    ) -> int:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        await connection.execute(sql, parameters=parameters)
        # For INSERT/UPDATE/DELETE, psqlpy returns an empty list but the operation succeeded
        # if no error was raised
        return 1

    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
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
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        **kwargs: Any,
    ) -> str:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        await connection.execute(sql, parameters=parameters)
        return sql

    def _connection(self, connection: "Optional[PsqlpyConnection]" = None) -> "PsqlpyConnection":
        """Get the connection to use.

        Args:
            connection: Optional connection to use. If not provided, use the default connection.

        Returns:
            The connection to use.
        """
        return connection or self.connection
