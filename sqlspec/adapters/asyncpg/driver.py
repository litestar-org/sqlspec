import logging
import re
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from asyncpg import Connection
from typing_extensions import TypeAlias

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin
from sqlspec.statement import PARAM_REGEX, QMARK_REGEX

if TYPE_CHECKING:
    from collections.abc import Sequence

    from asyncpg.connection import Connection
    from asyncpg.pool import PoolConnectionProxy

    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("AsyncpgConnection", "AsyncpgDriver")

logger = logging.getLogger("sqlspec")

AsyncpgConnection: TypeAlias = "Union[Connection[Any], PoolConnectionProxy[Any]]"

# Compile the row count regex once for efficiency
ROWCOUNT_REGEX = re.compile(r"^(?:INSERT|UPDATE|DELETE) \d+ (\d+)$")


class AsyncpgDriver(
    SQLTranslatorMixin["AsyncpgConnection"],
    AsyncDriverAdapterProtocol["AsyncpgConnection"],
):
    """AsyncPG Postgres Driver Adapter."""

    connection: "AsyncpgConnection"
    dialect: str = "postgres"

    def __init__(self, connection: "AsyncpgConnection") -> None:
        self.connection = connection

    def _process_sql_params(  # noqa: PLR0912, PLR0915
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        # 1. Merge parameters and kwargs
        merged_params: Optional[Union[dict[str, Any], Sequence[Any]]] = None
        merged_params_type: Optional[type] = None

        if kwargs:
            merged_params_type = dict
            if isinstance(parameters, dict):
                merged_params = {**parameters, **kwargs}
            elif parameters is not None:
                msg = "Cannot mix positional parameters with keyword arguments for asyncpg driver."
                raise ParameterStyleMismatchError(msg)
            else:
                merged_params = kwargs
        elif parameters is not None:
            merged_params_type = type(parameters)
            # Preserve original type (dict, list, tuple, scalar) for now
            merged_params = parameters

        # 2. Process based on merged parameter type

        # Case 0: No parameters provided
        if merged_params_type is None:
            # Basic validation: Check if SQL contains placeholders
            has_placeholders = False
            for match in PARAM_REGEX.finditer(sql):
                if not (match.group("dquote") or match.group("squote") or match.group("comment")) and match.group(
                    "var_name"
                ):
                    has_placeholders = True
                    break
            if not has_placeholders:
                for match in QMARK_REGEX.finditer(sql):
                    if not (match.group("dquote") or match.group("squote") or match.group("comment")) and match.group(
                        "qmark"
                    ):
                        has_placeholders = True
                        break

            if has_placeholders:
                msg = f"asyncpg: SQL contains parameter placeholders, but no parameters were provided. SQL: {sql}"
                raise SQLParsingError(msg)
            return sql, ()  # asyncpg expects a sequence, even if empty

        # Case 1: Parameters are effectively a dictionary
        if merged_params_type is dict:
            parameter_dict = cast("dict[str, Any]", merged_params)
            processed_sql_parts: list[str] = []
            ordered_params = []
            last_end = 0
            param_index = 1
            found_params_regex: list[str] = []
            has_qmark = False

            # Check for qmarks first, as they are invalid with dict params
            for match in QMARK_REGEX.finditer(sql):
                if not (match.group("dquote") or match.group("squote") or match.group("comment")) and match.group(
                    "qmark"
                ):
                    has_qmark = True
                    break
            if has_qmark:
                msg = (
                    f"asyncpg: Cannot use dictionary parameters with positional placeholders ('?') found in SQL: {sql}"
                )
                raise ParameterStyleMismatchError(msg)

            # Manually parse the *original* SQL for :name -> $n conversion
            for match in PARAM_REGEX.finditer(sql):
                # Skip matches inside quotes or comments
                if match.group("dquote") or match.group("squote") or match.group("comment"):
                    continue

                if match.group("var_name"):  # Finds :var_name
                    var_name = match.group("var_name")
                    found_params_regex.append(var_name)
                    start = match.start("var_name") - 1  # Include the ':'
                    end = match.end("var_name")

                    if var_name not in parameter_dict:
                        msg = f"asyncpg: Named parameter ':{var_name}' found in SQL but not provided. SQL: {sql}"
                        raise SQLParsingError(msg)

                    processed_sql_parts.extend((sql[last_end:start], f"${param_index}"))
                    ordered_params.append(parameter_dict[var_name])
                    last_end = end
                    param_index += 1

            processed_sql_parts.append(sql[last_end:])
            final_sql = "".join(processed_sql_parts)

            # Validation
            if not found_params_regex and parameter_dict:
                msg = f"asyncpg: Dictionary parameters provided, but no named placeholders (:name) found. SQL: {sql}"
                raise ParameterStyleMismatchError(msg)  # Or log warning?

            provided_keys = set(parameter_dict.keys())
            required_keys = set(found_params_regex)
            missing_keys = required_keys - provided_keys
            if missing_keys:
                msg = f"asyncpg: Named parameters found in SQL ({required_keys}) but not provided: {missing_keys}. SQL: {sql}"
                raise SQLParsingError(msg)
            # Allow extra keys

            return final_sql, tuple(ordered_params)

        # Case 2: Parameters are a sequence or scalar (? style)
        if isinstance(merged_params, (list, tuple)):
            params_tuple = tuple(merged_params)
            final_sql, expected_params = self._convert_qmarks_to_dollar(sql)
            actual_params = len(params_tuple)

            if expected_params != actual_params:
                msg = (
                    f"asyncpg: Parameter count mismatch. SQL requires {expected_params} positional parameters ($n), "
                    f"but {actual_params} were provided. Processed SQL: {final_sql}"
                )
                raise SQLParsingError(msg)

            return final_sql, params_tuple
        # Scalar
        scalar_param_tuple = (merged_params,)
        final_sql, expected_params = self._convert_qmarks_to_dollar(sql)
        if expected_params != 1:
            msg = (
                f"asyncpg: Parameter count mismatch. SQL requires {expected_params} positional parameters ($n), "
                f"but 1 (scalar) was provided. Processed SQL: {final_sql}"
            )
            raise SQLParsingError(msg)
        return final_sql, scalar_param_tuple

    @staticmethod
    def _convert_qmarks_to_dollar(sql: str) -> tuple[str, int]:
        """Converts '?' placeholders to '$n' and counts them.

        Args:
            sql (str): The SQL string to process.

        Returns:
            tuple[str, int]: A tuple containing the processed SQL string and the number of '?' placeholders found.
        """
        processed_parts: list[str] = []
        param_index = 1
        last_end = 0
        qmark_found_count = 0

        for match in QMARK_REGEX.finditer(sql):
            if match.group("dquote") or match.group("squote") or match.group("comment"):
                continue

            if match.group("qmark"):
                qmark_found_count += 1
                start = match.start("qmark")
                end = match.end("qmark")
                processed_parts.extend((sql[last_end:start], f"${param_index}"))
                last_end = end
                param_index += 1

        processed_parts.append(sql[last_end:])
        final_sql = "".join(processed_parts)
        return final_sql, qmark_found_count

    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
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
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else {}

        results = await connection.fetch(sql, *parameters)  # pyright: ignore
        if not results:
            return []
        if schema_type is None:
            return [dict(row.items()) for row in results]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        return [cast("ModelDTOT", schema_type(**dict(row.items()))) for row in results]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
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
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else {}
        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore
        result = self.check_not_found(result)

        if schema_type is None:
            # Always return as dictionary
            return dict(result.items())  # type: ignore[attr-defined]
        return cast("ModelDTOT", schema_type(**dict(result.items())))  # type: ignore[attr-defined]

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
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
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else {}
        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore
        if result is None:
            return None
        if schema_type is None:
            # Always return as dictionary
            return dict(result.items())
        return cast("ModelDTOT", schema_type(**dict(result.items())))

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
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
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else {}
        result = await connection.fetchval(sql, *parameters)  # pyright: ignore
        result = self.check_not_found(result)
        if schema_type is None:
            return result
        return schema_type(result)  # type: ignore[call-arg]

    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
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
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else {}
        result = await connection.fetchval(sql, *parameters)  # pyright: ignore
        if result is None:
            return None
        if schema_type is None:
            return result
        return schema_type(result)  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AsyncpgConnection"] = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            **kwargs: Additional keyword arguments.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else {}
        result = await connection.execute(sql, *parameters)  # pyright: ignore
        # asyncpg returns e.g. 'INSERT 0 1', 'UPDATE 0 2', etc.
        match = ROWCOUNT_REGEX.match(result)
        if match:
            return int(match.group(1))
        return 0

    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
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
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return the affected row.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The affected row data as either a model instance or dictionary.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else {}
        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore
        if result is None:
            return None
        if schema_type is None:
            return dict(result.items())  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        return cast("ModelDTOT", schema_type(**dict(result.items())))  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType, reportUnknownVariableType]

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            **kwargs: Additional keyword arguments.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else {}
        return await connection.execute(sql, *parameters)  # pyright: ignore

    def _connection(self, connection: "Optional[AsyncpgConnection]" = None) -> "AsyncpgConnection":
        """Return the connection to use. If None, use the default connection."""
        return connection if connection is not None else self.connection
