import logging
import re
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from asyncpg import Connection
from typing_extensions import TypeAlias

from sqlspec.base import AsyncDriverAdapterProtocol, T
from sqlspec.exceptions import SQLParsingError
from sqlspec.statement import PARAM_REGEX, SQLStatement

if TYPE_CHECKING:
    from asyncpg.connection import Connection
    from asyncpg.pool import PoolConnectionProxy

    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("AsyncpgConnection", "AsyncpgDriver")

logger = logging.getLogger("sqlspec")

# Regex to find '?' placeholders, skipping those inside quotes or SQL comments
# Simplified version, assumes standard SQL quoting/comments
QMARK_REGEX = re.compile(
    r"""(?P<dquote>"[^"]*") | # Double-quoted strings
         (?P<squote>\'[^\']*\') | # Single-quoted strings
         (?P<comment>--[^\n]*|/\*.*?\*/) | # SQL comments (single/multi-line)
         (?P<qmark>\?) # The question mark placeholder
      """,
    re.VERBOSE | re.DOTALL,
)

AsyncpgConnection: TypeAlias = "Union[Connection[Any], PoolConnectionProxy[Any]]"  # pyright: ignore[reportMissingTypeArgument]


class AsyncpgDriver(AsyncDriverAdapterProtocol["AsyncpgConnection"]):
    """AsyncPG Postgres Driver Adapter."""

    connection: "AsyncpgConnection"
    dialect: str = "postgres"

    def __init__(self, connection: "AsyncpgConnection") -> None:
        self.connection = connection

    def _process_sql_params(  # noqa: C901, PLR0912, PLR0915
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        # Use SQLStatement for parameter validation and merging first
        # It also handles potential dialect-specific logic if implemented there.
        stmt = SQLStatement(sql=sql, parameters=parameters, dialect=self.dialect, kwargs=kwargs or None)
        sql, parameters = stmt.process()

        # Case 1: Parameters are effectively a dictionary (either passed as dict or via kwargs merged by SQLStatement)
        if isinstance(parameters, dict):
            processed_sql_parts: list[str] = []
            ordered_params = []
            last_end = 0
            param_index = 1
            found_params_regex: list[str] = []

            # Manually parse the PROCESSED SQL for :name -> $n conversion
            for match in PARAM_REGEX.finditer(sql):
                # Skip matches inside quotes or comments
                if match.group("dquote") or match.group("squote") or match.group("comment"):
                    continue

                if match.group("var_name"):  # Finds :var_name
                    var_name = match.group("var_name")
                    found_params_regex.append(var_name)
                    start = match.start("var_name") - 1  # Include the ':'
                    end = match.end("var_name")

                    # SQLStatement should have already validated parameter existence,
                    # but we double-check here during ordering.
                    if var_name not in parameters:
                        # This should ideally not happen if SQLStatement validation is robust.
                        msg = (
                            f"Named parameter ':{var_name}' found in SQL but missing from processed parameters. "
                            f"Processed SQL: {sql}"
                        )
                        raise SQLParsingError(msg)

                    processed_sql_parts.extend((sql[last_end:start], f"${param_index}"))
                    ordered_params.append(parameters[var_name])
                    last_end = end
                    param_index += 1

            processed_sql_parts.append(sql[last_end:])
            final_sql = "".join(processed_sql_parts)

            # --- Validation ---
            # Check if named placeholders were found if dict params were provided
            # SQLStatement might handle this validation, but a warning here can be useful.
            if not found_params_regex and parameters:
                logger.warning(
                    "Dict params provided (%s), but no :name placeholders found. SQL: %s",
                    list(parameters.keys()),
                    sql,
                )
                # If no placeholders, return original SQL from SQLStatement and empty tuple for asyncpg
                return sql, ()

            # Additional checks (potentially redundant if SQLStatement covers them):
            # 1. Ensure all found placeholders have corresponding params (covered by check inside loop)
            # 2. Ensure all provided params correspond to a placeholder
            provided_keys = set(parameters.keys())
            found_keys = set(found_params_regex)
            unused_keys = provided_keys - found_keys
            if unused_keys:
                # SQLStatement might handle this, but log a warning just in case.
                logger.warning(
                    "Parameters provided but not used in SQL: %s. SQL: %s",
                    unused_keys,
                    sql,
                )

            return final_sql, tuple(ordered_params)  # asyncpg expects a sequence

        # Case 2: Parameters are effectively a sequence/scalar (merged by SQLStatement)
        if isinstance(parameters, (list, tuple)):
            # Parameters are a sequence, need to convert ? -> $n
            sequence_processed_parts: list[str] = []
            param_index = 1
            last_end = 0
            qmark_found = False

            # Manually parse the PROCESSED SQL to find '?' outside comments/quotes and convert to $n
            for match in QMARK_REGEX.finditer(sql):
                if match.group("dquote") or match.group("squote") or match.group("comment"):
                    continue  # Skip quotes and comments

                if match.group("qmark"):
                    qmark_found = True
                    start = match.start("qmark")
                    end = match.end("qmark")
                    sequence_processed_parts.extend((sql[last_end:start], f"${param_index}"))
                    last_end = end
                    param_index += 1

            sequence_processed_parts.append(sql[last_end:])
            final_sql = "".join(sequence_processed_parts)

            # --- Validation ---
            # Check if '?' was found if parameters were provided
            if parameters and not qmark_found:
                # SQLStatement might allow this, log a warning.
                logger.warning(
                    "Sequence/scalar parameters provided, but no '?' placeholders found. SQL: %s",
                    sql,
                )
                # Return PROCESSED SQL from SQLStatement as no conversion happened here
                return sql, parameters

            # Check parameter count match (using count from manual parsing vs count from stmt)
            expected_params = param_index - 1
            actual_params = len(parameters)
            if expected_params != actual_params:
                msg = (
                    f"Parameter count mismatch: Processed SQL expected {expected_params} parameters ('$n'), "
                    f"but {actual_params} were provided by SQLStatement. "
                    f"Final Processed SQL: {final_sql}"
                )
                raise SQLParsingError(msg)

            return final_sql, parameters

        # Case 3: Parameters are None (as determined by SQLStatement)
        # processed_params is None
        # Check if the SQL contains any placeholders unexpectedly
        # Check for :name style
        named_placeholders_found = False
        for match in PARAM_REGEX.finditer(sql):
            if not (match.group("dquote") or match.group("squote") or match.group("comment")) and match.group(
                "var_name"
            ):
                named_placeholders_found = True
                break
        if named_placeholders_found:
            msg = f"Processed SQL contains named parameters (:name) but no parameters were provided. SQL: {sql}"
            raise SQLParsingError(msg)

        # Check for ? style
        qmark_placeholders_found = False
        for match in QMARK_REGEX.finditer(sql):
            if not (match.group("dquote") or match.group("squote") or match.group("comment")) and match.group("qmark"):
                qmark_placeholders_found = True
                break
        if qmark_placeholders_found:
            msg = f"Processed SQL contains positional parameters (?) but no parameters were provided. SQL: {sql}"
            raise SQLParsingError(msg)

        # No parameters provided and none found in SQL, return original SQL from SQLStatement and empty tuple
        return sql, ()  # asyncpg expects a sequence, even if empty

    async def select(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AsyncpgConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]":
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

    async def select_one(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AsyncpgConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
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

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AsyncpgConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
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
        status = await connection.execute(sql, *parameters)  # pyright: ignore
        # AsyncPG returns a string like "INSERT 0 1" where the last number is the affected rows
        try:
            return int(status.split()[-1])  # pyright: ignore[reportUnknownMemberType]
        except (ValueError, IndexError, AttributeError):
            return -1  # Fallback if we can't parse the status

    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AsyncpgConnection"] = None,
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
            # Always return as dictionary
            return dict(result.items())  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        return cast("ModelDTOT", schema_type(**dict(result.items())))  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType, reportUnknownVariableType]

    async def execute_script(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AsyncpgConnection"] = None,
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

    def _connection(self, connection: Optional["AsyncpgConnection"] = None) -> "AsyncpgConnection":
        """Return the connection to use. If None, use the default connection."""
        return connection if connection is not None else self.connection
