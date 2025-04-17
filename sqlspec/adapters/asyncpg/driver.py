from typing import TYPE_CHECKING, Any, Optional, Union, cast

from asyncpg import Connection
from typing_extensions import TypeAlias

from sqlspec.base import AsyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from asyncpg.connection import Connection
    from asyncpg.pool import PoolConnectionProxy

    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("AsyncpgDriver",)


PgConnection: TypeAlias = "Union[Connection[Any], PoolConnectionProxy[Any]]"  # pyright: ignore[reportMissingTypeArgument]


class AsyncpgDriver(AsyncDriverAdapterProtocol["PgConnection"]):
    """AsyncPG Postgres Driver Adapter."""

    connection: "PgConnection"

    def __init__(self, connection: "PgConnection") -> None:
        self.connection = connection

    def _process_sql_params(
        self, sql: str, parameters: "Optional[StatementParameterType]" = None
    ) -> "tuple[str, Union[tuple[Any, ...], list[Any], dict[str, Any]]]":
        sql, parameters = super()._process_sql_params(sql, parameters)
        return sql, parameters if parameters is not None else ()

    async def select(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["PgConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        results = await connection.fetch(sql, *parameters)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
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
        connection: Optional["PgConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, params = self._process_sql_params(sql, parameters)
        # Use empty tuple if params is None
        params = params if params is not None else ()

        result = await connection.fetchrow(sql, *params)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
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
        connection: Optional["PgConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        result = self.check_not_found(result)
        if schema_type is None:
            # Always return as dictionary
            return dict(result.items())  # type: ignore[attr-defined]
        return cast("ModelDTOT", schema_type(**dict(result.items())))  # type: ignore[attr-defined]

    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[PgConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, params = self._process_sql_params(sql, parameters)
        # Use empty tuple if params is None
        params = params if params is not None else ()

        result = await connection.fetchval(sql, *params)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        result = self.check_not_found(result)
        if schema_type is None:
            return result[0]
        return schema_type(result[0])  # type: ignore[call-arg]

    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[PgConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, params = self._process_sql_params(sql, parameters)
        # Use empty tuple if params is None
        params = params if params is not None else ()

        result = await connection.fetchval(sql, *params)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if result is None:
            return None
        if schema_type is None:
            return result[0]
        return schema_type(result[0])  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["PgConnection"] = None,
    ) -> int:
        """Insert, update, or delete data from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, params = self._process_sql_params(sql, parameters)
        # Use empty tuple if params is None
        params = params if params is not None else ()

        status = await connection.execute(sql, *params)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
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
        connection: Optional["PgConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, params = self._process_sql_params(sql, parameters)
        # Use empty tuple if params is None
        params = params if params is not None else ()

        result = await connection.fetchrow(sql, *params)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
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
        connection: Optional["PgConnection"] = None,
    ) -> str:
        """Execute a script.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, params = self._process_sql_params(sql, parameters)
        # Use empty tuple if params is None
        params = params if params is not None else ()

        return await connection.execute(sql, *params)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

    async def execute_script_returning(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["PgConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Execute a script and return result.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, params = self._process_sql_params(sql, parameters)
        # Use empty tuple if params is None
        params = params if params is not None else ()

        result = await connection.fetchrow(sql, *params)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if result is None:
            return None
        if schema_type is None:
            # Always return as dictionary
            return dict(result.items())  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        return cast("ModelDTOT", schema_type(**dict(result.items())))  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType, reportUnknownVariableType]
