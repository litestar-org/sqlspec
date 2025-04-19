from typing import TYPE_CHECKING, Any, Optional, Union, cast

from asyncpg import Connection
from typing_extensions import TypeAlias

from sqlspec.base import AsyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from asyncpg.connection import Connection
    from asyncpg.pool import PoolConnectionProxy

    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("AsyncpgConnection", "AsyncpgDriver")


AsyncpgConnection: TypeAlias = "Union[Connection[Any], PoolConnectionProxy[Any]]"  # pyright: ignore[reportMissingTypeArgument]


class AsyncpgDriver(AsyncDriverAdapterProtocol["AsyncpgConnection"]):
    """AsyncPG Postgres Driver Adapter."""

    connection: "AsyncpgConnection"

    def __init__(self, connection: "AsyncpgConnection") -> None:
        self.connection = connection

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
        parameters = parameters if parameters is not None else ()

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
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
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
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
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
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchval(sql, *parameters)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        result = self.check_not_found(result)
        if schema_type is None:
            return result[0]
        return schema_type(result[0])  # type: ignore[call-arg]

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
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchval(sql, *parameters)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
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
        parameters = parameters if parameters is not None else ()
        status = await connection.execute(sql, *parameters)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
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
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
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
        parameters = parameters if parameters is not None else ()
        return await connection.execute(sql, *parameters)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
