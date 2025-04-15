from typing import TYPE_CHECKING, Any, Optional, Union, cast

from asyncpg import Connection
from typing_extensions import TypeAlias

from sqlspec.base import AsyncDriverAdapterProtocol, T
from sqlspec.typing import ModelDTOT, StatementParameterType

if TYPE_CHECKING:
    from collections.abc import AsyncIterable

    from asyncpg.connection import Connection
    from asyncpg.pool import PoolConnectionProxy
PgConnection: TypeAlias = "Union[Connection, PoolConnectionProxy]"  # pyright: ignore[reportMissingTypeArgument]


class AsyncPGDriver(AsyncDriverAdapterProtocol[PgConnection]):
    """AsyncPG Postgres Driver Adapter."""

    connection: PgConnection
    results_as_dict: bool = True

    def __init__(self, connection: PgConnection, results_as_dict: bool = True) -> None:
        self.connection = connection
        self.results_as_dict = results_as_dict

    @staticmethod
    def _handle_statement_parameters(
        parameters: "StatementParameterType",
    ) -> "Union[list[Any], tuple[Any, ...]]":
        if isinstance(parameters, dict):
            return cast("list[Any]", parameters.values())
        if isinstance(parameters, tuple):
            return parameters
        msg = f"Parameters expected to be dict or tuple, received {parameters}"
        raise TypeError(msg)

    async def select(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[PgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "AsyncIterable[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch data from the database.

        Yields:
            Row data as either model instances or dictionaries.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        results = await connection.fetch(sql, *self._handle_statement_parameters(parameters))

        for row in results:
            if schema_type is not None:
                yield schema_type(**dict(row))
            if self.results_as_dict:  # pragma: no cover
                # strict=False: requires 3.10
                yield dict(row)
            else:
                yield tuple(row)

    async def select_one(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[PgConnection] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """

        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        result = await connection.fetchrow(sql, *self._handle_statement_parameters(parameters))
        if result is None:
            return None
        if schema_type is None and self.results_as_dict:
            return dict(result)
        if schema_type is not None:
            return schema_type(**dict(result))
        return tuple(result.values())

    async def select_value(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[PgConnection] = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        result = await connection.fetchval(sql, *self._handle_statement_parameters(parameters))
        if result is None:
            return None
        if schema_type is None:
            return result[0]
        return schema_type(result[0])  # pyright: ignore[reportCallIssue]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[PgConnection] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[int, Any,ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Insert, update, or delete data from the database.

        Returns:
            Row count if not returning data, otherwise the first row of results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        if returning is False:
            result = await connection.execute(sql, *self._handle_statement_parameters(parameters))
            if result is None:
                return None
            return result
        result = await connection.fetchrow(sql, *self._handle_statement_parameters(parameters))
        if result is None:
            return None
        if schema_type is None and self.results_as_dict:
            return dict(result)
        if schema_type is not None:
            return schema_type(**dict(result))
        return tuple(result.values())

    async def execute_script(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[PgConnection] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[Any,ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Execute a script.

        Returns:
            The number of rows affected by the script.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}

        if returning is False:
            return await connection.execute(sql, *self._handle_statement_parameters(parameters))

        result = await connection.fetch(sql, *self._handle_statement_parameters(parameters))
        if result is None or len(result) == 0:
            return None
        if schema_type is None and self.results_as_dict:
            return dict(result)
        if schema_type is not None:
            return schema_type(**dict(result))
        return tuple(result.values())  # pyright: ignore[reportAttributeAccessIssue]
