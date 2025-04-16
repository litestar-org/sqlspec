from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.base import AsyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from collections.abc import AsyncIterable

    from asyncmy import Connection  # pyright: ignore[reportUnknownVariableType,reportMissingTypeStubs]

    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("AsyncMyDriver",)


class AsyncMyDriver(AsyncDriverAdapterProtocol["Connection"]):
    """AsyncMy MySQL/MariaDB Driver Adapter."""

    connection: "Connection"
    results_as_dict: bool = True

    def __init__(self, connection: "Connection", results_as_dict: bool = True) -> None:  # pyright: ignore[reportUnknownParameterType]
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

    async def select(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[Connection]" = None,  # pyright: ignore[reportUnknownParameterType]
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "AsyncIterable[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch data from the database.

        Returns:
            Row data as either model instances or dictionaries.
        """
        connection = connection if connection is not None else self.connection  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        parameters = parameters if parameters is not None else {}

        async def _fetch_results() -> "AsyncIterable[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
            async with connection.cursor() as cursor:  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportOptionalMemberAccess]
                await cursor.execute(sql, *self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                results = await cursor.fetchall()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

                for row in results:  # pyright: ignore[reportUnknownVariableType]
                    if schema_type is not None:
                        yield cast("ModelDTOT", schema_type(**dict(row)))  # pyright: ignore[reportUnknownArgumentType]
                    elif self.results_as_dict:
                        yield dict(row)  # pyright: ignore[reportUnknownArgumentType]
                    else:
                        yield tuple(row)  # pyright: ignore[reportUnknownArgumentType]

        return _fetch_results()

    async def select_one(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[Connection]" = None,  # pyright: ignore[reportUnknownParameterType]
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = connection if connection is not None else self.connection  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        parameters = parameters if parameters is not None else {}

        async with connection.cursor() as cursor:  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportOptionalMemberAccess]
            await cursor.execute(sql, *self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if result is None:
                return None

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(result)))  # pyright: ignore[reportUnknownArgumentType]
            if self.results_as_dict:
                return dict(result)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
            return tuple(result)  # pyright: ignore[reportUnknownArgumentType]

    async def select_value(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[Connection]" = None,  # pyright: ignore[reportUnknownParameterType]
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = connection if connection is not None else self.connection  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        parameters = parameters if parameters is not None else {}

        async with connection.cursor() as cursor:  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportOptionalMemberAccess]
            await cursor.execute(sql, *self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if result is None:
                return None

            value = result[0]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is not None:
                return schema_type(value)  # type: ignore[call-arg]
            return value  # pyright: ignore[reportUnknownVariableType]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[Connection]" = None,  # pyright: ignore[reportUnknownParameterType]
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[int, Any, ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Insert, update, or delete data from the database.

        Returns:
            Row count if not returning data, otherwise the first row of results.
        """
        connection = connection if connection is not None else self.connection  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        parameters = parameters if parameters is not None else {}

        async with connection.cursor() as cursor:  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportOptionalMemberAccess]
            if returning:
                await cursor.execute(sql, *self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

                if result is None:
                    return None

                if schema_type is not None:
                    return cast("ModelDTOT", schema_type(**dict(result)))  # pyright: ignore[reportUnknownArgumentType]
                if self.results_as_dict:
                    return dict(result)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
                return tuple(result)  # pyright: ignore[reportUnknownArgumentType]
            return await cursor.execute(sql, *self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

    async def execute_script(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[Connection]" = None,  # pyright: ignore[reportUnknownParameterType]
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[Any, ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Execute a script.

        Returns:
            The number of rows affected by the script.
        """
        connection = connection if connection is not None else self.connection  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        parameters = parameters if parameters is not None else {}

        async with connection.cursor() as cursor:  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportOptionalMemberAccess]
            if returning:
                await cursor.execute(sql, *self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

                if result is None:
                    return None

                if schema_type is not None:
                    return cast("ModelDTOT", schema_type(**dict(result)))  # pyright: ignore[reportUnknownArgumentType]
                if self.results_as_dict:
                    return dict(result)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
                return tuple(result)  # pyright: ignore[reportUnknownArgumentType]
            return await cursor.execute(sql, *self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
