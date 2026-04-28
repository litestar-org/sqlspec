"""Service base classes for SQLSpec application services."""

from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Generic, cast, overload

from typing_extensions import TypeVar

from sqlspec.core._pagination import OffsetPagination
from sqlspec.core.filters import LimitOffsetFilter
from sqlspec.driver._async import AsyncDriverAdapterBase
from sqlspec.driver._sync import SyncDriverAdapterBase
from sqlspec.exceptions import NotFoundError
from sqlspec.typing import SchemaT

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlspec.builder import QueryBuilder
    from sqlspec.core.filters import StatementFilter
    from sqlspec.core.statement import Statement
    from sqlspec.typing import StatementParameters


__all__ = ("SQLSpecAsyncService", "SQLSpecSyncService")

AsyncDriverT = TypeVar("AsyncDriverT", bound=AsyncDriverAdapterBase, default=AsyncDriverAdapterBase)
SyncDriverT = TypeVar("SyncDriverT", bound=SyncDriverAdapterBase, default=SyncDriverAdapterBase)


class SQLSpecAsyncService(Generic[AsyncDriverT]):
    """Base class for asynchronous SQLSpec services.

    Provides common database operations and pagination support using a driver session.

    Args:
        session: The driver session instance.
    """

    __slots__ = ("_session",)

    def __init__(self, session: AsyncDriverT) -> None:
        self._session = session

    @property
    def session(self) -> AsyncDriverT:
        """Return the driver session."""
        return self._session

    @property
    def driver(self) -> AsyncDriverT:
        """Alias for :attr:`session` matching the recipe-doc terminology."""
        return self._session

    @overload
    async def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> OffsetPagination[SchemaT]: ...

    @overload
    async def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> OffsetPagination[dict[str, Any]]: ...

    async def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> "OffsetPagination[SchemaT] | OffsetPagination[dict[str, Any]]":
        """Execute a paginated query and return an OffsetPagination container.

        Args:
            statement: The SQL statement or QueryBuilder instance.
            *parameters: Statement parameters or filters.
            schema_type: The schema type to map results to.
            count_with_window: Whether to use COUNT(*) OVER() for total count.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            An OffsetPagination instance containing items and total count.
        """
        limit_offset: LimitOffsetFilter | None = self._session.find_filter(LimitOffsetFilter, parameters)

        items, total = await self._session.select_with_total(
            statement, *parameters, schema_type=schema_type, count_with_window=count_with_window, **kwargs
        )

        if schema_type is None:
            return OffsetPagination(
                items=cast("list[dict[str, Any]]", items),
                limit=limit_offset.limit if limit_offset is not None else len(items),
                offset=limit_offset.offset if limit_offset is not None else 0,
                total=total,
            )

        return OffsetPagination(
            items=cast("list[SchemaT]", items),
            limit=limit_offset.limit if limit_offset is not None else len(items),
            offset=limit_offset.offset if limit_offset is not None else 0,
            total=total,
        )

    @overload
    async def get_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        error_message: str | None = None,
        **kwargs: Any,
    ) -> SchemaT: ...

    @overload
    async def get_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        error_message: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]: ...

    @overload
    async def get_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        error_message: str | None = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]": ...

    async def get_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        error_message: str | None = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]":
        """Fetch one row or raise :class:`~sqlspec.exceptions.NotFoundError`.

        HTTP status mapping (e.g. translating ``NotFoundError`` to a 404 response)
        is the responsibility of the calling framework integration. The Litestar
        extension registers a default mapping; other framework integrations do
        not.

        Args:
            statement: The SQL statement or QueryBuilder instance.
            *parameters: Statement parameters or filters.
            schema_type: The schema type to map the row to.
            error_message: Optional message for the raised :class:`NotFoundError`.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            The single matched row, mapped to ``schema_type`` when provided.

        Raises:
            NotFoundError: If the query returns zero rows.
        """
        result = await self._session.select_one_or_none(statement, *parameters, schema_type=schema_type, **kwargs)
        if result is None:
            raise NotFoundError(error_message or "Record not found")
        return result

    async def exists(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        **kwargs: Any,
    ) -> bool:
        """Check if any rows exist for the given query.

        Args:
            statement: The SQL statement or QueryBuilder instance.
            *parameters: Statement parameters or filters.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            True if at least one row exists, False otherwise.
        """
        return await self._session.select_one_or_none(statement, *parameters, **kwargs) is not None

    async def begin(self) -> None:
        """Begin a database transaction on the underlying session."""
        await self._session.begin()

    async def commit(self) -> None:
        """Commit the current database transaction."""
        await self._session.commit()

    async def rollback(self) -> None:
        """Roll back the current database transaction."""
        await self._session.rollback()

    @asynccontextmanager
    async def begin_transaction(self) -> "AsyncIterator[AsyncDriverT]":
        """Context manager that commits on success and rolls back on error.

        Yields:
            The underlying driver session bound to the active transaction.
        """
        await self.begin()
        try:
            yield self._session
        except Exception:
            await self.rollback()
            raise
        else:
            await self.commit()


class SQLSpecSyncService(Generic[SyncDriverT]):
    """Base class for synchronous SQLSpec services.

    Provides common database operations and pagination support using a driver session.

    Args:
        session: The driver session instance.
    """

    __slots__ = ("_session",)

    def __init__(self, session: SyncDriverT) -> None:
        self._session = session

    @property
    def session(self) -> SyncDriverT:
        """Return the driver session."""
        return self._session

    @property
    def driver(self) -> SyncDriverT:
        """Alias for :attr:`session` matching the recipe-doc terminology."""
        return self._session

    @overload
    def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> OffsetPagination[SchemaT]: ...

    @overload
    def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> OffsetPagination[dict[str, Any]]: ...

    def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> "OffsetPagination[SchemaT] | OffsetPagination[dict[str, Any]]":
        """Execute a paginated query and return an OffsetPagination container.

        Args:
            statement: The SQL statement or QueryBuilder instance.
            *parameters: Statement parameters or filters.
            schema_type: The schema type to map results to.
            count_with_window: Whether to use COUNT(*) OVER() for total count.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            An OffsetPagination instance containing items and total count.
        """
        limit_offset: LimitOffsetFilter | None = self._session.find_filter(LimitOffsetFilter, parameters)

        items, total = self._session.select_with_total(
            statement, *parameters, schema_type=schema_type, count_with_window=count_with_window, **kwargs
        )

        if schema_type is None:
            return OffsetPagination(
                items=cast("list[dict[str, Any]]", items),
                limit=limit_offset.limit if limit_offset is not None else len(items),
                offset=limit_offset.offset if limit_offset is not None else 0,
                total=total,
            )

        return OffsetPagination(
            items=cast("list[SchemaT]", items),
            limit=limit_offset.limit if limit_offset is not None else len(items),
            offset=limit_offset.offset if limit_offset is not None else 0,
            total=total,
        )

    @overload
    def get_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        error_message: str | None = None,
        **kwargs: Any,
    ) -> SchemaT: ...

    @overload
    def get_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        error_message: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]: ...

    @overload
    def get_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        error_message: str | None = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]": ...

    def get_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        error_message: str | None = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]":
        """Fetch one row or raise :class:`~sqlspec.exceptions.NotFoundError`.

        HTTP status mapping (e.g. translating ``NotFoundError`` to a 404 response)
        is the responsibility of the calling framework integration. The Litestar
        extension registers a default mapping; other framework integrations do
        not.

        Args:
            statement: The SQL statement or QueryBuilder instance.
            *parameters: Statement parameters or filters.
            schema_type: The schema type to map the row to.
            error_message: Optional message for the raised :class:`NotFoundError`.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            The single matched row, mapped to ``schema_type`` when provided.

        Raises:
            NotFoundError: If the query returns zero rows.
        """
        result = self._session.select_one_or_none(statement, *parameters, schema_type=schema_type, **kwargs)
        if result is None:
            raise NotFoundError(error_message or "Record not found")
        return result

    def exists(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        **kwargs: Any,
    ) -> bool:
        """Check if any rows exist for the given query.

        Args:
            statement: The SQL statement or QueryBuilder instance.
            *parameters: Statement parameters or filters.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            True if at least one row exists, False otherwise.
        """
        return self._session.select_one_or_none(statement, *parameters, **kwargs) is not None

    def begin(self) -> None:
        """Begin a database transaction on the underlying session."""
        self._session.begin()

    def commit(self) -> None:
        """Commit the current database transaction."""
        self._session.commit()

    def rollback(self) -> None:
        """Roll back the current database transaction."""
        self._session.rollback()

    @contextmanager
    def begin_transaction(self) -> "Iterator[SyncDriverT]":
        """Context manager that commits on success and rolls back on error.

        Yields:
            The underlying driver session bound to the active transaction.
        """
        self.begin()
        try:
            yield self._session
        except Exception:
            self.rollback()
            raise
        else:
            self.commit()
