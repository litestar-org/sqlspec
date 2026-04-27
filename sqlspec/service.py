"""Service base classes for SQLSpec application services."""

from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Generic

from typing_extensions import TypeVar

from sqlspec.core._pagination import OffsetPagination
from sqlspec.core.filters import LimitOffsetFilter
from sqlspec.exceptions import NotFoundError
from sqlspec.typing import SchemaT

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlspec.builder import QueryBuilder
    from sqlspec.core.filters import StatementFilter
    from sqlspec.core.parameters import StatementParameters
    from sqlspec.core.statement import Statement


__all__ = ("SQLSpecAsyncService", "SQLSpecSyncService")

DriverT = TypeVar("DriverT")


class SQLSpecAsyncService(Generic[DriverT]):
    """Base class for asynchronous SQLSpec services.

    Provides common database operations and pagination support using a driver session.

    Args:
        session: The driver session instance.
    """

    __slots__ = ("_session",)

    def __init__(self, session: DriverT) -> None:
        self._session = session

    @property
    def session(self) -> DriverT:
        """Return the driver session."""
        return self._session

    @property
    def driver(self) -> DriverT:
        """Alias for :attr:`session` matching the recipe-doc terminology."""
        return self._session

    async def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> OffsetPagination[SchemaT]:
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
        session: Any = self.session
        limit_offset: LimitOffsetFilter | None = session.find_filter(LimitOffsetFilter, parameters)

        items, total = await session.select_with_total(
            statement, *parameters, schema_type=schema_type, count_with_window=count_with_window, **kwargs
        )

        return OffsetPagination(
            items=items,
            limit=limit_offset.limit if limit_offset is not None else len(items),
            offset=limit_offset.offset if limit_offset is not None else 0,
            total=total,
        )

    async def get_or_404(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        error_message: str | None = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]":
        """Fetch one row or raise :class:`~sqlspec.exceptions.NotFoundError`.

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
        session: Any = self.session
        result = await session.select_one_or_none(statement, *parameters, schema_type=schema_type, **kwargs)
        if result is None:
            raise NotFoundError(error_message or "Record not found")
        return result  # type: ignore[no-any-return]

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
        session: Any = self.session
        return await session.select_one_or_none(statement, *parameters, **kwargs) is not None

    async def begin(self) -> None:
        """Begin a database transaction on the underlying session."""
        session: Any = self.session
        await session.begin()

    async def commit(self) -> None:
        """Commit the current database transaction."""
        session: Any = self.session
        await session.commit()

    async def rollback(self) -> None:
        """Roll back the current database transaction."""
        session: Any = self.session
        await session.rollback()

    @asynccontextmanager
    async def begin_transaction(self) -> "AsyncIterator[DriverT]":
        """Context manager that commits on success and rolls back on error."""
        await self.begin()
        try:
            yield self._session
        except Exception:
            await self.rollback()
            raise
        else:
            await self.commit()


class SQLSpecSyncService(Generic[DriverT]):
    """Base class for synchronous SQLSpec services.

    Provides common database operations and pagination support using a driver session.

    Args:
        session: The driver session instance.
    """

    __slots__ = ("_session",)

    def __init__(self, session: DriverT) -> None:
        self._session = session

    @property
    def session(self) -> DriverT:
        """Return the driver session."""
        return self._session

    @property
    def driver(self) -> DriverT:
        """Alias for :attr:`session` matching the recipe-doc terminology."""
        return self._session

    def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> OffsetPagination[SchemaT]:
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
        session: Any = self.session
        limit_offset: LimitOffsetFilter | None = session.find_filter(LimitOffsetFilter, parameters)

        items, total = session.select_with_total(
            statement, *parameters, schema_type=schema_type, count_with_window=count_with_window, **kwargs
        )

        return OffsetPagination(
            items=items,
            limit=limit_offset.limit if limit_offset is not None else len(items),
            offset=limit_offset.offset if limit_offset is not None else 0,
            total=total,
        )

    def get_or_404(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        error_message: str | None = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]":
        """Fetch one row or raise :class:`~sqlspec.exceptions.NotFoundError`.

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
        session: Any = self.session
        result = session.select_one_or_none(statement, *parameters, schema_type=schema_type, **kwargs)
        if result is None:
            raise NotFoundError(error_message or "Record not found")
        return result  # type: ignore[no-any-return]

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
        session: Any = self.session
        return session.select_one_or_none(statement, *parameters, **kwargs) is not None

    def begin(self) -> None:
        """Begin a database transaction on the underlying session."""
        session: Any = self.session
        session.begin()

    def commit(self) -> None:
        """Commit the current database transaction."""
        session: Any = self.session
        session.commit()

    def rollback(self) -> None:
        """Roll back the current database transaction."""
        session: Any = self.session
        session.rollback()

    @contextmanager
    def begin_transaction(self) -> "Iterator[DriverT]":
        """Context manager that commits on success and rolls back on error."""
        self.begin()
        try:
            yield self._session
        except Exception:
            self.rollback()
            raise
        else:
            self.commit()
