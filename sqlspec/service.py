"""Service base classes for SQLSpec application services."""

from typing import TYPE_CHECKING, Any, Generic

from typing_extensions import TypeVar

from sqlspec.core._pagination import OffsetPagination
from sqlspec.typing import SchemaT

if TYPE_CHECKING:
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

    async def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
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
        # Determine limit and offset from filters if present
        limit: int | None = None
        offset: int | None = None

        from sqlspec.core.filters import LimitOffsetFilter

        for param in parameters:
            if isinstance(param, LimitOffsetFilter):
                limit = param.limit
                offset = param.offset
                break

        # select_with_total returns (list[SchemaT], int)
        session: Any = self.session
        items, total = await session.select_with_total(
            statement, *parameters, schema_type=schema_type, count_with_window=count_with_window, **kwargs
        )

        return OffsetPagination(
            items=items,
            limit=limit if limit is not None else len(items),
            offset=offset if offset is not None else 0,
            total=total,
        )

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
        result = await session.execute(statement, *parameters, **kwargs)
        return result.one_or_none() is not None


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

    def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
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
        limit: int | None = None
        offset: int | None = None

        from sqlspec.core.filters import LimitOffsetFilter

        for param in parameters:
            if isinstance(param, LimitOffsetFilter):
                limit = param.limit
                offset = param.offset
                break

        session: Any = self.session
        items, total = session.select_with_total(
            statement, *parameters, schema_type=schema_type, count_with_window=count_with_window, **kwargs
        )

        return OffsetPagination(
            items=items,
            limit=limit if limit is not None else len(items),
            offset=offset if offset is not None else 0,
            total=total,
        )

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
        result = session.execute(statement, *parameters, **kwargs)
        return result.one_or_none() is not None
