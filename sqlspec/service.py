"""Service base classes for SQLSpec application services."""

from contextlib import AbstractAsyncContextManager, AbstractContextManager, nullcontext
from typing import TYPE_CHECKING, Any, Generic, Literal, cast, overload

from mypy_extensions import mypyc_attr
from typing_extensions import TypeVar

from sqlspec.core import OffsetPagination
from sqlspec.core.filters import LimitOffsetFilter
from sqlspec.driver._async import AsyncDriverAdapterBase
from sqlspec.driver._sync import SyncDriverAdapterBase
from sqlspec.exceptions import ImproperConfigurationError, NotFoundError
from sqlspec.typing import SchemaT

if TYPE_CHECKING:
    from types import TracebackType

    from sqlspec.builder import QueryBuilder
    from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig
    from sqlspec.core.filters import StatementFilter
    from sqlspec.core.statement import Statement
    from sqlspec.loader import SQLFileLoader
    from sqlspec.typing import StatementParameters


__all__ = ("SQLSpecAsyncService", "SQLSpecSyncService")

AsyncDriverT = TypeVar("AsyncDriverT", bound=AsyncDriverAdapterBase, default=AsyncDriverAdapterBase)
SyncDriverT = TypeVar("SyncDriverT", bound=SyncDriverAdapterBase, default=SyncDriverAdapterBase)


@mypyc_attr(allow_interpreted_subclasses=True)
class SQLSpecAsyncService(Generic[AsyncDriverT]):
    """Base class for asynchronous SQLSpec services.

    Config-built services acquire and release a short session for each query helper.
    Session-built services borrow the caller's session without closing it.

    Args:
        session: The caller-owned driver session, mutually exclusive with config.
        config: Database configuration used to acquire sessions per helper call.
        loader: Optional SQL file loader to expose without resolving named queries.
    """

    __slots__ = ("_config", "_loader", "_session")

    def __init__(
        self,
        session: AsyncDriverT | None = None,
        *,
        config: "AsyncDatabaseConfig[Any, Any, AsyncDriverT] | NoPoolAsyncConfig[Any, AsyncDriverT] | None" = None,
        loader: "SQLFileLoader | None" = None,
    ) -> None:
        if (session is None) == (config is None):
            msg = "Provide exactly one of session or config."
            raise ImproperConfigurationError(msg)
        if config is not None and not config.is_async:
            msg = "SQLSpecAsyncService requires an async database config."
            raise ImproperConfigurationError(msg)
        self._session = session
        self._config = config
        self._loader = loader

    @property
    def session(self) -> AsyncDriverT:
        """Return the driver session."""
        if self._session is None:
            msg = "No session is available; use begin_transaction() or pass session=."
            raise ImproperConfigurationError(msg)
        return self._session

    @property
    def driver(self) -> AsyncDriverT:
        """Alias for :attr:`session` matching the recipe-doc terminology."""
        return self.session

    @property
    def loader(self) -> "SQLFileLoader | None":
        """Return the optional SQL file loader."""
        return self._loader

    def provide_session(self, session: AsyncDriverT | None = None) -> AbstractAsyncContextManager[AsyncDriverT]:
        """Borrow an available session or acquire a short config-owned session.

        Args:
            session: Caller-owned override, which this context does not close.

        Returns:
            A context yielding the driver and releasing only an acquired session.
        """
        if session is not None:
            return nullcontext(session)
        if self._config is not None:
            return self._config.provide_session()
        return nullcontext(self.session)

    @overload
    async def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        count_with_window: bool = False,
        session: AsyncDriverT | None = None,
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
        session: AsyncDriverT | None = None,
        **kwargs: Any,
    ) -> OffsetPagination[dict[str, Any]]: ...

    async def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        count_with_window: bool = False,
        session: AsyncDriverT | None = None,
        **kwargs: Any,
    ) -> "OffsetPagination[SchemaT] | OffsetPagination[dict[str, Any]]":
        """Execute a paginated query and return an OffsetPagination container.

        Args:
            statement: The SQL statement or QueryBuilder instance.
            *parameters: Statement parameters or filters.
            schema_type: The schema type to map results to.
            count_with_window: Whether to use COUNT(*) OVER() for total count.
            session: Caller-owned driver override; no new session is acquired.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            An OffsetPagination instance containing items and total count.
        """
        async with self.provide_session(session) as driver:
            limit_offset: LimitOffsetFilter | None = driver.find_filter(LimitOffsetFilter, parameters)

            items, total = await driver.select_with_total(
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
        session: AsyncDriverT | None = None,
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
        session: AsyncDriverT | None = None,
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
        session: AsyncDriverT | None = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]": ...

    async def get_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        error_message: str | None = None,
        session: AsyncDriverT | None = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]":
        """Fetch one row or raise :class:`~sqlspec.exceptions.NotFoundError`.

        HTTP status mapping
        is the responsibility of the calling framework integration. The Litestar
        extension registers a default mapping; other framework integrations do
        not.

        Args:
            statement: The SQL statement or QueryBuilder instance.
            *parameters: Statement parameters or filters.
            schema_type: The schema type to map the row to.
            error_message: Optional message for the raised :class:`NotFoundError`.
            session: Caller-owned driver override; no new session is acquired.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            The single matched row, mapped to ``schema_type`` when provided.

        Raises:
            NotFoundError: If the query returns zero rows.
        """
        async with self.provide_session(session) as driver:
            result = await driver.select_one_or_none(statement, *parameters, schema_type=schema_type, **kwargs)
            if result is None:
                raise NotFoundError(error_message or "Record not found")
            return result

    async def exists(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        session: AsyncDriverT | None = None,
        **kwargs: Any,
    ) -> bool:
        """Check if any rows exist for the given query.

        Args:
            statement: The SQL statement or QueryBuilder instance.
            *parameters: Statement parameters or filters.
            session: Caller-owned driver override; no new session is acquired.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            True if at least one row exists, False otherwise.
        """
        async with self.provide_session(session) as driver:
            return await driver.select_one_or_none(statement, *parameters, **kwargs) is not None

    async def begin(self, *, session: AsyncDriverT | None = None) -> None:
        """Begin a database transaction on the underlying session.

        Args:
            session: Caller-owned driver override for manual transaction control.
        """
        await (session if session is not None else self.session).begin()

    async def commit(self, *, session: AsyncDriverT | None = None) -> None:
        """Commit the current database transaction.

        Args:
            session: Caller-owned driver override for manual transaction control.
        """
        await (session if session is not None else self.session).commit()

    async def rollback(self, *, session: AsyncDriverT | None = None) -> None:
        """Roll back the current database transaction.

        Args:
            session: Caller-owned driver override for manual transaction control.
        """
        await (session if session is not None else self.session).rollback()

    def begin_transaction(self) -> "_AsyncBeginTransactionContext[AsyncDriverT]":
        """Context manager that commits on success and rolls back on error.

        Returns:
            The underlying driver session bound to the active transaction.
        """
        return _AsyncBeginTransactionContext(self)


@mypyc_attr(allow_interpreted_subclasses=True)
class SQLSpecSyncService(Generic[SyncDriverT]):
    """Base class for synchronous SQLSpec services.

    Config-built services acquire and release a short session for each query helper.
    Session-built services borrow the caller's session without closing it.

    Args:
        session: The caller-owned driver session, mutually exclusive with config.
        config: Database configuration used to acquire sessions per helper call.
        loader: Optional SQL file loader to expose without resolving named queries.
    """

    __slots__ = ("_config", "_loader", "_session")

    def __init__(
        self,
        session: SyncDriverT | None = None,
        *,
        config: "SyncDatabaseConfig[Any, Any, SyncDriverT] | NoPoolSyncConfig[Any, SyncDriverT] | None" = None,
        loader: "SQLFileLoader | None" = None,
    ) -> None:
        if (session is None) == (config is None):
            msg = "Provide exactly one of session or config."
            raise ImproperConfigurationError(msg)
        if config is not None and config.is_async:
            msg = "SQLSpecSyncService requires a sync database config."
            raise ImproperConfigurationError(msg)
        self._session = session
        self._config = config
        self._loader = loader

    @property
    def session(self) -> SyncDriverT:
        """Return the driver session."""
        if self._session is None:
            msg = "No session is available; use begin_transaction() or pass session=."
            raise ImproperConfigurationError(msg)
        return self._session

    @property
    def driver(self) -> SyncDriverT:
        """Alias for :attr:`session` matching the recipe-doc terminology."""
        return self.session

    @property
    def loader(self) -> "SQLFileLoader | None":
        """Return the optional SQL file loader."""
        return self._loader

    def provide_session(self, session: SyncDriverT | None = None) -> AbstractContextManager[SyncDriverT]:
        """Borrow an available session or acquire a short config-owned session.

        Args:
            session: Caller-owned override, which this context does not close.

        Returns:
            A context yielding the driver and releasing only an acquired session.
        """
        if session is not None:
            return nullcontext(session)
        if self._config is not None:
            return self._config.provide_session()
        return nullcontext(self.session)

    @overload
    def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        count_with_window: bool = False,
        session: SyncDriverT | None = None,
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
        session: SyncDriverT | None = None,
        **kwargs: Any,
    ) -> OffsetPagination[dict[str, Any]]: ...

    def paginate(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        count_with_window: bool = False,
        session: SyncDriverT | None = None,
        **kwargs: Any,
    ) -> "OffsetPagination[SchemaT] | OffsetPagination[dict[str, Any]]":
        """Execute a paginated query and return an OffsetPagination container.

        Args:
            statement: The SQL statement or QueryBuilder instance.
            *parameters: Statement parameters or filters.
            schema_type: The schema type to map results to.
            count_with_window: Whether to use COUNT(*) OVER() for total count.
            session: Caller-owned driver override; no new session is acquired.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            An OffsetPagination instance containing items and total count.
        """
        with self.provide_session(session) as driver:
            limit_offset: LimitOffsetFilter | None = driver.find_filter(LimitOffsetFilter, parameters)

            items, total = driver.select_with_total(
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
        session: SyncDriverT | None = None,
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
        session: SyncDriverT | None = None,
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
        session: SyncDriverT | None = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]": ...

    def get_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        error_message: str | None = None,
        session: SyncDriverT | None = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]":
        """Fetch one row or raise :class:`~sqlspec.exceptions.NotFoundError`.

        HTTP status mapping
        is the responsibility of the calling framework integration. The Litestar
        extension registers a default mapping; other framework integrations do
        not.

        Args:
            statement: The SQL statement or QueryBuilder instance.
            *parameters: Statement parameters or filters.
            schema_type: The schema type to map the row to.
            error_message: Optional message for the raised :class:`NotFoundError`.
            session: Caller-owned driver override; no new session is acquired.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            The single matched row, mapped to ``schema_type`` when provided.

        Raises:
            NotFoundError: If the query returns zero rows.
        """
        with self.provide_session(session) as driver:
            result = driver.select_one_or_none(statement, *parameters, schema_type=schema_type, **kwargs)
            if result is None:
                raise NotFoundError(error_message or "Record not found")
            return result

    def exists(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        session: SyncDriverT | None = None,
        **kwargs: Any,
    ) -> bool:
        """Check if any rows exist for the given query.

        Args:
            statement: The SQL statement or QueryBuilder instance.
            *parameters: Statement parameters or filters.
            session: Caller-owned driver override; no new session is acquired.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            True if at least one row exists, False otherwise.
        """
        with self.provide_session(session) as driver:
            return driver.select_one_or_none(statement, *parameters, **kwargs) is not None

    def begin(self, *, session: SyncDriverT | None = None) -> None:
        """Begin a database transaction on the underlying session.

        Args:
            session: Caller-owned driver override for manual transaction control.
        """
        (session if session is not None else self.session).begin()

    def commit(self, *, session: SyncDriverT | None = None) -> None:
        """Commit the current database transaction.

        Args:
            session: Caller-owned driver override for manual transaction control.
        """
        (session if session is not None else self.session).commit()

    def rollback(self, *, session: SyncDriverT | None = None) -> None:
        """Roll back the current database transaction.

        Args:
            session: Caller-owned driver override for manual transaction control.
        """
        (session if session is not None else self.session).rollback()

    def begin_transaction(self) -> "_SyncBeginTransactionContext[SyncDriverT]":
        """Context manager that commits on success and rolls back on error.

        Returns:
            The underlying driver session bound to the active transaction.
        """
        return _SyncBeginTransactionContext(self)


class _AsyncBeginTransactionContext(Generic[AsyncDriverT]):
    __slots__ = ("_service",)

    def __init__(self, service: "SQLSpecAsyncService[AsyncDriverT]") -> None:
        self._service = service

    async def __aenter__(self) -> AsyncDriverT:
        service = self._service
        await service.begin()
        return service.session

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc: "BaseException | None", traceback: "TracebackType | None"
    ) -> "Literal[False]":
        service = self._service
        if exc_type is None:
            await service.commit()
        else:
            await service.rollback()
        return False


class _SyncBeginTransactionContext(Generic[SyncDriverT]):
    __slots__ = ("_service",)

    def __init__(self, service: "SQLSpecSyncService[SyncDriverT]") -> None:
        self._service = service

    def __enter__(self) -> SyncDriverT:
        service = self._service
        service.begin()
        return service.session

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc: "BaseException | None", traceback: "TracebackType | None"
    ) -> "Literal[False]":
        service = self._service
        if exc_type is None:
            service.commit()
        else:
            service.rollback()
        return False
