"""Service base classes for SQLSpec application services."""

import asyncio
import sys
from contextlib import AbstractAsyncContextManager, AbstractContextManager, AsyncExitStack, ExitStack, nullcontext
from contextvars import ContextVar, Token
from threading import get_ident
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


class _TransactionState:
    __slots__ = ("driver", "owner")

    def __init__(self, driver: AsyncDriverAdapterBase | SyncDriverAdapterBase) -> None:
        self.driver: AsyncDriverAdapterBase | SyncDriverAdapterBase | None = driver
        self.owner = _execution_owner()


_TRANSACTIONS: ContextVar[dict[object, _TransactionState] | None] = ContextVar(
    "sqlspec_service_transactions", default=None
)


def _execution_owner() -> tuple[int, object]:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return get_ident(), task


def _transaction_session(key: object) -> AsyncDriverAdapterBase | SyncDriverAdapterBase | None:
    state = (_TRANSACTIONS.get() or {}).get(key)
    if state is None:
        return None
    if state.driver is None:
        msg = "The inherited service transaction is no longer active."
        raise ImproperConfigurationError(msg)
    if state.owner != _execution_owner():
        msg = "A service transaction cannot be implicitly reused by another task or thread; pass session= explicitly."
        raise ImproperConfigurationError(msg)
    return state.driver


def _bind_transaction(
    key: object, driver: AsyncDriverAdapterBase | SyncDriverAdapterBase
) -> tuple[_TransactionState, Token[dict[object, _TransactionState] | None]]:
    state = _TransactionState(driver)
    token = _TRANSACTIONS.set({**(_TRANSACTIONS.get() or {}), key: state})
    return state, token


def _release_transaction(state: _TransactionState, token: Token[dict[object, _TransactionState] | None]) -> None:
    state.driver = None
    state.owner = (0, None)
    _TRANSACTIONS.reset(token)


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

    __slots__ = ("_config", "_loader", "_session", "_transaction_key")

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
        self._transaction_key = object()

    @property
    def session(self) -> AsyncDriverT:
        """Return the driver session."""
        active = _transaction_session(self._transaction_key)
        if active is not None:
            return cast("AsyncDriverT", active)
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
        active = _transaction_session(self._transaction_key)
        if active is not None:
            return nullcontext(cast("AsyncDriverT", active))
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

    __slots__ = ("_config", "_loader", "_session", "_transaction_key")

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
        self._transaction_key = object()

    @property
    def session(self) -> SyncDriverT:
        """Return the driver session."""
        active = _transaction_session(self._transaction_key)
        if active is not None:
            return cast("SyncDriverT", active)
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
        active = _transaction_session(self._transaction_key)
        if active is not None:
            return nullcontext(cast("SyncDriverT", active))
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
    __slots__ = ("_service", "_stack")

    def __init__(self, service: "SQLSpecAsyncService[AsyncDriverT]") -> None:
        self._service = service
        self._stack: AsyncExitStack | None = None

    async def __aenter__(self) -> AsyncDriverT:
        service = self._service
        if service._config is None:
            await service.begin()
            return service.session
        if _transaction_session(service._transaction_key) is not None:
            msg = "Nested config-service transactions are not supported."
            raise ImproperConfigurationError(msg)
        stack = AsyncExitStack()
        try:
            driver = await stack.enter_async_context(service.provide_session())
            await driver.begin()
            state, token = _bind_transaction(service._transaction_key, driver)
            stack.callback(_release_transaction, state, token)
        except BaseException:
            await stack.__aexit__(*sys.exc_info())
            raise
        self._stack = stack
        return driver

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc: "BaseException | None", traceback: "TracebackType | None"
    ) -> "Literal[False]":
        service = self._service
        stack = self._stack
        if stack is None:
            if exc_type is None:
                await service.commit()
            else:
                await service.rollback()
            return False
        self._stack = None
        try:
            if exc_type is None:
                await service.session.commit()
            else:
                await service.session.rollback()
        except BaseException:
            await stack.__aexit__(*sys.exc_info())
            raise
        await stack.__aexit__(exc_type, exc, traceback)
        return False


class _SyncBeginTransactionContext(Generic[SyncDriverT]):
    __slots__ = ("_service", "_stack")

    def __init__(self, service: "SQLSpecSyncService[SyncDriverT]") -> None:
        self._service = service
        self._stack: ExitStack | None = None

    def __enter__(self) -> SyncDriverT:
        service = self._service
        if service._config is None:
            service.begin()
            return service.session
        if _transaction_session(service._transaction_key) is not None:
            msg = "Nested config-service transactions are not supported."
            raise ImproperConfigurationError(msg)
        stack = ExitStack()
        try:
            driver = stack.enter_context(service.provide_session())
            driver.begin()
            state, token = _bind_transaction(service._transaction_key, driver)
            stack.callback(_release_transaction, state, token)
        except BaseException:
            stack.__exit__(*sys.exc_info())
            raise
        self._stack = stack
        return driver

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc: "BaseException | None", traceback: "TracebackType | None"
    ) -> "Literal[False]":
        service = self._service
        stack = self._stack
        if stack is None:
            if exc_type is None:
                service.commit()
            else:
                service.rollback()
            return False
        self._stack = None
        try:
            if exc_type is None:
                service.session.commit()
            else:
                service.session.rollback()
        except BaseException:
            stack.__exit__(*sys.exc_info())
            raise
        stack.__exit__(exc_type, exc, traceback)
        return False
