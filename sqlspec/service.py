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
from sqlspec.utils.logging import get_logger

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

logger = get_logger("sqlspec.service")


class _TransactionState:
    __slots__ = ("driver", "origin", "owner")

    def __init__(self, driver: AsyncDriverAdapterBase | SyncDriverAdapterBase) -> None:
        self.driver: AsyncDriverAdapterBase | SyncDriverAdapterBase | None = driver
        self.owner = _execution_owner()
        self.origin = _owner_identity(self.owner)


_TRANSACTIONS: ContextVar[dict[object, _TransactionState] | None] = ContextVar(
    "sqlspec_service_transactions", default=None
)


def _execution_owner() -> tuple[int, object]:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return get_ident(), task


def _owner_identity(owner: tuple[int, object]) -> tuple[int, int]:
    return owner[0], id(owner[1])


def _active_transaction(key: object) -> _TransactionState | None:
    state = (_TRANSACTIONS.get() or {}).get(key)
    if state is None:
        return None
    if state.driver is None:
        if state.origin == _owner_identity(_execution_owner()):
            _discard_transaction(state)
            return None
        msg = "The inherited service transaction is no longer active."
        raise ImproperConfigurationError(msg)
    if state.owner != _execution_owner():
        msg = "A service transaction cannot be implicitly reused by another task or thread; pass session= explicitly."
        raise ImproperConfigurationError(msg)
    return state


def _live_transaction(key: object) -> _TransactionState | None:
    current = _TRANSACTIONS.get()
    state = None if current is None else current.get(key)
    if state is None:
        return None
    if state.driver is None:
        _discard_transaction(state)
        return None
    return _active_transaction(key)


def _discard_transaction(state: _TransactionState) -> None:
    current = _TRANSACTIONS.get()
    if current is not None and any(value is state for value in current.values()):
        _TRANSACTIONS.set({key: value for key, value in current.items() if value is not state} or None)


def _session_transaction(state: _TransactionState | None) -> _TransactionState | None:
    if state is None or state.driver is None:
        return None
    if state.owner != _execution_owner():
        msg = (
            "A service transaction is active in another task or thread; nested begin_transaction() blocks must "
            "run in the task or thread that entered the outer block."
        )
        raise ImproperConfigurationError(msg)
    return state


def _connection_in_transaction(driver: AsyncDriverAdapterBase | SyncDriverAdapterBase) -> bool:
    try:
        return driver._connection_in_transaction()
    except NotImplementedError:
        return False


def _transaction_session(key: object) -> AsyncDriverAdapterBase | SyncDriverAdapterBase | None:
    state = _active_transaction(key)
    return None if state is None else state.driver


def _bind_transaction(
    key: object, driver: AsyncDriverAdapterBase | SyncDriverAdapterBase
) -> tuple[_TransactionState, Token[dict[object, _TransactionState] | None]]:
    state = _TransactionState(driver)
    token = _TRANSACTIONS.set({**(_TRANSACTIONS.get() or {}), key: state})
    return state, token


def _release_transaction(state: _TransactionState, token: Token[dict[object, _TransactionState] | None]) -> None:
    state.driver = None
    state.owner = (0, None)
    try:
        _TRANSACTIONS.reset(token)
    except ValueError:
        _discard_transaction(state)


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

    __slots__ = ("_config", "_loader", "_session", "_transaction_key", "_transaction_state")

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
            msg = f"{type(self).__name__} requires an async database config."
            raise ImproperConfigurationError(msg)
        self._session = session
        self._config = config
        self._loader = loader
        self._transaction_key = object()
        self._transaction_state: _TransactionState | None = None

    @property
    def session(self) -> AsyncDriverT:
        """Return the active transaction driver or the constructor session.

        Raises:
            ImproperConfigurationError: If a config-built service has no active transaction.
        """
        if self._session is not None:
            return self._session
        active = _transaction_session(self._transaction_key)
        if active is None:
            msg = "No session is available; use begin_transaction() or pass session=."
            raise ImproperConfigurationError(msg)
        return cast("AsyncDriverT", active)

    @property
    def driver(self) -> AsyncDriverT:
        """Alias for :attr:`session` matching the recipe-doc terminology."""
        return self.session

    @property
    def config(self) -> "AsyncDatabaseConfig[Any, Any, AsyncDriverT] | NoPoolAsyncConfig[Any, AsyncDriverT] | None":
        """Return the database configuration this service was built from, or None for session-bound services."""
        return self._config

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
        if self._session is not None:
            return nullcontext(self._session)
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

        Nested blocks run in a savepoint on the outer session.

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

    __slots__ = ("_config", "_loader", "_session", "_transaction_key", "_transaction_state")

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
            msg = f"{type(self).__name__} requires a sync database config."
            raise ImproperConfigurationError(msg)
        self._session = session
        self._config = config
        self._loader = loader
        self._transaction_key = object()
        self._transaction_state: _TransactionState | None = None

    @property
    def session(self) -> SyncDriverT:
        """Return the active transaction driver or the constructor session.

        Raises:
            ImproperConfigurationError: If a config-built service has no active transaction.
        """
        if self._session is not None:
            return self._session
        active = _transaction_session(self._transaction_key)
        if active is None:
            msg = "No session is available; use begin_transaction() or pass session=."
            raise ImproperConfigurationError(msg)
        return cast("SyncDriverT", active)

    @property
    def driver(self) -> SyncDriverT:
        """Alias for :attr:`session` matching the recipe-doc terminology."""
        return self.session

    @property
    def config(self) -> "SyncDatabaseConfig[Any, Any, SyncDriverT] | NoPoolSyncConfig[Any, SyncDriverT] | None":
        """Return the database configuration this service was built from, or None for session-bound services."""
        return self._config

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
        if self._session is not None:
            return nullcontext(self._session)
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

        Nested blocks run in a savepoint on the outer session.

        Returns:
            The underlying driver session bound to the active transaction.
        """
        return _SyncBeginTransactionContext(self)


class _AsyncBeginTransactionContext(Generic[AsyncDriverT]):
    __slots__ = ("_nested", "_service", "_stack", "_state")

    def __init__(self, service: "SQLSpecAsyncService[AsyncDriverT]") -> None:
        self._service = service
        self._stack: AsyncExitStack | None = None
        self._state: _TransactionState | None = None
        self._nested: Any = None

    async def __aenter__(self) -> AsyncDriverT:
        service = self._service
        if service._config is None:
            active = _session_transaction(service._transaction_state)
            session = service.session
            if active is not None or session._transaction_depth > 0:
                nested = session.transaction()
                await nested.__aenter__()
                self._nested = nested
                return session
            if not _connection_in_transaction(session):
                await service.begin()
            session._transaction_depth += 1
            state = _TransactionState(session)
            service._transaction_state = state
            self._state = state
            return session
        key = service._transaction_key
        live = _live_transaction(key)
        if live is not None:
            driver = cast("AsyncDriverT", live.driver)
            nested = driver.transaction()
            await nested.__aenter__()
            self._nested = nested
            return driver
        stack = AsyncExitStack()
        try:
            driver = await stack.enter_async_context(service.provide_session())
            await stack.enter_async_context(driver.transaction())
            state, token = _bind_transaction(key, driver)
            stack.callback(_release_transaction, state, token)
        except BaseException:
            await stack.__aexit__(*sys.exc_info())
            raise
        self._stack = stack
        self._state = state
        return driver

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc: "BaseException | None", traceback: "TracebackType | None"
    ) -> "Literal[False]":
        service = self._service
        nested = self._nested
        state = self._state
        stack = self._stack
        self._nested = None
        self._state = None
        self._stack = None
        if nested is not None:
            await nested.__aexit__(exc_type, exc, traceback)
            return False
        if stack is not None:
            await stack.__aexit__(exc_type, exc, traceback)
            return False
        if state is None:
            return False
        session = cast("AsyncDriverAdapterBase", state.driver)
        try:
            if exc_type is not None:
                await service.rollback()
                return False
            try:
                await service.commit()
            except BaseException:
                try:
                    await service.rollback()
                except Exception:
                    logger.debug("Rollback after a failed commit also failed", exc_info=True)
                raise
            return False
        finally:
            if session._transaction_depth > 0:
                session._transaction_depth -= 1
            state.driver = None
            if service._transaction_state is state:
                service._transaction_state = None


class _SyncBeginTransactionContext(Generic[SyncDriverT]):
    __slots__ = ("_nested", "_service", "_stack", "_state")

    def __init__(self, service: "SQLSpecSyncService[SyncDriverT]") -> None:
        self._service = service
        self._stack: ExitStack | None = None
        self._state: _TransactionState | None = None
        self._nested: Any = None

    def __enter__(self) -> SyncDriverT:
        service = self._service
        if service._config is None:
            active = _session_transaction(service._transaction_state)
            session = service.session
            if active is not None or session._transaction_depth > 0:
                nested = session.transaction()
                nested.__enter__()
                self._nested = nested
                return session
            if not _connection_in_transaction(session):
                service.begin()
            session._transaction_depth += 1
            state = _TransactionState(session)
            service._transaction_state = state
            self._state = state
            return session
        key = service._transaction_key
        live = _live_transaction(key)
        if live is not None:
            driver = cast("SyncDriverT", live.driver)
            nested = driver.transaction()
            nested.__enter__()
            self._nested = nested
            return driver
        stack = ExitStack()
        try:
            driver = stack.enter_context(service.provide_session())
            stack.enter_context(driver.transaction())
            state, token = _bind_transaction(key, driver)
            stack.callback(_release_transaction, state, token)
        except BaseException:
            stack.__exit__(*sys.exc_info())
            raise
        self._stack = stack
        self._state = state
        return driver

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc: "BaseException | None", traceback: "TracebackType | None"
    ) -> "Literal[False]":
        service = self._service
        nested = self._nested
        state = self._state
        stack = self._stack
        self._nested = None
        self._state = None
        self._stack = None
        if nested is not None:
            nested.__exit__(exc_type, exc, traceback)
            return False
        if stack is not None:
            stack.__exit__(exc_type, exc, traceback)
            return False
        if state is None:
            return False
        session = cast("SyncDriverAdapterBase", state.driver)
        try:
            if exc_type is not None:
                service.rollback()
                return False
            try:
                service.commit()
            except BaseException:
                try:
                    service.rollback()
                except Exception:
                    logger.debug("Rollback after a failed commit also failed", exc_info=True)
                raise
            return False
        finally:
            if session._transaction_depth > 0:
                session._transaction_depth -= 1
            state.driver = None
            if service._transaction_state is state:
                service._transaction_state = None
