"""Compiled query and transaction runtime for the Python service bases."""

import asyncio
import sys
from contextlib import AbstractAsyncContextManager, AbstractContextManager, AsyncExitStack, ExitStack
from contextvars import ContextVar, Token
from threading import get_ident
from typing import TYPE_CHECKING, Any, Generic, Literal, cast

from typing_extensions import TypeVar

from sqlspec.core import SQL, CursorFilter, CursorPagination, LimitOffsetFilter, OffsetPagination, OrderByFilter
from sqlspec.core.filters import find_filter
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.exceptions import ImproperConfigurationError, NotFoundError
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from types import TracebackType

    from sqlspec.builder import QueryBuilder
    from sqlspec.core.filters import StatementFilter
    from sqlspec.core.statement import Statement
    from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService
    from sqlspec.typing import SchemaT, StatementParameters

AsyncDriverT = TypeVar("AsyncDriverT", bound=AsyncDriverAdapterBase, default=AsyncDriverAdapterBase)
SyncDriverT = TypeVar("SyncDriverT", bound=SyncDriverAdapterBase, default=SyncDriverAdapterBase)

logger = get_logger("sqlspec.service")


def _execution_owner() -> tuple[int, object]:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return get_ident(), task


def _owner_identity(owner: tuple[int, object]) -> tuple[int, int]:
    return owner[0], id(owner[1])


class _TransactionState:
    __slots__ = ("driver", "origin", "owner")

    def __init__(self, driver: AsyncDriverAdapterBase | SyncDriverAdapterBase) -> None:
        self.driver: AsyncDriverAdapterBase | SyncDriverAdapterBase | None = driver
        self.owner = _execution_owner()
        self.origin = _owner_identity(self.owner)


_TRANSACTIONS: ContextVar[dict[object, _TransactionState] | None] = ContextVar(
    "sqlspec_service_transactions", default=None
)


async def _async_paginate(
    context: AbstractAsyncContextManager[AsyncDriverAdapterBase],
    statement: "Statement | QueryBuilder",
    parameters: "tuple[StatementParameters | StatementFilter, ...]",
    schema_type: "type[SchemaT] | None",
    count_with_window: bool,
    kwargs: dict[str, Any],
    mode: Literal["limit_offset", "cursor"] | None = None,
) -> "OffsetPagination[SchemaT] | OffsetPagination[dict[str, Any]] | CursorPagination[SchemaT] | CursorPagination[dict[str, Any]]":
    """Execute the async service paginate operation in the supplied session context."""
    cursor_parameters = _cursor_pagination_parameters(statement, parameters)
    if mode == "cursor" and cursor_parameters is None:
        msg = "paginate_cursor() requires a CursorFilter"
        raise ImproperConfigurationError(msg)
    if mode == "limit_offset" and cursor_parameters is not None:
        msg = "paginate_limit_offset() does not accept CursorFilter"
        raise ImproperConfigurationError(msg)
    if cursor_parameters is not None:
        if count_with_window:
            msg = "count_with_window is incompatible with cursor pagination"
            raise ImproperConfigurationError(msg)
        statement, parameters = cursor_parameters
        return await _async_paginate_cursor(context, statement, parameters, schema_type, kwargs)
    async with context as driver:
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


def _cursor_pagination_parameters(
    statement: "Statement | QueryBuilder", parameters: "tuple[StatementParameters | StatementFilter, ...]"
) -> "tuple[Statement | QueryBuilder, tuple[StatementParameters | StatementFilter, ...]] | None":
    pending = statement.filters if isinstance(statement, SQL) else []
    filters = (*pending, *parameters)
    cursor_filter = find_filter(CursorFilter, filters)
    if cursor_filter is None:
        return None
    if any(isinstance(value, (LimitOffsetFilter, OrderByFilter)) for value in filters):
        msg = "CursorFilter owns ordering and limits; remove OrderByFilter/LimitOffsetFilter"
        raise ImproperConfigurationError(msg)
    if sum(isinstance(value, CursorFilter) for value in filters) != 1:
        msg = "paginate() requires exactly one CursorFilter"
        raise ImproperConfigurationError(msg)
    if pending and isinstance(statement, SQL):
        statement, _ = statement._take_pending_filters()
    return statement, (*(value for value in filters if not isinstance(value, CursorFilter)), cursor_filter)


async def _async_paginate_cursor(
    context: AbstractAsyncContextManager[AsyncDriverAdapterBase],
    statement: "Statement | QueryBuilder",
    parameters: "tuple[StatementParameters | StatementFilter, ...]",
    schema_type: "type[SchemaT] | None",
    kwargs: dict[str, Any],
) -> "CursorPagination[SchemaT] | CursorPagination[dict[str, Any]]":
    """Execute cursor pagination inside the supplied service session context."""
    async with context as driver:
        cursor_filter = cast("CursorFilter", parameters[-1])
        items = await driver.select(statement, *parameters, **kwargs)
        page = cursor_filter.build_page(items)
        if schema_type is None:
            return page
        return CursorPagination(
            items=cast("list[SchemaT]", driver.to_schema(list(page.items), schema_type=schema_type)),
            limit=page.limit,
            next_cursor=page.next_cursor,
            previous_cursor=page.previous_cursor,
            has_next=page.has_next,
            has_previous=page.has_previous,
        )


async def _async_get_one(
    context: AbstractAsyncContextManager[AsyncDriverAdapterBase],
    statement: "Statement | QueryBuilder",
    parameters: "tuple[StatementParameters | StatementFilter, ...]",
    schema_type: "type[SchemaT] | None",
    error_message: str | None,
    kwargs: dict[str, Any],
) -> "SchemaT | dict[str, Any]":
    """Execute the async service get_one operation in the supplied session context."""
    async with context as driver:
        result = await driver.select_one_or_none(statement, *parameters, schema_type=schema_type, **kwargs)
        if result is None:
            raise NotFoundError(error_message or "Record not found")
        return result


async def _async_exists(
    context: AbstractAsyncContextManager[AsyncDriverAdapterBase],
    statement: "Statement | QueryBuilder",
    parameters: "tuple[StatementParameters | StatementFilter, ...]",
    kwargs: dict[str, Any],
) -> bool:
    """Execute the async service exists operation in the supplied session context."""
    async with context as driver:
        return await driver.select_one_or_none(statement, *parameters, **kwargs) is not None


def _sync_paginate(
    context: AbstractContextManager[SyncDriverAdapterBase],
    statement: "Statement | QueryBuilder",
    parameters: "tuple[StatementParameters | StatementFilter, ...]",
    schema_type: "type[SchemaT] | None",
    count_with_window: bool,
    kwargs: dict[str, Any],
    mode: Literal["limit_offset", "cursor"] | None = None,
) -> "OffsetPagination[SchemaT] | OffsetPagination[dict[str, Any]] | CursorPagination[SchemaT] | CursorPagination[dict[str, Any]]":
    """Execute the sync service paginate operation in the supplied session context."""
    cursor_parameters = _cursor_pagination_parameters(statement, parameters)
    if mode == "cursor" and cursor_parameters is None:
        msg = "paginate_cursor() requires a CursorFilter"
        raise ImproperConfigurationError(msg)
    if mode == "limit_offset" and cursor_parameters is not None:
        msg = "paginate_limit_offset() does not accept CursorFilter"
        raise ImproperConfigurationError(msg)
    if cursor_parameters is not None:
        if count_with_window:
            msg = "count_with_window is incompatible with cursor pagination"
            raise ImproperConfigurationError(msg)
        statement, parameters = cursor_parameters
        return _sync_paginate_cursor(context, statement, parameters, schema_type, kwargs)
    with context as driver:
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


def _sync_paginate_cursor(
    context: AbstractContextManager[SyncDriverAdapterBase],
    statement: "Statement | QueryBuilder",
    parameters: "tuple[StatementParameters | StatementFilter, ...]",
    schema_type: "type[SchemaT] | None",
    kwargs: dict[str, Any],
) -> "CursorPagination[SchemaT] | CursorPagination[dict[str, Any]]":
    """Execute cursor pagination inside the supplied service session context."""
    with context as driver:
        cursor_filter = cast("CursorFilter", parameters[-1])
        items = driver.select(statement, *parameters, **kwargs)
        page = cursor_filter.build_page(items)
        if schema_type is None:
            return page
        return CursorPagination(
            items=cast("list[SchemaT]", driver.to_schema(list(page.items), schema_type=schema_type)),
            limit=page.limit,
            next_cursor=page.next_cursor,
            previous_cursor=page.previous_cursor,
            has_next=page.has_next,
            has_previous=page.has_previous,
        )


def _sync_get_one(
    context: AbstractContextManager[SyncDriverAdapterBase],
    statement: "Statement | QueryBuilder",
    parameters: "tuple[StatementParameters | StatementFilter, ...]",
    schema_type: "type[SchemaT] | None",
    error_message: str | None,
    kwargs: dict[str, Any],
) -> "SchemaT | dict[str, Any]":
    """Execute the sync service get_one operation in the supplied session context."""
    with context as driver:
        result = driver.select_one_or_none(statement, *parameters, schema_type=schema_type, **kwargs)
        if result is None:
            raise NotFoundError(error_message or "Record not found")
        return result


def _sync_exists(
    context: AbstractContextManager[SyncDriverAdapterBase],
    statement: "Statement | QueryBuilder",
    parameters: "tuple[StatementParameters | StatementFilter, ...]",
    kwargs: dict[str, Any],
) -> bool:
    """Execute the sync service exists operation in the supplied session context."""
    with context as driver:
        return driver.select_one_or_none(statement, *parameters, **kwargs) is not None


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
