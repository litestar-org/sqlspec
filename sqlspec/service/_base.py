"""Service base classes for SQLSpec application services."""

from contextlib import AbstractAsyncContextManager, AbstractContextManager, nullcontext
from typing import TYPE_CHECKING, Any, Generic, cast, overload

from typing_extensions import TypeVar

from sqlspec.core import CursorPagination, OffsetPagination
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.service._core import (
    _async_exists,
    _async_get_one,
    _async_paginate,
    _async_paginate_cursor,
    _AsyncBeginTransactionContext,
    _sync_exists,
    _sync_get_one,
    _sync_paginate,
    _sync_paginate_cursor,
    _SyncBeginTransactionContext,
    _transaction_session,
    _TransactionState,
)
from sqlspec.typing import SchemaT

if TYPE_CHECKING:
    from sqlspec.builder import QueryBuilder
    from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig
    from sqlspec.core.filters import StatementFilter
    from sqlspec.core.statement import Statement
    from sqlspec.loader import SQLFileLoader
    from sqlspec.typing import StatementParameters


__all__ = ("SQLSpecAsyncService", "SQLSpecSyncService")

AsyncDriverT = TypeVar("AsyncDriverT", bound=AsyncDriverAdapterBase, default=AsyncDriverAdapterBase)
SyncDriverT = TypeVar("SyncDriverT", bound=SyncDriverAdapterBase, default=SyncDriverAdapterBase)


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
        return await _async_paginate(
            self.provide_session(session), statement, parameters, schema_type, count_with_window, kwargs
        )

    @overload
    async def paginate_cursor(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        session: AsyncDriverT | None = None,
        **kwargs: Any,
    ) -> "CursorPagination[SchemaT]": ...

    @overload
    async def paginate_cursor(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        session: AsyncDriverT | None = None,
        **kwargs: Any,
    ) -> "CursorPagination[dict[str, Any]]": ...

    async def paginate_cursor(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        session: AsyncDriverT | None = None,
        **kwargs: Any,
    ) -> "CursorPagination[SchemaT] | CursorPagination[dict[str, Any]]":
        """Execute a cursor-paginated query.

        Args:
            statement: SQL statement or query builder instance.
            *parameters: Statement parameters or filters; must include one CursorFilter.
            schema_type: Schema type to map results to.
            session: Caller-owned driver override; no new session is acquired.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            A CursorPagination instance containing items and page cursors.
        """
        return await _async_paginate_cursor(self.provide_session(session), statement, parameters, schema_type, kwargs)

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
        return await _async_get_one(
            self.provide_session(session), statement, parameters, schema_type, error_message, kwargs
        )

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
        return await _async_exists(self.provide_session(session), statement, parameters, kwargs)

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
        return _sync_paginate(
            self.provide_session(session), statement, parameters, schema_type, count_with_window, kwargs
        )

    @overload
    def paginate_cursor(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        session: SyncDriverT | None = None,
        **kwargs: Any,
    ) -> "CursorPagination[SchemaT]": ...

    @overload
    def paginate_cursor(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        session: SyncDriverT | None = None,
        **kwargs: Any,
    ) -> "CursorPagination[dict[str, Any]]": ...

    def paginate_cursor(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        session: SyncDriverT | None = None,
        **kwargs: Any,
    ) -> "CursorPagination[SchemaT] | CursorPagination[dict[str, Any]]":
        """Execute a cursor-paginated query.

        Args:
            statement: SQL statement or query builder instance.
            *parameters: Statement parameters or filters; must include one CursorFilter.
            schema_type: Schema type to map results to.
            session: Caller-owned driver override; no new session is acquired.
            **kwargs: Additional keyword arguments for the driver.

        Returns:
            A CursorPagination instance containing items and page cursors.
        """
        return _sync_paginate_cursor(self.provide_session(session), statement, parameters, schema_type, kwargs)

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
        return _sync_get_one(self.provide_session(session), statement, parameters, schema_type, error_message, kwargs)

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
        return _sync_exists(self.provide_session(session), statement, parameters, kwargs)

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
