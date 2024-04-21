from __future__ import annotations

from types import MethodType
from typing import TYPE_CHECKING, Any, Self, cast

from sqlspec.types.protocols import DriverAdapterProtocol, QueryDataTree, QueryDatum, QueryFn, SQLOperationType

if TYPE_CHECKING:
    import inspect
    from collections.abc import Callable
    from pathlib import Path


class Queries:
    """Container object with dynamic methods built from SQL queries.

    The ``-- name:`` definition comments in the content of the SQL determine what the dynamic
    methods of this class will be named.

    Parameters:

    - driver_adapter: Either a string to designate one of the aiosql built-in database driver
      adapters (e.g. "sqlite3", "psycopg").
      If you have defined your own adapter class, you can pass its constructor.
    - kwargs_only: whether to reject positional parameters.
    """

    def __init__(self, driver_adapter: DriverAdapterProtocol, kwargs_only: bool = False) -> None:
        self.driver_adapter: DriverAdapterProtocol = driver_adapter
        self.is_asyncio: bool = getattr(driver_adapter, "is_asyncio", False)
        self._kwargs_only = kwargs_only
        self._available_queries: set[str] = set()

    #
    # INTERNAL UTILS
    #
    def _params(
        self,
        args: list[Any] | tuple[Any],
        kwargs: dict[str, Any],
    ) -> list[Any] | tuple[Any] | dict[str, Any]:
        """Execute parameter handling."""
        if self._kwargs_only and args:
            msg = "cannot use positional parameters under kwargs_only"
            raise ValueError(msg)
        if kwargs and args:
            msg = "cannot mix positional and named parameters in query"
            raise ValueError(msg)
        return args or kwargs

    def _query_fn(  # noqa: PLR6301
        self,
        fn: Callable[..., Any],
        name: str,
        doc: str | None,
        sql: str,
        operation: SQLOperationType,
        signature: inspect.Signature | None,
        floc: tuple[Path | str, int] | None = None,
    ) -> QueryFn:
        """Add custom-made metadata to a dynamically generated function."""
        fname, lineno = floc if floc is not None else ("<unknown>", 0)
        fn.__code__ = fn.__code__.replace(co_filename=str(fname), co_firstlineno=lineno)
        qfn = cast(QueryFn, fn)
        qfn.__name__ = name
        qfn.__doc__ = doc
        qfn.__signature__ = signature
        qfn.sql = sql
        qfn.operation = operation
        return qfn

    # NOTE about coverage: because __code__ is set to reflect the actual SQL file
    # source, coverage does note detect that the "fn" functions are actually called,
    # hence the "no cover" hints.
    def _make_sync_fn(self: Self, query_datum: QueryDatum) -> QueryFn:
        """Build a dynamic method from a parsed query."""
        query_name, doc_comments, operation_type, sql, record_class, signature, floc = query_datum
        if operation_type == SQLOperationType.INSERT_RETURNING:

            def fn(self: Self, conn: Any, *args: Any, kwargs: Any):  # pragma: no cover
                return self.driver_adapter.insert_returning(
                    conn,
                    query_name,
                    sql,
                    self._params(args, kwargs),
                )

        elif operation_type == SQLOperationType.INSERT_UPDATE_DELETE:

            def fn(self: Self, conn: Any, *args: Any, kwargs: Any):  # pragma: no cover
                return self.driver_adapter.insert_update_delete(
                    conn,
                    query_name,
                    sql,
                    self._params(args, kwargs),
                )

        elif operation_type == SQLOperationType.INSERT_UPDATE_DELETE_MANY:

            def fn(self: Self, conn: Any, *args: Any, kwargs: Any):  # pragma: no cover
                assert not kwargs, "cannot use named parameters in many query"  # help type checker
                return self.driver_adapter.insert_update_delete_many(conn, query_name, sql, *args)

        elif operation_type == SQLOperationType.SCRIPT:

            def fn(self: Self, conn: Any, *args: Any, kwargs: Any):  # pragma: no cover
                # FIXME parameters are ignored?
                return self.driver_adapter.execute_script(conn, sql)

        elif operation_type == SQLOperationType.SELECT:

            def fn(self: Self, conn: Any, *args: Any, kwargs: Any):  # pragma: no cover
                return self.driver_adapter.select(
                    conn,
                    query_name,
                    sql,
                    self._params(args, kwargs),
                    record_class,
                )

        elif operation_type == SQLOperationType.SELECT_ONE:

            def fn(self: Self, conn: Any, *args: Any, kwargs: Any):  # pragma: no cover
                return self.driver_adapter.select_one(
                    conn,
                    query_name,
                    sql,
                    self._params(args, kwargs),
                    record_class,
                )

        elif operation_type == SQLOperationType.SELECT_VALUE:

            def fn(self: Self, conn: Any, *args: Any, kwargs: Any):  # pragma: no cover
                return self.driver_adapter.select_value(
                    conn,
                    query_name,
                    sql,
                    self._params(args, kwargs),
                )

        else:
            msg = f"Unknown operation_type: {operation_type}"
            raise ValueError(msg)

        return self._query_fn(fn, query_name, doc_comments, sql, operation_type, signature, floc)

    # NOTE does this make sense?
    def _make_async_fn(self: Self, fn: QueryFn) -> QueryFn:
        """Wrap in an async function."""

        async def afn(self: Self, conn: Any, *args: Any, kwargs: Any):  # pragma: no cover
            return await fn(self, conn, *args, kwargs)

        return self._query_fn(afn, fn.__name__, fn.__doc__, fn.sql, fn.operation, fn.__signature__)

    def _make_ctx_mgr(self: Self, fn: QueryFn) -> QueryFn:
        """Wrap in a context manager function."""

        def ctx_mgr(self: Self, conn: Any, *args: Any, kwargs: Any):  # pragma: no cover
            return self.driver_adapter.select_cursor(
                conn,
                fn.__name__,
                fn.sql,
                self._params(args, kwargs),
            )

        return self._query_fn(
            ctx_mgr,
            f"{fn.__name__}_cursor",
            fn.__doc__,
            fn.sql,
            fn.operation,
            fn.__signature__,
        )

    def _create_methods(self, query_datum: QueryDatum, is_aio: bool) -> list[QueryFn]:
        """Internal function to feed add_queries."""
        fn = self._make_sync_fn(query_datum)
        if is_aio:
            fn = self._make_async_fn(fn)

        ctx_mgr = self._make_ctx_mgr(fn)

        if query_datum.operation_type == SQLOperationType.SELECT:
            return [fn, ctx_mgr]
        return [fn]

    #
    # PUBLIC INTERFACE
    #
    @property
    def available_queries(self) -> list[str]:
        """Returns listing of all the available query methods loaded in this class.

        Returns: ``List[str]`` List of dot-separated method accessor names.
        """
        return sorted(self._available_queries)

    def __repr__(self) -> str:
        return f"Queries({self.available_queries.__repr__()})"

    def add_query(self, query_name: str, fn: Callable) -> None:
        """Adds a new dynamic method to this class.

        Parameters:

        query_name - The method name as found in the SQL content.
        fn - The loaded query function.
        """
        setattr(self, query_name, fn)
        self._available_queries.add(query_name)

    def add_queries(self, queries: list[QueryFn]) -> None:
        """Add query methods to `Queries` instance."""
        for fn in queries:
            query_name = fn.__name__.rpartition(".")[2]
            self.add_query(query_name, MethodType(fn, self))

    def add_child_queries(self, child_name: str, child_queries: Queries) -> None:
        """Adds a Queries object as a property.

        Parameters:

        child_name - The property name to group the child queries under.
        child_queries - Queries instance to add as sub-queries.
        """
        setattr(self, child_name, child_queries)
        for child_query_name in child_queries.available_queries:
            self._available_queries.add(f"{child_name}.{child_query_name}")

    def load_from_list(self, query_data: list[QueryDatum]) -> Self:
        """Load Queries from a list of `QueryDatum`"""
        for query_datum in query_data:
            self.add_queries(self._create_methods(query_datum, self.is_asyncio))
        return self

    def load_from_tree(self, query_data_tree: QueryDataTree) -> Self:
        """Load Queries from a `QueryDataTree`"""
        for key, value in query_data_tree.items():
            if isinstance(value, dict):
                self.add_child_queries(key, Queries(self.driver_adapter).load_from_tree(value))
            else:
                self.add_queries(self._create_methods(value, self.is_asyncio))
        return self
