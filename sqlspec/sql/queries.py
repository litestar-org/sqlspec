from __future__ import annotations

from types import MethodType
from typing import TYPE_CHECKING, Any, Callable, cast

from sqlspec.exceptions import SQLLoadingError
from sqlspec.types.protocols import DriverAdapterProtocol, SQLStatements, StatementDetails, StatementFn, StatementType

if TYPE_CHECKING:
    import inspect
    from pathlib import Path


__all__ = ("Statements",)


class Statements:
    """Container object with dynamic methods built from SQL statements.

    The ``-- name:`` definition comments in the content of the SQL determine what the dynamic
    methods of this class will be named.

    Much of the needed pre-processing is performed in ``QueryLoader``.

    **Parameters:**

    - **driver_adapter**: Either a string to designate one of the aiosql built-in database driver
      adapters (e.g. "sqlite3", "psycopg").
      If you have defined your own adapter class, you can pass its constructor.
    - **kwargs_only**: whether to reject positional parameters, defaults to false.
    """

    def __init__(
        self,
        driver_adapter: DriverAdapterProtocol,
        kwargs_only: bool = False,
    ):
        self.driver_adapter: DriverAdapterProtocol = driver_adapter
        self.is_async: bool = getattr(driver_adapter, "is_async", False)
        self._kwargs_only = kwargs_only
        self._available_statements: set[str] = set()

    #
    # INTERNAL UTILS
    #
    def _params(
        self, attributes, args: list[Any] | tuple[Any], kwargs: dict[str, Any]
    ) -> list[Any] | tuple[Any] | dict[str, Any]:
        """Handle query parameters.

        - update attribute references ``:u.a`` to ``:u__a``.
        - check whether non kwargs are allowed and other checks.
        - return the parameters, either ``args`` or ``kwargs``.
        """

        if attributes and kwargs:
            # switch o.a to o<attribute>a
            for var, atts in attributes.items():
                if var not in kwargs:
                    raise ValueError(f"missing named parameter {var}")
                val = kwargs.pop(var)
                for att, var_name in atts.items():
                    if not hasattr(val, att):
                        raise ValueError(f"parameter {var} is missing attribute {att}")
                    kwargs[var_name] = getattr(val, att)

        if self._kwargs_only:
            if args:
                raise ValueError(
                    "cannot use positional parameters under kwargs_only, use named parameters (name=value, …)"
                )
            return kwargs
        if kwargs:
            # FIXME is this true?
            if args:
                raise ValueError("cannot mix positional and named parameters in query")
            return kwargs
        return args

    def _query_fn(
        self,
        fn: Callable[..., Any],
        name: str,
        doc: str | None,
        sql: str,
        operation: StatementType,
        signature: inspect.Signature | None,
        floc: tuple[Path | str, int] = ("<unknown>", 0),
        attributes: dict[str, dict[str, str]] | None = None,
    ) -> StatementFn:
        """Add custom-made metadata to a dynamically generated function."""
        fname, lineno = floc
        fn.__code__ = fn.__code__.replace(co_filename=str(fname), co_firstlineno=lineno)
        qfn = cast(StatementFn, fn)
        qfn.__name__ = name
        qfn.__doc__ = doc
        qfn.__signature__ = signature
        qfn.sql = sql
        qfn.operation = operation
        qfn.attributes = attributes
        return qfn

    # NOTE about coverage: because __code__ is set to reflect the actual SQL file
    # source, coverage does note detect that the "fn" functions are actually called,
    # hence the "no cover" hints.
    def _make_sync_fn(self, statement_details: StatementDetails) -> StatementFn:
        """Build a dynamic method from a parsed query."""

        query_name, doc_comments, operation_type, sql, record_class, signature, floc, attributes = statement_details
        if operation_type == StatementType.INSERT_UPDATE_DELETE:

            def fn(self, conn, *args, **kwargs):  # pragma: no cover
                return self.driver_adapter.insert_update_delete(
                    conn, query_name, sql, self._params(attributes, args, kwargs)
                )
        elif operation_type == StatementType.INSERT_UPDATE_DELETE_RETURNING:

            def fn(self, conn, *args, **kwargs):  # pragma: no cover
                return self.driver_adapter.insert_update_delete_returning(
                    conn, query_name, sql, self._params(attributes, args, kwargs)
                )

        elif operation_type == StatementType.INSERT_UPDATE_DELETE_MANY:

            def fn(self, conn, *args, **kwargs):  # pragma: no cover
                return self.driver_adapter.insert_update_delete_many(conn, query_name, sql, *args)

        elif operation_type == StatementType.INSERT_UPDATE_DELETE_MANY_RETURNING:

            def fn(self, conn, *args, **kwargs):  # pragma: no cover
                return self.driver_adapter.insert_update_delete_many_returning(conn, query_name, sql, *args)

        elif operation_type == StatementType.SCRIPT:

            def fn(self, conn, *args, **kwargs):  # pragma: no cover
                # FIXME parameters are ignored?
                return self.driver_adapter.execute_script(conn, sql)

        elif operation_type == StatementType.SELECT:

            def fn(self, conn, *args, **kwargs):  # pragma: no cover
                return self.driver_adapter.select(
                    conn, query_name, sql, self._params(attributes, args, kwargs), record_class
                )

        elif operation_type == StatementType.SELECT_ONE:

            def fn(self, conn, *args, **kwargs):  # pragma: no cover
                return self.driver_adapter.select_one(
                    conn, query_name, sql, self._params(attributes, args, kwargs), record_class
                )

        elif operation_type == StatementType.SELECT_SCALAR:

            def fn(self, conn, *args, **kwargs):  # pragma: no cover
                return self.driver_adapter.select_scalar(conn, query_name, sql, self._params(attributes, args, kwargs))

        else:
            raise ValueError(f"Unknown operation_type: {operation_type}")
        if floc is None:
            floc = ("<no_source_file>", 0)
        return self._query_fn(fn, query_name, doc_comments, sql, operation_type, signature, floc, attributes)

    # NOTE does this make sense?
    def _make_async_fn(self, fn: StatementFn) -> StatementFn:
        """Wrap in an async function."""

        async def afn(self, conn, *args, **kwargs):  # pragma: no cover
            return await fn(self, conn, *args, **kwargs)

        return self._query_fn(afn, fn.__name__, fn.__doc__, fn.sql, fn.operation, fn.__signature__)

    def _make_ctx_mgr(self, fn: StatementFn) -> StatementFn:
        """Wrap in a context manager function."""

        def ctx_mgr(self, conn, *args, **kwargs):  # pragma: no cover
            return self.driver_adapter.select_cursor(
                conn, fn.__name__, fn.sql, self._params(fn.attributes, args, kwargs)
            )

        return self._query_fn(ctx_mgr, f"{fn.__name__}_cursor", fn.__doc__, fn.sql, fn.operation, fn.__signature__)

    def _create_methods(self, query_datum: StatementDetails, is_async: bool) -> list[StatementFn]:
        """Internal function to feed add_queries."""
        fn = self._make_sync_fn(query_datum)
        if is_async:
            fn = self._make_async_fn(fn)

        ctx_mgr = self._make_ctx_mgr(fn)

        if query_datum.operation_type == StatementType.SELECT:
            return [fn, ctx_mgr]
        return [fn]

    @property
    def available_statements(self) -> list[str]:
        """Returns listing of all the available Query/Statement methods loaded in this class.

        **Returns:** ``list[str]`` List of dot-separated method accessor names.
        """
        return sorted(self._available_statements)

    def __repr__(self) -> str:
        return "Statements(" + self.available_statements.__repr__() + ")"

    def add_statement(self, statement_name: str, fn: Callable) -> None:
        """Adds a new dynamic method to this class.

        **Parameters:**

        - **statement_name** - The method name as found in the SQL content.
        - **fn** - The loaded query function.
        """
        if hasattr(self, statement_name):
            # this is filtered out because it can lead to hard to find bugs.
            raise SQLLoadingError(f"cannot override existing attribute with statement: {statement_name}")
        setattr(self, statement_name, fn)
        self._available_statements.add(statement_name)

    def add_statements(self, statements: list[StatementFn]) -> None:
        """Add SQL Statement methods to `Statements` instance."""
        for fn in statements:
            statement_name = fn.__name__.rpartition(".")[2]
            self.add_statement(statement_name, MethodType(fn, self))

    def add_child_statements(self, child_name: str, child_statements: Statements) -> None:
        """Adds a Statement object as a property.

        **Parameters:**

        - **child_name** - The property name to group the child queries under.
        - **child_statements** - Queries instance to add as sub-queries.
        """
        if hasattr(self, child_name):  # pragma: no cover
            # this is filtered out because it can lead to hard to find bugs.
            raise SQLLoadingError(f"cannot override existing attribute with child: {child_name}")
        setattr(self, child_name, child_statements)
        for child_query_name in child_statements.available_statements:
            self._available_statements.add(f"{child_name}.{child_query_name}")

    def load_from_list(self, sql_statements: list[StatementDetails]):
        """Load Statements from a list of `StatementDetails`"""
        for sql_statement in sql_statements:
            self.add_statements(self._create_methods(sql_statement, self.is_async))
        return self

    def load_from_tree(self, sql_statements_tree: SQLStatements):
        """Load Statements from a `SQLStatementsTree`"""
        for key, value in sql_statements_tree.items():
            if isinstance(value, dict):
                self.add_child_statements(key, Statements(self.driver_adapter).load_from_tree(value))
            else:
                self.add_statements(self._create_methods(value, self.is_async))
        return self
