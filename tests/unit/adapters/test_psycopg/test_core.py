# pyright: reportArgumentType=false
"""Unit tests for psycopg core helpers."""

import asyncio
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any, Self, cast

import pytest
from psycopg import errors as pg_errors

from sqlspec.adapters.psycopg.core import (
    build_async_pipeline_execution_result,
    build_copy_from_command,
    build_pipeline_execution_result,
    create_mapped_exception,
    default_statement_config,
    escape_literal_percent,
    resolve_many_rowcount,
)
from sqlspec.exceptions import ConnectionTimeoutError, SQLParsingError, UniqueViolationError


def test_resolve_many_rowcount_prefers_positive_driver_rowcount() -> None:
    """resolve_many_rowcount should keep positive cursor rowcount values."""
    cursor = SimpleNamespace(rowcount=7)
    affected_rows = resolve_many_rowcount(cursor, [("a",), ("b",), ("c",)])
    assert affected_rows == 7


def test_resolve_many_rowcount_falls_back_to_parameter_count() -> None:
    """resolve_many_rowcount should fallback to parameter count when rowcount is unknown."""
    cursor = SimpleNamespace(rowcount=-1)
    affected_rows = resolve_many_rowcount(cursor, [("a",), ("b",), ("c",)])
    assert affected_rows == 3


def test_resolve_many_rowcount_returns_zero_for_unsized_parameters() -> None:
    """resolve_many_rowcount should return zero when no rowcount or length is available."""
    cursor = SimpleNamespace(rowcount=-1)

    def _parameter_stream() -> "Iterator[tuple[str]]":
        yield ("a",)

    affected_rows = resolve_many_rowcount(cursor, _parameter_stream())
    assert affected_rows == 0


def test_resolve_many_rowcount_prefers_precomputed_fallback_for_unsized_parameters() -> None:
    """resolve_many_rowcount should use a precomputed fallback before probing length."""
    cursor = SimpleNamespace(rowcount=-1)

    def _parameter_stream() -> "Iterator[tuple[str]]":
        yield ("a",)

    affected_rows = resolve_many_rowcount(cursor, _parameter_stream(), fallback_count=4)
    assert affected_rows == 4


def test_build_copy_from_command_preserves_quoted_dots_in_table_identifier() -> None:
    statement = build_copy_from_command('"analytics.schema"."orders.table"', ["id"])

    rendered = repr(statement)
    assert "Identifier('analytics.schema')" in rendered
    assert "Identifier('orders.table')" in rendered
    assert "Identifier('analytics')" not in rendered


def test_create_mapped_exception_dispatches_native_psycopg_error() -> None:
    """Native psycopg error types should resolve through the adapter-local dispatch table."""
    error = pg_errors.SyntaxError("syntax error at or near SELECT")
    mapped = create_mapped_exception(error)
    assert isinstance(mapped, SQLParsingError)
    assert mapped.__cause__ is error
    assert "[42601]" in str(mapped)


def test_create_mapped_exception_dispatches_native_psycopg_subclass() -> None:
    """Native psycopg subclasses should use cached MRO dispatch without changing mappings."""

    class CustomUniqueViolation(pg_errors.UniqueViolation):
        pass

    error = CustomUniqueViolation("duplicate key value violates unique constraint")
    mapped = create_mapped_exception(error)
    assert isinstance(mapped, UniqueViolationError)
    assert mapped.__cause__ is error
    assert "[23505]" in str(mapped)


def test_build_pipeline_execution_result_uses_column_resolver_fast_path() -> None:

    class _Statement:
        def returns_rows(self) -> bool:
            return True

    class _Cursor:
        def __init__(self) -> None:
            self.description = [SimpleNamespace(name="id"), SimpleNamespace(name="name")]

        def fetchall(self) -> list[tuple[int, str]]:
            return [(1, "alice"), (2, "bob")]

    cursor = _Cursor()
    result = build_pipeline_execution_result(
        cast("Any", _Statement()), cursor, column_name_resolver=lambda description: ["id", "name"]
    )
    assert result.selected_data == [(1, "alice"), (2, "bob")]
    assert result.column_names == ["id", "name"]
    assert result.data_row_count == 2


def test_build_pipeline_execution_result_detects_record_row_format() -> None:

    class _Statement:
        def returns_rows(self) -> bool:
            return True

    class _Row:
        def __init__(self, data: dict[str, Any]) -> None:
            self._data = data

        def keys(self) -> Any:
            return self._data.keys()

        def __getitem__(self, key: str) -> Any:
            return self._data[key]

    class _Cursor:
        def __init__(self) -> None:
            self.description = [SimpleNamespace(name="id"), SimpleNamespace(name="name")]

        def fetchall(self) -> list[_Row]:
            return [_Row({"id": 1, "name": "alice"})]

    cursor = _Cursor()
    result = build_pipeline_execution_result(cast("Any", _Statement()), cursor)
    assert result.row_format == "record"
    selected_data = result.selected_data
    assert selected_data is not None
    assert dict(selected_data[0]) == {"id": 1, "name": "alice"}


def test_build_async_pipeline_execution_result_uses_column_resolver_fast_path() -> None:

    class _Statement:
        def returns_rows(self) -> bool:
            return True

    class _Cursor:
        def __init__(self) -> None:
            self.description = [SimpleNamespace(name="id"), SimpleNamespace(name="name")]

        async def fetchall(self) -> list[tuple[int, str]]:
            return [(1, "alice"), (2, "bob")]

    async def _run() -> None:
        cursor = _Cursor()
        result = await build_async_pipeline_execution_result(
            cast("Any", _Statement()), cursor, column_name_resolver=lambda description: ["id", "name"]
        )
        assert result.selected_data == [(1, "alice"), (2, "bob")]
        assert result.column_names == ["id", "name"]
        assert result.data_row_count == 2

    asyncio.run(_run())


def test_build_async_pipeline_execution_result_detects_record_row_format() -> None:

    class _Statement:
        def returns_rows(self) -> bool:
            return True

    class _Row:
        def __init__(self, data: dict[str, Any]) -> None:
            self._data = data

        def keys(self) -> Any:
            return self._data.keys()

        def __getitem__(self, key: str) -> Any:
            return self._data[key]

    class _Cursor:
        def __init__(self) -> None:
            self.description = [SimpleNamespace(name="id"), SimpleNamespace(name="name")]

        async def fetchall(self) -> list[_Row]:
            return [_Row({"id": 1, "name": "alice"})]

    async def _run() -> None:
        cursor = _Cursor()
        result = await build_async_pipeline_execution_result(cast("Any", _Statement()), cursor)
        assert result.row_format == "record"
        selected_data = result.selected_data
        assert selected_data is not None
        assert dict(selected_data[0]) == {"id": 1, "name": "alice"}

    asyncio.run(_run())


@pytest.mark.parametrize("error_name, sqlstate", [("AdminShutdown", "57P01"), ("CannotConnectNow", "57P03")])
def test_create_mapped_exception_dispatches_connection_errors(error_name: str, sqlstate: str) -> None:
    error = getattr(pg_errors, error_name)("server unavailable")
    mapped = create_mapped_exception(error)
    assert isinstance(mapped, ConnectionTimeoutError)
    assert mapped.__cause__ is error
    assert sqlstate in str(mapped)


def test_build_pipeline_execution_result_fetches_rows_and_types_when_cursor_description_present() -> None:
    """Pipeline execution should collect rows and column_types when cursor.description is present even if returns_rows() is False."""

    class _Statement:
        def returns_rows(self) -> bool:
            return False

    class _Cursor:
        def __init__(self) -> None:
            self.description = [SimpleNamespace(name="id", type_code=23)]

        def fetchall(self) -> list[tuple[int]]:
            return [(42,)]

    result = build_pipeline_execution_result(
        cast("Any", _Statement()),
        _Cursor(),
        column_types_resolver=lambda desc: {c.name: str(c.type_code) for c in desc},
    )
    assert result.is_select_result is True
    assert result.selected_data == [(42,)]
    assert result.column_names == ["id"]
    assert result.column_types == {"id": "23"}


def test_escape_literal_percent_preserves_placeholders_and_doubles_literals() -> None:
    """escape_literal_percent should escape literal % signs while keeping %s placeholders intact."""
    validator = default_statement_config.parameter_validator
    sql = "SELECT * FROM items WHERE label LIKE '100%' AND code = %s"
    escaped = escape_literal_percent(sql, ("abc",), validator)
    assert escaped == "SELECT * FROM items WHERE label LIKE '100%%' AND code = %s"


@pytest.mark.parametrize(
    ("precision", "scale", "expected"),
    [
        (12, 2, {"amount": "decimal(12,2)"}),
        (12, 0, {"amount": "decimal(12,0)"}),
        (None, None, {"amount": "decimal"}),
        (90, 2, {"amount": "decimal"}),
    ],
)
def test_psycopg_resolve_column_types_numeric_precision(
    precision: int | None, scale: int | None, expected: dict[str, str]
) -> None:
    """_resolve_column_types emits decimal(P,S) for constrained PostgreSQL numeric OID 1700."""
    from sqlspec.adapters.psycopg.driver import _resolve_column_types

    column = SimpleNamespace(name="amount", type_code=1700, precision=precision, scale=scale)
    assert _resolve_column_types([column]) == expected


def test_psycopg_sync_stream_source_column_types_and_arrow_batches() -> None:
    """PsycopgSyncStreamSource populates column_types on start() when given a resolver and leaves None otherwise."""
    from decimal import Decimal

    import pyarrow as pa

    from sqlspec.adapters.psycopg.core import PsycopgSyncStreamSource
    from sqlspec.adapters.psycopg.driver import _resolve_column_types
    from sqlspec.driver import SyncRowStream
    from sqlspec.utils.arrow_helpers import arrow_batches_from_rows

    description = [
        SimpleNamespace(name="id", type_code=20, precision=None, scale=None),
        SimpleNamespace(name="amount", type_code=1700, precision=12, scale=2),
    ]

    class _SyncTx:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    class _SyncCursor:
        def __init__(self, chunks: list[list[tuple[Any, ...]]]) -> None:
            self.description = description
            self._chunks = list(chunks)
            self.itersize = 0
            self.closed = False

        def execute(self, sql: str, params: Any = None) -> None:
            _ = (sql, params)

        def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
            _ = size
            return self._chunks.pop(0) if self._chunks else []

        def close(self) -> None:
            self.closed = True

    class _SyncConnection:
        def __init__(self, cursor: _SyncCursor) -> None:
            self._cursor = cursor

        def transaction(self) -> _SyncTx:
            return _SyncTx()

        def cursor(self, name: str | None = None) -> _SyncCursor:
            _ = name
            return self._cursor

    class _SyncDriverDouble:
        def __init__(self, connection: _SyncConnection) -> None:
            self.connection = connection

        def handle_database_exceptions(self) -> object:
            return object()

        def _run_with_exception_handler(self, _handler: object, fn: Any, *args: Any) -> Any:
            return fn(*args)

        def _check_pending_exception(self, _handler: object) -> None:
            return None

    no_resolver_source = PsycopgSyncStreamSource(
        _SyncDriverDouble(_SyncConnection(_SyncCursor([]))), "SELECT 1", (), 10
    )
    no_resolver_source.start()
    assert no_resolver_source.column_types is None
    no_resolver_source.close()

    with_resolver_source = PsycopgSyncStreamSource(
        _SyncDriverDouble(_SyncConnection(_SyncCursor([]))),
        "SELECT 1",
        (),
        10,
        column_types_resolver=_resolve_column_types,
    )
    with_resolver_source.start()
    assert with_resolver_source.column_types == {"id": "int64", "amount": "decimal(12,2)"}
    with_resolver_source.close()

    read_ahead: list[tuple[str, ...]] = []
    arrow_cursor = _SyncCursor([[(1, Decimal("12.34"))], [(2, Decimal("56.78"))]])
    stream: SyncRowStream[dict[str, Any]] = SyncRowStream(
        PsycopgSyncStreamSource(
            _SyncDriverDouble(_SyncConnection(arrow_cursor)),
            "SELECT id, amount FROM demo",
            (),
            1,
            column_types_resolver=_resolve_column_types,
        )
    )
    reader = arrow_batches_from_rows(stream, batch_size=1, on_read_ahead=read_ahead.append)
    batches = list(reader) if reader is not None else []
    assert len(batches) == 2
    assert batches[0].schema.field("amount").type == pa.decimal128(12, 2)
    assert read_ahead == []


async def test_psycopg_async_stream_source_and_cockroach_delegation() -> None:
    """PsycopgAsyncStreamSource sets column_types on start() and Cockroach wrappers delegate column_types."""
    from sqlspec.adapters.cockroach_psycopg.driver import (
        CockroachPsycopgAsyncStreamSource,
        CockroachPsycopgSyncStreamSource,
    )
    from sqlspec.adapters.psycopg.core import PsycopgAsyncStreamSource
    from sqlspec.adapters.psycopg.driver import _resolve_column_types

    description = [
        SimpleNamespace(name="id", type_code=20, precision=None, scale=None),
        SimpleNamespace(name="amount", type_code=1700, precision=12, scale=2),
    ]

    class _AsyncTx:
        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

    class _AsyncCursor:
        def __init__(self) -> None:
            self.description = description
            self.itersize = 0
            self.closed = False

        async def execute(self, sql: str, params: Any = None) -> None:
            _ = (sql, params)

        async def close(self) -> None:
            self.closed = True

    class _AsyncConnection:
        def __init__(self, cursor: _AsyncCursor) -> None:
            self._cursor = cursor

        def transaction(self) -> _AsyncTx:
            return _AsyncTx()

        def cursor(self, name: str | None = None) -> _AsyncCursor:
            _ = name
            return self._cursor

    class _AsyncDriverDouble:
        def __init__(self, connection: _AsyncConnection) -> None:
            self.connection = connection

        def handle_database_exceptions(self) -> object:
            return object()

        async def _run_with_exception_handler(self, _handler: object, fn: Any, *args: Any) -> Any:
            return await fn(*args)

        def _check_pending_exception(self, _handler: object) -> None:
            return None

    no_resolver_async = PsycopgAsyncStreamSource(
        _AsyncDriverDouble(_AsyncConnection(_AsyncCursor())), "SELECT 1", (), 10
    )
    await no_resolver_async.start()
    assert no_resolver_async.column_types is None
    await no_resolver_async.close()

    with_resolver_async = PsycopgAsyncStreamSource(
        _AsyncDriverDouble(_AsyncConnection(_AsyncCursor())),
        "SELECT 1",
        (),
        10,
        column_types_resolver=_resolve_column_types,
    )
    await with_resolver_async.start()
    assert with_resolver_async.column_types == {"id": "int64", "amount": "decimal(12,2)"}
    await with_resolver_async.close()

    underlying_double = SimpleNamespace(column_types={"a": "int64"})
    sync_wrapper = CockroachPsycopgSyncStreamSource(cast("Any", object()), cast("Any", underlying_double), False)
    async_wrapper = CockroachPsycopgAsyncStreamSource(
        cast("Any", object()), cast("Any", object()), cast("Any", underlying_double)
    )
    assert sync_wrapper.column_types == {"a": "int64"}
    assert async_wrapper.column_types == {"a": "int64"}


def test_read_only_transaction_maps_native_error_with_cause() -> None:
    from psycopg.errors import ReadOnlySqlTransaction

    from sqlspec.adapters.psycopg.core import create_mapped_exception
    from sqlspec.exceptions import ReadOnlyTransactionError

    error = ReadOnlySqlTransaction("cannot write")

    mapped = create_mapped_exception(error)
    assert isinstance(mapped, ReadOnlyTransactionError)
    assert mapped.__cause__ is error
    assert "read-only transaction" in str(mapped).lower()
