"""Public behavior helpers for adapter-local and central contract tests."""

import contextlib
import inspect
import json
import math
from collections.abc import Awaitable, Callable, Iterator, Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast
from uuid import NAMESPACE_DNS, UUID, uuid1, uuid4, uuid5

import msgspec
import pytest

from sqlspec import SQL, SQLResult, StackExecutionError, StatementConfig, StatementStack, sql
from sqlspec.builder import Explain
from sqlspec.core.filters import InCollectionFilter, LimitOffsetFilter, OrderByFilter, SearchFilter
from sqlspec.data_dictionary import VersionInfo
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.exceptions import ImproperConfigurationError, OperationalError, SQLParsingError, SQLSpecError
from sqlspec.utils.serializers import from_json, to_json
from tests.integration.adapters.contracts._assertions import assert_result_data, assert_sql_result
from tests.integration.adapters.contracts._cases import DriverCase
from tests.integration.adapters.contracts._inputs import (
    ExceptionViolationCase,
    ExplainCase,
    ParameterProfileCase,
    ParameterStyleCase,
    StatementInputCase,
)
from tests.integration.adapters.contracts._schema import (
    DEFAULT_CONTRACT_TABLE,
    DUCKDB_CONTRACT_TABLE,
    ContractRow,
    ContractTable,
)

if TYPE_CHECKING:
    from sqlspec.typing import ArrowRecordBatch

SyncExtraAssertion = Callable[[object, DriverCase], None]
AsyncExtraAssertion = Callable[[object, DriverCase], Awaitable[None]]

_SYNC_EXTRA_ASSERTIONS: "dict[str, tuple[str, SyncExtraAssertion]]" = {}
_ASYNC_EXTRA_ASSERTIONS: "dict[str, tuple[str, AsyncExtraAssertion]]" = {}


def register_sync_extra_assertion(key: str, scope: str, fn: SyncExtraAssertion) -> SyncExtraAssertion:
    """Register an additive sync proof under a unique key, owned by a behavior scope."""
    if key in _SYNC_EXTRA_ASSERTIONS:
        msg = f"duplicate sync extra assertion key {key!r}"
        raise ValueError(msg)
    _SYNC_EXTRA_ASSERTIONS[key] = (scope, fn)
    return fn


def register_async_extra_assertion(key: str, scope: str, fn: AsyncExtraAssertion) -> AsyncExtraAssertion:
    """Register an additive async proof under a unique key, owned by a behavior scope."""
    if key in _ASYNC_EXTRA_ASSERTIONS:
        msg = f"duplicate async extra assertion key {key!r}"
        raise ValueError(msg)
    _ASYNC_EXTRA_ASSERTIONS[key] = (scope, fn)
    return fn


def known_extra_assertion_keys() -> "set[str]":
    """Return every registered proof key across both sync and async registries."""
    return set(_SYNC_EXTRA_ASSERTIONS) | set(_ASYNC_EXTRA_ASSERTIONS)


def validate_extra_assertions(case: DriverCase) -> None:
    """Fail loud if a case opts into a proof key registered in neither registry (no silent coverage loss)."""
    known = known_extra_assertion_keys()
    unknown = [key for key in case.extra_assertions if key not in known]
    if unknown:
        msg = f"{case.id} declares unregistered extra_assertions {unknown!r}"
        raise KeyError(msg)


def dispatch_sync_extra_assertions(driver: object, case: DriverCase, scope: str) -> None:
    """Run the case's additive sync proofs owned by ``scope``; ignore keys owned elsewhere."""
    for key in case.extra_assertions:
        entry = _SYNC_EXTRA_ASSERTIONS.get(key)
        if entry is None or entry[0] != scope:
            continue
        entry[1](driver, case)


async def dispatch_async_extra_assertions(driver: object, case: DriverCase, scope: str) -> None:
    """Await the case's additive async proofs owned by ``scope``; ignore keys owned elsewhere."""
    for key in case.extra_assertions:
        entry = _ASYNC_EXTRA_ASSERTIONS.get(key)
        if entry is None or entry[0] != scope:
            continue
        await entry[1](driver, case)


DRIVER_BASICS_SCOPE = "driver_basics"
DRIVER_BASICS_PROOF_KEY = "driver_basics:noop"


def _driver_basics_noop_proof(driver: object, case: DriverCase) -> None:
    """No-op proof demonstrating the hook fires in the live matrix without changing pass/fail."""


async def _driver_basics_noop_proof_async(driver: object, case: DriverCase) -> None:
    """No-op async proof demonstrating the hook fires in the live matrix without changing pass/fail."""


register_sync_extra_assertion(DRIVER_BASICS_PROOF_KEY, DRIVER_BASICS_SCOPE, _driver_basics_noop_proof)
register_async_extra_assertion(DRIVER_BASICS_PROOF_KEY, DRIVER_BASICS_SCOPE, _driver_basics_noop_proof_async)


class SyncContractDriver(Protocol):
    """Sync driver surface used by adapter contract helpers."""

    statement_config: StatementConfig

    def begin(self) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def execute(self, statement: object, /, *parameters: object, **kwargs: Any) -> SQLResult: ...

    def execute_many(self, statement: object, parameters: object, /, **kwargs: Any) -> SQLResult: ...

    def execute_script(self, statement: object, /, *parameters: object, **kwargs: Any) -> SQLResult: ...

    def execute_stack(self, stack: object, /, *, continue_on_error: bool = False) -> "tuple[Any, ...]": ...

    def select_one(self, statement: object, /, *parameters: object, **kwargs: Any) -> dict[str, Any]: ...

    def select_one_or_none(self, statement: object, /, *parameters: object, **kwargs: Any) -> dict[str, Any] | None: ...

    def select_value(self, statement: object, /, *parameters: object, **kwargs: Any) -> object: ...

    def select_stream(self, statement: object, /, *parameters: object, **kwargs: Any) -> Any: ...

    def select_to_arrow(self, statement: object, /, *parameters: object, **kwargs: Any) -> Any: ...

    def select_to_storage(
        self, statement: object, destination: object, /, *parameters: object, **kwargs: Any
    ) -> Any: ...

    def load_from_arrow(self, table: str, source: Any, /, **kwargs: Any) -> Any: ...

    def load_from_records(self, table: str, records: Any, /, **kwargs: Any) -> Any: ...

    def load_from_storage(self, table: str, source: object, /, **kwargs: Any) -> Any: ...


class AsyncContractDriver(Protocol):
    """Async driver surface used by adapter contract helpers."""

    statement_config: StatementConfig

    async def begin(self) -> None: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...

    async def execute(self, statement: object, /, *parameters: object, **kwargs: Any) -> SQLResult: ...

    async def execute_many(self, statement: object, parameters: object, /, **kwargs: Any) -> SQLResult: ...

    async def execute_script(self, statement: object, /, *parameters: object, **kwargs: Any) -> SQLResult: ...

    async def execute_stack(self, stack: object, /, *, continue_on_error: bool = False) -> "tuple[Any, ...]": ...

    async def select_one(self, statement: object, /, *parameters: object, **kwargs: Any) -> dict[str, Any]: ...

    async def select_one_or_none(
        self, statement: object, /, *parameters: object, **kwargs: Any
    ) -> dict[str, Any] | None: ...

    async def select_value(self, statement: object, /, *parameters: object, **kwargs: Any) -> object: ...

    def select_stream(self, statement: object, /, *parameters: object, **kwargs: Any) -> Any: ...

    async def select_to_arrow(self, statement: object, /, *parameters: object, **kwargs: Any) -> Any: ...

    async def select_to_storage(
        self, statement: object, destination: object, /, *parameters: object, **kwargs: Any
    ) -> Any: ...

    async def load_from_arrow(self, table: str, source: Any, /, **kwargs: Any) -> Any: ...

    async def load_from_records(self, table: str, records: Any, /, **kwargs: Any) -> Any: ...

    async def load_from_storage(self, table: str, source: object, /, **kwargs: Any) -> Any: ...


def _with_table(value: object, table: ContractTable) -> object:
    """Rewrite the canonical ``contract_items`` table reference to the case's table name.

    A no-op for adapters whose table is literally ``contract_items``; for adapters that
    require a qualified identifier (e.g. BigQuery ``project.dataset.contract_items``) it
    substitutes the resolved name into raw SQL strings and ``SQL`` statement objects.
    """
    if table.name == "contract_items":
        return value
    if isinstance(value, str):
        return value.replace("contract_items", table.name)
    if isinstance(value, SQL):
        rewritten = value.raw_sql.replace("contract_items", table.name)
        if value.named_parameters:
            return SQL(rewritten, **value.named_parameters)
        if value.positional_parameters:
            return SQL(rewritten, value.positional_parameters)
        return SQL(rewritten)
    return value


def _row_parameters(rows: tuple[ContractRow, ...]) -> list[tuple[object, ...]]:
    return [(row.name, row.value, row.note) for row in rows]


def _execute_sync(driver: SyncContractDriver, statement: object, parameters: object | None = None) -> SQLResult:
    if parameters is None:
        return driver.execute(statement)
    return driver.execute(statement, parameters)


async def _execute_async(driver: AsyncContractDriver, statement: object, parameters: object | None = None) -> SQLResult:
    if parameters is None:
        return await driver.execute(statement)
    return await driver.execute(statement, parameters)


def _rowcount_policy(case: DriverCase, method: Literal["execute", "execute_many"] = "execute") -> str:
    return case.execute_many_rowcount_policy if method == "execute_many" else case.execute_rowcount_policy


def _reports_execute_rows_affected(case: DriverCase, method: Literal["execute", "execute_many"] = "execute") -> bool:
    return _rowcount_policy(case, method) == "exact"


def _assert_sql_result_rows_affected(
    result: object, case: DriverCase, rows_affected: int, method: Literal["execute", "execute_many"] = "execute"
) -> SQLResult:
    policy = _rowcount_policy(case, method)
    sql_result = assert_sql_result(result, rows_affected=rows_affected if policy == "exact" else None)
    if policy == "non_negative":
        assert sql_result.rows_affected >= 0
    return sql_result


def _sqlglot_dialect(case: DriverCase) -> str:
    return "tsql" if case.dialect == "mssql" else case.dialect


def _contract_rows_to_arrow(rows: tuple[ContractRow, ...]) -> Any:
    import pyarrow as pa

    return pa.table({
        "name": pa.array([row.name for row in rows], type=pa.string()),
        "value": pa.array([row.value for row in rows], type=pa.int64()),
        "note": pa.array([None if row.note is None else str(row.note) for row in rows], type=pa.string()),
    })


def _seed_sync(
    driver: SyncContractDriver,
    rows: tuple[ContractRow, ...],
    table: ContractTable = DEFAULT_CONTRACT_TABLE,
    case: DriverCase | None = None,
) -> None:
    if rows:
        if case is not None and not case.supports_execute_many:
            if not case.supports_native_bulk_ingest:
                pytest.skip(f"{case.adapter} cannot seed contract rows without execute_many or native bulk ingest")
            driver.load_from_arrow(table.name, _contract_rows_to_arrow(rows), overwrite=True)
            driver.commit()
            return
        driver.execute_many(table.insert_qmark_sql, _row_parameters(rows))
        driver.commit()


async def _seed_async(
    driver: AsyncContractDriver,
    rows: tuple[ContractRow, ...],
    table: ContractTable = DEFAULT_CONTRACT_TABLE,
    case: DriverCase | None = None,
) -> None:
    if rows:
        if case is not None and not case.supports_execute_many:
            if not case.supports_native_bulk_ingest:
                pytest.skip(f"{case.adapter} cannot seed contract rows without execute_many or native bulk ingest")
            await driver.load_from_arrow(table.name, _contract_rows_to_arrow(rows), overwrite=True)
            await driver.commit()
            return
        await driver.execute_many(table.insert_qmark_sql, _row_parameters(rows))
        await driver.commit()


def _update_value_sql(table: ContractTable) -> str:
    return f"UPDATE {table.name} SET value = ? WHERE name = ?"


def _delete_by_name_sql(table: ContractTable) -> str:
    return f"DELETE FROM {table.name} WHERE name = ?"


def assert_sync_driver_basics_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers run the CRUD lifecycle and expose result column metadata."""
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    assert_execute_rows = _reports_execute_rows_affected(case)

    insert_result = sync_driver.execute(table.insert_qmark_sql, ("basics", 1, None))
    if assert_execute_rows:
        assert_sql_result(insert_result, rows_affected=1)
    sync_driver.commit()

    selected = assert_sql_result(sync_driver.execute(table.select_by_name_qmark_sql, ("basics",)))
    assert selected.column_names == ["name", "value", "note"]
    assert selected.get_data()[0] == {"name": "basics", "value": 1, "note": None}

    update_result = sync_driver.execute(_update_value_sql(table), (2, "basics"))
    if assert_execute_rows:
        assert_sql_result(update_result, rows_affected=1)
    sync_driver.commit()
    updated = assert_sql_result(sync_driver.execute(table.select_by_name_qmark_sql, ("basics",)))
    assert updated.get_data()[0]["value"] == 2
    assert sync_driver.select_value(table.select_count_sql) == 1

    delete_result = sync_driver.execute(_delete_by_name_sql(table), ("basics",))
    if assert_execute_rows:
        assert_sql_result(delete_result, rows_affected=1)
    sync_driver.commit()
    assert sync_driver.select_value(table.select_count_sql) == 0

    dispatch_sync_extra_assertions(driver, case, DRIVER_BASICS_SCOPE)


async def assert_async_driver_basics_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers run the CRUD lifecycle and expose result column metadata."""
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    assert_execute_rows = _reports_execute_rows_affected(case)

    insert_result = await async_driver.execute(table.insert_qmark_sql, ("basics", 1, None))
    if assert_execute_rows:
        assert_sql_result(insert_result, rows_affected=1)
    await async_driver.commit()

    selected = assert_sql_result(await async_driver.execute(table.select_by_name_qmark_sql, ("basics",)))
    assert selected.column_names == ["name", "value", "note"]
    assert selected.get_data()[0] == {"name": "basics", "value": 1, "note": None}

    update_result = await async_driver.execute(_update_value_sql(table), (2, "basics"))
    if assert_execute_rows:
        assert_sql_result(update_result, rows_affected=1)
    await async_driver.commit()
    updated = assert_sql_result(await async_driver.execute(table.select_by_name_qmark_sql, ("basics",)))
    assert updated.get_data()[0]["value"] == 2
    assert await async_driver.select_value(table.select_count_sql) == 1

    delete_result = await async_driver.execute(_delete_by_name_sql(table), ("basics",))
    if assert_execute_rows:
        assert_sql_result(delete_result, rows_affected=1)
    await async_driver.commit()
    assert await async_driver.select_value(table.select_count_sql) == 0

    await dispatch_async_extra_assertions(driver, case, DRIVER_BASICS_SCOPE)


_STREAMING_SCOPE = "streaming"
_STREAM_CHUNK_SIZE = 100
_STREAMING_MISSING_TABLE_SQL = "SELECT * FROM sqlspec_streaming_missing_table"


def _stream_seed_rows(count: int) -> "list[tuple[str, int, None]]":
    return [(f"row-{i:06d}", i, None) for i in range(count)]


def _stream_contract_rows(count: int) -> tuple[ContractRow, ...]:
    return tuple(ContractRow(name, value, note) for name, value, note in _stream_seed_rows(count))


def _seed_stream_table(driver: SyncContractDriver, table: ContractTable, count: int, case: DriverCase) -> None:
    driver.execute(table.delete_sql)
    driver.commit()
    _seed_sync(driver, _stream_contract_rows(count), table, case)


async def _seed_stream_table_async(
    driver: AsyncContractDriver, table: ContractTable, count: int, case: DriverCase
) -> None:
    await driver.execute(table.delete_sql)
    await driver.commit()
    await _seed_async(driver, _stream_contract_rows(count), table, case)


def assert_sync_streaming_unsupported_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers without native streaming raise under native_only and fall back eagerly."""
    if case.supports_native_row_streaming:
        pytest.skip(f"{case.adapter} is covered by the native row-streaming contract")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    sync_driver.execute(table.delete_sql)
    sync_driver.commit()
    _seed_sync(sync_driver, (ContractRow("a", 1), ContractRow("b", 2)), table, case)

    with pytest.raises(ImproperConfigurationError):
        sync_driver.select_stream(table.select_ordered_sql, native_only=True)

    with sync_driver.select_stream(table.select_ordered_sql, chunk_size=1) as stream:
        rows = list(stream)
    assert [row["value"] for row in rows] == [1, 2]


async def assert_async_streaming_unsupported_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers without native streaming raise under native_only and fall back eagerly."""
    if case.supports_native_row_streaming:
        pytest.skip(f"{case.adapter} is covered by the native row-streaming contract")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    await async_driver.execute(table.delete_sql)
    await async_driver.commit()
    await _seed_async(async_driver, (ContractRow("a", 1), ContractRow("b", 2)), table, case)

    with pytest.raises(ImproperConfigurationError):
        async_driver.select_stream(table.select_ordered_sql, native_only=True)

    async with async_driver.select_stream(table.select_ordered_sql, chunk_size=1) as stream:
        rows = [row async for row in stream]
    assert [row["value"] for row in rows] == [1, 2]


def assert_sync_streaming_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers stream dict rows in bounded chunks with cleanup guarantees."""
    if not case.supports_native_row_streaming:
        pytest.skip(f"{case.adapter} has no native row streaming")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    count = case.streaming_row_count
    _seed_stream_table(sync_driver, table, count, case)

    stream = sync_driver.select_stream(table.select_ordered_sql, chunk_size=_STREAM_CHUNK_SIZE)
    iterator = iter(stream)
    first = next(iterator)
    assert first["value"] == 0
    if case.stream_chunk_policy == "bounded":
        assert len(stream._buffer) <= _STREAM_CHUNK_SIZE  # pyright: ignore[reportPrivateUsage]
    remaining = list(iterator)
    assert len(remaining) == count - 1
    assert [row["value"] for row in remaining[:3]] == [1, 2, 3]
    assert remaining[-1]["value"] == count - 1

    if case.supports_stream_reopen_after_partial_iteration:
        with sync_driver.select_stream(table.select_ordered_sql, chunk_size=_STREAM_CHUNK_SIZE) as partial:
            partial_iterator = iter(partial)
            for _ in range(50):
                next(partial_iterator)
        assert int(cast("int", sync_driver.select_value(table.select_count_sql))) == count

    if case.invalid_sql_error_policy == "raises":
        bad_stream = sync_driver.select_stream(_STREAMING_MISSING_TABLE_SQL)
        with pytest.raises(Exception):
            next(iter(bad_stream))
        with contextlib.suppress(Exception):
            sync_driver.rollback()
        assert int(cast("int", sync_driver.select_value(table.select_count_sql))) == count

    dispatch_sync_extra_assertions(driver, case, _STREAMING_SCOPE)


async def assert_async_streaming_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers stream dict rows in bounded chunks with cleanup guarantees."""
    if not case.supports_native_row_streaming:
        pytest.skip(f"{case.adapter} has no native row streaming")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    count = case.streaming_row_count
    await _seed_stream_table_async(async_driver, table, count, case)

    stream = async_driver.select_stream(table.select_ordered_sql, chunk_size=_STREAM_CHUNK_SIZE)
    iterator = aiter(stream)
    first = await anext(iterator)
    assert first["value"] == 0
    if case.stream_chunk_policy == "bounded":
        assert len(stream._buffer) <= _STREAM_CHUNK_SIZE  # pyright: ignore[reportPrivateUsage]
    remaining: list[dict[str, Any]] = [row async for row in iterator]
    assert len(remaining) == count - 1
    assert [row["value"] for row in remaining[:3]] == [1, 2, 3]
    assert remaining[-1]["value"] == count - 1

    if case.supports_stream_reopen_after_partial_iteration:
        async with async_driver.select_stream(table.select_ordered_sql, chunk_size=_STREAM_CHUNK_SIZE) as partial:
            partial_iterator = aiter(partial)
            for _ in range(50):
                await anext(partial_iterator)
        assert int(cast("int", await async_driver.select_value(table.select_count_sql))) == count

    if case.invalid_sql_error_policy == "raises":
        bad_stream = async_driver.select_stream(_STREAMING_MISSING_TABLE_SQL)
        with pytest.raises(Exception):
            await anext(aiter(bad_stream))
        with contextlib.suppress(Exception):
            await async_driver.rollback()
        assert int(cast("int", await async_driver.select_value(table.select_count_sql))) == count

    await dispatch_async_extra_assertions(driver, case, _STREAMING_SCOPE)


def _assert_sqlite_stream_arraysize(driver: object, case: DriverCase) -> None:
    sync_driver = cast("SyncContractDriver", driver)
    stream = sync_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    try:
        next(iter(stream))
        assert stream._source._cursor.arraysize == 10  # pyright: ignore[reportPrivateUsage]
    finally:
        stream.close()


async def _assert_aiosqlite_stream_arraysize(driver: object, case: DriverCase) -> None:
    async_driver = cast("AsyncContractDriver", driver)
    stream = async_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    try:
        await anext(aiter(stream))
        assert stream._source._cursor.arraysize == 10  # pyright: ignore[reportPrivateUsage]
    finally:
        await stream.aclose()


async def _assert_asyncpg_stream_transaction(driver: object, case: DriverCase) -> None:
    async_driver = cast("AsyncContractDriver", driver)
    stream = async_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    await anext(aiter(stream))
    assert stream._source._transaction is not None  # pyright: ignore[reportPrivateUsage]
    assert stream._source._cursor is not None  # pyright: ignore[reportPrivateUsage]
    await stream.aclose()
    assert stream._source._transaction is None  # pyright: ignore[reportPrivateUsage]
    assert stream._source._cursor is None  # pyright: ignore[reportPrivateUsage]
    await async_driver.execute(case.table.select_count_sql)


def _assert_psycopg_named_cursor(driver: object, case: DriverCase) -> None:
    sync_driver = cast("SyncContractDriver", driver)
    stream = sync_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    try:
        next(iter(stream))
        assert stream._source._cursor.name  # pyright: ignore[reportPrivateUsage]
    finally:
        stream.close()


async def _assert_psycopg_named_cursor_async(driver: object, case: DriverCase) -> None:
    async_driver = cast("AsyncContractDriver", driver)
    stream = async_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    try:
        await anext(aiter(stream))
        assert stream._source._cursor.name  # pyright: ignore[reportPrivateUsage]
    finally:
        await stream.aclose()


def _assert_pymysql_sscursor(driver: object, case: DriverCase) -> None:
    sync_driver = cast("SyncContractDriver", driver)
    stream = sync_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    try:
        next(iter(stream))
        assert type(stream._source._cursor).__name__ == "SSCursor"  # pyright: ignore[reportPrivateUsage]
    finally:
        stream.close()


async def _assert_async_sscursor(driver: object, case: DriverCase) -> None:
    async_driver = cast("AsyncContractDriver", driver)
    stream = async_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    try:
        await anext(aiter(stream))
        assert type(stream._source._cursor).__name__ == "SSCursor"  # pyright: ignore[reportPrivateUsage]
    finally:
        await stream.aclose()


def _assert_mysqlconnector_unbuffered(driver: object, case: DriverCase) -> None:
    sync_driver = cast("SyncContractDriver", driver)
    stream = sync_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    try:
        next(iter(stream))
        assert getattr(stream._source._cursor, "_buffered", False) is False  # pyright: ignore[reportPrivateUsage]
    finally:
        stream.close()


async def _assert_mysqlconnector_unbuffered_async(driver: object, case: DriverCase) -> None:
    async_driver = cast("AsyncContractDriver", driver)
    stream = async_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    try:
        await anext(aiter(stream))
        assert getattr(stream._source._cursor, "_buffered", False) is False  # pyright: ignore[reportPrivateUsage]
    finally:
        await stream.aclose()


register_sync_extra_assertion("streaming_native:sqlite", _STREAMING_SCOPE, _assert_sqlite_stream_arraysize)
register_async_extra_assertion("streaming_native:aiosqlite", _STREAMING_SCOPE, _assert_aiosqlite_stream_arraysize)
register_async_extra_assertion("streaming_native:asyncpg", _STREAMING_SCOPE, _assert_asyncpg_stream_transaction)
register_sync_extra_assertion("streaming_native:psycopg", _STREAMING_SCOPE, _assert_psycopg_named_cursor)
register_async_extra_assertion("streaming_native:psycopg", _STREAMING_SCOPE, _assert_psycopg_named_cursor_async)
register_sync_extra_assertion("streaming_native:pymysql", _STREAMING_SCOPE, _assert_pymysql_sscursor)
register_async_extra_assertion("streaming_native:aiomysql", _STREAMING_SCOPE, _assert_async_sscursor)
register_async_extra_assertion("streaming_native:asyncmy", _STREAMING_SCOPE, _assert_async_sscursor)
register_sync_extra_assertion("streaming_native:mysqlconnector", _STREAMING_SCOPE, _assert_mysqlconnector_unbuffered)
register_async_extra_assertion(
    "streaming_native:mysqlconnector", _STREAMING_SCOPE, _assert_mysqlconnector_unbuffered_async
)


async def _assert_psqlpy_cursor(driver: object, case: DriverCase) -> None:
    async_driver = cast("AsyncContractDriver", driver)
    stream = async_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    try:
        await anext(aiter(stream))
        assert type(stream._source._cursor).__name__ == "Cursor"  # pyright: ignore[reportPrivateUsage]
    finally:
        await stream.aclose()


register_async_extra_assertion("streaming_native:psqlpy", _STREAMING_SCOPE, _assert_psqlpy_cursor)


def _assert_oracledb_arraysize(driver: object, case: DriverCase) -> None:
    sync_driver = cast("SyncContractDriver", driver)
    stream = sync_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    try:
        next(iter(stream))
        assert stream._source._cursor.arraysize == 10  # pyright: ignore[reportPrivateUsage]
    finally:
        stream.close()


async def _assert_oracledb_arraysize_async(driver: object, case: DriverCase) -> None:
    async_driver = cast("AsyncContractDriver", driver)
    stream = async_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    try:
        await anext(aiter(stream))
        assert stream._source._cursor.arraysize == 10  # pyright: ignore[reportPrivateUsage]
    finally:
        await stream.aclose()


register_sync_extra_assertion("streaming_native:oracledb", _STREAMING_SCOPE, _assert_oracledb_arraysize)
register_async_extra_assertion("streaming_native:oracledb", _STREAMING_SCOPE, _assert_oracledb_arraysize_async)


def _assert_bigquery_pages(driver: object, case: DriverCase) -> None:
    sync_driver = cast("SyncContractDriver", driver)
    stream = sync_driver.select_stream(case.table.select_ordered_sql, chunk_size=10)
    try:
        next(iter(stream))
        assert stream._source._pages is not None  # pyright: ignore[reportPrivateUsage]
    finally:
        stream.close()


register_sync_extra_assertion("streaming_native:bigquery", _STREAMING_SCOPE, _assert_bigquery_pages)


_FOR_UPDATE_LOCK_ROW = ("lock-row", 100, None)


def assert_sync_for_update_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers honor FOR UPDATE, FOR UPDATE SKIP LOCKED, and FOR SHARE row locking."""
    if not case.supports_for_update:
        pytest.skip(f"{case.adapter} has no verified row-locking support")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    sync_driver.execute(table.insert_qmark_sql, _FOR_UPDATE_LOCK_ROW)
    sync_driver.commit()
    builders = [
        sql.select("name", "value").from_(table.name).where_eq("name", "lock-row").for_update(),
        sql.select("name", "value").from_(table.name).where_eq("name", "lock-row").for_update(skip_locked=True),
    ]
    if case.supports_for_share:
        builders.append(sql.select("name", "value").from_(table.name).where_eq("name", "lock-row").for_share())
    try:
        for builder in builders:
            sync_driver.begin()
            locked = sync_driver.select_one(builder)
            assert locked["name"] == "lock-row"
            assert locked["value"] == 100
            sync_driver.commit()
    finally:
        with contextlib.suppress(Exception):
            sync_driver.rollback()
        sync_driver.execute(_delete_by_name_sql(table), ("lock-row",))
        sync_driver.commit()


async def assert_async_for_update_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers honor FOR UPDATE, FOR UPDATE SKIP LOCKED, and FOR SHARE row locking."""
    if not case.supports_for_update:
        pytest.skip(f"{case.adapter} has no verified row-locking support")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    await async_driver.execute(table.insert_qmark_sql, _FOR_UPDATE_LOCK_ROW)
    await async_driver.commit()
    builders = [
        sql.select("name", "value").from_(table.name).where_eq("name", "lock-row").for_update(),
        sql.select("name", "value").from_(table.name).where_eq("name", "lock-row").for_update(skip_locked=True),
    ]
    if case.supports_for_share:
        builders.append(sql.select("name", "value").from_(table.name).where_eq("name", "lock-row").for_share())
    try:
        for builder in builders:
            await async_driver.begin()
            locked = await async_driver.select_one(builder)
            assert locked["name"] == "lock-row"
            assert locked["value"] == 100
            await async_driver.commit()
    finally:
        with contextlib.suppress(Exception):
            await async_driver.rollback()
        await async_driver.execute(_delete_by_name_sql(table), ("lock-row",))
        await async_driver.commit()


_FILTER_SEED_ROWS = (
    ContractRow("alpha", 10),
    ContractRow("beta", 20),
    ContractRow("gamma", 30),
    ContractRow("delta", 40),
    ContractRow("epsilon", 50),
)
_GROUPED_SEED_ROWS = (
    ContractRow("alpha", 10),
    ContractRow("beta", 20),
    ContractRow("gamma", 20),
    ContractRow("delta", 30),
    ContractRow("epsilon", 30),
)


def assert_sync_filter_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers apply OrderBy/LimitOffset, InCollection, and Search filters."""
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    _seed_sync(sync_driver, _FILTER_SEED_ROWS, table, case)
    base = f"SELECT name, value FROM {table.name}"

    paged = sync_driver.execute(base, OrderByFilter("value", "desc"), LimitOffsetFilter(limit=2, offset=1))
    assert [row["name"] for row in paged.get_data()] == ["delta", "gamma"]

    in_collection = sync_driver.execute(base, InCollectionFilter("value", [20, 40]), OrderByFilter("value", "asc"))
    assert [row["name"] for row in in_collection.get_data()] == ["beta", "delta"]

    if case.supports_search_filter:
        searched = sync_driver.execute(base, SearchFilter("name", "lta"))
        assert [row["name"] for row in searched.get_data()] == ["delta"]


async def assert_async_filter_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers apply OrderBy/LimitOffset, InCollection, and Search filters."""
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    await _seed_async(async_driver, _FILTER_SEED_ROWS, table, case)
    base = f"SELECT name, value FROM {table.name}"

    paged = await async_driver.execute(base, OrderByFilter("value", "desc"), LimitOffsetFilter(limit=2, offset=1))
    assert [row["name"] for row in paged.get_data()] == ["delta", "gamma"]

    in_collection = await async_driver.execute(
        base, InCollectionFilter("value", [20, 40]), OrderByFilter("value", "asc")
    )
    assert [row["name"] for row in in_collection.get_data()] == ["beta", "delta"]

    if case.supports_search_filter:
        searched = await async_driver.execute(base, SearchFilter("name", "lta"))
        assert [row["name"] for row in searched.get_data()] == ["delta"]


def assert_sync_complex_query_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers run grouped aggregation and correlated subquery selects."""
    if not case.supports_grouped_subquery:
        pytest.skip(f"{case.adapter} does not support grouped aggregation / correlated subqueries")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    _seed_sync(sync_driver, _GROUPED_SEED_ROWS, table, case)

    grouped = sync_driver.execute(
        f"SELECT value, COUNT(*) AS count FROM {table.name} GROUP BY value HAVING COUNT(*) >= 2 ORDER BY value"
    )
    assert [(row["value"], row["count"]) for row in grouped.get_data()] == [(20, 2), (30, 2)]

    top = sync_driver.execute(
        f"SELECT name FROM {table.name} WHERE value = (SELECT MAX(value) FROM {table.name}) ORDER BY name"
    )
    assert [row["name"] for row in top.get_data()] == ["delta", "epsilon"]


async def assert_async_complex_query_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers run grouped aggregation and correlated subquery selects."""
    if not case.supports_grouped_subquery:
        pytest.skip(f"{case.adapter} does not support grouped aggregation / correlated subqueries")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    await _seed_async(async_driver, _GROUPED_SEED_ROWS, table, case)

    grouped = await async_driver.execute(
        f"SELECT value, COUNT(*) AS count FROM {table.name} GROUP BY value HAVING COUNT(*) >= 2 ORDER BY value"
    )
    assert [(row["value"], row["count"]) for row in grouped.get_data()] == [(20, 2), (30, 2)]

    top = await async_driver.execute(
        f"SELECT name FROM {table.name} WHERE value = (SELECT MAX(value) FROM {table.name}) ORDER BY name"
    )
    assert [row["name"] for row in top.get_data()] == ["delta", "epsilon"]


STATEMENT_STACK_SCOPE = "statement_stack"
STATEMENT_STACK_PARITY_PROOF_KEY = "statement_stack:native_fallback_parity"


def assert_sync_statement_stack_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers execute a StatementStack sequentially with per-operation results."""
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    assert_execute_rows = _reports_execute_rows_affected(case)

    sync_driver.execute(table.delete_sql)
    sync_driver.commit()
    stack = (
        StatementStack()
        .push_execute(table.insert_qmark_sql, ("stack-one", 10, None))
        .push_execute(table.insert_qmark_sql, ("stack-two", 20, None))
        .push_execute(table.select_count_sql)
    )
    results = sync_driver.execute_stack(stack)
    sync_driver.commit()
    try:
        assert len(results) == 3
        if assert_execute_rows:
            assert results[0].rows_affected == 1
            assert results[1].rows_affected == 1
        count_result = results[2].result
        assert isinstance(count_result, SQLResult)
        assert count_result.get_data()[0]["count"] == 2
        dispatch_sync_extra_assertions(driver, case, STATEMENT_STACK_SCOPE)
    finally:
        sync_driver.execute(table.delete_sql)
        sync_driver.commit()


async def assert_async_statement_stack_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers execute a StatementStack sequentially with per-operation results."""
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    assert_execute_rows = _reports_execute_rows_affected(case)

    await async_driver.execute(table.delete_sql)
    await async_driver.commit()
    stack = (
        StatementStack()
        .push_execute(table.insert_qmark_sql, ("stack-one", 10, None))
        .push_execute(table.insert_qmark_sql, ("stack-two", 20, None))
        .push_execute(table.select_count_sql)
    )
    results = await async_driver.execute_stack(stack)
    await async_driver.commit()
    try:
        assert len(results) == 3
        if assert_execute_rows:
            assert results[0].rows_affected == 1
            assert results[1].rows_affected == 1
        count_result = results[2].result
        assert isinstance(count_result, SQLResult)
        assert count_result.get_data()[0]["count"] == 2
        await dispatch_async_extra_assertions(driver, case, STATEMENT_STACK_SCOPE)
    finally:
        await async_driver.execute(table.delete_sql)
        await async_driver.commit()


def _statement_stack_parity_input(table: ContractTable) -> StatementStack:
    return (
        StatementStack()
        .push_execute(table.insert_qmark_sql, ("parity-first", 10, None))
        .push_execute(table.insert_qmark_sql, (None, 20, None))
        .push_execute(table.insert_qmark_sql, ("parity-third", 30, None))
    )


def _sync_statement_stack_observation(
    driver: SyncContractDriver, case: DriverCase, *, fallback: bool, continue_on_error: bool
) -> "tuple[tuple[str, ...], type[Exception], int]":
    table = case.table
    driver.execute(table.delete_sql)
    driver.commit()
    stack_error: StackExecutionError | None = None
    try:
        stack = _statement_stack_parity_input(table)
        if continue_on_error:
            if fallback:
                results = SyncDriverAdapterBase.execute_stack(cast("Any", driver), stack, continue_on_error=True)
            else:
                driver_any = cast("Any", driver)
                prepared_operations = driver_any._prepare_pipeline_operations(stack)
                assert prepared_operations is not None
                results = driver_any._execute_stack_pipeline(stack, prepared_operations)
            assert len(results) == 3
            assert results[0].error is None
            assert results[2].error is None
            result_error = results[1].error
            assert isinstance(result_error, StackExecutionError)
            stack_error = result_error
        else:
            try:
                if fallback:
                    SyncDriverAdapterBase.execute_stack(cast("Any", driver), stack, continue_on_error=False)
                else:
                    driver_any = cast("Any", driver)
                    prepared_operations = driver_any._prepare_pipeline_operations(stack)
                    assert prepared_operations is not None
                    driver_any._execute_stack_pipeline(stack, prepared_operations)
            except StackExecutionError as exc:
                stack_error = exc
            else:
                msg = "fail-fast statement stack did not raise StackExecutionError"
                raise AssertionError(msg)

        driver.rollback()
        durable_rows = driver.execute(table.select_ordered_sql).get_data()
        assert stack_error is not None
        return tuple(row["name"] for row in durable_rows), type(stack_error), stack_error.operation_index
    finally:
        with contextlib.suppress(Exception):
            driver.rollback()
        driver.execute(table.delete_sql)
        driver.commit()


async def _async_statement_stack_observation(
    driver: AsyncContractDriver, case: DriverCase, *, fallback: bool, continue_on_error: bool
) -> "tuple[tuple[str, ...], type[Exception], int]":
    table = case.table
    await driver.execute(table.delete_sql)
    await driver.commit()
    stack_error: StackExecutionError | None = None
    try:
        stack = _statement_stack_parity_input(table)
        if continue_on_error:
            if fallback:
                results = await AsyncDriverAdapterBase.execute_stack(cast("Any", driver), stack, continue_on_error=True)
            elif case.adapter == "psycopg":
                driver_any = cast("Any", driver)
                prepared_operations = driver_any._prepare_pipeline_operations(stack)
                assert prepared_operations is not None
                results = await driver_any._execute_stack_pipeline(stack, prepared_operations)
            else:
                results = await cast("Any", driver)._execute_stack_native(stack, continue_on_error=True)
            assert len(results) == 3
            assert results[0].error is None
            assert results[2].error is None
            result_error = results[1].error
            assert isinstance(result_error, StackExecutionError)
            stack_error = result_error
        else:
            try:
                if fallback:
                    await AsyncDriverAdapterBase.execute_stack(cast("Any", driver), stack, continue_on_error=False)
                elif case.adapter == "psycopg":
                    driver_any = cast("Any", driver)
                    prepared_operations = driver_any._prepare_pipeline_operations(stack)
                    assert prepared_operations is not None
                    await driver_any._execute_stack_pipeline(stack, prepared_operations)
                else:
                    await cast("Any", driver)._execute_stack_native(stack, continue_on_error=False)
            except StackExecutionError as exc:
                stack_error = exc
            else:
                msg = "fail-fast statement stack did not raise StackExecutionError"
                raise AssertionError(msg)

        await driver.rollback()
        durable_rows = (await driver.execute(table.select_ordered_sql)).get_data()
        assert stack_error is not None
        return tuple(row["name"] for row in durable_rows), type(stack_error), stack_error.operation_index
    finally:
        with contextlib.suppress(Exception):
            await driver.rollback()
        await driver.execute(table.delete_sql)
        await driver.commit()


def assert_sync_statement_stack_parity_contract(driver: object, case: DriverCase) -> None:
    """Assert psycopg native fail-fast stacks match the base fallback contract."""
    if case.adapter != "psycopg":
        pytest.skip(f"{case.adapter} has no active sync native stack parity case")
    from sqlspec.adapters.psycopg.core import pipeline_supported

    if not pipeline_supported():
        pytest.skip("psycopg pipeline unavailable")
    sync_driver = cast("SyncContractDriver", driver)
    driver_any = cast("Any", driver)
    if driver_any.stack_native_disabled:
        pytest.skip("native statement stacks disabled")

    fallback = _sync_statement_stack_observation(sync_driver, case, fallback=True, continue_on_error=False)
    native = _sync_statement_stack_observation(sync_driver, case, fallback=False, continue_on_error=False)
    assert native == fallback
    assert native == ((), StackExecutionError, 1)


async def assert_async_statement_stack_parity_contract(driver: object, case: DriverCase) -> None:
    """Assert async native stacks match valid base fallback error and durability semantics."""
    if case.adapter not in {"asyncpg", "psycopg", "oracledb"}:
        pytest.skip(f"{case.adapter} has no active async native stack parity case")
    if case.adapter == "psycopg":
        from sqlspec.adapters.psycopg.core import pipeline_supported

        if not pipeline_supported():
            pytest.skip("psycopg pipeline unavailable")
    async_driver = cast("AsyncContractDriver", driver)
    driver_any = cast("Any", driver)
    if driver_any.stack_native_disabled:
        pytest.skip("native statement stacks disabled")
    if case.adapter == "oracledb" and not await driver_any._pipeline_native_supported():
        pytest.skip("Oracle native pipeline unavailable")

    continue_on_error = case.adapter != "psycopg"
    fallback = await _async_statement_stack_observation(
        async_driver, case, fallback=True, continue_on_error=continue_on_error
    )
    native = await _async_statement_stack_observation(
        async_driver, case, fallback=False, continue_on_error=continue_on_error
    )
    assert native == fallback
    expected_durable_names = () if case.adapter == "psycopg" else ("parity-first", "parity-third")
    assert native == (expected_durable_names, StackExecutionError, 1)


register_sync_extra_assertion(
    STATEMENT_STACK_PARITY_PROOF_KEY, STATEMENT_STACK_SCOPE, assert_sync_statement_stack_parity_contract
)
register_async_extra_assertion(
    STATEMENT_STACK_PARITY_PROOF_KEY, STATEMENT_STACK_SCOPE, assert_async_statement_stack_parity_contract
)


async def _oracle_native_statement_stack(driver: object, case: DriverCase) -> None:
    """Fold Oracle async native pipeline and continue-on-error stack proofs."""
    async_driver = cast("AsyncContractDriver", driver)
    driver_any = cast("Any", driver)
    if not await driver_any._pipeline_native_supported():
        pytest.skip("Oracle native pipeline unavailable")

    native_table = _pc_table(case, "stack_native")
    error_table = _pc_table(case, "stack_errors")
    await _async_drop_table(async_driver, native_table)
    await _async_drop_table(async_driver, error_table)
    await async_driver.execute(f"CREATE TABLE {native_table} (id NUMBER PRIMARY KEY, name VARCHAR2(50))")
    await async_driver.execute(f"CREATE TABLE {error_table} (id NUMBER PRIMARY KEY, name VARCHAR2(50))")
    await async_driver.commit()

    driver_type = type(driver)
    original_execute_stack_native = getattr(driver_type, "_execute_stack_native")
    call_counter = {"count": 0}

    async def tracking_execute_stack_native(
        self: object, stack: StatementStack, *, continue_on_error: bool
    ) -> tuple[Any, ...]:
        call_counter["count"] += 1
        result = await original_execute_stack_native(self, stack, continue_on_error=continue_on_error)
        return cast("tuple[Any, ...]", result)

    setattr(driver_type, "_execute_stack_native", tracking_execute_stack_native)
    try:
        stack = (
            StatementStack()
            .push_execute(f"INSERT INTO {native_table} (id, name) VALUES (:id, :name)", {"id": 1, "name": "alpha"})
            .push_execute(f"INSERT INTO {native_table} (id, name) VALUES (:id, :name)", {"id": 2, "name": "beta"})
            .push_execute(f"SELECT name FROM {native_table} WHERE id = :id", {"id": 2})
        )
        results = await async_driver.execute_stack(stack)
        assert call_counter["count"] == 1
        assert len(results) == 3
        assert results[0].rows_affected == 1
        assert results[1].rows_affected == 1
        last_result = results[2].result
        assert isinstance(last_result, SQLResult)
        assert last_result.get_data()[0]["name"] == "beta"

        error_stack = (
            StatementStack()
            .push_execute(f"INSERT INTO {error_table} (id, name) VALUES (:id, :name)", {"id": 1, "name": "alpha"})
            .push_execute(f"INSERT INTO {error_table} (id, name) VALUES (:id, :name)", {"id": 1, "name": "duplicate"})
            .push_execute(f"INSERT INTO {error_table} (id, name) VALUES (:id, :name)", {"id": 2, "name": "beta"})
        )
        error_results = await async_driver.execute_stack(error_stack, continue_on_error=True)
        assert len(error_results) == 3
        assert error_results[0].rows_affected == 1
        assert isinstance(error_results[1].error, StackExecutionError)
        assert error_results[2].rows_affected == 1

        verify_result = await async_driver.execute(
            f"SELECT COUNT(*) AS total_rows FROM {error_table} WHERE id = :id", {"id": 2}
        )
        assert verify_result.get_data()[0]["total_rows"] == 1
    finally:
        setattr(driver_type, "_execute_stack_native", original_execute_stack_native)
        await _async_drop_table(async_driver, native_table)
        await _async_drop_table(async_driver, error_table)


register_async_extra_assertion("statement_stack:oracle_native", STATEMENT_STACK_SCOPE, _oracle_native_statement_stack)


def assert_sync_transaction_semantics_contract(driver: object, case: DriverCase) -> None:
    """Assert sync transaction rollback, commit, and caller-owned stack boundaries."""
    if not case.supports_transactions:
        pytest.skip(f"{case.adapter} has no verified transaction support")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    sync_driver.execute(table.delete_sql)
    sync_driver.commit()
    try:
        sync_driver.begin()
        sync_driver.execute(table.insert_qmark_sql, ("transaction-rollback", 10, None))
        sync_driver.rollback()
        assert sync_driver.select_value(table.select_count_sql) == 0
        sync_driver.commit()

        sync_driver.begin()
        sync_driver.execute(table.insert_qmark_sql, ("transaction-commit", 20, None))
        sync_driver.commit()
        assert sync_driver.select_value(table.select_count_sql) == 1
        sync_driver.commit()

        sync_driver.execute(table.delete_sql)
        sync_driver.commit()
        sync_driver.begin()
        sync_driver.execute(table.insert_qmark_sql, ("transaction-outer-direct", 30, None))
        stack = StatementStack().push_execute(table.insert_qmark_sql, ("transaction-outer-stack", 40, None))
        stack_results = sync_driver.execute_stack(stack)
        assert len(stack_results) == 1
        assert stack_results[0].error is None
        sync_driver.rollback()
        assert sync_driver.select_value(table.select_count_sql) == 0
        sync_driver.commit()
    finally:
        with contextlib.suppress(Exception):
            sync_driver.rollback()
        sync_driver.execute(table.delete_sql)
        sync_driver.commit()


async def assert_async_transaction_semantics_contract(driver: object, case: DriverCase) -> None:
    """Assert async transaction rollback, commit, and caller-owned stack boundaries."""
    if not case.supports_transactions:
        pytest.skip(f"{case.adapter} has no verified transaction support")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    await async_driver.execute(table.delete_sql)
    await async_driver.commit()
    try:
        await async_driver.begin()
        await async_driver.execute(table.insert_qmark_sql, ("transaction-rollback", 10, None))
        await async_driver.rollback()
        assert await async_driver.select_value(table.select_count_sql) == 0
        await async_driver.commit()

        await async_driver.begin()
        await async_driver.execute(table.insert_qmark_sql, ("transaction-commit", 20, None))
        await async_driver.commit()
        assert await async_driver.select_value(table.select_count_sql) == 1
        await async_driver.commit()

        await async_driver.execute(table.delete_sql)
        await async_driver.commit()
        await async_driver.begin()
        await async_driver.execute(table.insert_qmark_sql, ("transaction-outer-direct", 30, None))
        stack = StatementStack().push_execute(table.insert_qmark_sql, ("transaction-outer-stack", 40, None))
        stack_results = await async_driver.execute_stack(stack)
        assert len(stack_results) == 1
        assert stack_results[0].error is None
        await async_driver.rollback()
        assert await async_driver.select_value(table.select_count_sql) == 0
        await async_driver.commit()
    finally:
        with contextlib.suppress(Exception):
            await async_driver.rollback()
        await async_driver.execute(table.delete_sql)
        await async_driver.commit()


def assert_sync_execute_many_contract(driver: object, case: DriverCase) -> None:
    """Assert sync execute-many behavior for a driver case."""
    if not case.supports_execute_many:
        pytest.skip(f"{case.adapter} has no verified execute_many support")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    result = sync_driver.execute_many(
        table.insert_qmark_sql, [("alpha", 10, None), ("beta", 20, None), ("gamma", 30, None)]
    )

    _assert_sql_result_rows_affected(result, case, 3, "execute_many")
    assert_result_data(
        sync_driver.execute(table.select_ordered_sql),
        (
            {"name": "alpha", "value": 10, "note": None},
            {"name": "beta", "value": 20, "note": None},
            {"name": "gamma", "value": 30, "note": None},
        ),
    )


async def assert_async_execute_many_contract(driver: object, case: DriverCase) -> None:
    """Assert async execute-many behavior for a driver case."""
    if not case.supports_execute_many:
        pytest.skip(f"{case.adapter} has no verified execute_many support")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    result = await async_driver.execute_many(
        table.insert_qmark_sql, [("alpha", 10, None), ("beta", 20, None), ("gamma", 30, None)]
    )

    _assert_sql_result_rows_affected(result, case, 3, "execute_many")
    assert_result_data(
        await async_driver.execute(table.select_ordered_sql),
        (
            {"name": "alpha", "value": 10, "note": None},
            {"name": "beta", "value": 20, "note": None},
            {"name": "gamma", "value": 30, "note": None},
        ),
    )


def assert_sync_execute_many_empty_contract(driver: object, case: DriverCase) -> None:
    """Assert an empty sync execute-many batch is a no-op with a zero row count."""
    if not case.supports_execute_many:
        pytest.skip(f"{case.adapter} has no verified execute_many support")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    result = sync_driver.execute_many(table.insert_qmark_sql, [])

    assert result.rows_affected == 0
    assert sync_driver.select_value(table.select_count_sql) == 0


async def assert_async_execute_many_empty_contract(driver: object, case: DriverCase) -> None:
    """Assert an empty async execute-many batch is a no-op with a zero row count."""
    if not case.supports_execute_many:
        pytest.skip(f"{case.adapter} has no verified execute_many support")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    result = await async_driver.execute_many(table.insert_qmark_sql, [])

    assert result.rows_affected == 0
    assert await async_driver.select_value(table.select_count_sql) == 0


def assert_sync_execute_many_mutation_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers batch insert, update, and delete with accurate row counts."""
    if not case.supports_execute_many:
        pytest.skip(f"{case.adapter} has no verified execute_many support")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    inserted = sync_driver.execute_many(table.insert_qmark_sql, [("a", 1, None), ("b", 2, None), ("c", 3, None)])
    _assert_sql_result_rows_affected(inserted, case, 3, "execute_many")
    sync_driver.commit()
    assert sync_driver.select_value(table.select_count_sql) == 3

    updated = sync_driver.execute_many(_update_value_sql(table), [(10, "a"), (20, "b")])
    _assert_sql_result_rows_affected(updated, case, 2, "execute_many")
    sync_driver.commit()

    deleted = sync_driver.execute_many(_delete_by_name_sql(table), [("a",), ("b",)])
    _assert_sql_result_rows_affected(deleted, case, 2, "execute_many")
    sync_driver.commit()
    assert sync_driver.select_value(table.select_count_sql) == 1


async def assert_async_execute_many_mutation_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers batch insert, update, and delete with accurate row counts."""
    if not case.supports_execute_many:
        pytest.skip(f"{case.adapter} has no verified execute_many support")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    inserted = await async_driver.execute_many(table.insert_qmark_sql, [("a", 1, None), ("b", 2, None), ("c", 3, None)])
    _assert_sql_result_rows_affected(inserted, case, 3, "execute_many")
    await async_driver.commit()
    assert await async_driver.select_value(table.select_count_sql) == 3

    updated = await async_driver.execute_many(_update_value_sql(table), [(10, "a"), (20, "b")])
    _assert_sql_result_rows_affected(updated, case, 2, "execute_many")
    await async_driver.commit()

    deleted = await async_driver.execute_many(_delete_by_name_sql(table), [("a",), ("b",)])
    _assert_sql_result_rows_affected(deleted, case, 2, "execute_many")
    await async_driver.commit()
    assert await async_driver.select_value(table.select_count_sql) == 1


def assert_sync_execute_many_input_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers batch a large sequence and an is_many SQL object."""
    if not case.supports_execute_many:
        pytest.skip(f"{case.adapter} has no verified execute_many support")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    large_batch = [(f"item-{index}", index, None) for index in range(200)]
    large_result = sync_driver.execute_many(table.insert_qmark_sql, large_batch)
    _assert_sql_result_rows_affected(large_result, case, 200, "execute_many")
    sync_driver.commit()
    assert sync_driver.select_value(table.select_count_sql) == 200

    sql_object = SQL(table.insert_qmark_sql, [("obj-1", 1, None), ("obj-2", 2, None)], is_many=True)
    object_result = sync_driver.execute(sql_object)
    _assert_sql_result_rows_affected(object_result, case, 2, "execute_many")
    sync_driver.commit()
    assert sync_driver.select_value(table.select_count_sql) == 202


async def assert_async_execute_many_input_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers batch a large sequence and an is_many SQL object."""
    if not case.supports_execute_many:
        pytest.skip(f"{case.adapter} has no verified execute_many support")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    large_batch = [(f"item-{index}", index, None) for index in range(200)]
    large_result = await async_driver.execute_many(table.insert_qmark_sql, large_batch)
    _assert_sql_result_rows_affected(large_result, case, 200, "execute_many")
    await async_driver.commit()
    assert await async_driver.select_value(table.select_count_sql) == 200

    sql_object = SQL(table.insert_qmark_sql, [("obj-1", 1, None), ("obj-2", 2, None)], is_many=True)
    object_result = await async_driver.execute(sql_object)
    _assert_sql_result_rows_affected(object_result, case, 2, "execute_many")
    await async_driver.commit()
    assert await async_driver.select_value(table.select_count_sql) == 202


EXECUTE_MANY_SPECIFICS_SCOPE = "execute_many_specifics"


def _em_table(case: DriverCase, suffix: str) -> str:
    return f"em_spec_{suffix}_{case.adapter}_{case.mode}"


def _postgres_execute_many_specifics(driver: object, case: DriverCase) -> None:
    """Fold PostgreSQL execute_many edge cases: empty batch, NULLs, RETURNING, arrays, JSONB, UPSERT (sync)."""
    sync_driver = cast("SyncContractDriver", driver)
    batch = _em_table(case, "batch")
    _sync_drop_table(sync_driver, batch)
    sync_driver.execute(
        f"CREATE TABLE {batch} (id SERIAL PRIMARY KEY, name TEXT NOT NULL, value INTEGER, category TEXT)"
    )
    sync_driver.commit()
    insert = f"INSERT INTO {batch} (name, value, category) VALUES (?, ?, ?)"
    try:
        empty = sync_driver.execute_many(insert, [])
        assert empty.rows_affected == 0
        assert sync_driver.select_value(f"SELECT COUNT(*) AS count FROM {batch}") == 0

        sync_driver.execute_many(
            insert, [("String Item", 123, "CAT1"), ("Null Item", 456, None), ("Neg Item", -50, "CAT3")]
        )
        sync_driver.commit()
        null_rows = sync_driver.execute(f"SELECT name FROM {batch} WHERE category IS NULL").get_data()
        assert len(null_rows) == 1
        assert null_rows[0]["name"] == "Null Item"

        sync_driver.execute(f"DELETE FROM {batch}")
        sync_driver.commit()
        returning = f"INSERT INTO {batch} (name, value, category) VALUES (?, ?, ?) RETURNING id, name"
        sync_driver.execute_many(returning, [("R1", 1, "RET"), ("R2", 2, "RET"), ("R3", 3, "RET")])
        sync_driver.commit()
        assert sync_driver.select_value(f"SELECT COUNT(*) AS count FROM {batch} WHERE category = 'RET'") == 3
    finally:
        _sync_drop_table(sync_driver, batch)

    _postgres_em_arrays_json_sync(sync_driver, case)
    _postgres_em_upsert_sync(sync_driver, case)


def _postgres_em_arrays_json_sync(sync_driver: "SyncContractDriver", case: DriverCase) -> None:
    arrays = _em_table(case, "arrays")
    _sync_drop_table(sync_driver, arrays)
    sync_driver.execute(f"CREATE TABLE {arrays} (id SERIAL PRIMARY KEY, name TEXT, tags TEXT[], scores INTEGER[])")
    sync_driver.commit()
    try:
        sync_driver.execute_many(
            f"INSERT INTO {arrays} (name, tags, scores) VALUES (?, ?, ?)",
            [("A1", ["tag1", "tag2"], [10, 20, 30]), ("A2", ["tag3"], [40, 50])],
        )
        sync_driver.commit()
        counts = sync_driver.execute(
            f"SELECT name, array_length(tags, 1) AS tag_count FROM {arrays} ORDER BY name"
        ).get_data()
        assert [row["tag_count"] for row in counts] == [2, 1]
    finally:
        _sync_drop_table(sync_driver, arrays)

    json_table = _em_table(case, "json")
    _sync_drop_table(sync_driver, json_table)
    sync_driver.execute(f"CREATE TABLE {json_table} (id SERIAL PRIMARY KEY, name TEXT, metadata JSONB)")
    sync_driver.commit()
    try:
        payloads = [("J1", to_json({"type": "test", "value": 100})), ("J2", to_json({"type": "prod", "value": 200}))]
        sync_driver.execute_many(f"INSERT INTO {json_table} (name, metadata) VALUES (?, ?)", payloads)
        sync_driver.commit()
        rows = sync_driver.execute(f"SELECT name, metadata->>'type' AS type FROM {json_table} ORDER BY name").get_data()
        assert [row["type"] for row in rows] == ["test", "prod"]
    finally:
        _sync_drop_table(sync_driver, json_table)


def _postgres_em_upsert_sync(sync_driver: "SyncContractDriver", case: DriverCase) -> None:
    upsert = _em_table(case, "upsert")
    _sync_drop_table(sync_driver, upsert)
    sync_driver.execute(f"CREATE TABLE {upsert} (id INTEGER PRIMARY KEY, name TEXT, counter INTEGER DEFAULT 1)")
    sync_driver.commit()
    try:
        sync_driver.execute_many(f"INSERT INTO {upsert} (id, name) VALUES (?, ?)", [(1, "Item 1"), (2, "Item 2")])
        sync_driver.commit()
        sync_driver.execute_many(
            f"INSERT INTO {upsert} (id, name) VALUES (?, ?) "
            f"ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, counter = {upsert}.counter + 1",
            [(1, "Updated 1"), (3, "Item 3")],
        )
        sync_driver.commit()
        assert sync_driver.select_value(f"SELECT COUNT(*) AS count FROM {upsert}") == 3
        assert sync_driver.select_value(f"SELECT counter FROM {upsert} WHERE id = 1") == 2
    finally:
        _sync_drop_table(sync_driver, upsert)


async def _postgres_execute_many_specifics_async(driver: object, case: DriverCase) -> None:
    """Fold PostgreSQL execute_many edge cases: empty batch, NULLs, RETURNING, arrays, JSONB (async)."""
    async_driver = cast("AsyncContractDriver", driver)
    batch = _em_table(case, "batch")
    await _async_drop_table(async_driver, batch)
    await async_driver.execute(
        f"CREATE TABLE {batch} (id SERIAL PRIMARY KEY, name TEXT NOT NULL, value INTEGER, category TEXT)"
    )
    await async_driver.commit()
    insert = f"INSERT INTO {batch} (name, value, category) VALUES (?, ?, ?)"
    try:
        empty = await async_driver.execute_many(insert, [])
        assert empty.rows_affected == 0
        assert await async_driver.select_value(f"SELECT COUNT(*) AS count FROM {batch}") == 0

        await async_driver.execute_many(
            insert, [("String Item", 123, "CAT1"), ("Null Item", 456, None), ("Neg Item", -50, "CAT3")]
        )
        await async_driver.commit()
        null_rows = (await async_driver.execute(f"SELECT name FROM {batch} WHERE category IS NULL")).get_data()
        assert len(null_rows) == 1
        assert null_rows[0]["name"] == "Null Item"

        await async_driver.execute(f"DELETE FROM {batch}")
        await async_driver.commit()
        returning = f"INSERT INTO {batch} (name, value, category) VALUES (?, ?, ?) RETURNING id, name"
        await async_driver.execute_many(returning, [("R1", 1, "RET"), ("R2", 2, "RET"), ("R3", 3, "RET")])
        await async_driver.commit()
        assert await async_driver.select_value(f"SELECT COUNT(*) AS count FROM {batch} WHERE category = 'RET'") == 3
    finally:
        await _async_drop_table(async_driver, batch)

    arrays = _em_table(case, "arrays")
    await _async_drop_table(async_driver, arrays)
    await async_driver.execute(
        f"CREATE TABLE {arrays} (id SERIAL PRIMARY KEY, name TEXT, tags TEXT[], scores INTEGER[])"
    )
    await async_driver.commit()
    try:
        await async_driver.execute_many(
            f"INSERT INTO {arrays} (name, tags, scores) VALUES (?, ?, ?)",
            [("A1", ["tag1", "tag2"], [10, 20, 30]), ("A2", ["tag3"], [40, 50])],
        )
        await async_driver.commit()
        counts = (
            await async_driver.execute(f"SELECT name, array_length(tags, 1) AS tag_count FROM {arrays} ORDER BY name")
        ).get_data()
        assert [row["tag_count"] for row in counts] == [2, 1]
    finally:
        await _async_drop_table(async_driver, arrays)

    json_table = _em_table(case, "json")
    await _async_drop_table(async_driver, json_table)
    await async_driver.execute(f"CREATE TABLE {json_table} (id SERIAL PRIMARY KEY, name TEXT, metadata JSONB)")
    await async_driver.commit()
    try:
        payloads = [("J1", to_json({"type": "test", "value": 100})), ("J2", to_json({"type": "prod", "value": 200}))]
        await async_driver.execute_many(f"INSERT INTO {json_table} (name, metadata) VALUES (?, ?)", payloads)
        await async_driver.commit()
        rows = (
            await async_driver.execute(f"SELECT name, metadata->>'type' AS type FROM {json_table} ORDER BY name")
        ).get_data()
        assert [row["type"] for row in rows] == ["test", "prod"]
    finally:
        await _async_drop_table(async_driver, json_table)


def _duckdb_execute_many_specifics(driver: object, case: DriverCase) -> None:
    """Fold DuckDB execute_many edge cases: empty batch, mixed types, arrays, analytics, time series."""
    from datetime import datetime, timedelta

    sync_driver = cast("SyncContractDriver", driver)
    batch = _em_table(case, "batch")
    _sync_drop_table(sync_driver, batch)
    sync_driver.execute(
        f"CREATE TABLE {batch} (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, value INTEGER, category VARCHAR)"
    )
    insert = f"INSERT INTO {batch} (id, name, value, category) VALUES (?, ?, ?, ?)"
    try:
        empty = sync_driver.execute_many(insert, [])
        assert empty.rows_affected == 0
        assert sync_driver.select_value(f"SELECT COUNT(*) AS count FROM {batch}") == 0

        mixed = sync_driver.execute_many(
            insert, [(1, "Item 1", 123, "CAT1"), (2, "Null Item", 456, None), (3, "Item 3", 0, "CAT2")]
        )
        assert mixed.rows_affected == 3
        null_rows = sync_driver.execute(f"SELECT name FROM {batch} WHERE category IS NULL").get_data()
        assert len(null_rows) == 1
        assert null_rows[0]["name"] == "Null Item"

        analytics = [(i + 10, f"A{i}", i * 10, f"G{i % 2}") for i in range(1, 11)]
        sync_driver.execute_many(insert, analytics)
        grouped = sync_driver.execute(
            f"SELECT category, COUNT(*) AS count FROM {batch} WHERE id >= 10 GROUP BY category ORDER BY category"
        ).get_data()
        assert {row["category"] for row in grouped} == {"G0", "G1"}
    finally:
        _sync_drop_table(sync_driver, batch)

    arrays = _em_table(case, "arrays")
    _sync_drop_table(sync_driver, arrays)
    sync_driver.execute(
        f"CREATE TABLE {arrays} (id INTEGER PRIMARY KEY, name VARCHAR, numbers INTEGER[], tags VARCHAR[])"
    )
    try:
        sync_driver.execute_many(
            f"INSERT INTO {arrays} (id, name, numbers, tags) VALUES (?, ?, ?, ?)",
            [(1, "A1", [10, 20, 30], ["t1", "t2"]), (2, "A2", [40], ["t3"])],
        )
        counts = sync_driver.execute(f"SELECT name, len(numbers) AS num_count FROM {arrays} ORDER BY name").get_data()
        assert [row["num_count"] for row in counts] == [3, 1]
    finally:
        _sync_drop_table(sync_driver, arrays)

    series = _em_table(case, "series")
    _sync_drop_table(sync_driver, series)
    sync_driver.execute(f"CREATE TABLE {series} (id INTEGER PRIMARY KEY, ts TIMESTAMP, metric VARCHAR, val DOUBLE)")
    try:
        base = datetime(2024, 1, 1)
        rows = [(i, base + timedelta(hours=i), f"m{i % 3}", float(i * 10.5)) for i in range(1, 13)]
        result = sync_driver.execute_many(f"INSERT INTO {series} (id, ts, metric, val) VALUES (?, ?, ?, ?)", rows)
        assert result.rows_affected == 12
        agg = sync_driver.execute(
            f"SELECT metric, COUNT(*) AS points FROM {series} GROUP BY metric ORDER BY metric"
        ).get_data()
        assert len(agg) == 3
    finally:
        _sync_drop_table(sync_driver, series)


register_sync_extra_assertion(
    "execute_many_specifics:postgres", EXECUTE_MANY_SPECIFICS_SCOPE, _postgres_execute_many_specifics
)
register_async_extra_assertion(
    "execute_many_specifics:postgres", EXECUTE_MANY_SPECIFICS_SCOPE, _postgres_execute_many_specifics_async
)
register_sync_extra_assertion(
    "execute_many_specifics:duckdb", EXECUTE_MANY_SPECIFICS_SCOPE, _duckdb_execute_many_specifics
)


def assert_sync_execute_many_specifics_contract(driver: object, case: DriverCase) -> None:
    """Run an adapter's folded driver-specific execute_many proofs (arrays/JSON/edge cases), if any."""
    dispatch_sync_extra_assertions(driver, case, EXECUTE_MANY_SPECIFICS_SCOPE)


async def assert_async_execute_many_specifics_contract(driver: object, case: DriverCase) -> None:
    """Run an adapter's folded driver-specific execute_many proofs (arrays/JSON/edge cases), if any."""
    await dispatch_async_extra_assertions(driver, case, EXECUTE_MANY_SPECIFICS_SCOPE)


PARAM_CODECS_SCOPE = "param_codecs"


def _pc_table(case: DriverCase, suffix: str) -> str:
    return f"pc_{suffix}_{case.adapter}_{case.mode}"


def _merge_dialect(case: DriverCase) -> str:
    return "oracle" if case.dialect == "oracle" else "postgres"


def _merge_table_sql(case: DriverCase, table: str) -> str:
    if case.dialect == "oracle":
        return (
            f"CREATE TABLE {table} ("
            "id NUMBER PRIMARY KEY, "
            "name VARCHAR2(100) NOT NULL, "
            "price NUMBER(10, 2), "
            "stock NUMBER DEFAULT 0, "
            "category VARCHAR2(50)"
            ")"
        )
    return (
        f"CREATE TABLE {table} ("
        "id INTEGER PRIMARY KEY, "
        "name TEXT NOT NULL, "
        "price NUMERIC(10, 2), "
        "stock INTEGER DEFAULT 0, "
        "category TEXT"
        ")"
    )


def _drop_merge_table_sync(driver: "SyncContractDriver", case: DriverCase, table: str) -> None:
    if case.dialect == "oracle":
        driver.execute_script(_oracle_drop_sql("TABLE", table))
    else:
        driver.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        driver.commit()


async def _drop_merge_table_async(driver: "AsyncContractDriver", case: DriverCase, table: str) -> None:
    if case.dialect == "oracle":
        await driver.execute_script(_oracle_drop_sql("TABLE", table))
    else:
        await driver.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        await driver.commit()


def _merge_source(rows: list[dict[str, object]], case: DriverCase) -> object:
    return rows[0] if case.dialect == "oracle" and len(rows) == 1 else rows


def _merge_upsert_query(table: str, rows: list[dict[str, object]], case: DriverCase) -> object:
    return (
        sql
        .merge(dialect=_merge_dialect(case))
        .into(table, alias="t")
        .using(_merge_source(rows, case), alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price", stock="src.stock", category="src.category")
        .when_not_matched_then_insert(
            id="src.id", name="src.name", price="src.price", stock="src.stock", category="src.category"
        )
    )


def _merge_price(value: object) -> float | None:
    if value is None:
        return None
    assert isinstance(value, int | float | Decimal)
    return float(value)


def assert_sync_merge_contract(driver: object, case: DriverCase) -> None:
    """Assert sync MERGE updates, inserts, expressions, conditions, NULLs, and table sources."""
    if not case.supports_merge:
        pytest.skip(f"{case.id} does not support MERGE")
    sync_driver = cast("SyncContractDriver", driver)
    target = _pc_table(case, "merge")
    source = _pc_table(case, "merge_src")
    _drop_merge_table_sync(sync_driver, case, source)
    _drop_merge_table_sync(sync_driver, case, target)
    sync_driver.execute(_merge_table_sql(case, target))
    sync_driver.execute(_merge_table_sql(case, source))
    sync_driver.execute(
        f"INSERT INTO {target} (id, name, price, stock, category) VALUES (:id, :name, :price, :stock, :category)",
        id=1,
        name="Existing Product",
        price=Decimal("19.99"),
        stock=10,
        category="old",
    )
    sync_driver.commit()
    try:
        result = sync_driver.execute(
            _merge_upsert_query(
                target,
                [{"id": 1, "name": "Updated Product", "price": Decimal("24.99"), "stock": 11, "category": "new"}],
                case,
            )
        )
        assert isinstance(result, SQLResult)
        row = _lower_keys(sync_driver.execute(f"SELECT name, price FROM {target} WHERE id = 1").get_data()[0])
        assert row["name"] == "Updated Product"
        assert _merge_price(row["price"]) == 24.99

        sync_driver.execute(
            _merge_upsert_query(
                target,
                [{"id": 2, "name": "Inserted Product", "price": Decimal("39.99"), "stock": 5, "category": "new"}],
                case,
            )
        )
        inserted = _lower_keys(sync_driver.execute(f"SELECT name, stock FROM {target} WHERE id = 2").get_data()[0])
        assert inserted["name"] == "Inserted Product"
        assert inserted["stock"] == 5

        sync_driver.execute(
            sql
            .merge(dialect=_merge_dialect(case))
            .into(target, alias="t")
            .using(_merge_source([{"id": 1, "additional": 5, "new_price": Decimal("5.00")}], case), alias="src")
            .on("t.id = src.id")
            .when_matched_then_update({"stock": "t.stock + src.additional", "price": "src.new_price"})
        )
        expression = _lower_keys(sync_driver.execute(f"SELECT price, stock FROM {target} WHERE id = 1").get_data()[0])
        assert _merge_price(expression["price"]) == 5.0
        assert expression["stock"] == 16

        sync_driver.execute(
            _merge_upsert_query(
                target, [{"id": 1, "name": "Null Product", "price": None, "stock": None, "category": None}], case
            )
        )
        null_row = _lower_keys(
            sync_driver.execute(f"SELECT price, stock, category FROM {target} WHERE id = 1").get_data()[0]
        )
        assert null_row["price"] is None
        assert null_row["stock"] is None
        assert null_row["category"] is None

        sync_driver.execute(
            f"INSERT INTO {source} (id, name, price, stock, category) VALUES (:id, :name, :price, :stock, :category)",
            id=1,
            name="Staged Update",
            price=Decimal("99.99"),
            stock=100,
            category="staged",
        )
        sync_driver.execute(
            f"INSERT INTO {source} (id, name, price, stock, category) VALUES (:id, :name, :price, :stock, :category)",
            id=3,
            name="Staged Insert",
            price=Decimal("149.99"),
            stock=50,
            category="staged",
        )
        sync_driver.execute(
            sql
            .merge(dialect=_merge_dialect(case))
            .into(target, alias="t")
            .using(source, alias="s")
            .on("t.id = s.id")
            .when_matched_then_update(name="s.name", price="s.price", stock="s.stock", category="s.category")
            .when_not_matched_then_insert(columns=["id", "name", "price", "stock", "category"])
        )
        assert sync_driver.select_value(f"SELECT COUNT(*) AS count FROM {target}") == 3
        staged = _lower_keys(sync_driver.execute(f"SELECT name, stock FROM {target} WHERE id = 3").get_data()[0])
        assert staged["name"] == "Staged Insert"
        assert staged["stock"] == 50
    finally:
        _drop_merge_table_sync(sync_driver, case, source)
        _drop_merge_table_sync(sync_driver, case, target)


async def assert_async_merge_contract(driver: object, case: DriverCase) -> None:
    """Async mirror of assert_sync_merge_contract."""
    if not case.supports_merge:
        pytest.skip(f"{case.id} does not support MERGE")
    async_driver = cast("AsyncContractDriver", driver)
    target = _pc_table(case, "merge")
    source = _pc_table(case, "merge_src")
    await _drop_merge_table_async(async_driver, case, source)
    await _drop_merge_table_async(async_driver, case, target)
    await async_driver.execute(_merge_table_sql(case, target))
    await async_driver.execute(_merge_table_sql(case, source))
    await async_driver.execute(
        f"INSERT INTO {target} (id, name, price, stock, category) VALUES (:id, :name, :price, :stock, :category)",
        id=1,
        name="Existing Product",
        price=Decimal("19.99"),
        stock=10,
        category="old",
    )
    await async_driver.commit()
    try:
        result = await async_driver.execute(
            _merge_upsert_query(
                target,
                [{"id": 1, "name": "Updated Product", "price": Decimal("24.99"), "stock": 11, "category": "new"}],
                case,
            )
        )
        assert isinstance(result, SQLResult)
        row = _lower_keys((await async_driver.execute(f"SELECT name, price FROM {target} WHERE id = 1")).get_data()[0])
        assert row["name"] == "Updated Product"
        assert _merge_price(row["price"]) == 24.99

        await async_driver.execute(
            _merge_upsert_query(
                target,
                [{"id": 2, "name": "Inserted Product", "price": Decimal("39.99"), "stock": 5, "category": "new"}],
                case,
            )
        )
        inserted = _lower_keys(
            (await async_driver.execute(f"SELECT name, stock FROM {target} WHERE id = 2")).get_data()[0]
        )
        assert inserted["name"] == "Inserted Product"
        assert inserted["stock"] == 5

        await async_driver.execute(
            sql
            .merge(dialect=_merge_dialect(case))
            .into(target, alias="t")
            .using(_merge_source([{"id": 1, "additional": 5, "new_price": Decimal("5.00")}], case), alias="src")
            .on("t.id = src.id")
            .when_matched_then_update({"stock": "t.stock + src.additional", "price": "src.new_price"})
        )
        expression = _lower_keys(
            (await async_driver.execute(f"SELECT price, stock FROM {target} WHERE id = 1")).get_data()[0]
        )
        assert _merge_price(expression["price"]) == 5.0
        assert expression["stock"] == 16

        await async_driver.execute(
            _merge_upsert_query(
                target, [{"id": 1, "name": "Null Product", "price": None, "stock": None, "category": None}], case
            )
        )
        null_row = _lower_keys(
            (await async_driver.execute(f"SELECT price, stock, category FROM {target} WHERE id = 1")).get_data()[0]
        )
        assert null_row["price"] is None
        assert null_row["stock"] is None
        assert null_row["category"] is None

        await async_driver.execute(
            f"INSERT INTO {source} (id, name, price, stock, category) VALUES (:id, :name, :price, :stock, :category)",
            id=1,
            name="Staged Update",
            price=Decimal("99.99"),
            stock=100,
            category="staged",
        )
        await async_driver.execute(
            f"INSERT INTO {source} (id, name, price, stock, category) VALUES (:id, :name, :price, :stock, :category)",
            id=3,
            name="Staged Insert",
            price=Decimal("149.99"),
            stock=50,
            category="staged",
        )
        await async_driver.execute(
            sql
            .merge(dialect=_merge_dialect(case))
            .into(target, alias="t")
            .using(source, alias="s")
            .on("t.id = s.id")
            .when_matched_then_update(name="s.name", price="s.price", stock="s.stock", category="s.category")
            .when_not_matched_then_insert(columns=["id", "name", "price", "stock", "category"])
        )
        assert await async_driver.select_value(f"SELECT COUNT(*) AS count FROM {target}") == 3
        staged = _lower_keys(
            (await async_driver.execute(f"SELECT name, stock FROM {target} WHERE id = 3")).get_data()[0]
        )
        assert staged["name"] == "Staged Insert"
        assert staged["stock"] == 50
    finally:
        await _drop_merge_table_async(async_driver, case, source)
        await _drop_merge_table_async(async_driver, case, target)


def _merge_bulk_rows(count: int, *, include_nulls: bool = False) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = [
        {
            "id": i,
            "name": f"Product {i}",
            "price": Decimal(f"{10 + i}.99"),
            "stock": i * 10,
            "category": "bulk" if i % 2 == 0 else "regular",
        }
        for i in range(1, count + 1)
    ]
    if include_nulls:
        rows.extend([
            {"id": count + 1, "name": "Null Price", "price": None, "stock": 10, "category": None},
            {"id": count + 2, "name": "Null Stock", "price": Decimal("30.99"), "stock": None, "category": "books"},
        ])
    return rows


def assert_sync_merge_bulk_contract(driver: object, case: DriverCase) -> None:
    """Assert sync bulk MERGE handles JSON/recordset source expansion, updates, inserts, and NULL values."""
    if not case.supports_merge_bulk:
        pytest.skip(f"{case.id} does not support bulk MERGE")
    sync_driver = cast("SyncContractDriver", driver)
    table = _pc_table(case, "mergebulk")
    _drop_merge_table_sync(sync_driver, case, table)
    sync_driver.execute(_merge_table_sql(case, table))
    sync_driver.commit()
    try:
        sync_driver.execute(_merge_upsert_query(table, _merge_bulk_rows(100), case))
        assert sync_driver.select_value(f"SELECT COUNT(*) AS count FROM {table}") == 100
        assert sync_driver.select_value(f"SELECT COUNT(*) AS count FROM {table} WHERE category = 'bulk'") == 50

        sync_driver.execute(
            f"INSERT INTO {table} (id, name, price, stock, category) VALUES (:id, :name, :price, :stock, :category)",
            id=200,
            name="Old Product",
            price=Decimal("5.00"),
            stock=100,
            category="old",
        )
        sync_driver.execute(
            _merge_upsert_query(
                table,
                [
                    {
                        "id": 200,
                        "name": "Updated Product",
                        "price": Decimal("15.00"),
                        "stock": 50,
                        "category": "updated",
                    },
                    {
                        "id": 201,
                        "name": "Inserted Product",
                        "price": Decimal("30.00"),
                        "stock": 10,
                        "category": "inserted",
                    },
                ],
                case,
            )
        )
        updated = _lower_keys(sync_driver.execute(f"SELECT name, category FROM {table} WHERE id = 200").get_data()[0])
        assert updated["name"] == "Updated Product"
        assert updated["category"] == "updated"

        sync_driver.execute(_merge_upsert_query(table, _merge_bulk_rows(10, include_nulls=True), case))
        null_row = _lower_keys(sync_driver.execute(f"SELECT price, category FROM {table} WHERE id = 11").get_data()[0])
        assert null_row["price"] is None
        assert null_row["category"] is None
    finally:
        _drop_merge_table_sync(sync_driver, case, table)


async def assert_async_merge_bulk_contract(driver: object, case: DriverCase) -> None:
    """Async mirror of assert_sync_merge_bulk_contract."""
    if not case.supports_merge_bulk:
        pytest.skip(f"{case.id} does not support bulk MERGE")
    async_driver = cast("AsyncContractDriver", driver)
    table = _pc_table(case, "mergebulk")
    await _drop_merge_table_async(async_driver, case, table)
    await async_driver.execute(_merge_table_sql(case, table))
    await async_driver.commit()
    try:
        await async_driver.execute(_merge_upsert_query(table, _merge_bulk_rows(100), case))
        assert await async_driver.select_value(f"SELECT COUNT(*) AS count FROM {table}") == 100
        assert await async_driver.select_value(f"SELECT COUNT(*) AS count FROM {table} WHERE category = 'bulk'") == 50

        await async_driver.execute(
            f"INSERT INTO {table} (id, name, price, stock, category) VALUES (:id, :name, :price, :stock, :category)",
            id=200,
            name="Old Product",
            price=Decimal("5.00"),
            stock=100,
            category="old",
        )
        await async_driver.execute(
            _merge_upsert_query(
                table,
                [
                    {
                        "id": 200,
                        "name": "Updated Product",
                        "price": Decimal("15.00"),
                        "stock": 50,
                        "category": "updated",
                    },
                    {
                        "id": 201,
                        "name": "Inserted Product",
                        "price": Decimal("30.00"),
                        "stock": 10,
                        "category": "inserted",
                    },
                ],
                case,
            )
        )
        updated = _lower_keys(
            (await async_driver.execute(f"SELECT name, category FROM {table} WHERE id = 200")).get_data()[0]
        )
        assert updated["name"] == "Updated Product"
        assert updated["category"] == "updated"

        await async_driver.execute(_merge_upsert_query(table, _merge_bulk_rows(10, include_nulls=True), case))
        null_row = _lower_keys(
            (await async_driver.execute(f"SELECT price, category FROM {table} WHERE id = 11")).get_data()[0]
        )
        assert null_row["price"] is None
        assert null_row["category"] is None
    finally:
        await _drop_merge_table_async(async_driver, case, table)


def _vector_table_sql(case: DriverCase, table: str, dimension: int = 3) -> str:
    if case.dialect == "oracle":
        return (
            f"CREATE TABLE {table} ("
            "id NUMBER PRIMARY KEY, "
            "content VARCHAR2(100) NOT NULL, "
            f"embedding VECTOR({dimension}, FLOAT32)"
            ")"
        )
    return f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, content TEXT NOT NULL, embedding DOUBLE[{dimension}])"


def _drop_vector_table_sync(driver: "SyncContractDriver", case: DriverCase, table: str) -> None:
    if case.dialect == "oracle":
        driver.execute_script(_oracle_drop_sql("TABLE", table))
    else:
        driver.execute_script(f"DROP TABLE IF EXISTS {table}")


async def _drop_vector_table_async(driver: "AsyncContractDriver", case: DriverCase, table: str) -> None:
    if case.dialect == "oracle":
        await driver.execute_script(_oracle_drop_sql("TABLE", table))
    else:
        await driver.execute_script(f"DROP TABLE IF EXISTS {table}")


def _enable_vector_runtime_sync(driver: "SyncContractDriver", case: DriverCase) -> None:
    if case.dialect != "duckdb":
        return
    try:
        driver.execute_script("INSTALL vss")
        driver.execute_script("LOAD vss")
    except Exception as exc:
        pytest.skip(f"DuckDB VSS unavailable: {exc}")


def _seed_vector_table_sync(driver: "SyncContractDriver", case: DriverCase, table: str) -> None:
    rows: tuple[tuple[int, str, object], ...]
    if case.dialect == "oracle":
        rows = ((1, "doc1", "[0.1, 0.2, 0.3]"), (2, "doc2", "[0.4, 0.5, 0.6]"), (3, "doc3", "[0.7, 0.8, 0.9]"))
        for row in rows:
            driver.execute(f"INSERT INTO {table} (id, content, embedding) VALUES (:1, :2, :3)", row)
    else:
        rows = ((1, "doc1", [0.1, 0.2, 0.3]), (2, "doc2", [0.4, 0.5, 0.6]), (3, "doc3", [0.7, 0.8, 0.9]))
        for row in rows:
            driver.execute(f"INSERT INTO {table} (id, content, embedding) VALUES (?, ?, ?)", row)
    driver.commit()


async def _seed_vector_table_async(driver: "AsyncContractDriver", case: DriverCase, table: str) -> None:
    if case.dialect == "oracle":
        rows: tuple[tuple[int, str, object], ...] = (
            (1, "doc1", "[0.1, 0.2, 0.3]"),
            (2, "doc2", "[0.4, 0.5, 0.6]"),
            (3, "doc3", "[0.7, 0.8, 0.9]"),
        )
        for row in rows:
            await driver.execute(f"INSERT INTO {table} (id, content, embedding) VALUES (:1, :2, :3)", row)
    else:
        rows = ((1, "doc1", [0.1, 0.2, 0.3]), (2, "doc2", [0.4, 0.5, 0.6]), (3, "doc3", [0.7, 0.8, 0.9]))
        for row in rows:
            await driver.execute(f"INSERT INTO {table} (id, content, embedding) VALUES (?, ?, ?)", row)
    await driver.commit()


def _assert_vector_result_order(rows: list[dict[str, Any]]) -> None:
    normalized = [_lower_keys(row) for row in rows]
    assert [row["content"] for row in normalized] == ["doc1", "doc2", "doc3"]
    assert normalized[0]["distance"] < normalized[1]["distance"]
    assert normalized[1]["distance"] < normalized[2]["distance"]


def _assert_vector_score_range(score: object) -> None:
    assert isinstance(score, int | float | Decimal)
    value = float(score)
    assert (
        -1 <= value <= 1
        or math.isclose(value, 1.0, rel_tol=1e-9, abs_tol=1e-9)
        or math.isclose(value, -1.0, rel_tol=1e-9, abs_tol=1e-9)
    )


def _insert_vector_sync(
    driver: "SyncContractDriver", case: DriverCase, table: str, row: tuple[int, str, object]
) -> None:
    if case.dialect == "oracle":
        driver.execute(f"INSERT INTO {table} (id, content, embedding) VALUES (:1, :2, :3)", row)
    else:
        driver.execute(f"INSERT INTO {table} (id, content, embedding) VALUES (?, ?, ?)", row)


async def _insert_vector_async(
    driver: "AsyncContractDriver", case: DriverCase, table: str, row: tuple[int, str, object]
) -> None:
    if case.dialect == "oracle":
        await driver.execute(f"INSERT INTO {table} (id, content, embedding) VALUES (:1, :2, :3)", row)
    else:
        await driver.execute(f"INSERT INTO {table} (id, content, embedding) VALUES (?, ?, ?)", row)


def _assert_sync_duckdb_vector_residuals(sync_driver: "SyncContractDriver", case: DriverCase, table: str) -> None:
    zero_value = [0.0, 0.0, 0.0]
    _insert_vector_sync(sync_driver, case, table, (5, "zero_vec", zero_value))
    zero = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.0, 0.0, 0.0]).alias("distance"))
        .from_(table)
        .where(sql.column("embedding").is_not_null())
        .order_by("distance")
    )
    zero_rows = [_lower_keys(row) for row in sync_driver.execute(zero).get_data()]
    assert zero_rows[0]["content"] == "zero_vec"
    assert zero_rows[0]["distance"] == 0

    aggregate_source = sql.select(
        "content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance")
    ).from_(table)
    aggregate = sql.select("MIN(distance) AS min_distance", "MAX(distance) AS max_distance").from_(
        aggregate_source, alias="distances"
    )
    aggregate_row = _lower_keys(sync_driver.execute(aggregate).get_data()[0])
    assert aggregate_row["min_distance"] < aggregate_row["max_distance"]

    large = _pc_table(case, "vector10")
    _drop_vector_table_sync(sync_driver, case, large)
    sync_driver.execute_script(_vector_table_sql(case, large, dimension=10))
    try:
        _insert_vector_sync(sync_driver, case, large, (1, "large1", [0.1] * 10))
        _insert_vector_sync(sync_driver, case, large, (2, "large2", [0.5] * 10))
        large_query = (
            sql
            .select("content", sql.column("embedding").vector_distance([0.1] * 10).alias("distance"))
            .from_(large)
            .order_by("distance")
        )
        large_rows = [_lower_keys(row) for row in sync_driver.execute(large_query).get_data()]
        assert large_rows[0]["content"] == "large1"
        assert large_rows[0]["distance"] < large_rows[1]["distance"]
    finally:
        _drop_vector_table_sync(sync_driver, case, large)


async def _assert_async_duckdb_vector_residuals(
    async_driver: "AsyncContractDriver", case: DriverCase, table: str
) -> None:
    zero_value = [0.0, 0.0, 0.0]
    await _insert_vector_async(async_driver, case, table, (5, "zero_vec", zero_value))
    zero = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.0, 0.0, 0.0]).alias("distance"))
        .from_(table)
        .where(sql.column("embedding").is_not_null())
        .order_by("distance")
    )
    zero_rows = [_lower_keys(row) for row in (await async_driver.execute(zero)).get_data()]
    assert zero_rows[0]["content"] == "zero_vec"
    assert zero_rows[0]["distance"] == 0

    aggregate_source = sql.select(
        "content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance")
    ).from_(table)
    aggregate = sql.select("MIN(distance) AS min_distance", "MAX(distance) AS max_distance").from_(
        aggregate_source, alias="distances"
    )
    aggregate_row = _lower_keys((await async_driver.execute(aggregate)).get_data()[0])
    assert aggregate_row["min_distance"] < aggregate_row["max_distance"]

    large = _pc_table(case, "vector10")
    await _drop_vector_table_async(async_driver, case, large)
    await async_driver.execute_script(_vector_table_sql(case, large, dimension=10))
    try:
        await _insert_vector_async(async_driver, case, large, (1, "large1", [0.1] * 10))
        await _insert_vector_async(async_driver, case, large, (2, "large2", [0.5] * 10))
        large_query = (
            sql
            .select("content", sql.column("embedding").vector_distance([0.1] * 10).alias("distance"))
            .from_(large)
            .order_by("distance")
        )
        large_rows = [_lower_keys(row) for row in (await async_driver.execute(large_query)).get_data()]
        assert large_rows[0]["content"] == "large1"
        assert large_rows[0]["distance"] < large_rows[1]["distance"]
    finally:
        await _drop_vector_table_async(async_driver, case, large)


def assert_sync_vector_contract(driver: object, case: DriverCase) -> None:
    """Assert sync vector distance/similarity builder execution for cases with native vector tables."""
    if not case.supports_vector:
        pytest.skip(f"{case.id} does not support vector execution")
    sync_driver = cast("SyncContractDriver", driver)
    table = _pc_table(case, "vector")
    _enable_vector_runtime_sync(sync_driver, case)
    _drop_vector_table_sync(sync_driver, case, table)
    sync_driver.execute_script(_vector_table_sql(case, table))
    try:
        _seed_vector_table_sync(sync_driver, case, table)

        distance = (
            sql
            .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
            .from_(table)
            .order_by("distance")
        )
        _assert_vector_result_order(sync_driver.execute(distance).get_data())

        threshold = (
            sql.select("content").from_(table).where(sql.column("embedding").vector_distance([0.1, 0.2, 0.3]) < 0.3)
        )
        threshold_rows = [_lower_keys(row) for row in sync_driver.execute(threshold).get_data()]
        assert len(threshold_rows) == 1
        assert threshold_rows[0]["content"] == "doc1"

        cosine = (
            sql
            .select(
                "content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("distance")
            )
            .from_(table)
            .order_by("distance")
        )
        assert _lower_keys(sync_driver.execute(cosine).get_data()[0])["content"] == "doc1"

        inner_product = (
            sql
            .select(
                "content",
                sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="inner_product").alias("distance"),
            )
            .from_(table)
            .order_by("distance")
        )
        assert len(sync_driver.execute(inner_product).get_data()) == 3

        if case.dialect == "oracle":
            euclidean_squared = (
                sql
                .select(
                    "content",
                    sql
                    .column("embedding")
                    .vector_distance([0.1, 0.2, 0.3], metric="euclidean_squared")
                    .alias("distance"),
                )
                .from_(table)
                .order_by("distance")
            )
            assert _lower_keys(sync_driver.execute(euclidean_squared).get_data()[0])["content"] == "doc1"

        similarity = (
            sql
            .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
            .from_(table)
            .order_by(sql.column("score").desc())
        )
        similarity_rows = [_lower_keys(row) for row in sync_driver.execute(similarity).get_data()]
        assert similarity_rows[0]["content"] == "doc1"
        assert similarity_rows[0]["score"] > similarity_rows[1]["score"]

        top_k = (
            sql
            .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
            .from_(table)
            .order_by(sql.column("score").desc())
            .limit(2)
        )
        top_k_rows = [_lower_keys(row) for row in sync_driver.execute(top_k).get_data()]
        assert [row["content"] for row in top_k_rows] == ["doc1", "doc2"]
        for row in similarity_rows:
            _assert_vector_score_range(row["score"])

        multi = sql.select(
            "content",
            sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="euclidean").alias("euclidean_dist"),
            sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("cosine_dist"),
        ).from_(table)
        for row in sync_driver.execute(multi).get_data():
            normalized = _lower_keys(row)
            assert normalized["euclidean_dist"] is not None
            assert normalized["cosine_dist"] is not None

        if case.dialect == "oracle":
            sync_driver.execute(f"INSERT INTO {table} (id, content, embedding) VALUES (:1, :2, NULL)", (4, "doc_null"))
        else:
            sync_driver.execute(f"INSERT INTO {table} (id, content, embedding) VALUES (?, ?, ?)", (4, "doc_null", None))
        not_null = (
            sql
            .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
            .from_(table)
            .where(sql.column("embedding").is_not_null())
            .order_by("distance")
        )
        assert all(_lower_keys(row)["content"] != "doc_null" for row in sync_driver.execute(not_null).get_data())

        combined = (
            sql
            .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
            .from_(table)
            .where(
                (sql.column("embedding").vector_distance([0.1, 0.2, 0.3]) < 1.0)
                & (sql.column("content").in_(["doc1", "doc2"]))
            )
            .order_by("distance")
        )
        assert len(sync_driver.execute(combined).get_data()) == 2

        if case.dialect == "duckdb":
            _assert_sync_duckdb_vector_residuals(sync_driver, case, table)
    finally:
        _drop_vector_table_sync(sync_driver, case, table)


async def assert_async_vector_contract(driver: object, case: DriverCase) -> None:
    """Async mirror of assert_sync_vector_contract."""
    if not case.supports_vector:
        pytest.skip(f"{case.id} does not support vector execution")
    async_driver = cast("AsyncContractDriver", driver)
    table = _pc_table(case, "vector")
    await _drop_vector_table_async(async_driver, case, table)
    await async_driver.execute_script(_vector_table_sql(case, table))
    try:
        await _seed_vector_table_async(async_driver, case, table)

        distance = (
            sql
            .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
            .from_(table)
            .order_by("distance")
        )
        _assert_vector_result_order((await async_driver.execute(distance)).get_data())

        threshold = (
            sql.select("content").from_(table).where(sql.column("embedding").vector_distance([0.1, 0.2, 0.3]) < 0.3)
        )
        threshold_rows = [_lower_keys(row) for row in (await async_driver.execute(threshold)).get_data()]
        assert len(threshold_rows) == 1
        assert threshold_rows[0]["content"] == "doc1"

        cosine = (
            sql
            .select(
                "content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("distance")
            )
            .from_(table)
            .order_by("distance")
        )
        assert _lower_keys((await async_driver.execute(cosine)).get_data()[0])["content"] == "doc1"

        inner_product = (
            sql
            .select(
                "content",
                sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="inner_product").alias("distance"),
            )
            .from_(table)
            .order_by("distance")
        )
        assert len((await async_driver.execute(inner_product)).get_data()) == 3

        if case.dialect == "oracle":
            euclidean_squared = (
                sql
                .select(
                    "content",
                    sql
                    .column("embedding")
                    .vector_distance([0.1, 0.2, 0.3], metric="euclidean_squared")
                    .alias("distance"),
                )
                .from_(table)
                .order_by("distance")
            )
            assert _lower_keys((await async_driver.execute(euclidean_squared)).get_data()[0])["content"] == "doc1"

        similarity = (
            sql
            .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
            .from_(table)
            .order_by(sql.column("score").desc())
        )
        similarity_rows = [_lower_keys(row) for row in (await async_driver.execute(similarity)).get_data()]
        assert similarity_rows[0]["content"] == "doc1"
        assert similarity_rows[0]["score"] > similarity_rows[1]["score"]

        top_k = (
            sql
            .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
            .from_(table)
            .order_by(sql.column("score").desc())
            .limit(2)
        )
        top_k_rows = [_lower_keys(row) for row in (await async_driver.execute(top_k)).get_data()]
        assert [row["content"] for row in top_k_rows] == ["doc1", "doc2"]
        for row in similarity_rows:
            _assert_vector_score_range(row["score"])

        multi = sql.select(
            "content",
            sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="euclidean").alias("euclidean_dist"),
            sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("cosine_dist"),
        ).from_(table)
        for row in (await async_driver.execute(multi)).get_data():
            normalized = _lower_keys(row)
            assert normalized["euclidean_dist"] is not None
            assert normalized["cosine_dist"] is not None

        if case.dialect == "oracle":
            await async_driver.execute(
                f"INSERT INTO {table} (id, content, embedding) VALUES (:1, :2, NULL)", (4, "doc_null")
            )
        else:
            await async_driver.execute(
                f"INSERT INTO {table} (id, content, embedding) VALUES (?, ?, ?)", (4, "doc_null", None)
            )
        not_null = (
            sql
            .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
            .from_(table)
            .where(sql.column("embedding").is_not_null())
            .order_by("distance")
        )
        assert all(
            _lower_keys(row)["content"] != "doc_null" for row in (await async_driver.execute(not_null)).get_data()
        )

        combined = (
            sql
            .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
            .from_(table)
            .where(
                (sql.column("embedding").vector_distance([0.1, 0.2, 0.3]) < 1.0)
                & (sql.column("content").in_(["doc1", "doc2"]))
            )
            .order_by("distance")
        )
        assert len((await async_driver.execute(combined)).get_data()) == 2

        if case.dialect == "duckdb":
            await _assert_async_duckdb_vector_residuals(async_driver, case, table)
    finally:
        await _drop_vector_table_async(async_driver, case, table)


async def _asyncpg_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold asyncpg native-codec params: arrays+ANY, native dict->JSONB, UUID/date/bool/NULL, float, count-mismatch."""
    import math
    from datetime import date
    from uuid import uuid4

    async_driver = cast("AsyncContractDriver", driver)

    arrays = _pc_table(case, "arrays")
    await _async_drop_table(async_driver, arrays)
    await async_driver.execute(
        f"CREATE TABLE {arrays} (id SERIAL PRIMARY KEY, name TEXT, tags TEXT[], scores INTEGER[])"
    )
    await async_driver.commit()
    try:
        await async_driver.execute_many(
            f"INSERT INTO {arrays} (name, tags, scores) VALUES (?, ?, ?)",
            [("Array 1", ["tag1", "tag2"], [10, 20, 30]), ("Array 2", ["tag3"], [40, 50])],
        )
        await async_driver.commit()
        any_rows = (await async_driver.execute(f"SELECT name FROM {arrays} WHERE ? = ANY(tags)", "tag2")).get_data()
        len_rows = (
            await async_driver.execute(f"SELECT name FROM {arrays} WHERE array_length(scores, 1) > ? ORDER BY name", 1)
        ).get_data()
        assert any_rows == [{"name": "Array 1"}]
        assert len_rows == [{"name": "Array 1"}, {"name": "Array 2"}]
    finally:
        await _async_drop_table(async_driver, arrays)

    jsonb = _pc_table(case, "jsonb")
    await _async_drop_table(async_driver, jsonb)
    await async_driver.execute(f"CREATE TABLE {jsonb} (id SERIAL PRIMARY KEY, name TEXT, metadata JSONB, config JSONB)")
    await async_driver.commit()
    try:
        row = (
            await async_driver.execute(
                f"INSERT INTO {jsonb} (name, metadata, config) VALUES (?, ?, ?) RETURNING name, metadata, config",
                "json-test",
                {"score": 100, "active": True},
                None,
            )
        ).get_data()
        assert row == [{"name": "json-test", "metadata": {"score": 100, "active": True}, "config": None}]
    finally:
        await _async_drop_table(async_driver, jsonb)

    none_values = _pc_table(case, "none")
    await _async_drop_table(async_driver, none_values)
    await async_driver.execute(
        f"CREATE TABLE {none_values} (id UUID PRIMARY KEY, text_col TEXT, nullable_text TEXT, "
        f"bool_col BOOLEAN, nullable_bool BOOLEAN, date_col DATE, nullable_date DATE)"
    )
    await async_driver.commit()
    test_id = uuid4()
    try:
        await async_driver.execute(
            f"INSERT INTO {none_values} (id, text_col, nullable_text, bool_col, nullable_bool, date_col, nullable_date) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?)",
            test_id,
            "test_value",
            None,
            True,
            None,
            date(2025, 1, 21),
            None,
        )
        await async_driver.commit()
        stored = await async_driver.select_one(f"SELECT * FROM {none_values} WHERE id = ?", test_id)
        assert stored["id"] == test_id
        assert stored["nullable_text"] is None
        assert stored["bool_col"] is True
        assert stored["nullable_bool"] is None
        assert stored["date_col"] is not None
        assert stored["nullable_date"] is None
    finally:
        await _async_drop_table(async_driver, none_values)

    float_row = (await async_driver.execute("SELECT ?::float AS value", math.pi)).get_data()
    assert abs(float_row[0]["value"] - math.pi) < 0.001

    with pytest.raises(Exception):
        await async_driver.execute("SELECT ?::text AS first, ?::int AS second", None)


async def _psqlpy_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold psqlpy native-codec params: scalar+JSON casts, JSONB dict/nested/NULL, decimal/timestamp, count-mismatch."""
    import decimal
    import math
    from datetime import datetime

    async_driver = cast("AsyncContractDriver", driver)

    typed = (
        await async_driver.execute(
            "SELECT ?::text AS text_val, ?::int AS int_val, ?::float AS float_val, "
            "?::bool AS bool_val, ?::json AS json_val",
            "string_value",
            42,
            math.pi,
            True,
            {"key": "value"},
        )
    ).get_data()[0]
    assert typed["text_val"] == "string_value"
    assert typed["int_val"] == 42
    assert abs(typed["float_val"] - math.pi) < 0.001
    assert typed["bool_val"] is True
    assert typed["json_val"]["key"] == "value"

    jsonb = _pc_table(case, "jsonb")
    await _async_drop_table(async_driver, jsonb)
    await async_driver.execute(
        f"CREATE TABLE {jsonb} (id SERIAL PRIMARY KEY, name TEXT, metadata JSONB, config JSONB, tags JSONB)"
    )
    await async_driver.commit()
    try:
        inserted = (
            await async_driver.execute(
                f"INSERT INTO {jsonb} (name, metadata, config, tags) VALUES (?, ?, ?, ?) "
                f"RETURNING name, metadata, config, tags",
                "json-test",
                {"user_id": 123},
                None,
                {"tags": ["one", None]},
            )
        ).get_data()[0]
        assert inserted["metadata"] == {"user_id": 123}
        assert inserted["config"] is None
        assert inserted["tags"] == {"tags": ["one", None]}
        updated = (
            await async_driver.execute(
                f"UPDATE {jsonb} SET metadata = ?, config = ? WHERE name = ? RETURNING metadata, config",
                None,
                {"updated": True},
                "json-test",
            )
        ).get_data()
        assert updated == [{"metadata": None, "config": {"updated": True}}]
    finally:
        await _async_drop_table(async_driver, jsonb)

    decimal_val = decimal.Decimal("123.456789")
    decimal_row = (await async_driver.execute("SELECT ?::float AS decimal_val", float(decimal_val))).get_data()
    assert abs(float(decimal_row[0]["decimal_val"]) - float(decimal_val)) < 0.000001
    timestamp_row = (
        await async_driver.execute("SELECT ?::timestamp AS datetime_val", datetime(2024, 1, 1, 12, 0, 0).isoformat())
    ).get_data()
    assert timestamp_row[0]["datetime_val"] is not None

    with pytest.raises(Exception):
        await async_driver.execute("SELECT ?::text AS val1, ?::int AS val2", None)


def _psycopg_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold psycopg native-codec params: arrays+ANY, serialized JSONB, UUID/date/bool/NULL, float, count-mismatch."""
    import math
    from datetime import date
    from uuid import uuid4

    sync_driver = cast("SyncContractDriver", driver)

    arrays = _pc_table(case, "arrays")
    _sync_drop_table(sync_driver, arrays)
    sync_driver.execute(f"CREATE TABLE {arrays} (id SERIAL PRIMARY KEY, name TEXT, tags TEXT[], scores INTEGER[])")
    sync_driver.commit()
    try:
        sync_driver.execute_many(
            f"INSERT INTO {arrays} (name, tags, scores) VALUES (?, ?, ?)",
            [("Array 1", ["tag1", "tag2"], [10, 20, 30]), ("Array 2", ["tag3"], [40, 50])],
        )
        sync_driver.commit()
        any_rows = sync_driver.execute(f"SELECT name FROM {arrays} WHERE ? = ANY(tags)", "tag2").get_data()
        len_rows = sync_driver.execute(
            f"SELECT name FROM {arrays} WHERE array_length(scores, 1) > ? ORDER BY name", 1
        ).get_data()
        assert any_rows == [{"name": "Array 1"}]
        assert len_rows == [{"name": "Array 1"}, {"name": "Array 2"}]
    finally:
        _sync_drop_table(sync_driver, arrays)

    jsonb = _pc_table(case, "jsonb")
    _sync_drop_table(sync_driver, jsonb)
    sync_driver.execute(f"CREATE TABLE {jsonb} (id SERIAL PRIMARY KEY, name TEXT, metadata JSONB, config JSONB)")
    sync_driver.commit()
    try:
        row = sync_driver.execute(
            f"INSERT INTO {jsonb} (name, metadata, config) VALUES (?, ?, ?) RETURNING name, metadata, config",
            "json-test",
            to_json({"score": 100, "active": True}),
            None,
        ).get_data()
        assert row == [{"name": "json-test", "metadata": {"score": 100, "active": True}, "config": None}]
    finally:
        _sync_drop_table(sync_driver, jsonb)

    none_values = _pc_table(case, "none")
    _sync_drop_table(sync_driver, none_values)
    sync_driver.execute(
        f"CREATE TABLE {none_values} (id UUID PRIMARY KEY, text_col TEXT, nullable_text TEXT, "
        f"bool_col BOOLEAN, nullable_bool BOOLEAN, date_col DATE, nullable_date DATE)"
    )
    sync_driver.commit()
    test_id = uuid4()
    try:
        sync_driver.execute(
            f"INSERT INTO {none_values} (id, text_col, nullable_text, bool_col, nullable_bool, date_col, nullable_date) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?)",
            test_id,
            "test_value",
            None,
            True,
            None,
            date(2025, 1, 21),
            None,
        )
        sync_driver.commit()
        stored = sync_driver.select_one(f"SELECT * FROM {none_values} WHERE id = ?", test_id)
        assert stored["id"] == test_id
        assert stored["nullable_text"] is None
        assert stored["bool_col"] is True
        assert stored["nullable_bool"] is None
        assert stored["date_col"] is not None
        assert stored["nullable_date"] is None
    finally:
        _sync_drop_table(sync_driver, none_values)

    float_row = sync_driver.execute("SELECT ?::float AS value", math.pi).get_data()
    assert abs(float_row[0]["value"] - math.pi) < 0.001

    with pytest.raises(Exception):
        sync_driver.execute("SELECT ?::text AS first, ?::int AS second", None)


async def _psycopg_param_codecs_async(driver: object, case: DriverCase) -> None:
    """Async mirror of the psycopg native-codec param fold (serialized JSONB, arrays, UUID/date/bool/NULL)."""
    import math
    from datetime import date
    from uuid import uuid4

    async_driver = cast("AsyncContractDriver", driver)

    arrays = _pc_table(case, "arrays")
    await _async_drop_table(async_driver, arrays)
    await async_driver.execute(
        f"CREATE TABLE {arrays} (id SERIAL PRIMARY KEY, name TEXT, tags TEXT[], scores INTEGER[])"
    )
    await async_driver.commit()
    try:
        await async_driver.execute_many(
            f"INSERT INTO {arrays} (name, tags, scores) VALUES (?, ?, ?)",
            [("Array 1", ["tag1", "tag2"], [10, 20, 30]), ("Array 2", ["tag3"], [40, 50])],
        )
        await async_driver.commit()
        any_rows = (await async_driver.execute(f"SELECT name FROM {arrays} WHERE ? = ANY(tags)", "tag2")).get_data()
        len_rows = (
            await async_driver.execute(f"SELECT name FROM {arrays} WHERE array_length(scores, 1) > ? ORDER BY name", 1)
        ).get_data()
        assert any_rows == [{"name": "Array 1"}]
        assert len_rows == [{"name": "Array 1"}, {"name": "Array 2"}]
    finally:
        await _async_drop_table(async_driver, arrays)

    jsonb = _pc_table(case, "jsonb")
    await _async_drop_table(async_driver, jsonb)
    await async_driver.execute(f"CREATE TABLE {jsonb} (id SERIAL PRIMARY KEY, name TEXT, metadata JSONB, config JSONB)")
    await async_driver.commit()
    try:
        row = (
            await async_driver.execute(
                f"INSERT INTO {jsonb} (name, metadata, config) VALUES (?, ?, ?) RETURNING name, metadata, config",
                "json-test",
                to_json({"score": 100, "active": True}),
                None,
            )
        ).get_data()
        assert row == [{"name": "json-test", "metadata": {"score": 100, "active": True}, "config": None}]
    finally:
        await _async_drop_table(async_driver, jsonb)

    none_values = _pc_table(case, "none")
    await _async_drop_table(async_driver, none_values)
    await async_driver.execute(
        f"CREATE TABLE {none_values} (id UUID PRIMARY KEY, text_col TEXT, nullable_text TEXT, "
        f"bool_col BOOLEAN, nullable_bool BOOLEAN, date_col DATE, nullable_date DATE)"
    )
    await async_driver.commit()
    test_id = uuid4()
    try:
        await async_driver.execute(
            f"INSERT INTO {none_values} (id, text_col, nullable_text, bool_col, nullable_bool, date_col, nullable_date) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?)",
            test_id,
            "test_value",
            None,
            True,
            None,
            date(2025, 1, 21),
            None,
        )
        await async_driver.commit()
        stored = await async_driver.select_one(f"SELECT * FROM {none_values} WHERE id = ?", test_id)
        assert stored["id"] == test_id
        assert stored["nullable_text"] is None
        assert stored["bool_col"] is True
        assert stored["nullable_bool"] is None
        assert stored["date_col"] is not None
        assert stored["nullable_date"] is None
    finally:
        await _async_drop_table(async_driver, none_values)

    float_row = (await async_driver.execute("SELECT ?::float AS value", math.pi)).get_data()
    assert abs(float_row[0]["value"] - math.pi) < 0.001

    with pytest.raises(Exception):
        await async_driver.execute("SELECT ?::text AS first, ?::int AS second", None)


def _duckdb_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold DuckDB native-codec params: typed arrays/REAL/BOOLEAN, array indexing, JSON strings, None+array, count."""
    from datetime import date

    sync_driver = cast("SyncContractDriver", driver)

    types_t = _pc_table(case, "types")
    _sync_drop_table(sync_driver, types_t)
    sync_driver.execute(
        f"CREATE TABLE {types_t} (id INTEGER PRIMARY KEY, int_val INTEGER, real_val REAL, "
        f"text_val VARCHAR, bool_val BOOLEAN, list_val INTEGER[])"
    )
    try:
        sync_driver.execute(
            f"INSERT INTO {types_t} (id, int_val, real_val, text_val, bool_val, list_val) VALUES (?, ?, ?, ?, ?, ?)",
            1,
            42,
            3.14159,
            "hello",
            True,
            [1, 2, 3],
        )
        row = sync_driver.select_one(f"SELECT * FROM {types_t} WHERE int_val = ?", 42)
        assert row["text_val"] == "hello"
        assert row["bool_val"] is True
        assert row["list_val"] == [1, 2, 3]
        assert 3.13 < row["real_val"] < 3.15
    finally:
        _sync_drop_table(sync_driver, types_t)

    arrays_t = _pc_table(case, "arrays")
    _sync_drop_table(sync_driver, arrays_t)
    sync_driver.execute(
        f"CREATE TABLE {arrays_t} (id INTEGER PRIMARY KEY, name VARCHAR, numbers INTEGER[], tags VARCHAR[])"
    )
    try:
        sync_driver.execute_many(
            f"INSERT INTO {arrays_t} (id, name, numbers, tags) VALUES (?, ?, ?, ?)",
            [(1, "A1", [1, 2, 3, 4, 5], ["t1"]), (2, "A2", [10, 20, 30], ["t2"]), (3, "A3", [100, 200], ["t3"])],
        )
        big = sync_driver.execute(f"SELECT name FROM {arrays_t} WHERE len(numbers) >= ? ORDER BY name", 3).get_data()
        assert [r["name"] for r in big] == ["A1", "A2"]
        indexed = sync_driver.execute(f"SELECT name FROM {arrays_t} WHERE numbers[?] > ?", 1, 5).get_data()
        assert len(indexed) >= 1
    finally:
        _sync_drop_table(sync_driver, arrays_t)

    json_t = _pc_table(case, "json")
    _sync_drop_table(sync_driver, json_t)
    sync_driver.execute(f"CREATE TABLE {json_t} (id INTEGER PRIMARY KEY, name VARCHAR, metadata VARCHAR)")
    try:
        sync_driver.execute_many(
            f"INSERT INTO {json_t} (id, name, metadata) VALUES (?, ?, ?)",
            [
                (1, "j1", to_json({"type": "test", "value": 100})),
                (2, "j2", to_json({"type": "test", "value": 200})),
                (3, "j3", to_json({"type": "prod", "value": 300})),
            ],
        )
        rows = sync_driver.execute(
            f"SELECT name FROM {json_t} WHERE json_extract_string(metadata, '$.type') = ? ORDER BY name", "test"
        ).get_data()
        assert [r["name"] for r in rows] == ["j1", "j2"]
    finally:
        _sync_drop_table(sync_driver, json_t)

    none_t = _pc_table(case, "none")
    _sync_drop_table(sync_driver, none_t)
    sync_driver.execute(
        f"CREATE TABLE {none_t} (id INTEGER, col1 VARCHAR, col2 INTEGER, col3 REAL, "
        f"col4 BOOLEAN, col5 DATE, col6 VARCHAR[])"
    )
    try:
        result = sync_driver.execute(
            f"INSERT INTO {none_t} (id, col1, col2, col3, col4, col5, col6) VALUES (?, ?, ?, ?, ?, ?, ?)",
            1,
            "complex_test",
            None,
            3.14159,
            None,
            date(2025, 1, 21),
            ["array", "with", "values"],
        )
        assert result.rows_affected == 1
        stored = sync_driver.select_one(f"SELECT * FROM {none_t} WHERE id = ?", 1)
        assert stored["col2"] is None
        assert stored["col4"] is None
        assert stored["col6"] == ["array", "with", "values"]
    finally:
        _sync_drop_table(sync_driver, none_t)

    count_t = _pc_table(case, "count")
    _sync_drop_table(sync_driver, count_t)
    sync_driver.execute(f"CREATE TABLE {count_t} (col1 VARCHAR, col2 INTEGER)")
    try:
        with pytest.raises(Exception):
            sync_driver.execute(f"INSERT INTO {count_t} (col1, col2) VALUES (?, ?)", "value1", None, "extra_param")
        with pytest.raises(Exception):
            sync_driver.execute(f"INSERT INTO {count_t} (col1, col2) VALUES (?, ?)", "value1")
        ok = sync_driver.execute(f"INSERT INTO {count_t} (col1, col2) VALUES (?, ?)", "value1", None)
        assert ok.rows_affected == 1
    finally:
        _sync_drop_table(sync_driver, count_t)


async def _cockroach_asyncpg_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold CockroachDB asyncpg params: SERIAL RETURNING round-trip, native dict->JSONB, float, count-mismatch."""
    import math

    async_driver = cast("AsyncContractDriver", driver)

    items = _pc_table(case, "items")
    await _async_drop_table(async_driver, items)
    await async_driver.execute(
        f"CREATE TABLE {items} (id SERIAL PRIMARY KEY, name STRING NOT NULL, value INT DEFAULT 0, description STRING)"
    )
    await async_driver.commit()
    try:
        inserted = await async_driver.execute(
            f"INSERT INTO {items} (name, value, description) VALUES (?, ?, ?) RETURNING id",
            "serial-native",
            500,
            "Inserted with native numeric parameters",
        )
        record_id = inserted.get_data()[0]["id"]
        await async_driver.commit()
        assert isinstance(record_id, int)
        fetched = await async_driver.execute(f"SELECT name, value FROM {items} WHERE id = ?", record_id)
        assert fetched.get_data() == [{"name": "serial-native", "value": 500}]
    finally:
        await _async_drop_table(async_driver, items)

    jsonb = _pc_table(case, "jsonb")
    await _async_drop_table(async_driver, jsonb)
    await async_driver.execute(
        f"CREATE TABLE {jsonb} (id SERIAL PRIMARY KEY, name STRING, metadata JSONB, config JSONB)"
    )
    await async_driver.commit()
    try:
        row = await async_driver.execute(
            f"INSERT INTO {jsonb} (name, metadata, config) VALUES (?, ?, ?) RETURNING name, metadata, config",
            "json-test",
            {"score": 100, "active": True},
            None,
        )
        await async_driver.commit()
        assert row.get_data() == [{"name": "json-test", "metadata": {"score": 100, "active": True}, "config": None}]
    finally:
        await _async_drop_table(async_driver, jsonb)

    float_row = (await async_driver.execute("SELECT ?::FLOAT AS value", math.pi)).get_data()
    assert abs(float_row[0]["value"] - math.pi) < 0.001

    with pytest.raises(Exception):
        await async_driver.execute("SELECT ?::STRING AS first, ?::INT AS second", None)


def _cockroach_psycopg_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold CockroachDB psycopg params: SERIAL RETURNING round-trip, serialized JSONB, count-mismatch (sync)."""
    sync_driver = cast("SyncContractDriver", driver)

    items = _pc_table(case, "items")
    _sync_drop_table(sync_driver, items)
    sync_driver.execute(
        f"CREATE TABLE {items} (id SERIAL PRIMARY KEY, name STRING NOT NULL, value INT DEFAULT 0, description STRING)"
    )
    sync_driver.commit()
    try:
        inserted = sync_driver.execute(
            f"INSERT INTO {items} (name, value, description) VALUES (?, ?, ?) RETURNING id",
            "serial-native",
            500,
            "Inserted with native pyformat parameters",
        )
        record_id = inserted.get_data()[0]["id"]
        sync_driver.commit()
        assert isinstance(record_id, int)
        fetched = sync_driver.execute(f"SELECT name, value FROM {items} WHERE id = ?", record_id)
        assert fetched.get_data() == [{"name": "serial-native", "value": 500}]
    finally:
        _sync_drop_table(sync_driver, items)

    jsonb = _pc_table(case, "jsonb")
    _sync_drop_table(sync_driver, jsonb)
    sync_driver.execute(f"CREATE TABLE {jsonb} (id SERIAL PRIMARY KEY, name STRING, metadata JSONB, config JSONB)")
    sync_driver.commit()
    try:
        row = sync_driver.execute(
            f"INSERT INTO {jsonb} (name, metadata, config) VALUES (?, ?, ?) RETURNING name, metadata, config",
            "json-test",
            to_json({"score": 100, "active": True}),
            None,
        )
        sync_driver.commit()
        assert row.get_data() == [{"name": "json-test", "metadata": {"score": 100, "active": True}, "config": None}]
    finally:
        _sync_drop_table(sync_driver, jsonb)

    with pytest.raises(Exception):
        sync_driver.execute("SELECT ?::STRING AS first, ?::INT AS second", None)


async def _cockroach_psycopg_param_codecs_async(driver: object, case: DriverCase) -> None:
    """Fold CockroachDB psycopg params: SERIAL RETURNING round-trip, serialized JSONB, count-mismatch (async)."""
    async_driver = cast("AsyncContractDriver", driver)

    items = _pc_table(case, "items")
    await _async_drop_table(async_driver, items)
    await async_driver.execute(
        f"CREATE TABLE {items} (id SERIAL PRIMARY KEY, name STRING NOT NULL, value INT DEFAULT 0, description STRING)"
    )
    await async_driver.commit()
    try:
        inserted = await async_driver.execute(
            f"INSERT INTO {items} (name, value, description) VALUES (?, ?, ?) RETURNING id",
            "serial-native",
            500,
            "Inserted with native pyformat parameters",
        )
        record_id = inserted.get_data()[0]["id"]
        await async_driver.commit()
        assert isinstance(record_id, int)
        fetched = await async_driver.execute(f"SELECT name, value FROM {items} WHERE id = ?", record_id)
        assert fetched.get_data() == [{"name": "serial-native", "value": 500}]
    finally:
        await _async_drop_table(async_driver, items)

    jsonb = _pc_table(case, "jsonb")
    await _async_drop_table(async_driver, jsonb)
    await async_driver.execute(
        f"CREATE TABLE {jsonb} (id SERIAL PRIMARY KEY, name STRING, metadata JSONB, config JSONB)"
    )
    await async_driver.commit()
    try:
        row = await async_driver.execute(
            f"INSERT INTO {jsonb} (name, metadata, config) VALUES (?, ?, ?) RETURNING name, metadata, config",
            "json-test",
            to_json({"score": 100, "active": True}),
            None,
        )
        await async_driver.commit()
        assert row.get_data() == [{"name": "json-test", "metadata": {"score": 100, "active": True}, "config": None}]
    finally:
        await _async_drop_table(async_driver, jsonb)

    with pytest.raises(Exception):
        await async_driver.execute("SELECT ?::STRING AS first, ?::INT AS second", None)


def _mysql_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold MySQL params: bool->TINYINT, float precision, empty-string!=NULL, special-char escaping, SQL object (sync)."""
    import math

    sync_driver = cast("SyncContractDriver", driver)
    items = _pc_table(case, "items")
    _sync_drop_table(sync_driver, items)
    sync_driver.execute(
        f"CREATE TABLE {items} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, "
        f"value INT DEFAULT 0, active TINYINT(1), float_value DOUBLE, description TEXT)"
    )
    sync_driver.commit()
    try:
        sync_driver.execute_many(
            f"INSERT INTO {items} (name, value, active, float_value, description) VALUES (?, ?, ?, ?, ?)",
            [
                ("test1", 100, True, 1.5, "First test"),
                ("test2", 200, False, 2.5, "Second test"),
                ("test3", 300, True, math.pi, None),
            ],
        )
        sync_driver.commit()

        bool_rows = sync_driver.execute(f"SELECT name FROM {items} WHERE active = ? ORDER BY value", True).get_data()
        assert bool_rows == [{"name": "test1"}, {"name": "test3"}]

        float_rows = sync_driver.execute(
            f"SELECT name, float_value FROM {items} WHERE float_value > ? ORDER BY value", 3.0
        ).get_data()
        assert len(float_rows) == 1
        assert float_rows[0]["name"] == "test3"
        assert abs(float_rows[0]["float_value"] - math.pi) < 0.0001

        sync_driver.execute(f"INSERT INTO {items} (name, value, description) VALUES (?, ?, ?)", "empty_desc", 0, "")
        sync_driver.commit()
        empty_rows = sync_driver.execute(f"SELECT name FROM {items} WHERE description = ?", "").get_data()
        assert empty_rows == [{"name": "empty_desc"}]

        special_value = 'O\'Reilly & Sons "Test" <script>'
        sync_driver.execute(
            f"INSERT INTO {items} (name, value, description) VALUES (?, ?, ?)", "special", 0, special_value
        )
        sync_driver.commit()
        special_rows = sync_driver.execute(f"SELECT description FROM {items} WHERE name = ?", "special").get_data()
        assert special_rows == [{"description": special_value}]

        between = sync_driver.execute(SQL(f"SELECT name, value FROM {items} WHERE value BETWEEN ? AND ?", 150, 250))
        assert between.get_data() == [{"name": "test2", "value": 200}]
    finally:
        _sync_drop_table(sync_driver, items)


async def _mysql_param_codecs_async(driver: object, case: DriverCase) -> None:
    """Fold MySQL params: bool->TINYINT, float precision, empty-string!=NULL, special-char escaping, SQL object (async)."""
    import math

    async_driver = cast("AsyncContractDriver", driver)
    items = _pc_table(case, "items")
    await _async_drop_table(async_driver, items)
    await async_driver.execute(
        f"CREATE TABLE {items} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, "
        f"value INT DEFAULT 0, active TINYINT(1), float_value DOUBLE, description TEXT)"
    )
    await async_driver.commit()
    try:
        await async_driver.execute_many(
            f"INSERT INTO {items} (name, value, active, float_value, description) VALUES (?, ?, ?, ?, ?)",
            [
                ("test1", 100, True, 1.5, "First test"),
                ("test2", 200, False, 2.5, "Second test"),
                ("test3", 300, True, math.pi, None),
            ],
        )
        await async_driver.commit()

        bool_rows = (
            await async_driver.execute(f"SELECT name FROM {items} WHERE active = ? ORDER BY value", True)
        ).get_data()
        assert bool_rows == [{"name": "test1"}, {"name": "test3"}]

        float_rows = (
            await async_driver.execute(
                f"SELECT name, float_value FROM {items} WHERE float_value > ? ORDER BY value", 3.0
            )
        ).get_data()
        assert len(float_rows) == 1
        assert float_rows[0]["name"] == "test3"
        assert abs(float_rows[0]["float_value"] - math.pi) < 0.0001

        await async_driver.execute(
            f"INSERT INTO {items} (name, value, description) VALUES (?, ?, ?)", "empty_desc", 0, ""
        )
        await async_driver.commit()
        empty_rows = (await async_driver.execute(f"SELECT name FROM {items} WHERE description = ?", "")).get_data()
        assert empty_rows == [{"name": "empty_desc"}]

        special_value = 'O\'Reilly & Sons "Test" <script>'
        await async_driver.execute(
            f"INSERT INTO {items} (name, value, description) VALUES (?, ?, ?)", "special", 0, special_value
        )
        await async_driver.commit()
        special_rows = (
            await async_driver.execute(f"SELECT description FROM {items} WHERE name = ?", "special")
        ).get_data()
        assert special_rows == [{"description": special_value}]

        between = await async_driver.execute(
            SQL(f"SELECT name, value FROM {items} WHERE value BETWEEN ? AND ?", 150, 250)
        )
        assert between.get_data() == [{"name": "test2", "value": 200}]
    finally:
        await _async_drop_table(async_driver, items)


def _lower_row(row: "dict[str, Any]") -> "dict[str, Any]":
    return {key.lower(): value for key, value in row.items()}


def _read_lob_sync(value: object) -> object:
    if hasattr(value, "read"):
        return cast(Any, value).read()
    return value


async def _read_lob_async(value: object) -> object:
    import inspect

    if not hasattr(value, "read"):
        return value
    maybe_value = cast(Any, value).read()
    if inspect.isawaitable(maybe_value):
        return await maybe_value
    return maybe_value


_ORACLE_TYPED_DDL = (
    "CREATE TABLE {table} (id NUMBER PRIMARY KEY, text_field VARCHAR2(100), number_field NUMBER, "
    "date_field DATE, clob_field CLOB, raw_field RAW(16))"
)
_ORACLE_TYPED_INSERT = (
    "INSERT INTO {table} (id, text_field, number_field, date_field, clob_field, raw_field) "
    "VALUES (:id, :text_field, :number_field, TO_DATE(:date_text, 'YYYY-MM-DD'), :clob_field, HEXTORAW(:raw_hex))"
)
_ORACLE_TYPED_SELECT = (
    "SELECT id, text_field, number_field, TO_CHAR(date_field, 'YYYY-MM-DD') AS date_text, "
    "clob_field, RAWTOHEX(raw_field) AS raw_hex FROM {table} ORDER BY id"
)


def _oracle_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold Oracle params: typed DATE/CLOB/RAW/NUMBER + NULL binds, identifier-case, bind-count-mismatch (sync)."""
    sync_driver = cast("SyncContractDriver", driver)

    typed = _pc_table(case, "typed")
    _sync_drop_table(sync_driver, typed)
    sync_driver.execute_script(_ORACLE_TYPED_DDL.format(table=typed))
    try:
        sync_driver.execute(
            _ORACLE_TYPED_INSERT.format(table=typed),
            {
                "id": 1,
                "text_field": "typed values",
                "number_field": 42,
                "date_text": "2024-06-15",
                "clob_field": "CLOB content",
                "raw_hex": "DEADBEEF",
            },
        )
        sync_driver.execute(
            _ORACLE_TYPED_INSERT.format(table=typed),
            {"id": 2, "text_field": None, "number_field": None, "date_text": None, "clob_field": None, "raw_hex": None},
        )
        rows = [_lower_row(row) for row in sync_driver.execute(_ORACLE_TYPED_SELECT.format(table=typed)).get_data()]
        assert rows[0]["text_field"] == "typed values"
        assert rows[0]["number_field"] == 42
        assert rows[0]["date_text"] == "2024-06-15"
        assert _read_lob_sync(rows[0]["clob_field"]) == "CLOB content"
        assert rows[0]["raw_hex"] == "DEADBEEF"
        assert rows[1] == {
            "id": 2,
            "text_field": None,
            "number_field": None,
            "date_text": None,
            "clob_field": None,
            "raw_hex": None,
        }
    finally:
        _sync_drop_table(sync_driver, typed)

    ident = sync_driver.execute(
        'SELECT :upper_value AS UPPER_VALUE, :mixed_value AS "MixedCaseValue" FROM dual',
        {"upper_value": "upper", "mixed_value": "mixed"},
    ).get_data()[0]
    assert ident["upper_value"] == "upper"
    assert ident["MixedCaseValue"] == "mixed"

    with pytest.raises(Exception):
        sync_driver.execute("SELECT ? AS first_value, ? AS second_value FROM dual", None)


async def _oracle_param_codecs_async(driver: object, case: DriverCase) -> None:
    """Fold Oracle params: typed DATE/CLOB/RAW/NUMBER + NULL binds, JSON column NULL, identifier-case (async)."""
    async_driver = cast("AsyncContractDriver", driver)

    typed = _pc_table(case, "typed")
    await _async_drop_table(async_driver, typed)
    await async_driver.execute_script(_ORACLE_TYPED_DDL.format(table=typed))
    try:
        await async_driver.execute(
            _ORACLE_TYPED_INSERT.format(table=typed),
            {
                "id": 1,
                "text_field": "async typed values",
                "number_field": 84,
                "date_text": "2025-01-21",
                "clob_field": "Async CLOB content",
                "raw_hex": "FEEDFACE",
            },
        )
        await async_driver.execute(
            _ORACLE_TYPED_INSERT.format(table=typed),
            {"id": 2, "text_field": None, "number_field": None, "date_text": None, "clob_field": None, "raw_hex": None},
        )
        rows = [
            _lower_row(row) for row in (await async_driver.execute(_ORACLE_TYPED_SELECT.format(table=typed))).get_data()
        ]
        assert rows[0]["text_field"] == "async typed values"
        assert rows[0]["number_field"] == 84
        assert rows[0]["date_text"] == "2025-01-21"
        assert await _read_lob_async(rows[0]["clob_field"]) == "Async CLOB content"
        assert rows[0]["raw_hex"] == "FEEDFACE"
        assert rows[1] == {
            "id": 2,
            "text_field": None,
            "number_field": None,
            "date_text": None,
            "clob_field": None,
            "raw_hex": None,
        }
    finally:
        await _async_drop_table(async_driver, typed)

    json_t = _pc_table(case, "json")
    await _async_drop_table(async_driver, json_t)
    await async_driver.execute_script(f"CREATE TABLE {json_t} (id NUMBER PRIMARY KEY, payload JSON)")
    try:
        await async_driver.execute(
            f"INSERT INTO {json_t} (id, payload) VALUES (:id, :payload)", {"id": 1, "payload": None}
        )
        await async_driver.execute(
            f"INSERT INTO {json_t} (id, payload) VALUES (:id, :payload)",
            {"id": 2, "payload": {"status": "ok", "missing": None}},
        )
        json_rows = [
            _lower_row(row)
            for row in (await async_driver.execute(f"SELECT id, payload FROM {json_t} ORDER BY id")).get_data()
        ]
        assert json_rows[0] == {"id": 1, "payload": None}
        assert json_rows[1]["payload"] == {"status": "ok", "missing": None}
    finally:
        await _async_drop_table(async_driver, json_t)

    ident = (
        await async_driver.execute(
            'SELECT :upper_value AS UPPER_VALUE, :mixed_value AS "MixedCaseValue" FROM dual',
            {"upper_value": "upper", "mixed_value": "mixed"},
        )
    ).get_data()[0]
    assert ident["upper_value"] == "upper"
    assert ident["MixedCaseValue"] == "mixed"

    with pytest.raises(Exception):
        await async_driver.execute("SELECT ? AS first_value, ? AS second_value FROM dual", None)


def _adbc_postgres_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold ADBC-PostgreSQL params and Arrow-backed result codecs."""
    from datetime import date

    sync_driver = cast("SyncContractDriver", driver)
    items = _pc_table(case, "items")
    jsonb = _pc_table(case, "jsonb")
    types = _pc_table(case, "types")
    _sync_drop_table(sync_driver, items)
    _sync_drop_table(sync_driver, jsonb)
    _sync_drop_table(sync_driver, types)
    sync_driver.execute(
        f"CREATE TABLE {items} (id INTEGER PRIMARY KEY, name TEXT, value INTEGER, active BOOLEAN, created_date DATE)"
    )
    sync_driver.execute(f"CREATE TABLE {jsonb} (id INTEGER PRIMARY KEY, metadata JSONB, config JSONB)")
    sync_driver.execute(
        f"CREATE TABLE {types} ("
        "id SERIAL PRIMARY KEY, "
        "label TEXT, "
        "amount NUMERIC(10, 2), "
        "flag BOOLEAN, "
        "tags TEXT[], "
        "big_integer BIGINT, "
        "small_integer SMALLINT, "
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        ")"
    )
    sync_driver.commit()
    try:
        pruned = sync_driver.execute(
            f"INSERT INTO {items} (id, name, value, active, created_date) VALUES ($1, $2, $3, $4, $5)",
            1,
            None,
            42,
            True,
            date(2025, 1, 21),
        )
        sync_driver.commit()
        assert pruned.rows_affected in (-1, 0, 1)
        row = sync_driver.select_one(f"SELECT id, name, value, active, created_date FROM {items} WHERE id = $1", 1)
        assert row["name"] is None
        assert row["value"] == 42
        assert row["active"] is True
        assert row["created_date"] is not None

        sync_driver.execute(f"DELETE FROM {items}")
        sync_driver.commit()
        sync_driver.execute_many(
            f"INSERT INTO {items} (id, name, value) VALUES ($1, $2, $3)", [(1, "named", 10), (2, None, 20)]
        )
        sync_driver.commit()
        repeated = sync_driver.execute(
            f"SELECT id, name FROM {items} WHERE name = $1 OR ($1 IS NULL AND name IS NULL) ORDER BY id", None
        ).get_data()
        assert repeated == [{"id": 2, "name": None}]

        sync_driver.execute(f"DELETE FROM {items}")
        sync_driver.commit()
        returning = sync_driver.execute(
            f"INSERT INTO {items} (id, name, value, active) VALUES ($1, $2, $3, $4) RETURNING id, name, value, active",
            10,
            None,
            200,
            None,
        ).get_data()
        sync_driver.commit()
        assert returning == [{"id": 10, "name": None, "value": 200, "active": None}]

        jrow = sync_driver.execute(
            f"INSERT INTO {jsonb} (id, metadata, config) VALUES ($1, $2::jsonb, $3::jsonb) "
            f"RETURNING metadata ->> 'score' AS score, metadata ->> 'active' AS active, config",
            20,
            {"score": 100, "active": True},
            None,
        ).get_data()
        sync_driver.commit()
        assert jrow[0]["score"] == "100"
        assert jrow[0]["active"] == "true"
        assert jrow[0]["config"] is None

        sync_driver.execute(
            f"INSERT INTO {types} (label, amount, flag, tags, big_integer, small_integer) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            "Product A",
            Decimal("19.99"),
            True,
            ["electronics", "gadget"],
            9223372036854775807,
            32767,
        )
        sync_driver.execute(
            f"INSERT INTO {types} (label, amount, flag, tags, big_integer, small_integer) "
            "VALUES ($1, NULL, NULL, NULL, $2, $3)",
            "Null Product",
            -9223372036854775808,
            -32768,
        )
        sync_driver.commit()
        typed = sync_driver.execute(
            f"SELECT label, amount, flag, tags, big_integer, small_integer FROM {types} ORDER BY id"
        ).get_data()
        assert typed[0]["label"] == "Product A"
        assert float(typed[0]["amount"]) == 19.99
        assert typed[0]["flag"] is True
        assert typed[0]["tags"] == ["electronics", "gadget"]
        assert typed[0]["big_integer"] == 9223372036854775807
        assert typed[0]["small_integer"] == 32767
        assert typed[1]["amount"] is None
        assert typed[1]["flag"] is None
        assert typed[1]["tags"] is None
        assert typed[1]["big_integer"] == -9223372036854775808
        assert typed[1]["small_integer"] == -32768

        alias_result = sync_driver.execute(
            f"SELECT label AS product_name, amount AS product_price, flag AS is_available "
            f"FROM {types} WHERE label = $1",
            "Product A",
        )
        assert {"product_name", "product_price", "is_available"} <= set(alias_result.column_names)
        alias_row = alias_result.get_data()[0]
        assert alias_row["product_name"] == "Product A"
        assert float(alias_row["product_price"]) == 19.99
        assert alias_row["is_available"] is True

        with pytest.raises(SQLSpecError):
            sync_driver.execute(f"INSERT INTO {items} (id, name) VALUES ($1, $2)", 30, None, "extra")
    finally:
        _sync_drop_table(sync_driver, items)
        _sync_drop_table(sync_driver, jsonb)
        _sync_drop_table(sync_driver, types)


def _adbc_sqlite_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold ADBC-SQLite params and dynamic/binary Arrow-backed result codecs."""
    sync_driver = cast("SyncContractDriver", driver)
    items = _pc_table(case, "items")
    dynamic = _pc_table(case, "dynamic")
    binary = _pc_table(case, "binary")
    _sync_drop_table(sync_driver, items)
    _sync_drop_table(sync_driver, dynamic)
    _sync_drop_table(sync_driver, binary)
    sync_driver.execute(f"CREATE TABLE {items} (id INTEGER PRIMARY KEY, name TEXT, value INTEGER, active BOOLEAN)")
    sync_driver.execute(f"CREATE TABLE {dynamic} (id INTEGER PRIMARY KEY, flexible_column)")
    sync_driver.execute(f"CREATE TABLE {binary} (id INTEGER PRIMARY KEY, name TEXT, data BLOB)")
    try:
        inserted = sync_driver.execute(
            f"INSERT INTO {items} (id, name, value, active) VALUES (?, ?, ?, ?)", 1, None, None, True
        )
        assert inserted.rows_affected in (-1, 0, 1)
        row = sync_driver.select_one(f"SELECT name, value, active FROM {items} WHERE id = ?", 1)
        assert row["name"] is None
        assert row["value"] is None
        assert row["active"] in (True, 1)

        flexible_data = [(1, "text_value"), (2, 42), (3, math.pi), (4, b"binary_data"), (5, None)]
        for row_id, value in flexible_data:
            sync_driver.execute(f"INSERT INTO {dynamic} (id, flexible_column) VALUES (?, ?)", row_id, value)
        dynamic_rows = sync_driver.execute(
            f"SELECT id, flexible_column, typeof(flexible_column) AS column_type FROM {dynamic} ORDER BY id"
        ).get_data()
        assert dynamic_rows[0]["flexible_column"] in ("text_value", b"text_value")
        assert dynamic_rows[1]["flexible_column"] in (42, b"42")
        assert dynamic_rows[4]["flexible_column"] is None
        types_found = {row["column_type"].lower() for row in dynamic_rows if row["column_type"]}
        assert {"text", "integer", "real"} <= types_found

        binary_rows = [
            (1, "small", b"small data"),
            (2, "empty", b""),
            (3, "null", None),
            (4, "large", b"x" * 1000),
            (5, "range", bytes(range(256))),
        ]
        sync_driver.execute_many(f"INSERT INTO {binary} (id, name, data) VALUES (?, ?, ?)", binary_rows)
        blob_rows = sync_driver.execute(
            f"SELECT id, name, data, length(data) AS data_size FROM {binary} ORDER BY id"
        ).get_data()
        assert blob_rows[0]["data"] == b"small data"
        assert blob_rows[1]["data"] == b""
        assert blob_rows[2]["data"] is None
        assert blob_rows[3]["data_size"] == 1000
        assert blob_rows[4]["data"] == bytes(range(256))
    finally:
        _sync_drop_table(sync_driver, items)
        _sync_drop_table(sync_driver, dynamic)
        _sync_drop_table(sync_driver, binary)


def _adbc_duckdb_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold ADBC-DuckDB params and nested/list/map Arrow-backed result codecs."""
    from sqlspec.typing import PYARROW_INSTALLED

    sync_driver = cast("SyncContractDriver", driver)
    items = _pc_table(case, "items")
    advanced = _pc_table(case, "advanced")
    _sync_drop_table(sync_driver, items)
    _sync_drop_table(sync_driver, advanced)
    sync_driver.execute(f"CREATE TABLE {items} (id INTEGER PRIMARY KEY, name VARCHAR, value INTEGER)")
    try:
        for entry in [(1, "low", 10), (2, "mid", 20), (3, "high", 30)]:
            sync_driver.execute(f"INSERT INTO {items} (id, name, value) VALUES (?, ?, ?)", *entry)
        numeric = sync_driver.execute(
            f"SELECT name, value FROM {items} WHERE value >= $1 ORDER BY value", 20
        ).get_data()
        assert numeric == [{"name": "mid", "value": 20}, {"name": "high", "value": 30}]

        if PYARROW_INSTALLED:
            arrow = sync_driver.select_to_arrow(f"SELECT name, value FROM {items} WHERE value > ? ORDER BY value", 10)
            frame = arrow.to_pandas()
            assert list(frame["name"]) == ["mid", "high"]
            assert list(frame["value"]) == [20, 30]

        sync_driver.execute(
            f"CREATE TABLE {advanced} ("
            "id INTEGER, "
            "numbers INTEGER[], "
            "nested_data STRUCT(name VARCHAR, values INTEGER[]), "
            "map_data MAP(VARCHAR, INTEGER), "
            "json_col JSON"
            ")"
        )
        sync_driver.execute(
            f"INSERT INTO {advanced} VALUES ("
            "1, "
            "[1, 2, 3, 4, 5], "
            "{'name': 'nested', 'values': [10, 20, 30]}, "
            "MAP(['key1', 'key2'], [100, 200]), "
            '\'{"type": "test", "version": 1}\''
            ")"
        )
        advanced_row = sync_driver.execute(
            f"SELECT id, numbers, nested_data, map_data, json_extract_string(json_col, '$.type') AS json_type "
            f"FROM {advanced}"
        ).get_data()[0]
        assert advanced_row["id"] == 1
        assert advanced_row["numbers"] == [1, 2, 3, 4, 5]
        assert advanced_row["nested_data"] is not None
        assert advanced_row["map_data"] is not None
        assert advanced_row["json_type"] == "test"
    finally:
        _sync_drop_table(sync_driver, items)
        _sync_drop_table(sync_driver, advanced)


def _bigquery_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold the sqlspec-side BigQuery empty-ARRAY type-inference guard (raised before the query runs).

    @-prefix key normalization, UNNEST(@values), and STRUCT(@...) parameter expressions need a typed
    column or native BigQuery for the emulator to bind them, so they stay adapter-local.
    """
    sync_driver = cast("SyncContractDriver", driver)

    with pytest.raises(SQLSpecError, match="Cannot determine BigQuery ARRAY type"):
        sync_driver.execute("SELECT ARRAY_LENGTH(@values)", {"values": []})


register_sync_extra_assertion("param_codecs:bigquery", PARAM_CODECS_SCOPE, _bigquery_param_codecs)
register_sync_extra_assertion("param_codecs:adbc_postgres", PARAM_CODECS_SCOPE, _adbc_postgres_param_codecs)
register_sync_extra_assertion("param_codecs:adbc_sqlite", PARAM_CODECS_SCOPE, _adbc_sqlite_param_codecs)
register_sync_extra_assertion("param_codecs:adbc_duckdb", PARAM_CODECS_SCOPE, _adbc_duckdb_param_codecs)
register_sync_extra_assertion("param_codecs:oracle", PARAM_CODECS_SCOPE, _oracle_param_codecs)
register_async_extra_assertion("param_codecs:oracle", PARAM_CODECS_SCOPE, _oracle_param_codecs_async)
register_sync_extra_assertion("param_codecs:duckdb", PARAM_CODECS_SCOPE, _duckdb_param_codecs)
register_async_extra_assertion("param_codecs:cockroach_asyncpg", PARAM_CODECS_SCOPE, _cockroach_asyncpg_param_codecs)
register_sync_extra_assertion("param_codecs:cockroach_psycopg", PARAM_CODECS_SCOPE, _cockroach_psycopg_param_codecs)
register_async_extra_assertion(
    "param_codecs:cockroach_psycopg", PARAM_CODECS_SCOPE, _cockroach_psycopg_param_codecs_async
)
register_sync_extra_assertion("param_codecs:mysql", PARAM_CODECS_SCOPE, _mysql_param_codecs)
register_async_extra_assertion("param_codecs:mysql", PARAM_CODECS_SCOPE, _mysql_param_codecs_async)
register_async_extra_assertion("param_codecs:asyncpg", PARAM_CODECS_SCOPE, _asyncpg_param_codecs)
register_async_extra_assertion("param_codecs:psqlpy", PARAM_CODECS_SCOPE, _psqlpy_param_codecs)
register_sync_extra_assertion("param_codecs:psycopg", PARAM_CODECS_SCOPE, _psycopg_param_codecs)
register_async_extra_assertion("param_codecs:psycopg", PARAM_CODECS_SCOPE, _psycopg_param_codecs_async)


def assert_sync_param_codecs_contract(driver: object, case: DriverCase) -> None:
    """Run an adapter's folded driver-specific parameter-codec proofs (arrays/JSON/type fidelity), if any."""
    dispatch_sync_extra_assertions(driver, case, PARAM_CODECS_SCOPE)


async def assert_async_param_codecs_contract(driver: object, case: DriverCase) -> None:
    """Run an adapter's folded driver-specific parameter-codec proofs (arrays/JSON/type fidelity), if any."""
    await dispatch_async_extra_assertions(driver, case, PARAM_CODECS_SCOPE)


DRIVER_FEATURES_SCOPE = "driver_features"

_COPY_TEXT_DATA = "1\ttext-a\t100\n2\ttext-b\t200\n"
_COPY_CSV_DATA = "5,csv-a,500\n6,csv-b,600\n7,csv-c,700\n"


def _psycopg_copy(driver: object, case: DriverCase) -> None:
    """Fold psycopg COPY FROM STDIN: the data string binds as a single param for both text and csv formats."""
    sync_driver = cast("SyncContractDriver", driver)
    table = _pc_table(case, "copy")
    _sync_drop_table(sync_driver, table)
    sync_driver.execute_script(f"CREATE TABLE {table} (id INTEGER, name TEXT, value INTEGER)")
    try:
        text_result = sync_driver.execute(f"COPY {table} FROM STDIN WITH (FORMAT text)", _COPY_TEXT_DATA)
        assert isinstance(text_result, SQLResult)
        assert text_result.rows_affected >= 0
        text_rows = sync_driver.execute(f"SELECT id, name, value FROM {table} ORDER BY id").get_data()
        assert len(text_rows) == 2
        assert text_rows[0]["name"] == "text-a"
        assert text_rows[1]["value"] == 200

        sync_driver.execute_script(f"DELETE FROM {table}")
        csv_result = sync_driver.execute(f"COPY {table} FROM STDIN WITH (FORMAT csv)", _COPY_CSV_DATA)
        assert isinstance(csv_result, SQLResult)
        assert csv_result.rows_affected == 3
        csv_rows = sync_driver.execute(f"SELECT id, name, value FROM {table} ORDER BY id").get_data()
        assert len(csv_rows) == 3
        assert csv_rows[0]["name"] == "csv-a"
        assert csv_rows[2]["value"] == 700
    finally:
        _sync_drop_table(sync_driver, table)


async def _psycopg_copy_async(driver: object, case: DriverCase) -> None:
    """Async mirror of _psycopg_copy."""
    async_driver = cast("AsyncContractDriver", driver)
    table = _pc_table(case, "copy")
    await _async_drop_table(async_driver, table)
    await async_driver.execute_script(f"CREATE TABLE {table} (id INTEGER, name TEXT, value INTEGER)")
    try:
        text_result = await async_driver.execute(f"COPY {table} FROM STDIN WITH (FORMAT text)", _COPY_TEXT_DATA)
        assert isinstance(text_result, SQLResult)
        assert text_result.rows_affected >= 0
        text_rows = (await async_driver.execute(f"SELECT id, name, value FROM {table} ORDER BY id")).get_data()
        assert len(text_rows) == 2
        assert text_rows[0]["name"] == "text-a"
        assert text_rows[1]["value"] == 200

        await async_driver.execute_script(f"DELETE FROM {table}")
        csv_result = await async_driver.execute(f"COPY {table} FROM STDIN WITH (FORMAT csv)", _COPY_CSV_DATA)
        assert isinstance(csv_result, SQLResult)
        assert csv_result.rows_affected == 3
        csv_rows = (await async_driver.execute(f"SELECT id, name, value FROM {table} ORDER BY id")).get_data()
        assert len(csv_rows) == 3
        assert csv_rows[0]["name"] == "csv-a"
        assert csv_rows[2]["value"] == 700
    finally:
        await _async_drop_table(async_driver, table)


register_sync_extra_assertion("driver_features:psycopg_copy", DRIVER_FEATURES_SCOPE, _psycopg_copy)
register_async_extra_assertion("driver_features:psycopg_copy", DRIVER_FEATURES_SCOPE, _psycopg_copy_async)


def _duckdb_set_variable(driver: object, case: DriverCase) -> None:
    """Fold duckdb SET VARIABLE persistence across execute() calls (issue #341): set/read, update, multi-var, ETL filter."""
    sync_driver = cast("SyncContractDriver", driver)

    sync_driver.execute("SET VARIABLE contract_var = 'first'")
    assert sync_driver.execute("SELECT getvariable('contract_var') AS v").get_data()[0]["v"] == "first"
    sync_driver.execute("SET VARIABLE contract_var = 42")
    assert sync_driver.execute("SELECT getvariable('contract_var') AS v").get_data()[0]["v"] == 42
    sync_driver.execute("SET VARIABLE contract_var = 'now_string'")
    assert sync_driver.execute("SELECT getvariable('contract_var') AS v").get_data()[0]["v"] == "now_string"

    sync_driver.execute("SET VARIABLE var_a = 'alpha'")
    sync_driver.execute("SET VARIABLE var_b = 100")
    multi = sync_driver.execute("SELECT getvariable('var_a') AS a, getvariable('var_b') AS b").get_data()[0]
    assert multi == {"a": "alpha", "b": 100}

    sync_driver.execute("SET VARIABLE multiplier = 10")
    sync_driver.execute("SET VARIABLE base_val = 5")
    calc = sync_driver.execute(
        "SELECT getvariable('multiplier') * 3 + getvariable('base_val') AS calculated"
    ).get_data()[0]
    assert calc["calculated"] == 35

    table = _pc_table(case, "ws")
    _sync_drop_table(sync_driver, table)
    sync_driver.execute_script(f"CREATE TABLE {table} (workspace_id TEXT, item_name TEXT, value INTEGER)")
    try:
        sync_driver.execute_many(
            f"INSERT INTO {table} (workspace_id, item_name, value) VALUES (?, ?, ?)",
            [("ws_001", "item_a", 10), ("ws_001", "item_b", 20), ("ws_002", "item_c", 30)],
        )
        sync_driver.execute("SET VARIABLE current_workspace = 'ws_001'")
        ws1 = sync_driver.execute(
            f"SELECT item_name FROM {table} WHERE workspace_id = getvariable('current_workspace') ORDER BY item_name"
        ).get_data()
        assert ws1 == [{"item_name": "item_a"}, {"item_name": "item_b"}]
        sync_driver.execute("SET VARIABLE current_workspace = 'ws_002'")
        ws2 = sync_driver.execute(
            f"SELECT item_name FROM {table} WHERE workspace_id = getvariable('current_workspace') ORDER BY item_name"
        ).get_data()
        assert ws2 == [{"item_name": "item_c"}]
    finally:
        _sync_drop_table(sync_driver, table)


register_sync_extra_assertion("driver_features:duckdb_set_variable", DRIVER_FEATURES_SCOPE, _duckdb_set_variable)


_ORACLE_DROP_CODE = {"TABLE": -942, "SEQUENCE": -2289, "PROCEDURE": -4043}


def _oracle_drop_sql(kind: str, name: str) -> str:
    return (
        f"BEGIN EXECUTE IMMEDIATE 'DROP {kind} {name}'; "
        f"EXCEPTION WHEN OTHERS THEN IF SQLCODE != {_ORACLE_DROP_CODE[kind]} THEN RAISE; END IF; END;"
    )


def _oracle_sequence(driver: object, case: DriverCase) -> None:
    """Fold Oracle sequences: insert via seq.NEXTVAL, read seq.CURRVAL FROM dual, verify the row round-trips."""
    sync_driver = cast("SyncContractDriver", driver)
    seq = f"contract_seq_{case.adapter}_{case.mode}"
    table = _pc_table(case, "seq")
    sync_driver.execute_script(_oracle_drop_sql("TABLE", table))
    sync_driver.execute_script(_oracle_drop_sql("SEQUENCE", seq))
    sync_driver.execute_script(f"CREATE SEQUENCE {seq} START WITH 1 INCREMENT BY 1")
    sync_driver.execute_script(f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(50))")
    try:
        sync_driver.execute(f"INSERT INTO {table} (id, name) VALUES ({seq}.NEXTVAL, :1)", ("seq_name",))
        last_id = sync_driver.select_value(f"SELECT {seq}.CURRVAL FROM dual")
        row = sync_driver.select_one(f"SELECT id, name FROM {table} WHERE id = :1", (last_id,))
        assert row["name"] == "seq_name"
        assert row["id"] == last_id
    finally:
        sync_driver.execute_script(_oracle_drop_sql("TABLE", table))
        sync_driver.execute_script(_oracle_drop_sql("SEQUENCE", seq))


async def _oracle_sequence_async(driver: object, case: DriverCase) -> None:
    """Async mirror of _oracle_sequence."""
    async_driver = cast("AsyncContractDriver", driver)
    seq = f"contract_seq_{case.adapter}_{case.mode}"
    table = _pc_table(case, "seq")
    await async_driver.execute_script(_oracle_drop_sql("TABLE", table))
    await async_driver.execute_script(_oracle_drop_sql("SEQUENCE", seq))
    await async_driver.execute_script(f"CREATE SEQUENCE {seq} START WITH 1 INCREMENT BY 1")
    await async_driver.execute_script(f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(50))")
    try:
        await async_driver.execute(f"INSERT INTO {table} (id, name) VALUES ({seq}.NEXTVAL, :1)", ("seq_name",))
        last_id = await async_driver.select_value(f"SELECT {seq}.CURRVAL FROM dual")
        row = await async_driver.select_one(f"SELECT id, name FROM {table} WHERE id = :1", (last_id,))
        assert row["name"] == "seq_name"
        assert row["id"] == last_id
    finally:
        await async_driver.execute_script(_oracle_drop_sql("TABLE", table))
        await async_driver.execute_script(_oracle_drop_sql("SEQUENCE", seq))


register_sync_extra_assertion("driver_features:oracle_sequence", DRIVER_FEATURES_SCOPE, _oracle_sequence)
register_async_extra_assertion("driver_features:oracle_sequence", DRIVER_FEATURES_SCOPE, _oracle_sequence_async)


def _assert_oracle_batch_errors_result(result: SQLResult) -> None:
    """Verify Oracle batch-errors metadata from a three-row insert with one duplicate."""
    assert result.rows_affected == 2
    errors = result.metadata["oracle_batch_errors"]
    assert len(errors) == 1
    assert errors[0]["offset"] == 1
    assert errors[0]["code"] == 1
    assert "ORA-" in errors[0]["message"]

    row_counts = result.metadata["oracle_dml_row_counts"]
    assert sum(row_counts) == 2


def _oracle_batch_errors(driver: object, case: DriverCase) -> None:
    """Fold Oracle batcherrors/arraydmlrowcounts metadata via per-call execution_args."""
    sync_driver = cast("SyncContractDriver", driver)
    table = _pc_table(case, "batcherr")
    sync_driver.execute_script(_oracle_drop_sql("TABLE", table))
    sync_driver.execute_script(f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(50))")
    config = sync_driver.statement_config.replace(
        execution_args={"oracle_batch_errors": True, "oracle_array_dml_row_counts": True}
    )
    try:
        result = sync_driver.execute_many(
            f"INSERT INTO {table} (id, name) VALUES (:1, :2)",
            [(1, "first"), (1, "duplicate"), (3, "third")],
            statement_config=config,
        )
        _assert_oracle_batch_errors_result(result)
    finally:
        sync_driver.execute_script(_oracle_drop_sql("TABLE", table))


async def _oracle_batch_errors_async(driver: object, case: DriverCase) -> None:
    """Async mirror of _oracle_batch_errors."""
    async_driver = cast("AsyncContractDriver", driver)
    table = _pc_table(case, "batcherr")
    await async_driver.execute_script(_oracle_drop_sql("TABLE", table))
    await async_driver.execute_script(f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(50))")
    config = async_driver.statement_config.replace(
        execution_args={"oracle_batch_errors": True, "oracle_array_dml_row_counts": True}
    )
    try:
        result = await async_driver.execute_many(
            f"INSERT INTO {table} (id, name) VALUES (:1, :2)",
            [(1, "first"), (1, "duplicate"), (3, "third")],
            statement_config=config,
        )
        _assert_oracle_batch_errors_result(result)
    finally:
        await async_driver.execute_script(_oracle_drop_sql("TABLE", table))


register_sync_extra_assertion("driver_features:oracle_batch_errors", DRIVER_FEATURES_SCOPE, _oracle_batch_errors)
register_async_extra_assertion("driver_features:oracle_batch_errors", DRIVER_FEATURES_SCOPE, _oracle_batch_errors_async)


def _lower_keys(row: dict[str, object]) -> dict[str, Any]:
    return {key.lower(): value for key, value in row.items()}


def _oracle_plsql(driver: object, case: DriverCase) -> None:
    """Fold Oracle PL/SQL script execution with local variables, control flow, loops, and DML."""
    sync_driver = cast("SyncContractDriver", driver)
    table = _pc_table(case, "plsql")
    sync_driver.execute_script(_oracle_drop_sql("TABLE", table))
    sync_driver.execute_script(
        f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(50), calculated_value NUMBER)"
    )
    try:
        result = sync_driver.execute_script(
            f"""
            DECLARE
                v_base_value NUMBER := 10;
                v_multiplier NUMBER := 3;
                v_result NUMBER;
                v_name VARCHAR2(50) := 'plsql_test';
            BEGIN
                v_result := v_base_value * v_multiplier;

                IF v_result > 25 THEN
                    v_result := v_result + 100;
                END IF;

                INSERT INTO {table} (id, name, calculated_value)
                VALUES (1, v_name, v_result);

                FOR i IN 2..4 LOOP
                    INSERT INTO {table} (id, name, calculated_value)
                    VALUES (i, v_name || '_' || i, v_result + i);
                END LOOP;

                COMMIT;
            END;
            """
        )
        assert isinstance(result, SQLResult)
        rows = [_lower_keys(row) for row in sync_driver.execute(f"SELECT * FROM {table} ORDER BY id").get_data()]
        assert len(rows) == 4
        assert rows[0]["name"] == "plsql_test"
        assert rows[0]["calculated_value"] == 130
    finally:
        sync_driver.execute_script(_oracle_drop_sql("TABLE", table))


async def _oracle_plsql_async(driver: object, case: DriverCase) -> None:
    """Async mirror of _oracle_plsql with a stored procedure invocation path."""
    async_driver = cast("AsyncContractDriver", driver)
    table = _pc_table(case, "proc")
    procedure = _pc_table(case, "plsql_proc")
    await async_driver.execute_script(_oracle_drop_sql("PROCEDURE", procedure))
    await async_driver.execute_script(_oracle_drop_sql("TABLE", table))
    await async_driver.execute_script(
        f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, input_value NUMBER, output_value NUMBER)"
    )
    try:
        await async_driver.execute_script(
            f"""
            CREATE OR REPLACE PROCEDURE {procedure}(
                p_input IN NUMBER,
                p_output OUT NUMBER
            ) AS
            BEGIN
                p_output := p_input * 2 + 10;

                INSERT INTO {table} (id, input_value, output_value)
                VALUES (p_input, p_input, p_output);

                COMMIT;
            END {procedure};
            """
        )
        result = await async_driver.execute_script(
            f"""
            DECLARE
                v_output NUMBER;
            BEGIN
                {procedure}(5, v_output);
                {procedure}(10, v_output);
            END;
            """
        )
        assert isinstance(result, SQLResult)
        rows = [
            _lower_keys(row) for row in (await async_driver.execute(f"SELECT * FROM {table} ORDER BY id")).get_data()
        ]
        assert len(rows) == 2
        assert rows[0]["input_value"] == 5
        assert rows[0]["output_value"] == 20
        assert rows[1]["input_value"] == 10
        assert rows[1]["output_value"] == 30
    finally:
        await async_driver.execute_script(_oracle_drop_sql("PROCEDURE", procedure))
        await async_driver.execute_script(_oracle_drop_sql("TABLE", table))


register_sync_extra_assertion("driver_features:oracle_plsql", DRIVER_FEATURES_SCOPE, _oracle_plsql)
register_async_extra_assertion("driver_features:oracle_plsql", DRIVER_FEATURES_SCOPE, _oracle_plsql_async)


def _oracle_json_payloads() -> "list[object]":
    """Payloads exercising the native DB_TYPE_JSON path: dict, list[dict], >4000B dict, special values."""
    return [
        {"foo": "bar", "n": 42, "nested": {"x": [1, 2, 3]}},
        [{"a": 1}, {"b": 2}, {"c": [10, 20, 30]}],
        {"big": "x" * 8000, "n": list(range(500))},
        {
            "active": True,
            "deleted": False,
            "missing": None,
            "count": 42,
            "tags": ["alpha", "beta", "gamma"],
            "labels": {"env": "prod", "tier": "primary"},
        },
    ]


def _oracle_json_native(driver: object, case: DriverCase) -> None:
    """Fold Oracle native JSON: dict / list[dict] / >4000B / special-value payloads + executemany round-trip."""
    sync_driver = cast("SyncContractDriver", driver)
    table = _pc_table(case, "json")
    sync_driver.execute_script(_oracle_drop_sql("TABLE", table))
    sync_driver.execute_script(f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, payload JSON)")
    try:
        for row_id, payload in enumerate(_oracle_json_payloads(), start=1):
            sync_driver.execute(f"INSERT INTO {table} (id, payload) VALUES (:1, :2)", (row_id, payload))
            stored = sync_driver.select_one(f"SELECT payload FROM {table} WHERE id = :1", (row_id,))
            assert stored["payload"] == payload
        sync_driver.execute_script(f"DELETE FROM {table}")
        many = [(i, {"index": i, "label": f"row-{i}"}) for i in range(1, 6)]
        sync_driver.execute_many(f"INSERT INTO {table} (id, payload) VALUES (:1, :2)", many)
        rows = sync_driver.execute(f"SELECT id, payload FROM {table} ORDER BY id").get_data()
        assert [(r["id"], r["payload"]) for r in rows] == many
    finally:
        sync_driver.execute_script(_oracle_drop_sql("TABLE", table))


async def _oracle_json_native_async(driver: object, case: DriverCase) -> None:
    """Async mirror of _oracle_json_native."""
    async_driver = cast("AsyncContractDriver", driver)
    table = _pc_table(case, "json")
    await async_driver.execute_script(_oracle_drop_sql("TABLE", table))
    await async_driver.execute_script(f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, payload JSON)")
    try:
        for row_id, payload in enumerate(_oracle_json_payloads(), start=1):
            await async_driver.execute(f"INSERT INTO {table} (id, payload) VALUES (:1, :2)", (row_id, payload))
            stored = await async_driver.select_one(f"SELECT payload FROM {table} WHERE id = :1", (row_id,))
            assert stored["payload"] == payload
        await async_driver.execute_script(f"DELETE FROM {table}")
        many = [(i, {"index": i, "label": f"row-{i}"}) for i in range(1, 6)]
        await async_driver.execute_many(f"INSERT INTO {table} (id, payload) VALUES (:1, :2)", many)
        rows = (await async_driver.execute(f"SELECT id, payload FROM {table} ORDER BY id")).get_data()
        assert [(r["id"], r["payload"]) for r in rows] == many
    finally:
        await async_driver.execute_script(_oracle_drop_sql("TABLE", table))


register_sync_extra_assertion("driver_features:oracle_json_native", DRIVER_FEATURES_SCOPE, _oracle_json_native)
register_async_extra_assertion("driver_features:oracle_json_native", DRIVER_FEATURES_SCOPE, _oracle_json_native_async)


ORACLE_LOB_FETCH_SCOPE = "oracle_lob_fetch"
_ORACLE_LOB_JSON_PAYLOAD: dict[str, object] = {"integer": 42, "fraction": 1.25, "nested": {"label": "metadata-driven"}}
_ORACLE_OSON_PAYLOAD: dict[str, object] = {"kind": "oson", "tags": ["contract", "oracle"]}
_ORACLE_PLAIN_JSON_TEXT = to_json({"looks": "json", "but": "plain-clob"})
_ORACLE_LONG_CLOB_TEXT = "long oracle clob " + ("x" * 5000)
_ORACLE_PLAIN_BLOB_BYTES = b"plain-oracle-blob"


def _oracle_lob_table(case: DriverCase, suffix: str) -> str:
    return _pc_table(case, f"lob_{suffix}")


def _oracle_lob_select_sql(table: str) -> str:
    return f"SELECT native_json, json_clob, json_blob, plain_clob, plain_blob, long_clob FROM {table} WHERE id = 1"


@contextlib.contextmanager
def _oracle_driver_feature(driver: object, key: str, value: object) -> "Iterator[None]":
    features = cast("Any", driver).driver_features
    missing = object()
    previous = features.get(key, missing)
    features[key] = value
    try:
        yield
    finally:
        if previous is missing:
            features.pop(key, None)
        else:
            features[key] = previous


def _json_dict(value: object) -> dict[str, Any]:
    if isinstance(value, memoryview):
        return cast("dict[str, Any]", from_json(value.tobytes()))
    if isinstance(value, bytes | bytearray):
        return cast("dict[str, Any]", from_json(bytes(value)))
    if isinstance(value, str):
        return cast("dict[str, Any]", from_json(value))
    return cast("dict[str, Any]", value)


def _bytes_value(value: object) -> bytes:
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
    return cast("bytes", value)


def _assert_oracle_json_lanes(row: dict[str, object]) -> None:
    native_json = _json_dict(row["native_json"])
    json_clob = _json_dict(row["json_clob"])
    json_blob = _json_dict(row["json_blob"])

    assert native_json["nested"] == _ORACLE_LOB_JSON_PAYLOAD["nested"]
    assert isinstance(native_json["integer"], Decimal)
    assert isinstance(native_json["fraction"], Decimal)
    assert json_clob == _ORACLE_LOB_JSON_PAYLOAD
    assert json_blob == _ORACLE_LOB_JSON_PAYLOAD
    assert type(json_clob["integer"]) is int
    assert type(json_clob["fraction"]) is float
    assert type(json_blob["integer"]) is int
    assert type(json_blob["fraction"]) is float


def _assert_oracle_lob_materialized_row(row: Mapping[str, object]) -> None:
    normalized = _lower_keys(dict(row))
    _assert_oracle_json_lanes(normalized)
    assert normalized["plain_clob"] == _ORACLE_PLAIN_JSON_TEXT
    assert _bytes_value(normalized["plain_blob"]) == _ORACLE_PLAIN_BLOB_BYTES
    assert normalized["long_clob"] == _ORACLE_LONG_CLOB_TEXT


def _assert_oracle_lob_arrow_row(row: Mapping[str, object]) -> None:
    normalized = _lower_keys(dict(row))
    assert normalized["plain_clob"] == _ORACLE_PLAIN_JSON_TEXT
    assert _bytes_value(normalized["plain_blob"]) == _ORACLE_PLAIN_BLOB_BYTES
    assert normalized["long_clob"] == _ORACLE_LONG_CLOB_TEXT


def _read_sync_lob(value: object) -> object:
    read = getattr(value, "read", None)
    assert callable(read)
    return read()


async def _read_async_lob(value: object) -> object:
    read = getattr(value, "read", None)
    assert callable(read)
    result = read()
    if inspect.isawaitable(result):
        result = await result
    return result


def _seed_oracle_lob_fetch_sync(driver: "SyncContractDriver", case: DriverCase, table: str) -> None:
    from sqlspec.adapters.oracledb import OracleBlob, OracleClob

    _sync_drop_table(driver, table)
    driver.execute_script(
        f"""
        CREATE TABLE {table} (
            id NUMBER PRIMARY KEY,
            native_json JSON,
            json_clob CLOB CHECK (json_clob IS JSON),
            json_blob BLOB CHECK (json_blob IS JSON),
            plain_clob CLOB,
            plain_blob BLOB,
            long_clob CLOB
        )
        """
    )
    driver.execute(
        f"""
        INSERT INTO {table}
            (id, native_json, json_clob, json_blob, plain_clob, plain_blob, long_clob)
        VALUES
            (:id, :native_json, :json_clob, :json_blob, :plain_clob, :plain_blob, :long_clob)
        """,
        {
            "id": 1,
            "native_json": _ORACLE_LOB_JSON_PAYLOAD,
            "json_clob": OracleClob(to_json(_ORACLE_LOB_JSON_PAYLOAD)),
            "json_blob": OracleBlob(to_json(_ORACLE_LOB_JSON_PAYLOAD, as_bytes=True)),
            "plain_clob": OracleClob(_ORACLE_PLAIN_JSON_TEXT),
            "plain_blob": OracleBlob(_ORACLE_PLAIN_BLOB_BYTES),
            "long_clob": _ORACLE_LONG_CLOB_TEXT,
        },
    )
    driver.commit()


async def _seed_oracle_lob_fetch_async(driver: "AsyncContractDriver", case: DriverCase, table: str) -> None:
    from sqlspec.adapters.oracledb import OracleBlob, OracleClob

    await _async_drop_table(driver, table)
    await driver.execute_script(
        f"""
        CREATE TABLE {table} (
            id NUMBER PRIMARY KEY,
            native_json JSON,
            json_clob CLOB CHECK (json_clob IS JSON),
            json_blob BLOB CHECK (json_blob IS JSON),
            plain_clob CLOB,
            plain_blob BLOB,
            long_clob CLOB
        )
        """
    )
    await driver.execute(
        f"""
        INSERT INTO {table}
            (id, native_json, json_clob, json_blob, plain_clob, plain_blob, long_clob)
        VALUES
            (:id, :native_json, :json_clob, :json_blob, :plain_clob, :plain_blob, :long_clob)
        """,
        {
            "id": 1,
            "native_json": _ORACLE_LOB_JSON_PAYLOAD,
            "json_clob": OracleClob(to_json(_ORACLE_LOB_JSON_PAYLOAD)),
            "json_blob": OracleBlob(to_json(_ORACLE_LOB_JSON_PAYLOAD, as_bytes=True)),
            "plain_clob": OracleClob(_ORACLE_PLAIN_JSON_TEXT),
            "plain_blob": OracleBlob(_ORACLE_PLAIN_BLOB_BYTES),
            "long_clob": _ORACLE_LONG_CLOB_TEXT,
        },
    )
    await driver.commit()


def _assert_oracle_lob_fetch_true_stream_sync(driver: "SyncContractDriver", table: str) -> None:
    with driver.select_stream(
        f"SELECT plain_clob, plain_blob FROM {table} WHERE id = 1", chunk_size=1, fetch_lobs=True
    ) as stream:
        row = _lower_keys(next(iter(stream)))
        assert _read_sync_lob(row["plain_clob"]) == _ORACLE_PLAIN_JSON_TEXT
        assert _read_sync_lob(row["plain_blob"]) == _ORACLE_PLAIN_BLOB_BYTES


async def _assert_oracle_lob_fetch_true_stream_async(driver: "AsyncContractDriver", table: str) -> None:
    async with driver.select_stream(
        f"SELECT plain_clob, plain_blob FROM {table} WHERE id = 1", chunk_size=1, fetch_lobs=True
    ) as stream:
        row = _lower_keys(await anext(aiter(stream)))
        assert await _read_async_lob(row["plain_clob"]) == _ORACLE_PLAIN_JSON_TEXT
        assert await _read_async_lob(row["plain_blob"]) == _ORACLE_PLAIN_BLOB_BYTES


def _assert_oracle_lob_oson_sync(driver: "SyncContractDriver", case: DriverCase) -> None:
    from sqlspec.adapters.oracledb import OracleBlob

    connection = cast("Any", driver).connection
    encode_oson = getattr(connection, "encode_oson", None)
    if not callable(encode_oson):
        return
    table = _oracle_lob_table(case, "oson")
    _sync_drop_table(driver, table)
    try:
        try:
            driver.execute_script(
                f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, payload BLOB CHECK (payload IS JSON FORMAT OSON))"
            )
        except Exception as exc:
            if "ORA-" in str(exc):
                return
            raise
        driver.execute(
            f"INSERT INTO {table} (id, payload) VALUES (:id, :payload)",
            {"id": 1, "payload": OracleBlob(cast("bytes", encode_oson(_ORACLE_OSON_PAYLOAD)))},
        )
        row = _lower_keys(driver.select_one(f"SELECT payload FROM {table} WHERE id = 1"))
        assert _json_dict(row["payload"]) == _ORACLE_OSON_PAYLOAD
    finally:
        _sync_drop_table(driver, table)


async def _assert_oracle_lob_oson_async(driver: "AsyncContractDriver", case: DriverCase) -> None:
    from sqlspec.adapters.oracledb import OracleBlob

    connection = cast("Any", driver).connection
    encode_oson = getattr(connection, "encode_oson", None)
    if not callable(encode_oson):
        return
    encoded = encode_oson(_ORACLE_OSON_PAYLOAD)
    if inspect.isawaitable(encoded):
        encoded = await encoded
    table = _oracle_lob_table(case, "oson")
    await _async_drop_table(driver, table)
    try:
        try:
            await driver.execute_script(
                f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, payload BLOB CHECK (payload IS JSON FORMAT OSON))"
            )
        except Exception as exc:
            if "ORA-" in str(exc):
                return
            raise
        await driver.execute(
            f"INSERT INTO {table} (id, payload) VALUES (:id, :payload)",
            {"id": 1, "payload": OracleBlob(cast("bytes", encoded))},
        )
        row = _lower_keys(await driver.select_one(f"SELECT payload FROM {table} WHERE id = 1"))
        assert _json_dict(row["payload"]) == _ORACLE_OSON_PAYLOAD
    finally:
        await _async_drop_table(driver, table)


def _oracle_lob_fetch_matrix(driver: object, case: DriverCase) -> None:
    sync_driver = cast("SyncContractDriver", driver)
    table = _oracle_lob_table(case, "matrix")
    _seed_oracle_lob_fetch_sync(sync_driver, case, table)
    select_sql = _oracle_lob_select_sql(table)
    try:
        _assert_oracle_lob_materialized_row(sync_driver.select_one(select_sql))
        with _oracle_driver_feature(sync_driver, "fetch_lobs", True):
            _assert_oracle_lob_materialized_row(sync_driver.select_one(select_sql))
        with sync_driver.select_stream(select_sql, chunk_size=1) as stream:
            _assert_oracle_lob_materialized_row(next(iter(stream)))
        _assert_oracle_lob_fetch_true_stream_sync(sync_driver, table)
        _assert_oracle_lob_arrow_row(
            sync_driver.select_to_arrow(
                f"SELECT plain_clob, plain_blob, long_clob FROM {table} WHERE id = 1"
            ).data.to_pylist()[0]
        )
        with _oracle_driver_feature(sync_driver, "fetch_lobs", True):
            _assert_oracle_lob_arrow_row(
                sync_driver.select_to_arrow(
                    f"SELECT plain_clob, plain_blob, long_clob FROM {table} WHERE id = 1"
                ).data.to_pylist()[0]
            )
        _assert_oracle_lob_oson_sync(sync_driver, case)
    finally:
        _sync_drop_table(sync_driver, table)


async def _oracle_lob_fetch_matrix_async(driver: object, case: DriverCase) -> None:
    async_driver = cast("AsyncContractDriver", driver)
    table = _oracle_lob_table(case, "matrix")
    await _seed_oracle_lob_fetch_async(async_driver, case, table)
    select_sql = _oracle_lob_select_sql(table)
    try:
        _assert_oracle_lob_materialized_row(await async_driver.select_one(select_sql))
        with _oracle_driver_feature(async_driver, "fetch_lobs", True):
            _assert_oracle_lob_materialized_row(await async_driver.select_one(select_sql))
        async with async_driver.select_stream(select_sql, chunk_size=1) as stream:
            _assert_oracle_lob_materialized_row(await anext(aiter(stream)))
        await _assert_oracle_lob_fetch_true_stream_async(async_driver, table)
        _assert_oracle_lob_arrow_row(
            (
                await async_driver.select_to_arrow(
                    f"SELECT plain_clob, plain_blob, long_clob FROM {table} WHERE id = 1"
                )
            ).data.to_pylist()[0]
        )
        with _oracle_driver_feature(async_driver, "fetch_lobs", True):
            _assert_oracle_lob_arrow_row(
                (
                    await async_driver.select_to_arrow(
                        f"SELECT plain_clob, plain_blob, long_clob FROM {table} WHERE id = 1"
                    )
                ).data.to_pylist()[0]
            )
        await _assert_oracle_lob_oson_async(async_driver, case)
    finally:
        await _async_drop_table(async_driver, table)


register_sync_extra_assertion("oracle_lob_fetch:matrix", ORACLE_LOB_FETCH_SCOPE, _oracle_lob_fetch_matrix)
register_async_extra_assertion("oracle_lob_fetch:matrix", ORACLE_LOB_FETCH_SCOPE, _oracle_lob_fetch_matrix_async)


def assert_sync_oracle_lob_fetch_contract(driver: object, case: DriverCase) -> None:
    """Run Oracle's shared LOB fetch matrix proof."""
    dispatch_sync_extra_assertions(driver, case, ORACLE_LOB_FETCH_SCOPE)


async def assert_async_oracle_lob_fetch_contract(driver: object, case: DriverCase) -> None:
    """Run Oracle's shared async LOB fetch matrix proof."""
    await dispatch_async_extra_assertions(driver, case, ORACLE_LOB_FETCH_SCOPE)


def _bigquery_sql_features(driver: object, case: DriverCase) -> None:
    """Fold BigQuery dialect scalar SQL: math/string/conditional/specific functions + ARRAY/STRUCT.

    Window functions + schema DDL are left as bigquery-local residuals because
    the emulator capability profile does not support them reliably.
    """
    sync_driver = cast("SyncContractDriver", driver)

    math_row = sync_driver.execute(
        "SELECT ABS(-42) AS abs_value, ROUND(3.15159234, 2) AS rounded, MOD(17, 5) AS mod_result, "
        "POWER(2, 3) AS power_result"
    ).get_data()[0]
    assert math_row == {"abs_value": 42, "rounded": 3.15, "mod_result": 2, "power_result": 8}

    string_row = sync_driver.execute(
        "SELECT UPPER('hello') AS u, LOWER('WORLD') AS l, LENGTH('BigQuery') AS n, CONCAT('Hello', ' ', 'World') AS c"
    ).get_data()[0]
    assert string_row == {"u": "HELLO", "l": "world", "n": 8, "c": "Hello World"}

    cond_row = sync_driver.execute(
        "SELECT CASE WHEN 1 > 0 THEN 'positive' ELSE 'negative' END AS case_result, "
        "IF(10 > 5, 'greater', 'lesser') AS if_result, IFNULL(NULL, 'default_value') AS ifnull_result, "
        "NULLIF(5, 5) AS nullif_result, COALESCE(NULL, NULL, 'first_non_null') AS coalesce_result"
    ).get_data()[0]
    assert cond_row == {
        "case_result": "positive",
        "if_result": "greater",
        "ifnull_result": "default_value",
        "nullif_result": None,
        "coalesce_result": "first_non_null",
    }

    fn_row = sync_driver.execute(
        "SELECT GENERATE_UUID() AS uuid_val, FARM_FINGERPRINT('test') AS fingerprint"
    ).get_data()[0]
    assert fn_row["uuid_val"] is not None
    assert fn_row["fingerprint"] is not None

    array_row = sync_driver.execute(
        "SELECT ARRAY[1, 2, 3, 4, 5] AS numbers, ARRAY_LENGTH(ARRAY[1, 2, 3, 4, 5]) AS array_len"
    ).get_data()[0]
    assert array_row["numbers"] == [1, 2, 3, 4, 5]
    assert array_row["array_len"] == 5

    struct_row = sync_driver.execute(
        "SELECT STRUCT('Alice' AS name, 25 AS age) AS person, STRUCT('Alice' AS name, 25 AS age).name AS person_name"
    ).get_data()[0]
    assert struct_row["person"]["name"] == "Alice"
    assert struct_row["person"]["age"] == 25
    assert struct_row["person_name"] == "Alice"


register_sync_extra_assertion("driver_features:bigquery_sql_features", DRIVER_FEATURES_SCOPE, _bigquery_sql_features)


def _bigquery_job_controls(driver: object, case: DriverCase) -> None:
    """Fold BigQuery job-control wiring into the shared driver-feature contract."""
    assert case.adapter == "bigquery"
    sync_driver = cast("SyncContractDriver", driver)
    assert not hasattr(driver, "execute_with_job")
    assert not hasattr(driver, "export_table_to_storage")

    execute_parameters = inspect.signature(sync_driver.execute).parameters
    for parameter_name in ("job_config", "job_retry", "job_result_timeout", "request_timeout", "use_query_and_wait"):
        assert parameter_name not in execute_parameters

    assert getattr(driver, "_job_result_timeout") == 30.0
    assert getattr(driver, "_job_retry_deadline") == 0.0
    assert getattr(driver, "_job_retry") is None
    assert getattr(driver, "_request_timeout") == 15.0
    assert getattr(driver, "_use_query_and_wait") is False


register_sync_extra_assertion("driver_features:bigquery_job_controls", DRIVER_FEATURES_SCOPE, _bigquery_job_controls)


def assert_sync_driver_features_contract(driver: object, case: DriverCase) -> None:
    """Run an adapter's folded driver-feature proofs (COPY, SET-variable persistence, native types), if any."""
    dispatch_sync_extra_assertions(driver, case, DRIVER_FEATURES_SCOPE)


async def assert_async_driver_features_contract(driver: object, case: DriverCase) -> None:
    """Run an adapter's folded driver-feature proofs (COPY, SET-variable persistence, native types), if any."""
    await dispatch_async_extra_assertions(driver, case, DRIVER_FEATURES_SCOPE)


class SyncLifecycleConfig(Protocol):
    """Sync config surface the pooling/connection-hook contracts exercise."""

    connection_instance: object

    def provide_session(self) -> "contextlib.AbstractContextManager[SyncContractDriver]": ...

    def provide_pool(self) -> object: ...

    def close_pool(self) -> None: ...


class AsyncLifecycleConfig(Protocol):
    """Async config surface the pooling/connection-hook contracts exercise."""

    connection_instance: object

    def provide_session(self) -> "contextlib.AbstractAsyncContextManager[AsyncContractDriver]": ...

    async def provide_pool(self) -> object: ...

    async def close_pool(self) -> None: ...


SyncConfigFactory = Callable[..., SyncLifecycleConfig]
AsyncConfigFactory = Callable[..., AsyncLifecycleConfig]
ConnectionHook = Callable[[object], None]


def _pool_contract_table(case: DriverCase) -> str:
    return f"pool_contract_{case.adapter}_{case.mode}"


def _connection_probe_sql(case: DriverCase) -> str:
    """Trivial query used to force a connection to be drawn so the create-hook fires."""
    return "SELECT 1 FROM DUAL" if case.dialect == "oracle" else "SELECT 1"


def assert_sync_connection_hook_contract(make_config: SyncConfigFactory, case: DriverCase) -> None:
    """Assert the on_connection_create driver-feature hook fires for sync pooled/direct connections."""
    hook_calls = 0

    def hook(connection: object, *_args: object) -> None:
        # *_args absorbs adapter-specific extra positionals (oracledb passes (connection, tag)).
        nonlocal hook_calls
        hook_calls += 1

    config = make_config(driver_features={"on_connection_create": hook})
    try:
        with config.provide_session() as session:
            session.execute(_connection_probe_sql(case))
        assert hook_calls >= 1, f"{case.adapter} on_connection_create hook should fire at least once"
    finally:
        config.close_pool()


async def assert_async_connection_hook_contract(make_config: AsyncConfigFactory, case: DriverCase) -> None:
    """Assert the on_connection_create driver-feature hook fires for async pooled/direct connections."""
    hook_calls = 0

    async def hook(connection: object, *_args: object) -> None:
        # *_args absorbs adapter-specific extra positionals (oracledb passes (connection, tag)).
        nonlocal hook_calls
        hook_calls += 1

    config = make_config(driver_features={"on_connection_create": hook})
    try:
        async with config.provide_session() as session:
            await session.execute(_connection_probe_sql(case))
        assert hook_calls >= 1, f"{case.adapter} on_connection_create hook should fire at least once"
    finally:
        await config.close_pool()


def assert_sync_pooling_contract(make_config: SyncConfigFactory, case: DriverCase) -> None:
    """Assert sync pooled configs share data across sessions drawn from the same pool."""
    config = make_config(pooled=True)
    table = _pool_contract_table(case)
    try:
        with config.provide_session() as session:
            session.execute_script(case.table.pooling_create_sql.format(table=table))
            session.execute(f"INSERT INTO {table} (id, value) VALUES (1, 'shared')")
            session.commit()
        with config.provide_session() as session:
            assert session.select_value(f"SELECT value FROM {table} WHERE id = 1") == "shared"
            session.execute_script(f"DROP TABLE {table}")
            session.commit()
    finally:
        config.close_pool()


async def assert_async_pooling_contract(make_config: AsyncConfigFactory, case: DriverCase) -> None:
    """Assert async pooled configs share data across sessions drawn from the same pool."""
    config = make_config(pooled=True)
    table = _pool_contract_table(case)
    try:
        async with config.provide_session() as session:
            await session.execute_script(case.table.pooling_create_sql.format(table=table))
            await session.execute(f"INSERT INTO {table} (id, value) VALUES (1, 'shared')")
            await session.commit()
        async with config.provide_session() as session:
            assert await session.select_value(f"SELECT value FROM {table} WHERE id = 1") == "shared"
            await session.execute_script(f"DROP TABLE {table}")
            await session.commit()
    finally:
        await config.close_pool()


def assert_sync_connection_instance_contract(make_config: SyncConfigFactory, case: DriverCase) -> None:
    """Assert an injected connection_instance pool is honored end-to-end; None builds a fresh pool."""
    source = make_config(pooled=True)
    injected_pool = source.provide_pool()
    table = _pool_contract_table(case)
    try:
        config = make_config(pooled=True, connection_instance=injected_pool)
        assert config.connection_instance is injected_pool
        assert config.provide_pool() is injected_pool
        with config.provide_session() as session:
            session.execute_script(case.table.pooling_create_sql.format(table=table))
            session.execute(f"INSERT INTO {table} (id, value) VALUES (1, 'injected')")
            session.commit()
        with config.provide_session() as session:
            assert session.select_value(f"SELECT value FROM {table} WHERE id = 1") == "injected"
            session.execute_script(f"DROP TABLE {table}")
            session.commit()
    finally:
        source.close_pool()

    fresh = make_config(pooled=True, connection_instance=None)
    fresh_instance = fresh.connection_instance
    assert fresh_instance is None
    try:
        assert fresh.provide_pool() is not None
        with fresh.provide_session() as session:
            assert session.select_value("SELECT 1") == 1
    finally:
        fresh.close_pool()


async def assert_async_connection_instance_contract(make_config: AsyncConfigFactory, case: DriverCase) -> None:
    """Assert an injected connection_instance pool is honored end-to-end; None builds a fresh pool."""
    source = make_config(pooled=True)
    injected_pool = await source.provide_pool()
    table = _pool_contract_table(case)
    try:
        config = make_config(pooled=True, connection_instance=injected_pool)
        assert config.connection_instance is injected_pool
        assert await config.provide_pool() is injected_pool
        async with config.provide_session() as session:
            await session.execute_script(case.table.pooling_create_sql.format(table=table))
            await session.execute(f"INSERT INTO {table} (id, value) VALUES (1, 'injected')")
            await session.commit()
        async with config.provide_session() as session:
            assert await session.select_value(f"SELECT value FROM {table} WHERE id = 1") == "injected"
            await session.execute_script(f"DROP TABLE {table}")
            await session.commit()
    finally:
        await source.close_pool()

    fresh = make_config(pooled=True, connection_instance=None)
    fresh_instance = fresh.connection_instance
    assert fresh_instance is None
    try:
        assert await fresh.provide_pool() is not None
        async with fresh.provide_session() as session:
            assert await session.select_value("SELECT 1") == 1
    finally:
        await fresh.close_pool()


class _ColumnCaseRow(msgspec.Struct):
    """Lowercase-field struct used to probe Oracle implicit column-name casing."""

    id: int
    name: str


def _column_case_table(case: DriverCase) -> str:
    return f"colcase_{case.adapter}_{case.mode}"


def assert_sync_lowercase_columns_contract(make_config: SyncConfigFactory, case: DriverCase) -> None:
    """Assert Oracle hydrates implicit columns lowercase by default and uppercase when the feature is disabled."""
    table = _column_case_table(case)
    create = f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(50))"

    default_config = make_config()
    try:
        with default_config.provide_session() as session:
            session.execute_script(_oracle_drop_sql("TABLE", table))
            session.execute_script(create)
            session.execute(f"INSERT INTO {table} (id, name) VALUES (:1, :2)", (1, "widget"))
            result = session.execute(f"SELECT id, name FROM {table}")
            row = result.get_first()
            assert row is not None
            assert "id" in row and "ID" not in row
            hydrated = result.get_first(schema_type=_ColumnCaseRow)
            assert hydrated is not None
            assert hydrated.id == 1
            assert hydrated.name == "widget"
            session.execute_script(_oracle_drop_sql("TABLE", table))
    finally:
        default_config.close_pool()

    disabled_config = make_config(driver_features={"enable_lowercase_column_names": False})
    try:
        with disabled_config.provide_session() as session:
            session.execute_script(_oracle_drop_sql("TABLE", table))
            session.execute_script(create)
            session.execute(f"INSERT INTO {table} (id, name) VALUES (:1, :2)", (1, "widget"))
            result = session.execute(f"SELECT id, name FROM {table}")
            row = result.get_first()
            assert row is not None
            assert "ID" in row and "id" not in row
            with pytest.raises(msgspec.ValidationError):
                result.get_first(schema_type=_ColumnCaseRow)
            session.execute_script(_oracle_drop_sql("TABLE", table))
    finally:
        disabled_config.close_pool()


async def assert_async_lowercase_columns_contract(make_config: AsyncConfigFactory, case: DriverCase) -> None:
    """Async mirror of assert_sync_lowercase_columns_contract."""
    table = _column_case_table(case)
    create = f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(50))"

    default_config = make_config()
    try:
        async with default_config.provide_session() as session:
            await session.execute_script(_oracle_drop_sql("TABLE", table))
            await session.execute_script(create)
            await session.execute(f"INSERT INTO {table} (id, name) VALUES (:1, :2)", (1, "widget"))
            result = await session.execute(f"SELECT id, name FROM {table}")
            row = result.get_first()
            assert row is not None
            assert "id" in row and "ID" not in row
            hydrated = result.get_first(schema_type=_ColumnCaseRow)
            assert hydrated is not None
            assert hydrated.id == 1
            assert hydrated.name == "widget"
            await session.execute_script(_oracle_drop_sql("TABLE", table))
    finally:
        await default_config.close_pool()

    disabled_config = make_config(driver_features={"enable_lowercase_column_names": False})
    try:
        async with disabled_config.provide_session() as session:
            await session.execute_script(_oracle_drop_sql("TABLE", table))
            await session.execute_script(create)
            await session.execute(f"INSERT INTO {table} (id, name) VALUES (:1, :2)", (1, "widget"))
            result = await session.execute(f"SELECT id, name FROM {table}")
            row = result.get_first()
            assert row is not None
            assert "ID" in row and "id" not in row
            with pytest.raises(msgspec.ValidationError):
                result.get_first(schema_type=_ColumnCaseRow)
            await session.execute_script(_oracle_drop_sql("TABLE", table))
    finally:
        await disabled_config.close_pool()


def _oracle_uuid_variants() -> "list[tuple[int, UUID]]":
    return [(10, uuid1()), (11, uuid4()), (12, uuid5(NAMESPACE_DNS, "example.com"))]


def _oracle_uuid_sync(make_config: SyncConfigFactory, case: DriverCase) -> None:
    table = _pc_table(case, "uuid")
    create = f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, uuid_col RAW(16))"

    enabled = make_config(driver_features={"enable_uuid_binary": True})
    try:
        with enabled.provide_session() as session:
            session.execute_script(_oracle_drop_sql("TABLE", table))
            session.execute_script(create)
            value = uuid4()
            session.execute(f"INSERT INTO {table} (id, uuid_col) VALUES (:1, :2)", (1, value))
            session.execute(f"INSERT INTO {table} (id, uuid_col) VALUES (:1, :2)", (2, None))
            row = session.select_one(f"SELECT uuid_col FROM {table} WHERE id = :1", (1,))
            assert isinstance(row["uuid_col"], UUID)
            assert row["uuid_col"] == value
            null_row = session.select_one(f"SELECT uuid_col FROM {table} WHERE id = :1", (2,))
            assert null_row["uuid_col"] is None
            # variants (v1/v4/v5) + executemany bulk binding
            batch = _oracle_uuid_variants() + [(i, uuid4()) for i in range(100, 150)]
            session.execute_many(f"INSERT INTO {table} (id, uuid_col) VALUES (:1, :2)", batch)
            stored = {
                r["id"]: r["uuid_col"]
                for r in session.execute(f"SELECT id, uuid_col FROM {table} WHERE id >= 10 ORDER BY id").get_data()
            }
            for rid, expected in batch:
                assert isinstance(stored[rid], UUID)
                assert stored[rid] == expected
            assert session.select_value(f"SELECT COUNT(*) FROM {table} WHERE id >= 100") == 50
            session.execute_script(_oracle_drop_sql("TABLE", table))
    finally:
        enabled.close_pool()

    disabled = make_config(driver_features={"enable_uuid_binary": False})
    try:
        with disabled.provide_session() as session:
            session.execute_script(_oracle_drop_sql("TABLE", table))
            session.execute_script(create)
            value = uuid4()
            session.execute(f"INSERT INTO {table} (id, uuid_col) VALUES (:1, :2)", (1, value.bytes))
            row = session.select_one(f"SELECT uuid_col FROM {table} WHERE id = :1", (1,))
            assert isinstance(row["uuid_col"], bytes)
            assert row["uuid_col"] == value.bytes
            session.execute_script(_oracle_drop_sql("TABLE", table))
    finally:
        disabled.close_pool()


async def _oracle_uuid_async(make_config: AsyncConfigFactory, case: DriverCase) -> None:
    table = _pc_table(case, "uuid")
    create = f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, uuid_col RAW(16))"

    enabled = make_config(driver_features={"enable_uuid_binary": True})
    try:
        async with enabled.provide_session() as session:
            await session.execute_script(_oracle_drop_sql("TABLE", table))
            await session.execute_script(create)
            value = uuid4()
            await session.execute(f"INSERT INTO {table} (id, uuid_col) VALUES (:1, :2)", (1, value))
            await session.execute(f"INSERT INTO {table} (id, uuid_col) VALUES (:1, :2)", (2, None))
            row = await session.select_one(f"SELECT uuid_col FROM {table} WHERE id = :1", (1,))
            assert isinstance(row["uuid_col"], UUID)
            assert row["uuid_col"] == value
            null_row = await session.select_one(f"SELECT uuid_col FROM {table} WHERE id = :1", (2,))
            assert null_row["uuid_col"] is None
            batch = _oracle_uuid_variants() + [(i, uuid4()) for i in range(100, 150)]
            await session.execute_many(f"INSERT INTO {table} (id, uuid_col) VALUES (:1, :2)", batch)
            stored = {
                r["id"]: r["uuid_col"]
                for r in (
                    await session.execute(f"SELECT id, uuid_col FROM {table} WHERE id >= 10 ORDER BY id")
                ).get_data()
            }
            for rid, expected in batch:
                assert isinstance(stored[rid], UUID)
                assert stored[rid] == expected
            assert await session.select_value(f"SELECT COUNT(*) FROM {table} WHERE id >= 100") == 50
            await session.execute_script(_oracle_drop_sql("TABLE", table))
    finally:
        await enabled.close_pool()

    disabled = make_config(driver_features={"enable_uuid_binary": False})
    try:
        async with disabled.provide_session() as session:
            await session.execute_script(_oracle_drop_sql("TABLE", table))
            await session.execute_script(create)
            value = uuid4()
            await session.execute(f"INSERT INTO {table} (id, uuid_col) VALUES (:1, :2)", (1, value.bytes))
            row = await session.select_one(f"SELECT uuid_col FROM {table} WHERE id = :1", (1,))
            assert isinstance(row["uuid_col"], bytes)
            assert row["uuid_col"] == value.bytes
            await session.execute_script(_oracle_drop_sql("TABLE", table))
    finally:
        await disabled.close_pool()


_DUCKDB_UUID_STR = "550e8400-e29b-41d4-a716-446655440000"


def _duckdb_uuid_sync(make_config: SyncConfigFactory, case: DriverCase) -> None:
    table = _pc_table(case, "uuid")
    create = f"CREATE TABLE {table} (id UUID)"

    default = make_config()
    try:
        with default.provide_session() as session:
            session.execute_script(f"DROP TABLE IF EXISTS {table}")
            session.execute_script(create)
            session.execute(f"INSERT INTO {table} (id) VALUES (?)", (_DUCKDB_UUID_STR,))
            row = session.select_one(f"SELECT id FROM {table}")
            assert isinstance(row["id"], UUID)
            assert str(row["id"]) == _DUCKDB_UUID_STR
            session.execute_script(f"DROP TABLE IF EXISTS {table}")
    finally:
        default.close_pool()

    # DuckDB returns native UUID objects from UUID columns even when input conversion is disabled.
    disabled = make_config(driver_features={"enable_uuid_conversion": False})
    try:
        with disabled.provide_session() as session:
            session.execute_script(f"DROP TABLE IF EXISTS {table}")
            session.execute_script(create)
            session.execute(f"INSERT INTO {table} (id) VALUES (?)", (_DUCKDB_UUID_STR,))
            row = session.select_one(f"SELECT id FROM {table}")
            assert isinstance(row["id"], UUID)
            session.execute_script(f"DROP TABLE IF EXISTS {table}")
    finally:
        disabled.close_pool()


def assert_sync_uuid_feature_contract(make_config: SyncConfigFactory, case: DriverCase) -> None:
    """Assert the UUID driver feature: enabled binds/returns uuid.UUID; disabled returns the raw form."""
    if case.dialect == "oracle":
        _oracle_uuid_sync(make_config, case)
    elif case.dialect == "duckdb":
        _duckdb_uuid_sync(make_config, case)
    else:
        pytest.skip(f"{case.adapter} has no UUID-feature contract mapping")


async def assert_async_uuid_feature_contract(make_config: AsyncConfigFactory, case: DriverCase) -> None:
    """Async UUID driver-feature contract (oracle only; duckdb has no async driver)."""
    if case.dialect == "oracle":
        await _oracle_uuid_async(make_config, case)
    else:
        pytest.skip(f"{case.adapter} has no async UUID-feature contract mapping")


def _marking_deserializer(value: "str | bytes") -> object:
    decoded = from_json(value)
    if isinstance(decoded, dict):
        decoded["extra_marker"] = True
    return decoded


def _assert_json_serializer_read(case: DriverCase, stored: object, payload: "dict[str, str]") -> None:
    """DuckDB returns the serialized JSON string; sqlite/mysql deserialize (custom deserializer adds a marker)."""
    if case.dialect == "duckdb":
        assert stored == to_json(payload)
    else:
        assert isinstance(stored, dict)
        assert stored["foo"] == "bar"
        assert stored["extra_marker"] is True


def assert_sync_custom_json_serializer_contract(make_config: SyncConfigFactory, case: DriverCase) -> None:
    """Assert a custom json_serializer driver feature is invoked when binding a dict to a JSON column."""
    payloads: list[object] = []

    def tracking_serializer(value: object) -> str:
        payloads.append(value)
        return to_json(value)

    features: dict[str, Any] = {"json_serializer": tracking_serializer, "json_deserializer": _marking_deserializer}
    if case.dialect == "sqlite":
        import sqlite3

        features["enable_custom_adapters"] = True
        config = make_config(driver_features=features, connection_overrides={"detect_types": sqlite3.PARSE_DECLTYPES})
    else:
        config = make_config(driver_features=features)

    table = _pc_table(case, "jsonser")
    payload = {"foo": "bar"}
    try:
        with config.provide_session() as session:
            session.execute_script(f"DROP TABLE IF EXISTS {table}")
            session.execute_script(f"CREATE TABLE {table} (data JSON)")
            session.execute(f"INSERT INTO {table} (data) VALUES (?)", (payload,))
            session.commit()
            assert payloads
            assert payloads[0] == payload
            stored = session.execute(f"SELECT data FROM {table}").get_data()[0]["data"]
            _assert_json_serializer_read(case, stored, payload)
            session.execute_script(f"DROP TABLE IF EXISTS {table}")
    finally:
        config.close_pool()


async def assert_async_custom_json_serializer_contract(make_config: AsyncConfigFactory, case: DriverCase) -> None:
    """Async mirror of assert_sync_custom_json_serializer_contract (mysql adapters)."""
    payloads: list[object] = []

    def tracking_serializer(value: object) -> str:
        payloads.append(value)
        return to_json(value)

    features: dict[str, Any] = {"json_serializer": tracking_serializer, "json_deserializer": _marking_deserializer}
    config = make_config(driver_features=features)

    table = _pc_table(case, "jsonser")
    payload = {"foo": "bar"}
    try:
        async with config.provide_session() as session:
            await session.execute_script(f"DROP TABLE IF EXISTS {table}")
            await session.execute_script(f"CREATE TABLE {table} (data JSON)")
            await session.execute(f"INSERT INTO {table} (data) VALUES (?)", (payload,))
            await session.commit()
            assert payloads
            assert payloads[0] == payload
            stored = (await session.execute(f"SELECT data FROM {table}")).get_data()[0]["data"]
            _assert_json_serializer_read(case, stored, payload)
            await session.execute_script(f"DROP TABLE IF EXISTS {table}")
    finally:
        await config.close_pool()


def assert_sync_custom_type_adapters_contract(make_config: SyncConfigFactory, case: DriverCase) -> None:
    """Assert the sqlite enable_custom_adapters feature hydrates JSON columns to dict/list (str without it)."""
    import sqlite3

    table = _pc_table(case, "adapters")
    test_dict = {"key": "value", "count": 42}
    test_list = [1, 2, 3, "four"]

    enabled = make_config(
        driver_features={"enable_custom_adapters": True}, connection_overrides={"detect_types": sqlite3.PARSE_DECLTYPES}
    )
    try:
        with enabled.provide_session() as session:
            session.execute_script(f"DROP TABLE IF EXISTS {table}")
            session.execute(f"CREATE TABLE {table} (id INTEGER, data JSON, items JSON)")
            session.execute(
                f"INSERT INTO {table} (id, data, items) VALUES (?, ?, ?)",
                (1, json.dumps(test_dict), json.dumps(test_list)),
            )
            session.commit()
            row = session.select_one(f"SELECT data, items FROM {table} WHERE id = 1")
            assert row["data"] == test_dict
            assert row["items"] == test_list
    finally:
        enabled.close_pool()

    disabled = make_config()
    try:
        with disabled.provide_session() as session:
            session.execute_script(f"DROP TABLE IF EXISTS {table}")
            session.execute(f"CREATE TABLE {table} (id INTEGER, data TEXT)")
            session.execute(f"INSERT INTO {table} (id, data) VALUES (?, ?)", (1, json.dumps(test_dict)))
            session.commit()
            row = session.select_one(f"SELECT data FROM {table} WHERE id = 1")
            assert isinstance(row["data"], str)
            assert json.loads(row["data"]) == test_dict
    finally:
        disabled.close_pool()


async def assert_async_custom_type_adapters_contract(make_config: AsyncConfigFactory, case: DriverCase) -> None:
    """Async mirror of the sqlite custom-adapter enable-false semantics contract."""
    import sqlite3

    table = _pc_table(case, "adapters")
    test_dict = {"key": "value", "count": 42}
    test_list = [1, 2, 3, "four"]

    enabled = make_config(
        driver_features={"enable_custom_adapters": True}, connection_overrides={"detect_types": sqlite3.PARSE_DECLTYPES}
    )
    try:
        async with enabled.provide_session() as session:
            await session.execute_script(f"DROP TABLE IF EXISTS {table}")
            await session.execute(f"CREATE TABLE {table} (id INTEGER, data JSON, items JSON)")
            await session.execute(
                f"INSERT INTO {table} (id, data, items) VALUES (?, ?, ?)",
                (1, json.dumps(test_dict), json.dumps(test_list)),
            )
            await session.commit()
            row = await session.select_one(f"SELECT data, items FROM {table} WHERE id = 1")
            assert row["data"] == test_dict
            assert row["items"] == test_list
    finally:
        await enabled.close_pool()

    disabled = make_config()
    try:
        async with disabled.provide_session() as session:
            await session.execute_script(f"DROP TABLE IF EXISTS {table}")
            await session.execute(f"CREATE TABLE {table} (id INTEGER, data TEXT)")
            await session.execute(f"INSERT INTO {table} (id, data) VALUES (?, ?)", (1, json.dumps(test_dict)))
            await session.commit()
            row = await session.select_one(f"SELECT data FROM {table} WHERE id = 1")
            assert isinstance(row["data"], str)
            assert json.loads(row["data"]) == test_dict
    finally:
        await disabled.close_pool()


_DRIVER_FEATURE_PARITY_ROWS = (
    ContractRow("feature-a", 1, "note-a"),
    ContractRow("feature-b", 2, None),
    ContractRow("feature-c", 3, "note-c"),
)


def _expected_feature_rows(rows: tuple[ContractRow, ...] = _DRIVER_FEATURE_PARITY_ROWS) -> list[dict[str, object]]:
    return [{"name": row.name, "value": row.value, "note": row.note} for row in rows]


def _assert_row_format_matches_data(result: SQLResult, expected_rows: list[dict[str, object]]) -> None:
    assert result.get_data() == expected_rows
    assert result.data is not None
    if not result.data:
        return
    row_format = result._row_format  # pyright: ignore[reportPrivateUsage]
    first = result.data[0]
    if isinstance(first, dict):
        assert row_format == "dict"
        return
    if isinstance(first, Mapping):
        assert row_format in {"dict", "record"}
        return
    if hasattr(first, "keys"):
        assert row_format == "record"
        return
    assert row_format == "tuple"


def _reset_contract_table_sync(driver: SyncContractDriver, table: ContractTable) -> None:
    with contextlib.suppress(Exception):
        driver.execute_script(f"DROP TABLE IF EXISTS {table.name}")
    driver.execute_script(table.create_sql)
    driver.commit()


async def _reset_contract_table_async(driver: AsyncContractDriver, table: ContractTable) -> None:
    with contextlib.suppress(Exception):
        await driver.execute_script(f"DROP TABLE IF EXISTS {table.name}")
    await driver.execute_script(table.create_sql)
    await driver.commit()


def _row_format_config_kwargs(case: DriverCase) -> dict[str, Any]:
    if case.adapter == "psycopg":
        from psycopg.rows import dict_row

        return {"connection_overrides": {"row_factory": dict_row}}
    if case.adapter == "sqlite":
        return {"driver_features": {"row_factory": "dict"}}
    return {}


def _async_row_format_config_kwargs(case: DriverCase) -> dict[str, Any]:
    if case.adapter == "psycopg":
        from psycopg.rows import dict_row

        return {"connection_overrides": {"row_factory": dict_row}}
    if case.adapter == "aiosqlite":
        return {"driver_features": {"row_factory": "dict"}}
    return {}


def assert_sync_driver_feature_parity_contract(driver: object, case: DriverCase) -> None:
    """Assert buffered, native row-stream, and Arrow rows agree for one canonical fixture."""
    if not (case.supports_native_row_streaming or case.supports_arrow):
        pytest.skip(f"{case.adapter} has no row-stream or Arrow feature path")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    sync_driver.execute(table.delete_sql)
    sync_driver.commit()
    _seed_sync(sync_driver, _DRIVER_FEATURE_PARITY_ROWS, table, case)
    expected = _expected_feature_rows()

    buffered = sync_driver.execute(table.select_ordered_sql).get_data()
    assert buffered == expected

    if case.supports_native_row_streaming:
        with sync_driver.select_stream(table.select_ordered_sql, chunk_size=1) as stream:
            assert list(stream) == expected

    if case.supports_arrow:
        assert sync_driver.select_to_arrow(table.select_ordered_sql).data.to_pylist() == expected


async def assert_async_driver_feature_parity_contract(driver: object, case: DriverCase) -> None:
    """Async mirror of the buffered/native-stream/Arrow parity contract."""
    if not (case.supports_native_row_streaming or case.supports_arrow):
        pytest.skip(f"{case.adapter} has no row-stream or Arrow feature path")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    await async_driver.execute(table.delete_sql)
    await async_driver.commit()
    await _seed_async(async_driver, _DRIVER_FEATURE_PARITY_ROWS, table, case)
    expected = _expected_feature_rows()

    buffered = (await async_driver.execute(table.select_ordered_sql)).get_data()
    assert buffered == expected

    if case.supports_native_row_streaming:
        async with async_driver.select_stream(table.select_ordered_sql, chunk_size=1) as stream:
            assert [row async for row in stream] == expected

    if case.supports_arrow:
        assert (await async_driver.select_to_arrow(table.select_ordered_sql)).data.to_pylist() == expected


def assert_sync_driver_feature_row_format_contract(make_config: SyncConfigFactory | None, case: DriverCase) -> None:
    """Assert configured row factories are tagged consistently with their materialized data."""
    if make_config is None:
        pytest.skip(f"{case.adapter} has no config factory for row-format verification")
    config = make_config(**_row_format_config_kwargs(case))
    try:
        with config.provide_session() as session:
            sync_driver = session
            _reset_contract_table_sync(sync_driver, case.table)
            _seed_sync(sync_driver, _DRIVER_FEATURE_PARITY_ROWS, case.table, case)
            result = assert_sql_result(sync_driver.execute(case.table.select_ordered_sql))
            _assert_row_format_matches_data(result, _expected_feature_rows())
    finally:
        config.close_pool()


async def assert_async_driver_feature_row_format_contract(
    make_config: AsyncConfigFactory | None, case: DriverCase
) -> None:
    """Async mirror of the configured row-factory row-format contract."""
    if make_config is None:
        pytest.skip(f"{case.adapter} has no config factory for row-format verification")
    config = make_config(**_async_row_format_config_kwargs(case))
    try:
        async with config.provide_session() as session:
            async_driver = session
            await _reset_contract_table_async(async_driver, case.table)
            await _seed_async(async_driver, _DRIVER_FEATURE_PARITY_ROWS, case.table, case)
            result = assert_sql_result(await async_driver.execute(case.table.select_ordered_sql))
            _assert_row_format_matches_data(result, _expected_feature_rows())
    finally:
        await config.close_pool()


def assert_sync_statement_input_contract(driver: object, case: DriverCase, input_case: StatementInputCase) -> None:
    """Assert sync drivers return equivalent rows for one statement input shape."""
    sync_driver = cast("SyncContractDriver", driver)
    _seed_sync(sync_driver, input_case.setup_rows, case.table, case)

    statement = input_case.statement_factory(case.table.name, _sqlglot_dialect(case))
    result = _execute_sync(sync_driver, statement, input_case.parameters)
    assert_result_data(result, input_case.expected_data)

    repeat_result = _execute_sync(sync_driver, statement, input_case.parameters)
    assert_result_data(repeat_result, input_case.expected_data)


async def assert_async_statement_input_contract(
    driver: object, case: DriverCase, input_case: StatementInputCase
) -> None:
    """Assert async drivers return equivalent rows for one statement input shape."""
    async_driver = cast("AsyncContractDriver", driver)
    await _seed_async(async_driver, input_case.setup_rows, case.table, case)

    statement = input_case.statement_factory(case.table.name, _sqlglot_dialect(case))
    result = await _execute_async(async_driver, statement, input_case.parameters)
    assert_result_data(result, input_case.expected_data)

    repeat_result = await _execute_async(async_driver, statement, input_case.parameters)
    assert_result_data(repeat_result, input_case.expected_data)


def assert_sync_parameter_contract(driver: object, case: DriverCase, parameter_case: ParameterProfileCase) -> None:
    """Assert sync drivers bind one parameter profile case correctly."""
    sync_driver = cast("SyncContractDriver", driver)
    _seed_sync(sync_driver, parameter_case.setup_rows, case.table, case)

    result = _execute_sync(sync_driver, _with_table(parameter_case.statement, case.table), parameter_case.parameters)
    if parameter_case.expected_rows_affected is not None and _reports_execute_rows_affected(case):
        assert_sql_result(result, rows_affected=parameter_case.expected_rows_affected)
    if parameter_case.expected_result_data is not None:
        assert_result_data(result, parameter_case.expected_result_data)
    if parameter_case.verification_statement is not None and parameter_case.expected_verification_data is not None:
        verification = _execute_sync(
            sync_driver,
            _with_table(parameter_case.verification_statement, case.table),
            parameter_case.verification_parameters,
        )
        assert_result_data(verification, parameter_case.expected_verification_data)


async def assert_async_parameter_contract(
    driver: object, case: DriverCase, parameter_case: ParameterProfileCase
) -> None:
    """Assert async drivers bind one parameter profile case correctly."""
    async_driver = cast("AsyncContractDriver", driver)
    await _seed_async(async_driver, parameter_case.setup_rows, case.table, case)

    result = await _execute_async(
        async_driver, _with_table(parameter_case.statement, case.table), parameter_case.parameters
    )
    if parameter_case.expected_rows_affected is not None and _reports_execute_rows_affected(case):
        assert_sql_result(result, rows_affected=parameter_case.expected_rows_affected)
    if parameter_case.expected_result_data is not None:
        assert_result_data(result, parameter_case.expected_result_data)
    if parameter_case.verification_statement is not None and parameter_case.expected_verification_data is not None:
        verification = await _execute_async(
            async_driver,
            _with_table(parameter_case.verification_statement, case.table),
            parameter_case.verification_parameters,
        )
        assert_result_data(verification, parameter_case.expected_verification_data)


def assert_sync_parameter_style_contract(
    driver: object, case: DriverCase, parameter_style_case: ParameterStyleCase
) -> None:
    """Assert sync drivers bind one parameter style case correctly."""
    if parameter_style_case.method == "execute_many" and not case.supports_execute_many:
        pytest.skip(f"{case.adapter} has no verified execute_many support")
    sync_driver = cast("SyncContractDriver", driver)
    _seed_sync(sync_driver, parameter_style_case.setup_rows, case.table, case)

    style_statement = _with_table(parameter_style_case.statement, case.table)
    if parameter_style_case.method == "execute_many":
        result = sync_driver.execute_many(style_statement, parameter_style_case.parameters)
    else:
        result = _execute_sync(sync_driver, style_statement, parameter_style_case.parameters)

    if parameter_style_case.expected_rows_affected is not None and _reports_execute_rows_affected(
        case, parameter_style_case.method
    ):
        assert_sql_result(result, rows_affected=parameter_style_case.expected_rows_affected)
    if parameter_style_case.expected_result_data is not None:
        assert_result_data(result, parameter_style_case.expected_result_data)
    if (
        parameter_style_case.verification_statement is not None
        and parameter_style_case.expected_verification_data is not None
    ):
        verification = _execute_sync(
            sync_driver,
            _with_table(parameter_style_case.verification_statement, case.table),
            parameter_style_case.verification_parameters,
        )
        assert_result_data(verification, parameter_style_case.expected_verification_data)


async def assert_async_parameter_style_contract(
    driver: object, case: DriverCase, parameter_style_case: ParameterStyleCase
) -> None:
    """Assert async drivers bind one parameter style case correctly."""
    if parameter_style_case.method == "execute_many" and not case.supports_execute_many:
        pytest.skip(f"{case.adapter} has no verified execute_many support")
    async_driver = cast("AsyncContractDriver", driver)
    await _seed_async(async_driver, parameter_style_case.setup_rows, case.table, case)

    style_statement = _with_table(parameter_style_case.statement, case.table)
    if parameter_style_case.method == "execute_many":
        result = await async_driver.execute_many(style_statement, parameter_style_case.parameters)
    else:
        result = await _execute_async(async_driver, style_statement, parameter_style_case.parameters)

    if parameter_style_case.expected_rows_affected is not None and _reports_execute_rows_affected(
        case, parameter_style_case.method
    ):
        assert_sql_result(result, rows_affected=parameter_style_case.expected_rows_affected)
    if parameter_style_case.expected_result_data is not None:
        assert_result_data(result, parameter_style_case.expected_result_data)
    if (
        parameter_style_case.verification_statement is not None
        and parameter_style_case.expected_verification_data is not None
    ):
        verification = await _execute_async(
            async_driver,
            _with_table(parameter_style_case.verification_statement, case.table),
            parameter_style_case.verification_parameters,
        )
        assert_result_data(verification, parameter_style_case.expected_verification_data)


def assert_sync_result_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers expose common result helper behavior."""
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    _seed_sync(
        sync_driver, (ContractRow("result1", 10), ContractRow("result2", 20), ContractRow("result3", 30)), table, case
    )

    result = assert_sql_result(sync_driver.execute(table.select_ordered_sql))
    assert result.get_first() == {"name": "result1", "value": 10, "note": None}
    assert result.get_count() == 3
    assert not result.is_empty()
    assert sync_driver.select_value(table.select_count_sql) == 3
    assert sync_driver.select_one(table.select_by_name_qmark_sql, ("result2",)) == {
        "name": "result2",
        "value": 20,
        "note": None,
    }
    assert sync_driver.select_one_or_none(table.select_by_name_qmark_sql, ("missing",)) is None

    empty_result = assert_sql_result(sync_driver.execute(table.select_by_name_qmark_sql, ("missing",)))
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


async def assert_async_result_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers expose common result helper behavior."""
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    await _seed_async(
        async_driver, (ContractRow("result1", 10), ContractRow("result2", 20), ContractRow("result3", 30)), table, case
    )

    result = assert_sql_result(await async_driver.execute(table.select_ordered_sql))
    assert result.get_first() == {"name": "result1", "value": 10, "note": None}
    assert result.get_count() == 3
    assert not result.is_empty()
    assert await async_driver.select_value(table.select_count_sql) == 3
    assert await async_driver.select_one(table.select_by_name_qmark_sql, ("result2",)) == {
        "name": "result2",
        "value": 20,
        "note": None,
    }
    assert await async_driver.select_one_or_none(table.select_by_name_qmark_sql, ("missing",)) is None

    empty_result = assert_sql_result(await async_driver.execute(table.select_by_name_qmark_sql, ("missing",)))
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


def assert_sync_script_error_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers execute scripts and normalize generic SQL errors."""
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table.name

    result = sync_driver.execute_script(f"""
        INSERT INTO {table} (name, value) VALUES ('script1', 10);
        INSERT INTO {table} (name, value) VALUES ('script2', 20);
        UPDATE {table} SET value = 30 WHERE name = 'script1';
    """)
    assert_sql_result(result)
    assert result.operation_type == "SCRIPT"
    assert_result_data(
        sync_driver.execute(f"SELECT name, value FROM {table} WHERE name LIKE 'script%' ORDER BY name"),
        ({"name": "script1", "value": 30}, {"name": "script2", "value": 20}),
    )

    if case.invalid_sql_error_policy == "raises":
        with pytest.raises(SQLParsingError):
            sync_driver.execute(f"SELCT * FROM {table}")
        with pytest.raises(SQLSpecError):
            sync_driver.execute("SELECT * FROM missing_contract_table")


async def assert_async_script_error_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers execute scripts and normalize generic SQL errors."""
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table.name

    result = await async_driver.execute_script(f"""
        INSERT INTO {table} (name, value) VALUES ('script1', 10);
        INSERT INTO {table} (name, value) VALUES ('script2', 20);
        UPDATE {table} SET value = 30 WHERE name = 'script1';
    """)
    assert_sql_result(result)
    assert result.operation_type == "SCRIPT"
    assert_result_data(
        await async_driver.execute(f"SELECT name, value FROM {table} WHERE name LIKE 'script%' ORDER BY name"),
        ({"name": "script1", "value": 30}, {"name": "script2", "value": 20}),
    )

    if case.invalid_sql_error_policy == "raises":
        with pytest.raises(SQLParsingError):
            await async_driver.execute(f"SELCT * FROM {table}")
        with pytest.raises(SQLSpecError):
            await async_driver.execute("SELECT * FROM missing_contract_table")


def _explain_skip_reason(case: DriverCase) -> str:
    if case.unsupported_explain_reason is not None:
        return case.unsupported_explain_reason
    return f"{case.adapter} ({case.dialect}) has no verified EXPLAIN support"


def assert_sync_explain_contract(driver: object, case: DriverCase, explain_case: ExplainCase) -> None:
    """Assert sync drivers execute one EXPLAIN artifact and return plan rows."""
    if not case.supports_explain:
        pytest.skip(_explain_skip_reason(case))
    sync_driver = cast("SyncContractDriver", driver)
    result = assert_sql_result(sync_driver.execute(explain_case.build(case.table, _sqlglot_dialect(case))))
    assert result.data is not None


async def assert_async_explain_contract(driver: object, case: DriverCase, explain_case: ExplainCase) -> None:
    """Assert async drivers execute one EXPLAIN artifact and return plan rows."""
    if not case.supports_explain:
        pytest.skip(_explain_skip_reason(case))
    async_driver = cast("AsyncContractDriver", driver)
    result = assert_sql_result(await async_driver.execute(explain_case.build(case.table, _sqlglot_dialect(case))))
    assert result.data is not None


EXPLAIN_MODIFIERS_SCOPE = "explain_modifiers"


def _explain_select_sql(table: ContractTable) -> str:
    return f"SELECT name, value FROM {table.name}"


def _postgres_explain_modifier_artifacts(table: ContractTable) -> "list[object]":
    base = _explain_select_sql(table)
    return [
        Explain(base, dialect="postgres").analyze().build(),
        Explain(base, dialect="postgres").format("json").build(),
        Explain(base, dialect="postgres").verbose().build(),
        Explain(base, dialect="postgres").analyze().buffers().build(),
        Explain(base, dialect="postgres").analyze().timing().build(),
        Explain(base, dialect="postgres").analyze().verbose().buffers().timing().format("json").build(),
        Explain(base, dialect="postgres").costs(False).build(),
        Explain(base, dialect="postgres").analyze().summary().build(),
    ]


def _mysql_explain_modifier_artifacts(table: ContractTable) -> "list[object]":
    base = _explain_select_sql(table)
    return [
        Explain(base, dialect="mysql").analyze().build(),
        Explain(base, dialect="mysql").format("json").build(),
        Explain(base, dialect="mysql").format("tree").build(),
        Explain(base, dialect="mysql").format("traditional").build(),
    ]


def _duckdb_explain_modifier_artifacts(table: ContractTable) -> "list[object]":
    base = _explain_select_sql(table)
    return [
        Explain(base, dialect="duckdb").analyze().build(),
        Explain(base, dialect="duckdb").format("json").build(),
        Explain(f"SELECT COUNT(*), SUM(value) FROM {table.name} GROUP BY name", dialect="duckdb").build(),
    ]


def _assert_sync_explain_artifacts(driver: object, artifacts: "list[object]") -> None:
    sync_driver = cast("SyncContractDriver", driver)
    for artifact in artifacts:
        result = assert_sql_result(sync_driver.execute(artifact))
        assert result.data is not None


async def _assert_async_explain_artifacts(driver: object, artifacts: "list[object]") -> None:
    async_driver = cast("AsyncContractDriver", driver)
    for artifact in artifacts:
        result = assert_sql_result(await async_driver.execute(artifact))
        assert result.data is not None


def _sync_explain_oracle_display(driver: object, case: DriverCase) -> None:
    """Fold Oracle's two-step EXPLAIN PLAN FOR + DBMS_XPLAN.DISPLAY workflow."""
    sync_driver = cast("SyncContractDriver", driver)
    sync_driver.execute(Explain(_explain_select_sql(case.table), dialect="oracle").build())
    plan = assert_sql_result(sync_driver.execute("SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())"))
    assert plan.data is not None
    assert len(plan.data) > 0


async def _async_explain_oracle_display(driver: object, case: DriverCase) -> None:
    """Fold Oracle's two-step EXPLAIN PLAN FOR + DBMS_XPLAN.DISPLAY workflow (async)."""
    async_driver = cast("AsyncContractDriver", driver)
    await async_driver.execute(Explain(_explain_select_sql(case.table), dialect="oracle").build())
    plan = assert_sql_result(await async_driver.execute("SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())"))
    assert plan.data is not None
    assert len(plan.data) > 0


register_sync_extra_assertion(
    "explain_modifiers:postgres",
    EXPLAIN_MODIFIERS_SCOPE,
    lambda driver, case: _assert_sync_explain_artifacts(driver, _postgres_explain_modifier_artifacts(case.table)),
)
register_async_extra_assertion(
    "explain_modifiers:postgres",
    EXPLAIN_MODIFIERS_SCOPE,
    lambda driver, case: _assert_async_explain_artifacts(driver, _postgres_explain_modifier_artifacts(case.table)),
)
register_sync_extra_assertion(
    "explain_modifiers:mysql",
    EXPLAIN_MODIFIERS_SCOPE,
    lambda driver, case: _assert_sync_explain_artifacts(driver, _mysql_explain_modifier_artifacts(case.table)),
)
register_async_extra_assertion(
    "explain_modifiers:mysql",
    EXPLAIN_MODIFIERS_SCOPE,
    lambda driver, case: _assert_async_explain_artifacts(driver, _mysql_explain_modifier_artifacts(case.table)),
)
register_sync_extra_assertion(
    "explain_modifiers:duckdb",
    EXPLAIN_MODIFIERS_SCOPE,
    lambda driver, case: _assert_sync_explain_artifacts(driver, _duckdb_explain_modifier_artifacts(case.table)),
)
register_sync_extra_assertion("explain_modifiers:oracle", EXPLAIN_MODIFIERS_SCOPE, _sync_explain_oracle_display)
register_async_extra_assertion("explain_modifiers:oracle", EXPLAIN_MODIFIERS_SCOPE, _async_explain_oracle_display)


def assert_sync_explain_modifiers_contract(driver: object, case: DriverCase) -> None:
    """Run an adapter's folded dialect-specific EXPLAIN modifier proofs, if any."""
    dispatch_sync_extra_assertions(driver, case, EXPLAIN_MODIFIERS_SCOPE)


async def assert_async_explain_modifiers_contract(driver: object, case: DriverCase) -> None:
    """Run an adapter's folded dialect-specific EXPLAIN modifier proofs, if any."""
    await dispatch_async_extra_assertions(driver, case, EXPLAIN_MODIFIERS_SCOPE)


def assert_sync_arrow_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers return Arrow tables, batches, filtered, and empty results."""
    if not case.supports_arrow:
        pytest.skip(f"{case.adapter} has no verified Arrow support")
    import pyarrow as pa

    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    _seed_sync(sync_driver, (ContractRow("a", 1), ContractRow("b", 2), ContractRow("c", 3)), table, case)

    table_result = sync_driver.select_to_arrow(table.select_ordered_sql)
    assert isinstance(table_result.data, pa.Table)
    assert table_result.rows_affected == 3
    assert table_result.data.column("name").to_pylist() == ["a", "b", "c"]

    batch_result = sync_driver.select_to_arrow(table.select_ordered_sql, return_format="batch")
    assert isinstance(batch_result.data, pa.RecordBatch)
    assert batch_result.rows_affected == 3

    filtered = sync_driver.select_to_arrow(table.select_by_name_qmark_sql, ("b",))
    assert filtered.rows_affected == 1
    assert filtered.data.column("value").to_pylist() == [2]

    empty = sync_driver.select_to_arrow(table.select_by_name_qmark_sql, ("missing",))
    assert empty.rows_affected == 0

    _assert_sync_arrow_streaming(sync_driver, case, table)
    _assert_sync_arrow_native_only(sync_driver, case, table)


async def assert_async_arrow_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers return Arrow tables, batches, filtered, and empty results."""
    if not case.supports_arrow:
        pytest.skip(f"{case.adapter} has no verified Arrow support")
    import pyarrow as pa

    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    await _seed_async(async_driver, (ContractRow("a", 1), ContractRow("b", 2), ContractRow("c", 3)), table, case)

    table_result = await async_driver.select_to_arrow(table.select_ordered_sql)
    assert isinstance(table_result.data, pa.Table)
    assert table_result.rows_affected == 3
    assert table_result.data.column("name").to_pylist() == ["a", "b", "c"]

    batch_result = await async_driver.select_to_arrow(table.select_ordered_sql, return_format="batch")
    assert isinstance(batch_result.data, pa.RecordBatch)
    assert batch_result.rows_affected == 3

    filtered = await async_driver.select_to_arrow(table.select_by_name_qmark_sql, ("b",))
    assert filtered.rows_affected == 1
    assert filtered.data.column("value").to_pylist() == [2]

    empty = await async_driver.select_to_arrow(table.select_by_name_qmark_sql, ("missing",))
    assert empty.rows_affected == 0

    await _assert_async_arrow_streaming(async_driver, case, table)
    await _assert_async_arrow_native_only(async_driver, case, table)


def _assert_batch_rows(batches: "list[ArrowRecordBatch]", expected_rows: int, *, batch_size: int, exact: bool) -> None:
    batch_sizes = [batch.num_rows for batch in batches]
    assert sum(batch_sizes) == expected_rows
    if exact:
        assert batch_sizes
        assert all(size <= batch_size for size in batch_sizes)
        assert all(size == batch_size for size in batch_sizes[:-1])


def _assert_reader_rows_affected(case: DriverCase, rows_affected: int, expected_rows: int) -> None:
    if case.adapter == "oracledb":
        assert rows_affected == expected_rows
        return
    assert rows_affected == -1


def _assert_sync_arrow_streaming(driver: SyncContractDriver, case: DriverCase, table: ContractTable) -> None:
    """Assert native Arrow streaming formats for adapters that advertise them."""
    if not case.supports_arrow_streaming:
        return
    import pyarrow as pa

    batch_size = 2
    reader_result = driver.select_to_arrow(table.select_ordered_sql, return_format="reader", batch_size=batch_size)
    assert isinstance(reader_result.data, pa.RecordBatchReader)
    _assert_reader_rows_affected(case, reader_result.rows_affected, 3)
    assert reader_result.data.read_all().column("name").to_pylist() == ["a", "b", "c"]

    batches_result = driver.select_to_arrow(table.select_ordered_sql, return_format="batches", batch_size=batch_size)
    assert isinstance(batches_result.data, list)
    _assert_batch_rows(batches_result.data, 3, batch_size=batch_size, exact=case.arrow_reader_honors_batch_size)


async def _assert_async_arrow_streaming(driver: AsyncContractDriver, case: DriverCase, table: ContractTable) -> None:
    """Assert native Arrow streaming formats for async adapters that advertise them."""
    if not case.supports_arrow_streaming:
        return
    import pyarrow as pa

    batch_size = 2
    reader_result = await driver.select_to_arrow(
        table.select_ordered_sql, return_format="reader", batch_size=batch_size
    )
    assert isinstance(reader_result.data, pa.RecordBatchReader)
    _assert_reader_rows_affected(case, reader_result.rows_affected, 3)
    assert reader_result.data.read_all().column("name").to_pylist() == ["a", "b", "c"]

    batches_result = await driver.select_to_arrow(
        table.select_ordered_sql, return_format="batches", batch_size=batch_size
    )
    assert isinstance(batches_result.data, list)
    _assert_batch_rows(batches_result.data, 3, batch_size=batch_size, exact=case.arrow_reader_honors_batch_size)


def _assert_sync_arrow_native_only(driver: SyncContractDriver, case: DriverCase, table: ContractTable) -> None:
    """Assert native_only direction for Arrow-capable sync adapters."""
    from sqlspec.exceptions import ImproperConfigurationError

    if case.supports_native_arrow:
        result = driver.select_to_arrow(table.select_ordered_sql, native_only=True)
        assert result.rows_affected == 3
        return
    with pytest.raises(ImproperConfigurationError):
        driver.select_to_arrow(table.select_ordered_sql, native_only=True)


async def _assert_async_arrow_native_only(driver: AsyncContractDriver, case: DriverCase, table: ContractTable) -> None:
    """Assert native_only direction for Arrow-capable async adapters."""
    from sqlspec.exceptions import ImproperConfigurationError

    if case.supports_native_arrow:
        result = await driver.select_to_arrow(table.select_ordered_sql, native_only=True)
        assert result.rows_affected == 3
        return
    with pytest.raises(ImproperConfigurationError):
        await driver.select_to_arrow(table.select_ordered_sql, native_only=True)


_ARROW_LARGE_ROW_COUNT = 1000


def assert_sync_arrow_extras_contract(driver: object, case: DriverCase) -> None:
    """Assert sync Arrow output preserves NULLs and scales to a large result set."""
    if not case.supports_arrow:
        pytest.skip(f"{case.adapter} has no verified Arrow support")
    import pyarrow as pa

    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    _seed_sync(sync_driver, (ContractRow("a", 1, None), ContractRow("b", 2, "noted")), table, case)
    null_result = sync_driver.select_to_arrow(table.select_ordered_sql)
    assert isinstance(null_result.data, pa.Table)
    assert null_result.data.column("note").to_pylist() == [None, "noted"]

    sync_driver.execute(table.delete_sql)
    sync_driver.commit()
    _seed_sync(sync_driver, tuple(ContractRow(f"n{i}", i) for i in range(1, _ARROW_LARGE_ROW_COUNT + 1)), table, case)
    large_result = sync_driver.select_to_arrow(table.select_ordered_sql)
    assert large_result.rows_affected == _ARROW_LARGE_ROW_COUNT
    assert sum(large_result.data.column("value").to_pylist()) == sum(range(1, _ARROW_LARGE_ROW_COUNT + 1))


async def assert_async_arrow_extras_contract(driver: object, case: DriverCase) -> None:
    """Assert async Arrow output preserves NULLs and scales to a large result set."""
    if not case.supports_arrow:
        pytest.skip(f"{case.adapter} has no verified Arrow support")
    import pyarrow as pa

    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    await _seed_async(async_driver, (ContractRow("a", 1, None), ContractRow("b", 2, "noted")), table, case)
    null_result = await async_driver.select_to_arrow(table.select_ordered_sql)
    assert isinstance(null_result.data, pa.Table)
    assert null_result.data.column("note").to_pylist() == [None, "noted"]

    await async_driver.execute(table.delete_sql)
    await async_driver.commit()
    await _seed_async(
        async_driver, tuple(ContractRow(f"n{i}", i) for i in range(1, _ARROW_LARGE_ROW_COUNT + 1)), table, case
    )
    large_result = await async_driver.select_to_arrow(table.select_ordered_sql)
    assert large_result.rows_affected == _ARROW_LARGE_ROW_COUNT
    assert sum(large_result.data.column("value").to_pylist()) == sum(range(1, _ARROW_LARGE_ROW_COUNT + 1))


def assert_sync_arrow_polars_contract(driver: object, case: DriverCase) -> None:
    """Assert sync Arrow results convert to a Polars DataFrame."""
    if not case.supports_arrow:
        pytest.skip(f"{case.adapter} has no verified Arrow support")
    pytest.importorskip("polars")

    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    _seed_sync(sync_driver, (ContractRow("a", 1), ContractRow("b", 2)), table, case)

    frame = sync_driver.select_to_arrow(table.select_ordered_sql).to_polars()
    assert len(frame) == 2
    assert frame["name"].to_list() == ["a", "b"]


async def assert_async_arrow_polars_contract(driver: object, case: DriverCase) -> None:
    """Assert async Arrow results convert to a Polars DataFrame."""
    if not case.supports_arrow:
        pytest.skip(f"{case.adapter} has no verified Arrow support")
    pytest.importorskip("polars")

    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    await _seed_async(async_driver, (ContractRow("a", 1), ContractRow("b", 2)), table, case)

    frame = (await async_driver.select_to_arrow(table.select_ordered_sql)).to_polars()
    assert len(frame) == 2
    assert frame["name"].to_list() == ["a", "b"]


ARROW_SPECIFICS_SCOPE = "arrow_specifics"


def _arrow_spec_table(case: DriverCase, suffix: str) -> str:
    return f"arrow_spec_{suffix}_{case.adapter}_{case.mode}"


def _sync_drop_table(driver: "SyncContractDriver", name: str) -> None:
    with contextlib.suppress(Exception):
        driver.execute(f"DROP TABLE {name}")
    with contextlib.suppress(Exception):
        driver.commit()


async def _async_drop_table(driver: "AsyncContractDriver", name: str) -> None:
    with contextlib.suppress(Exception):
        await driver.execute(f"DROP TABLE {name}")
    with contextlib.suppress(Exception):
        await driver.commit()


def _as_json_obj(value: object) -> object:
    return json.loads(value) if isinstance(value, str) else value


def _duckdb_arrow_specifics(driver: object, case: DriverCase) -> None:
    """Fold DuckDB Arrow type preservation + JSON load_from_arrow."""
    import pyarrow as pa

    pytest.importorskip("pandas")
    from pandas.api.types import is_string_dtype

    sync_driver = cast("SyncContractDriver", driver)
    types_table = _arrow_spec_table(case, "types")
    _sync_drop_table(sync_driver, types_table)
    sync_driver.execute(
        f"CREATE TABLE {types_table} (id INTEGER, name VARCHAR, price DOUBLE, active BOOLEAN, created DATE)"
    )
    sync_driver.execute(
        f"INSERT INTO {types_table} VALUES (1, 'Product A', 19.99, true, '2024-01-01'), "
        f"(2, 'Product B', 29.99, false, '2024-01-02')"
    )
    try:
        df = sync_driver.select_to_arrow(f"SELECT * FROM {types_table} ORDER BY id").to_pandas()
        assert len(df) == 2
        assert df["id"].dtype == "int32"
        assert is_string_dtype(df["name"])
        assert df["price"].dtype == "float64"
        assert df["active"].dtype == "bool"
    finally:
        _sync_drop_table(sync_driver, types_table)

    json_table = _arrow_spec_table(case, "json")
    _sync_drop_table(sync_driver, json_table)
    sync_driver.execute(f"CREATE TABLE {json_table} (id INTEGER, payload JSON)")
    try:
        arrow_table = pa.table({"id": [1, 2], "payload": ['{"name":"alpha"}', '{"name":"beta"}']})
        job = sync_driver.load_from_arrow(json_table, arrow_table)
        rows = sync_driver.execute(f"SELECT id, payload::VARCHAR AS payload FROM {json_table} ORDER BY id").get_data()
        assert rows == [{"id": 1, "payload": '{"name":"alpha"}'}, {"id": 2, "payload": '{"name":"beta"}'}]
        assert job.telemetry["rows_processed"] == 2
    finally:
        _sync_drop_table(sync_driver, json_table)


def _sqlite_arrow_type_ddl(table: str) -> "tuple[str, str]":
    create = f"CREATE TABLE {table} (id INTEGER, name TEXT, price REAL, created_at TEXT, is_active INTEGER)"
    insert = (
        f"INSERT INTO {table} VALUES (1, 'Item 1', 19.99, '2025-01-01 10:00:00', 1), "
        f"(2, 'Item 2', 29.99, '2025-01-02 15:30:00', 0)"
    )
    return create, insert


async def _sqlite_arrow_specifics(driver: object, case: DriverCase) -> None:
    """Fold SQLite Arrow type preservation + JSON-as-text handling."""
    pytest.importorskip("pandas")
    from pandas.api.types import is_string_dtype

    async_driver = cast("AsyncContractDriver", driver)
    types_table = _arrow_spec_table(case, "types")
    await _async_drop_table(async_driver, types_table)
    create, insert = _sqlite_arrow_type_ddl(types_table)
    await async_driver.execute(create)
    await async_driver.execute(insert)
    try:
        df = (await async_driver.select_to_arrow(f"SELECT * FROM {types_table} ORDER BY id")).to_pandas()
        assert len(df) == 2
        assert is_string_dtype(df["name"])
        assert df["is_active"].dtype in (int, "int64", "Int64")
    finally:
        await _async_drop_table(async_driver, types_table)

    json_table = _arrow_spec_table(case, "json")
    await _async_drop_table(async_driver, json_table)
    await async_driver.execute(f"CREATE TABLE {json_table} (id INTEGER, data TEXT)")
    await async_driver.execute(
        f"""INSERT INTO {json_table} VALUES (1, '{{"name": "Alice"}}'), (2, '{{"name": "Bob"}}')"""
    )
    try:
        df = (await async_driver.select_to_arrow(f"SELECT * FROM {json_table} ORDER BY id")).to_pandas()
        assert len(df) == 2
        assert isinstance(df["data"].iloc[0], str)
        assert "Alice" in df["data"].iloc[0]
    finally:
        await _async_drop_table(async_driver, json_table)


async def _mysql_arrow_specifics(driver: object, case: DriverCase) -> None:
    """Fold MySQL Arrow type preservation + JSON column handling."""
    pytest.importorskip("pandas")
    from pandas.api.types import is_string_dtype

    async_driver = cast("AsyncContractDriver", driver)
    types_table = _arrow_spec_table(case, "types")
    await _async_drop_table(async_driver, types_table)
    await async_driver.execute(
        f"CREATE TABLE {types_table} (id INT, name VARCHAR(100), price DECIMAL(10, 2), "
        f"created_at DATETIME, is_active BOOLEAN)"
    )
    await async_driver.execute(
        f"INSERT INTO {types_table} VALUES (1, 'Item 1', 19.99, '2025-01-01 10:00:00', true), "
        f"(2, 'Item 2', 29.99, '2025-01-02 15:30:00', false)"
    )
    try:
        df = (await async_driver.select_to_arrow(f"SELECT * FROM {types_table} ORDER BY id")).to_pandas()
        assert len(df) == 2
        assert is_string_dtype(df["name"])
        assert df["is_active"].dtype in (bool, int, "int64", "Int64")
    finally:
        await _async_drop_table(async_driver, types_table)

    json_table = _arrow_spec_table(case, "json")
    await _async_drop_table(async_driver, json_table)
    await async_driver.execute(f"CREATE TABLE {json_table} (id INT, data JSON)")
    await async_driver.execute(
        f"""INSERT INTO {json_table} VALUES (1, '{{"name": "Alice"}}'), (2, '{{"name": "Bob"}}')"""
    )
    try:
        df = (await async_driver.select_to_arrow(f"SELECT * FROM {json_table} ORDER BY id")).to_pandas()
        assert len(df) == 2
    finally:
        await _async_drop_table(async_driver, json_table)


def _postgres_arrow_specifics(driver: object, case: DriverCase) -> None:
    """Fold PostgreSQL Arrow type preservation, array handling, and JSON/JSONB load_from_arrow (sync)."""
    import pyarrow as pa

    pytest.importorskip("pandas")
    from pandas.api.types import is_bool_dtype, is_string_dtype

    sync_driver = cast("SyncContractDriver", driver)
    types_table = _arrow_spec_table(case, "types")
    _sync_drop_table(sync_driver, types_table)
    sync_driver.execute(
        f"CREATE TABLE {types_table} (id INTEGER, name TEXT, price NUMERIC, created_at TIMESTAMP, is_active BOOLEAN)"
    )
    sync_driver.execute(
        f"INSERT INTO {types_table} VALUES (1, 'Item 1', 19.99, '2025-01-01 10:00:00', true), "
        f"(2, 'Item 2', 29.99, '2025-01-02 15:30:00', false)"
    )
    sync_driver.commit()
    try:
        df = sync_driver.select_to_arrow(f"SELECT * FROM {types_table} ORDER BY id").to_pandas()
        assert len(df) == 2
        assert is_string_dtype(df["name"])
        assert is_bool_dtype(df["is_active"])
    finally:
        _sync_drop_table(sync_driver, types_table)

    array_table = _arrow_spec_table(case, "arr")
    _sync_drop_table(sync_driver, array_table)
    sync_driver.execute(f"CREATE TABLE {array_table} (id INTEGER, tags TEXT[])")
    sync_driver.execute(f"INSERT INTO {array_table} VALUES (1, ARRAY['python', 'rust']), (2, ARRAY['js', 'ts'])")
    sync_driver.commit()
    try:
        df = sync_driver.select_to_arrow(f"SELECT * FROM {array_table} ORDER BY id").to_pandas()
        assert len(df) == 2
    finally:
        _sync_drop_table(sync_driver, array_table)

    load_table = _arrow_spec_table(case, "load")
    _sync_drop_table(sync_driver, load_table)
    sync_driver.execute(
        f"CREATE TABLE {load_table} (id INTEGER PRIMARY KEY, payload_json JSON NOT NULL, payload_jsonb JSONB NOT NULL)"
    )
    try:
        arrow_table = pa.table({
            "id": [1, 2],
            "payload_json": ['{"name":"alpha"}', '{"name":"beta"}'],
            "payload_jsonb": ['{"status":"ready"}', '{"status":"done"}'],
        })
        job = sync_driver.load_from_arrow(load_table, arrow_table)
        rows = sync_driver.execute(f"SELECT * FROM {load_table} ORDER BY id").get_data()
        assert len(rows) == 2
        assert _as_json_obj(rows[0]["payload_jsonb"]) == {"status": "ready"}
        assert job.telemetry["rows_processed"] == 2
    finally:
        _sync_drop_table(sync_driver, load_table)


async def _postgres_arrow_specifics_async(driver: object, case: DriverCase) -> None:
    """Fold PostgreSQL Arrow type preservation, array handling, and JSON/JSONB load_from_arrow (async)."""
    import pyarrow as pa

    pytest.importorskip("pandas")
    from pandas.api.types import is_bool_dtype, is_string_dtype

    async_driver = cast("AsyncContractDriver", driver)
    types_table = _arrow_spec_table(case, "types")
    await _async_drop_table(async_driver, types_table)
    await async_driver.execute(
        f"CREATE TABLE {types_table} (id INTEGER, name TEXT, price NUMERIC, created_at TIMESTAMP, is_active BOOLEAN)"
    )
    await async_driver.execute(
        f"INSERT INTO {types_table} VALUES (1, 'Item 1', 19.99, '2025-01-01 10:00:00', true), "
        f"(2, 'Item 2', 29.99, '2025-01-02 15:30:00', false)"
    )
    await async_driver.commit()
    try:
        df = (await async_driver.select_to_arrow(f"SELECT * FROM {types_table} ORDER BY id")).to_pandas()
        assert len(df) == 2
        assert is_string_dtype(df["name"])
        assert is_bool_dtype(df["is_active"])
    finally:
        await _async_drop_table(async_driver, types_table)

    array_table = _arrow_spec_table(case, "arr")
    await _async_drop_table(async_driver, array_table)
    await async_driver.execute(f"CREATE TABLE {array_table} (id INTEGER, tags TEXT[])")
    await async_driver.execute(f"INSERT INTO {array_table} VALUES (1, ARRAY['python', 'rust']), (2, ARRAY['js', 'ts'])")
    await async_driver.commit()
    try:
        df = (await async_driver.select_to_arrow(f"SELECT * FROM {array_table} ORDER BY id")).to_pandas()
        assert len(df) == 2
    finally:
        await _async_drop_table(async_driver, array_table)

    load_table = _arrow_spec_table(case, "load")
    await _async_drop_table(async_driver, load_table)
    await async_driver.execute(
        f"CREATE TABLE {load_table} (id INTEGER PRIMARY KEY, payload_json JSON NOT NULL, payload_jsonb JSONB NOT NULL)"
    )
    try:
        arrow_table = pa.table({
            "id": [1, 2],
            "payload_json": ['{"name":"alpha"}', '{"name":"beta"}'],
            "payload_jsonb": ['{"status":"ready"}', '{"status":"done"}'],
        })
        job = await async_driver.load_from_arrow(load_table, arrow_table)
        rows = (await async_driver.execute(f"SELECT * FROM {load_table} ORDER BY id")).get_data()
        assert len(rows) == 2
        assert _as_json_obj(rows[0]["payload_jsonb"]) == {"status": "ready"}
        assert job.telemetry["rows_processed"] == 2
    finally:
        await _async_drop_table(async_driver, load_table)


async def _oracle_arrow_specifics(driver: object, case: DriverCase) -> None:
    """Fold Oracle Arrow type preservation, CLOB streaming, and JSON load_from_arrow (async)."""
    import pyarrow as pa

    pytest.importorskip("pandas")
    from pandas.api.types import is_string_dtype

    async_driver = cast("AsyncContractDriver", driver)
    types_table = _arrow_spec_table(case, "types")
    await _async_drop_table(async_driver, types_table)
    await async_driver.execute(
        f"CREATE TABLE {types_table} (id NUMBER, name VARCHAR2(100), price NUMBER, created_at DATE, is_active NUMBER(1))"
    )
    await async_driver.execute(
        f"INSERT ALL INTO {types_table} VALUES (1, 'Item 1', 19.99, DATE '2025-01-01', 1) "
        f"INTO {types_table} VALUES (2, 'Item 2', 29.99, DATE '2025-01-02', 0) SELECT * FROM dual"
    )
    await async_driver.commit()
    try:
        df = (await async_driver.select_to_arrow(f"SELECT * FROM {types_table} ORDER BY id")).to_pandas()
        assert len(df) == 2
        assert is_string_dtype(df["name"])
        assert set(df["is_active"].unique()) <= {0, 1}
    finally:
        await _async_drop_table(async_driver, types_table)

    clob_table = _arrow_spec_table(case, "clob")
    await _async_drop_table(async_driver, clob_table)
    await async_driver.execute(f"CREATE TABLE {clob_table} (id NUMBER, description CLOB)")
    await async_driver.execute(f"INSERT INTO {clob_table} VALUES (1, RPAD('A', 2048, 'A'))")
    await async_driver.commit()
    try:
        df = (await async_driver.select_to_arrow(f"SELECT * FROM {clob_table}")).to_pandas()
        assert len(df) == 1
        assert isinstance(df["description"].iloc[0], str)
    finally:
        await _async_drop_table(async_driver, clob_table)

    json_table = _arrow_spec_table(case, "json")
    await _async_drop_table(async_driver, json_table)
    await async_driver.execute(f"CREATE TABLE {json_table} (id NUMBER PRIMARY KEY, payload JSON)")
    await async_driver.commit()
    try:
        payload = {"name": "alpha", "tags": ["north", "east"]}
        arrow_table = pa.table({"id": [1], "payload": pa.array([payload])})
        job = await async_driver.load_from_arrow(json_table, arrow_table)
        row = await async_driver.select_one(f"SELECT payload FROM {json_table} WHERE id = 1")
        assert _as_json_obj(row["payload"]) == payload
        assert job.telemetry["rows_processed"] == arrow_table.num_rows
    finally:
        await _async_drop_table(async_driver, json_table)


def _adbc_select_to_arrow_error(driver: object, case: DriverCase) -> None:
    """Fold ADBC select_to_arrow mapped-error coverage."""
    sync_driver = cast("SyncContractDriver", driver)
    missing_table = _pc_table(case, "missing_arrow")
    with pytest.raises(SQLParsingError):
        sync_driver.select_to_arrow(f"SELECT * FROM {missing_table}")


register_sync_extra_assertion("arrow_specifics:duckdb", ARROW_SPECIFICS_SCOPE, _duckdb_arrow_specifics)
register_sync_extra_assertion("arrow_specifics:postgres", ARROW_SPECIFICS_SCOPE, _postgres_arrow_specifics)
register_sync_extra_assertion(
    "arrow_specifics:adbc_select_to_arrow_error", ARROW_SPECIFICS_SCOPE, _adbc_select_to_arrow_error
)
register_async_extra_assertion("arrow_specifics:sqlite", ARROW_SPECIFICS_SCOPE, _sqlite_arrow_specifics)
register_async_extra_assertion("arrow_specifics:mysql", ARROW_SPECIFICS_SCOPE, _mysql_arrow_specifics)
register_async_extra_assertion("arrow_specifics:postgres", ARROW_SPECIFICS_SCOPE, _postgres_arrow_specifics_async)
register_async_extra_assertion("arrow_specifics:oracle", ARROW_SPECIFICS_SCOPE, _oracle_arrow_specifics)


def assert_sync_arrow_specifics_contract(driver: object, case: DriverCase) -> None:
    """Run an adapter's folded driver-specific Arrow proofs (type preservation, codecs, load), if any."""
    if not case.supports_arrow:
        pytest.skip(f"{case.adapter} has no verified Arrow support")
    dispatch_sync_extra_assertions(driver, case, ARROW_SPECIFICS_SCOPE)


async def assert_async_arrow_specifics_contract(driver: object, case: DriverCase) -> None:
    """Run an adapter's folded driver-specific Arrow proofs (type preservation, codecs, load), if any."""
    if not case.supports_arrow:
        pytest.skip(f"{case.adapter} has no verified Arrow support")
    await dispatch_async_extra_assertions(driver, case, ARROW_SPECIFICS_SCOPE)


_STORAGE_BRIDGE_EXPECTED = (
    {"name": "alpha", "value": 1, "note": "first"},
    {"name": "beta", "value": 2, "note": "second"},
)
STORAGE_BRIDGE_SCOPE = "storage_bridge"


def _storage_bridge_arrow_table() -> Any:
    import pyarrow as pa

    return pa.table({"name": ["alpha", "beta"], "value": [1, 2], "note": ["first", "second"]})


def assert_sync_storage_bridge_local_contract(driver: object, case: DriverCase, tmp_path: Any) -> None:
    """Assert sync drivers round-trip Arrow and local parquet through the storage bridge."""
    if not case.supports_storage_bridge:
        pytest.skip(f"{case.adapter} has no verified storage-bridge support")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    arrow_job = sync_driver.load_from_arrow(table.name, _storage_bridge_arrow_table(), overwrite=True)
    assert arrow_job.telemetry["rows_processed"] == 2
    assert_result_data(sync_driver.execute(table.select_ordered_sql), _STORAGE_BRIDGE_EXPECTED)

    destination = str(tmp_path / f"{case.id}.parquet")
    export_job = sync_driver.select_to_storage(table.select_ordered_sql, destination, format_hint="parquet")
    assert export_job.telemetry["rows_processed"] == 2

    load_job = sync_driver.load_from_storage(table.name, destination, file_format="parquet", overwrite=True)
    assert load_job.telemetry["rows_processed"] == 2
    assert_result_data(sync_driver.execute(table.select_ordered_sql), _STORAGE_BRIDGE_EXPECTED)
    dispatch_sync_extra_assertions(driver, case, STORAGE_BRIDGE_SCOPE)


async def assert_async_storage_bridge_local_contract(driver: object, case: DriverCase, tmp_path: Any) -> None:
    """Assert async drivers round-trip Arrow and local parquet through the storage bridge."""
    if not case.supports_storage_bridge:
        pytest.skip(f"{case.adapter} has no verified storage-bridge support")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    arrow_job = await async_driver.load_from_arrow(table.name, _storage_bridge_arrow_table(), overwrite=True)
    assert arrow_job.telemetry["rows_processed"] == 2
    assert_result_data(await async_driver.execute(table.select_ordered_sql), _STORAGE_BRIDGE_EXPECTED)

    destination = str(tmp_path / f"{case.id}.parquet")
    export_job = await async_driver.select_to_storage(table.select_ordered_sql, destination, format_hint="parquet")
    assert export_job.telemetry["rows_processed"] == 2

    load_job = await async_driver.load_from_storage(table.name, destination, file_format="parquet", overwrite=True)
    assert load_job.telemetry["rows_processed"] == 2
    assert_result_data(await async_driver.execute(table.select_ordered_sql), _STORAGE_BRIDGE_EXPECTED)
    await dispatch_async_extra_assertions(driver, case, STORAGE_BRIDGE_SCOPE)


async def _mysql_decimal_storage_bridge(driver: object, case: DriverCase) -> None:
    """Fold MySQL DECIMAL fidelity for parquet load_from_storage."""
    from pathlib import Path
    from tempfile import TemporaryDirectory

    import pyarrow as pa
    import pyarrow.parquet as pq

    async_driver = cast("AsyncContractDriver", driver)
    table = _pc_table(case, "storage_scores")
    await _async_drop_table(async_driver, table)
    await async_driver.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, score DECIMAL(5,2))")
    try:
        with TemporaryDirectory() as temp_dir:
            destination = Path(temp_dir) / "scores.parquet"
            pq.write_table(pa.table({"id": [5, 6], "score": [12.5, 99.1]}), destination)

            job = await async_driver.load_from_storage(table, str(destination), file_format="parquet", overwrite=True)
            assert job.telemetry["destination"] == table
            assert job.telemetry["extra"]["source"]["destination"].endswith("scores.parquet")  # type: ignore[index]
            assert job.telemetry["extra"]["source"]["backend"]  # type: ignore[index]

        rows = (await async_driver.execute(f"SELECT id, score FROM {table} ORDER BY id")).get_data()
        assert len(rows) == 2
        assert rows[0]["id"] == 5
        assert float(rows[0]["score"]) == pytest.approx(12.5)
        assert rows[1]["id"] == 6
        assert float(rows[1]["score"]) == pytest.approx(99.1)
    finally:
        await _async_drop_table(async_driver, table)


register_async_extra_assertion("storage_bridge:mysql_decimal", STORAGE_BRIDGE_SCOPE, _mysql_decimal_storage_bridge)


def _bulk_ingest_arrow(row_count: int) -> Any:
    import pyarrow as pa

    return pa.table({
        "name": [f"row{i}" for i in range(row_count)],
        "value": list(range(row_count)),
        "note": [f"note{i}" for i in range(row_count)],
    })


def assert_sync_native_bulk_ingest_contract(driver: object, case: DriverCase) -> None:
    """Assert native bulk-ingest row-count fidelity, overwrite/append, and error surfacing."""
    if not case.supports_native_bulk_ingest:
        pytest.skip(f"{case.adapter} has no verified native bulk-ingest support")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    job = sync_driver.load_from_arrow(table.name, _bulk_ingest_arrow(50), overwrite=True)
    assert job.telemetry["rows_processed"] == 50
    assert sync_driver.select_value(table.select_count_sql) == 50

    sync_driver.load_from_arrow(table.name, _bulk_ingest_arrow(10), overwrite=True)
    assert sync_driver.select_value(table.select_count_sql) == 10

    sync_driver.load_from_arrow(table.name, _bulk_ingest_arrow(50))
    assert sync_driver.select_value(table.select_count_sql) == 60


async def assert_async_native_bulk_ingest_contract(driver: object, case: DriverCase) -> None:
    """Assert async native bulk-ingest row-count fidelity, overwrite/append, and error surfacing."""
    if not case.supports_native_bulk_ingest:
        pytest.skip(f"{case.adapter} has no verified native bulk-ingest support")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    job = await async_driver.load_from_arrow(table.name, _bulk_ingest_arrow(50), overwrite=True)
    assert job.telemetry["rows_processed"] == 50
    assert await async_driver.select_value(table.select_count_sql) == 50

    await async_driver.load_from_arrow(table.name, _bulk_ingest_arrow(10), overwrite=True)
    assert await async_driver.select_value(table.select_count_sql) == 10

    await async_driver.load_from_arrow(table.name, _bulk_ingest_arrow(50))
    assert await async_driver.select_value(table.select_count_sql) == 60


def _records(row_count: int) -> "list[dict[str, Any]]":
    return [{"name": f"row{i}", "value": i, "note": f"note{i}"} for i in range(row_count)]


def assert_sync_load_from_records_contract(driver: object, case: DriverCase) -> None:
    """Assert load_from_records ingests dict and positional records and validates input."""
    if not case.supports_load_from_records:
        pytest.skip(f"{case.adapter} has no verified load_from_records support")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    job = sync_driver.load_from_records(table.name, _records(20), overwrite=True)
    assert job.telemetry["rows_processed"] == 20
    assert sync_driver.select_value(table.select_count_sql) == 20

    positional = [(f"p{i}", i, f"note{i}") for i in range(5)]
    sync_driver.load_from_records(table.name, positional, columns=["name", "value", "note"], overwrite=True)
    assert sync_driver.select_value(table.select_count_sql) == 5

    with pytest.raises(ImproperConfigurationError):
        sync_driver.load_from_records(table.name, [], overwrite=True)


async def assert_async_load_from_records_contract(driver: object, case: DriverCase) -> None:
    """Assert async load_from_records ingests dict and positional records and validates input."""
    if not case.supports_load_from_records:
        pytest.skip(f"{case.adapter} has no verified load_from_records support")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    job = await async_driver.load_from_records(table.name, _records(20), overwrite=True)
    assert job.telemetry["rows_processed"] == 20
    assert await async_driver.select_value(table.select_count_sql) == 20

    positional = [(f"p{i}", i, f"note{i}") for i in range(5)]
    await async_driver.load_from_records(table.name, positional, columns=["name", "value", "note"], overwrite=True)
    assert await async_driver.select_value(table.select_count_sql) == 5

    with pytest.raises(ImproperConfigurationError):
        await async_driver.load_from_records(table.name, [], overwrite=True)


def _storage_bridge_export_sql(table: ContractTable) -> str:
    return f"SELECT name, value, note FROM {table.name} WHERE value >= ? ORDER BY value"


def _storage_bridge_rustfs_names(case: DriverCase) -> "tuple[str, str, str, str]":
    alias = f"storage_bridge_contract_{case.id.replace('-', '_')}"
    prefix = f"contract-{case.id}"
    destination = f"alias://{alias}/export.parquet"
    object_name = f"{prefix}/export.parquet"
    return alias, prefix, destination, object_name


def _register_rustfs_alias(alias: str, rustfs_service: Any, bucket: str, *, prefix: str = "storage-bridge") -> str:
    """Register a storage registry alias backed by the pytest-databases RustFS service."""
    from sqlspec.storage.registry import storage_registry
    from tests.fixtures.rustfs import rustfs_fsspec_kwargs

    storage_registry.register_alias(
        alias, f"s3://{bucket}/{prefix}", backend="fsspec", **rustfs_fsspec_kwargs(rustfs_service)
    )
    return prefix


def assert_sync_storage_bridge_rustfs_contract(
    driver: object, case: DriverCase, rustfs_service: Any, rustfs_bucket_name: str
) -> None:
    """Assert sync drivers round-trip a SELECT through RustFS object storage."""
    if not case.supports_storage_bridge:
        pytest.skip(f"{case.adapter} has no verified storage-bridge support")
    from sqlspec.storage.registry import storage_registry
    from tests.fixtures.rustfs import rustfs_object_size

    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    alias, prefix, destination, object_name = _storage_bridge_rustfs_names(case)

    storage_registry.clear()
    try:
        _register_rustfs_alias(alias, rustfs_service, rustfs_bucket_name, prefix=prefix)
        _seed_sync(sync_driver, (ContractRow("alpha", 1, "first"), ContractRow("beta", 2, "second")), table, case)

        export_job = sync_driver.select_to_storage(
            _storage_bridge_export_sql(table), destination, 1, format_hint="parquet"
        )
        assert export_job.telemetry["rows_processed"] == 2

        load_job = sync_driver.load_from_storage(table.name, destination, file_format="parquet", overwrite=True)
        assert load_job.telemetry["rows_processed"] == 2
        assert_result_data(sync_driver.execute(table.select_ordered_sql), _STORAGE_BRIDGE_EXPECTED)

        assert rustfs_object_size(rustfs_service, rustfs_bucket_name, object_name) > 0
    finally:
        storage_registry.clear()


async def assert_async_storage_bridge_rustfs_contract(
    driver: object, case: DriverCase, rustfs_service: Any, rustfs_bucket_name: str
) -> None:
    """Assert async drivers round-trip a SELECT through RustFS object storage."""
    if not case.supports_storage_bridge:
        pytest.skip(f"{case.adapter} has no verified storage-bridge support")
    from sqlspec.storage.registry import storage_registry
    from tests.fixtures.rustfs import rustfs_object_size

    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    alias, prefix, destination, object_name = _storage_bridge_rustfs_names(case)

    storage_registry.clear()
    try:
        _register_rustfs_alias(alias, rustfs_service, rustfs_bucket_name, prefix=prefix)
        await _seed_async(
            async_driver, (ContractRow("alpha", 1, "first"), ContractRow("beta", 2, "second")), table, case
        )

        export_job = await async_driver.select_to_storage(
            _storage_bridge_export_sql(table), destination, 1, format_hint="parquet"
        )
        assert export_job.telemetry["rows_processed"] == 2

        load_job = await async_driver.load_from_storage(table.name, destination, file_format="parquet", overwrite=True)
        assert load_job.telemetry["rows_processed"] == 2
        assert_result_data(await async_driver.execute(table.select_ordered_sql), _STORAGE_BRIDGE_EXPECTED)

        assert rustfs_object_size(rustfs_service, rustfs_bucket_name, object_name) > 0
    finally:
        storage_registry.clear()


def assert_sync_exception_contract(driver: object, violation: ExceptionViolationCase) -> None:
    """Assert sync drivers normalize one constraint violation to its sqlspec exception type."""
    sync_driver = cast("SyncContractDriver", driver)

    sync_driver.execute_script(violation.setup_script)
    sync_driver.commit()
    if violation.seed_statement is not None:
        sync_driver.execute(violation.seed_statement, violation.seed_parameters)
        sync_driver.commit()

    try:
        with pytest.raises(violation.expected_exception):
            sync_driver.execute(violation.trigger_statement, violation.trigger_parameters)
    finally:
        with contextlib.suppress(Exception):
            sync_driver.rollback()
        with contextlib.suppress(Exception):
            sync_driver.execute_script(violation.teardown_script)
            sync_driver.commit()


async def assert_async_exception_contract(driver: object, violation: ExceptionViolationCase) -> None:
    """Assert async drivers normalize one constraint violation to its sqlspec exception type."""
    async_driver = cast("AsyncContractDriver", driver)

    await async_driver.execute_script(violation.setup_script)
    await async_driver.commit()
    if violation.seed_statement is not None:
        await async_driver.execute(violation.seed_statement, violation.seed_parameters)
        await async_driver.commit()

    try:
        with pytest.raises(violation.expected_exception):
            await async_driver.execute(violation.trigger_statement, violation.trigger_parameters)
    finally:
        with contextlib.suppress(Exception):
            await async_driver.rollback()
        with contextlib.suppress(Exception):
            await async_driver.execute_script(violation.teardown_script)
            await async_driver.commit()


def assert_sync_native_metadata_contract(driver: object, case: DriverCase) -> None:
    """Assert native metadata discovery returns the contract table and its columns."""
    if not case.supports_native_metadata:
        pytest.skip(f"{case.adapter} has no native metadata support")
    sync_driver = cast("SyncContractDriver", driver)
    data_dictionary = cast("Any", sync_driver).data_dictionary
    tables = data_dictionary.get_tables(sync_driver)
    table_names = {entry.get("table_name") for entry in tables}
    assert case.table.name in table_names
    columns = data_dictionary.get_columns(sync_driver, table=case.table.name)
    column_names = {entry["column_name"] for entry in columns}
    expected_columns = (
        {"name", "value", "note"} if case.table is DUCKDB_CONTRACT_TABLE else {"id", "name", "value", "note"}
    )
    assert expected_columns <= column_names
    typed = [entry for entry in columns if entry.get("data_type")]
    assert typed


async def assert_async_native_metadata_contract(driver: object, case: DriverCase) -> None:
    """Assert async native metadata discovery (no async adapter currently opts in)."""
    if not case.supports_native_metadata:
        pytest.skip(f"{case.adapter} has no native metadata support")
    pytest.fail("async native metadata behavior must be implemented when an async adapter opts in")


def assert_sync_native_statistics_contract(driver: object, case: DriverCase) -> None:
    """Assert native statistics succeed where supported and fail clearly elsewhere."""
    if not case.supports_native_metadata:
        pytest.skip(f"{case.adapter} has no native metadata support")
    sync_driver = cast("SyncContractDriver", driver)
    data_dictionary = cast("Any", sync_driver).data_dictionary
    if not hasattr(data_dictionary, "get_statistics"):
        pytest.skip(f"{case.adapter} data dictionary exposes no get_statistics")
    if not case.supports_native_statistics:
        with pytest.raises(OperationalError):
            data_dictionary.get_statistics(sync_driver, case.table.name)
        return
    statistics = data_dictionary.get_statistics(sync_driver, case.table.name)
    assert isinstance(statistics, list)
    for entry in statistics:
        assert entry["table_name"] == case.table.name
        assert isinstance(entry["statistic_name"], str)
        assert isinstance(entry["is_approximate"], bool)


_DATA_DICTIONARY_TYPE_CATEGORIES = ("text", "boolean", "timestamp", "blob", "unknown_type")


def _normalized_name(value: object) -> str:
    return str(value).strip('`"').split(".")[-1].lower()


def _assert_data_dictionary_version(version: object) -> None:
    assert isinstance(version, VersionInfo)
    assert version.major >= 0
    assert version.minor >= 0
    assert version.patch >= 0


def _assert_data_dictionary_version_cache(first: object, second: object) -> None:
    _assert_data_dictionary_version(first)
    _assert_data_dictionary_version(second)
    assert cast("VersionInfo", first).version_tuple == cast("VersionInfo", second).version_tuple


def _assert_data_dictionary_dialect(driver: object, case: DriverCase) -> None:
    dialect = getattr(driver, "dialect", None)
    assert dialect is not None
    assert _normalized_name(dialect) == _normalized_name(case.dialect)


def _data_dictionary_expected_columns(case: DriverCase) -> set[str]:
    expected = {"name", "value", "note"}
    if case.table is not DUCKDB_CONTRACT_TABLE:
        expected.add("id")
    return expected


def _assert_data_dictionary_tables(tables: object, case: DriverCase) -> None:
    assert isinstance(tables, list)
    table_names = {_normalized_name(entry.get("table_name")) for entry in tables if isinstance(entry, dict)}
    assert _normalized_name(case.table.name) in table_names


def _assert_data_dictionary_columns(columns: object, case: DriverCase) -> None:
    assert isinstance(columns, list)
    column_names = {_normalized_name(entry["column_name"]) for entry in columns if isinstance(entry, dict)}
    assert _data_dictionary_expected_columns(case) <= column_names


def _assert_data_dictionary_feature_list(features: object) -> tuple[str, ...]:
    assert isinstance(features, list)
    assert features
    assert all(isinstance(feature, str) for feature in features)
    return tuple(features)


def _data_dictionary_schema_for_case(case: DriverCase) -> str:
    if case.dialect == "postgres":
        return "public"
    return ""


def _data_dictionary_topology_sql(case: DriverCase, users: str, orders: str, items: str, index_name: str) -> str:
    if case.dialect == "mysql":
        return f"""
            CREATE TABLE {users} (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50)
            ) ENGINE=InnoDB;
            CREATE TABLE {orders} (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                amount INTEGER,
                FOREIGN KEY (user_id) REFERENCES {users}(id)
            ) ENGINE=InnoDB;
            CREATE TABLE {items} (
                id INTEGER PRIMARY KEY,
                order_id INTEGER,
                name VARCHAR(50),
                FOREIGN KEY (order_id) REFERENCES {orders}(id)
            ) ENGINE=InnoDB;
            CREATE INDEX {index_name} ON {users}(name);
        """
    if case.dialect not in {"postgres", "sqlite"}:
        msg = f"{case.id} has no topology DDL contract"
        raise ValueError(msg)
    return f"""
        CREATE TABLE {users} (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50)
        );
        CREATE TABLE {orders} (
            id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES {users}(id),
            amount INTEGER
        );
        CREATE TABLE {items} (
            id INTEGER PRIMARY KEY,
            order_id INTEGER REFERENCES {orders}(id),
            name VARCHAR(50)
        );
        CREATE INDEX {index_name} ON {users}(name);
    """


def _data_dictionary_topology_drop_sql(users: str, orders: str, items: str) -> str:
    return f"""
        DROP TABLE IF EXISTS {items};
        DROP TABLE IF EXISTS {orders};
        DROP TABLE IF EXISTS {users};
    """


def _assert_data_dictionary_topology(
    tables: object, foreign_keys: object, indexes: object, users: str, orders: str, items: str, index_name: str
) -> None:
    assert isinstance(tables, list)
    table_names = [_normalized_name(entry.get("table_name")) for entry in tables if isinstance(entry, dict)]
    expected = (users, orders, items)
    test_tables = [name for name in table_names if name in expected]
    assert test_tables == list(expected)

    assert isinstance(foreign_keys, list)
    matching_fk = next(
        (
            fk
            for fk in foreign_keys
            if _normalized_name(fk.referenced_table) == users and _normalized_name(fk.column_name) == "user_id"
        ),
        None,
    )
    assert matching_fk is not None

    assert isinstance(indexes, list)
    assert any(_normalized_name(index.get("index_name")) == index_name for index in indexes if isinstance(index, dict))


def assert_sync_data_dictionary_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers expose portable data-dictionary version, feature, type, table, and column metadata."""
    if not case.supports_data_dictionary:
        pytest.skip(f"{case.adapter} has no verified data-dictionary support")
    sync_driver = cast("SyncContractDriver", driver)
    data_dictionary = cast("Any", sync_driver).data_dictionary

    _assert_data_dictionary_dialect(sync_driver, case)
    _assert_data_dictionary_version_cache(
        data_dictionary.get_version(sync_driver), data_dictionary.get_version(sync_driver)
    )
    features = _assert_data_dictionary_feature_list(data_dictionary.list_available_features())
    for feature in features:
        assert isinstance(data_dictionary.get_feature_flag(sync_driver, feature), bool)
    for type_category in _DATA_DICTIONARY_TYPE_CATEGORIES:
        optimal_type = data_dictionary.get_optimal_type(sync_driver, type_category)
        assert isinstance(optimal_type, str)
        assert optimal_type

    _assert_data_dictionary_tables(data_dictionary.get_tables(sync_driver), case)
    _assert_data_dictionary_columns(data_dictionary.get_columns(sync_driver, table=case.table.name), case)


async def assert_async_data_dictionary_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers expose portable data-dictionary version, feature, type, table, and column metadata."""
    if not case.supports_data_dictionary:
        pytest.skip(f"{case.adapter} has no verified data-dictionary support")
    async_driver = cast("AsyncContractDriver", driver)
    data_dictionary = cast("Any", async_driver).data_dictionary

    _assert_data_dictionary_dialect(async_driver, case)
    _assert_data_dictionary_version_cache(
        await data_dictionary.get_version(async_driver), await data_dictionary.get_version(async_driver)
    )
    features = _assert_data_dictionary_feature_list(data_dictionary.list_available_features())
    for feature in features:
        assert isinstance(await data_dictionary.get_feature_flag(async_driver, feature), bool)
    for type_category in _DATA_DICTIONARY_TYPE_CATEGORIES:
        optimal_type = await data_dictionary.get_optimal_type(async_driver, type_category)
        assert isinstance(optimal_type, str)
        assert optimal_type

    _assert_data_dictionary_tables(await data_dictionary.get_tables(async_driver), case)
    _assert_data_dictionary_columns(await data_dictionary.get_columns(async_driver, table=case.table.name), case)


def assert_sync_data_dictionary_schema_contract(driver: object, case: DriverCase) -> None:
    """Assert sync data dictionaries honor schema-qualified column discovery."""
    if not case.supports_schema_qualified_data_dictionary:
        pytest.skip(f"{case.adapter} has no schema-qualified data-dictionary support")
    sync_driver = cast("SyncContractDriver", driver)
    data_dictionary = cast("Any", sync_driver).data_dictionary
    schema_name = _data_dictionary_schema_for_case(case)
    _assert_data_dictionary_columns(
        data_dictionary.get_columns(sync_driver, table=case.table.name, schema=schema_name), case
    )


async def assert_async_data_dictionary_schema_contract(driver: object, case: DriverCase) -> None:
    """Assert async data dictionaries honor schema-qualified column discovery."""
    if not case.supports_schema_qualified_data_dictionary:
        pytest.skip(f"{case.adapter} has no schema-qualified data-dictionary support")
    async_driver = cast("AsyncContractDriver", driver)
    data_dictionary = cast("Any", async_driver).data_dictionary
    schema_name = _data_dictionary_schema_for_case(case)
    columns = await data_dictionary.get_columns(async_driver, table=case.table.name, schema=schema_name)
    _assert_data_dictionary_columns(columns, case)


def assert_sync_data_dictionary_topology_contract(driver: object, case: DriverCase) -> None:
    """Assert sync data dictionaries sort table dependencies and surface FK/index metadata."""
    if not case.supports_data_dictionary_topology:
        pytest.skip(f"{case.adapter} has no data-dictionary topology support")
    sync_driver = cast("SyncContractDriver", driver)
    data_dictionary = cast("Any", sync_driver).data_dictionary
    suffix = uuid4().hex[:8]
    users = f"dd_users_{suffix}"
    orders = f"dd_orders_{suffix}"
    items = f"dd_items_{suffix}"
    index_name = f"idx_dd_users_{suffix}"
    sync_driver.execute_script(_data_dictionary_topology_sql(case, users, orders, items, index_name))
    sync_driver.commit()
    try:
        _assert_data_dictionary_topology(
            data_dictionary.get_tables(sync_driver),
            data_dictionary.get_foreign_keys(sync_driver, table=orders),
            data_dictionary.get_indexes(sync_driver, table=users),
            users,
            orders,
            items,
            index_name,
        )
    finally:
        with contextlib.suppress(Exception):
            sync_driver.execute_script(_data_dictionary_topology_drop_sql(users, orders, items))
            sync_driver.commit()


async def assert_async_data_dictionary_topology_contract(driver: object, case: DriverCase) -> None:
    """Assert async data dictionaries sort table dependencies and surface FK/index metadata."""
    if not case.supports_data_dictionary_topology:
        pytest.skip(f"{case.adapter} has no data-dictionary topology support")
    async_driver = cast("AsyncContractDriver", driver)
    data_dictionary = cast("Any", async_driver).data_dictionary
    suffix = uuid4().hex[:8]
    users = f"dd_users_{suffix}"
    orders = f"dd_orders_{suffix}"
    items = f"dd_items_{suffix}"
    index_name = f"idx_dd_users_{suffix}"
    await async_driver.execute_script(_data_dictionary_topology_sql(case, users, orders, items, index_name))
    await async_driver.commit()
    try:
        tables = await data_dictionary.get_tables(async_driver)
        foreign_keys = await data_dictionary.get_foreign_keys(async_driver, table=orders)
        indexes = await data_dictionary.get_indexes(async_driver, table=users)
        _assert_data_dictionary_topology(tables, foreign_keys, indexes, users, orders, items, index_name)
    finally:
        with contextlib.suppress(Exception):
            await async_driver.execute_script(_data_dictionary_topology_drop_sql(users, orders, items))
            await async_driver.commit()
