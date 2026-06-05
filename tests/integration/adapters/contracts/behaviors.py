"""Public behavior helpers for adapter-local and central contract tests."""

import contextlib
import json
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, cast

import pytest

from sqlspec import SQL, SQLResult, StatementStack, sql
from sqlspec.builder import Explain
from sqlspec.core.filters import InCollectionFilter, LimitOffsetFilter, OrderByFilter, SearchFilter
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.utils.serializers import to_json
from tests.integration.adapters.contracts._assertions import assert_result_data, assert_sql_result
from tests.integration.adapters.contracts._cases import DriverCase
from tests.integration.adapters.contracts._inputs import (
    ExceptionViolationCase,
    ExplainCase,
    ParameterProfileCase,
    ParameterStyleCase,
    StatementInputCase,
)
from tests.integration.adapters.contracts._schema import DEFAULT_CONTRACT_TABLE, ContractRow, ContractTable

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

    def select_to_arrow(self, statement: object, /, *parameters: object, **kwargs: Any) -> Any: ...

    def select_to_storage(
        self, statement: object, destination: object, /, *parameters: object, **kwargs: Any
    ) -> Any: ...

    def load_from_arrow(self, table: str, source: Any, /, **kwargs: Any) -> Any: ...

    def load_from_storage(self, table: str, source: object, /, **kwargs: Any) -> Any: ...


class AsyncContractDriver(Protocol):
    """Async driver surface used by adapter contract helpers."""

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

    async def select_to_arrow(self, statement: object, /, *parameters: object, **kwargs: Any) -> Any: ...

    async def select_to_storage(
        self, statement: object, destination: object, /, *parameters: object, **kwargs: Any
    ) -> Any: ...

    async def load_from_arrow(self, table: str, source: Any, /, **kwargs: Any) -> Any: ...

    async def load_from_storage(self, table: str, source: object, /, **kwargs: Any) -> Any: ...


def _with_table(value: object, table: ContractTable) -> object:
    """Rewrite the canonical ``contract_items`` table reference to the case's table name.

    A no-op for adapters whose table is literally ``contract_items``; for adapters that
    require a qualified identifier (e.g. BigQuery ``project.dataset.contract_items``) it
    substitutes the resolved name into raw SQL strings.
    """
    if isinstance(value, str) and table.name != "contract_items":
        return value.replace("contract_items", table.name)
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


def _should_assert_execute_rows_affected(case: DriverCase) -> bool:
    return "execute-rows-affected-unavailable" not in case.deviations


def _seed_sync(
    driver: SyncContractDriver, rows: tuple[ContractRow, ...], table: ContractTable = DEFAULT_CONTRACT_TABLE
) -> None:
    if rows:
        driver.execute_many(table.insert_qmark_sql, _row_parameters(rows))
        driver.commit()


async def _seed_async(
    driver: AsyncContractDriver, rows: tuple[ContractRow, ...], table: ContractTable = DEFAULT_CONTRACT_TABLE
) -> None:
    if rows:
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
    assert_execute_rows = _should_assert_execute_rows_affected(case)

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
    assert_execute_rows = _should_assert_execute_rows_affected(case)

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
    if "no-for-share" not in case.deviations:
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
    if "no-for-share" not in case.deviations:
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
    _seed_sync(sync_driver, _FILTER_SEED_ROWS, table)
    base = f"SELECT name, value FROM {table.name}"

    paged = sync_driver.execute(base, OrderByFilter("value", "desc"), LimitOffsetFilter(limit=2, offset=1))
    assert [row["name"] for row in paged.get_data()] == ["delta", "gamma"]

    in_collection = sync_driver.execute(base, InCollectionFilter("value", [20, 40]), OrderByFilter("value", "asc"))
    assert [row["name"] for row in in_collection.get_data()] == ["beta", "delta"]

    if "emulator-no-search-filter" not in case.deviations:
        searched = sync_driver.execute(base, SearchFilter("name", "lta"))
        assert [row["name"] for row in searched.get_data()] == ["delta"]


async def assert_async_filter_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers apply OrderBy/LimitOffset, InCollection, and Search filters."""
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    await _seed_async(async_driver, _FILTER_SEED_ROWS, table)
    base = f"SELECT name, value FROM {table.name}"

    paged = await async_driver.execute(base, OrderByFilter("value", "desc"), LimitOffsetFilter(limit=2, offset=1))
    assert [row["name"] for row in paged.get_data()] == ["delta", "gamma"]

    in_collection = await async_driver.execute(
        base, InCollectionFilter("value", [20, 40]), OrderByFilter("value", "asc")
    )
    assert [row["name"] for row in in_collection.get_data()] == ["beta", "delta"]

    if "emulator-no-search-filter" not in case.deviations:
        searched = await async_driver.execute(base, SearchFilter("name", "lta"))
        assert [row["name"] for row in searched.get_data()] == ["delta"]


def assert_sync_complex_query_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers run grouped aggregation and correlated subquery selects."""
    if "emulator-no-grouped-subquery" in case.deviations:
        pytest.skip(f"{case.adapter} emulator does not support grouped aggregation / correlated subqueries")
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    _seed_sync(sync_driver, _GROUPED_SEED_ROWS, table)

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
    if "emulator-no-grouped-subquery" in case.deviations:
        pytest.skip(f"{case.adapter} emulator does not support grouped aggregation / correlated subqueries")
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    await _seed_async(async_driver, _GROUPED_SEED_ROWS, table)

    grouped = await async_driver.execute(
        f"SELECT value, COUNT(*) AS count FROM {table.name} GROUP BY value HAVING COUNT(*) >= 2 ORDER BY value"
    )
    assert [(row["value"], row["count"]) for row in grouped.get_data()] == [(20, 2), (30, 2)]

    top = await async_driver.execute(
        f"SELECT name FROM {table.name} WHERE value = (SELECT MAX(value) FROM {table.name}) ORDER BY name"
    )
    assert [row["name"] for row in top.get_data()] == ["delta", "epsilon"]


def assert_sync_statement_stack_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers execute a StatementStack sequentially with per-operation results."""
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    assert_execute_rows = _should_assert_execute_rows_affected(case)

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
    finally:
        sync_driver.execute(table.delete_sql)
        sync_driver.commit()


async def assert_async_statement_stack_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers execute a StatementStack sequentially with per-operation results."""
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    assert_execute_rows = _should_assert_execute_rows_affected(case)

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
    finally:
        await async_driver.execute(table.delete_sql)
        await async_driver.commit()


def assert_sync_execute_many_contract(driver: object, case: DriverCase) -> None:
    """Assert sync execute-many behavior for a driver case."""
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    result = sync_driver.execute_many(
        table.insert_qmark_sql, [("alpha", 10, None), ("beta", 20, None), ("gamma", 30, None)]
    )

    assert_sql_result(result, rows_affected=3)
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
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    result = await async_driver.execute_many(
        table.insert_qmark_sql, [("alpha", 10, None), ("beta", 20, None), ("gamma", 30, None)]
    )

    assert_sql_result(result, rows_affected=3)
    assert_result_data(
        await async_driver.execute(table.select_ordered_sql),
        (
            {"name": "alpha", "value": 10, "note": None},
            {"name": "beta", "value": 20, "note": None},
            {"name": "gamma", "value": 30, "note": None},
        ),
    )


def assert_sync_execute_many_mutation_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers batch insert, update, and delete with accurate row counts."""
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    inserted = sync_driver.execute_many(table.insert_qmark_sql, [("a", 1, None), ("b", 2, None), ("c", 3, None)])
    assert_sql_result(inserted, rows_affected=3)
    sync_driver.commit()
    assert sync_driver.select_value(table.select_count_sql) == 3

    updated = sync_driver.execute_many(_update_value_sql(table), [(10, "a"), (20, "b")])
    assert_sql_result(updated, rows_affected=2)
    sync_driver.commit()

    deleted = sync_driver.execute_many(_delete_by_name_sql(table), [("a",), ("b",)])
    assert_sql_result(deleted, rows_affected=2)
    sync_driver.commit()
    assert sync_driver.select_value(table.select_count_sql) == 1


async def assert_async_execute_many_mutation_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers batch insert, update, and delete with accurate row counts."""
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    inserted = await async_driver.execute_many(table.insert_qmark_sql, [("a", 1, None), ("b", 2, None), ("c", 3, None)])
    assert_sql_result(inserted, rows_affected=3)
    await async_driver.commit()
    assert await async_driver.select_value(table.select_count_sql) == 3

    updated = await async_driver.execute_many(_update_value_sql(table), [(10, "a"), (20, "b")])
    assert_sql_result(updated, rows_affected=2)
    await async_driver.commit()

    deleted = await async_driver.execute_many(_delete_by_name_sql(table), [("a",), ("b",)])
    assert_sql_result(deleted, rows_affected=2)
    await async_driver.commit()
    assert await async_driver.select_value(table.select_count_sql) == 1


def assert_sync_execute_many_input_contract(driver: object, case: DriverCase) -> None:
    """Assert sync drivers batch a large sequence and an is_many SQL object."""
    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    large_batch = [(f"item-{index}", index, None) for index in range(200)]
    large_result = sync_driver.execute_many(table.insert_qmark_sql, large_batch)
    assert_sql_result(large_result, rows_affected=200)
    sync_driver.commit()
    assert sync_driver.select_value(table.select_count_sql) == 200

    sql_object = SQL(table.insert_qmark_sql, [("obj-1", 1, None), ("obj-2", 2, None)], is_many=True)
    object_result = sync_driver.execute(sql_object)
    assert_sql_result(object_result, rows_affected=2)
    sync_driver.commit()
    assert sync_driver.select_value(table.select_count_sql) == 202


async def assert_async_execute_many_input_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers batch a large sequence and an is_many SQL object."""
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table

    large_batch = [(f"item-{index}", index, None) for index in range(200)]
    large_result = await async_driver.execute_many(table.insert_qmark_sql, large_batch)
    assert_sql_result(large_result, rows_affected=200)
    await async_driver.commit()
    assert await async_driver.select_value(table.select_count_sql) == 200

    sql_object = SQL(table.insert_qmark_sql, [("obj-1", 1, None), ("obj-2", 2, None)], is_many=True)
    object_result = await async_driver.execute(sql_object)
    assert_sql_result(object_result, rows_affected=2)
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
        assert empty.rows_affected in (-1, 0)
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
        assert empty.rows_affected in (-1, 0)
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
    """Fold ADBC-PostgreSQL params: NULL-literal pruning, repeated NULL, RETURNING+NULL, JSONB cast, count-mismatch."""
    from datetime import date

    sync_driver = cast("SyncContractDriver", driver)
    items = _pc_table(case, "items")
    jsonb = _pc_table(case, "jsonb")
    _sync_drop_table(sync_driver, items)
    _sync_drop_table(sync_driver, jsonb)
    sync_driver.execute(
        f"CREATE TABLE {items} (id INTEGER PRIMARY KEY, name TEXT, value INTEGER, active BOOLEAN, created_date DATE)"
    )
    sync_driver.execute(f"CREATE TABLE {jsonb} (id INTEGER PRIMARY KEY, metadata JSONB, config JSONB)")
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

        with pytest.raises(SQLSpecError):
            sync_driver.execute(f"INSERT INTO {items} (id, name) VALUES ($1, $2)", 30, None, "extra")
    finally:
        _sync_drop_table(sync_driver, items)
        _sync_drop_table(sync_driver, jsonb)


def _adbc_sqlite_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold ADBC-SQLite params: qmark placeholders preserve NULLs and boolean values."""
    sync_driver = cast("SyncContractDriver", driver)
    items = _pc_table(case, "items")
    _sync_drop_table(sync_driver, items)
    sync_driver.execute(f"CREATE TABLE {items} (id INTEGER PRIMARY KEY, name TEXT, value INTEGER, active BOOLEAN)")
    try:
        inserted = sync_driver.execute(
            f"INSERT INTO {items} (id, name, value, active) VALUES (?, ?, ?, ?)", 1, None, None, True
        )
        assert inserted.rows_affected in (-1, 0, 1)
        row = sync_driver.select_one(f"SELECT name, value, active FROM {items} WHERE id = ?", 1)
        assert row["name"] is None
        assert row["value"] is None
        assert row["active"] in (True, 1)
    finally:
        _sync_drop_table(sync_driver, items)


def _adbc_duckdb_param_codecs(driver: object, case: DriverCase) -> None:
    """Fold ADBC-DuckDB params: numeric placeholders with backend DDL, qmark feeding the native Arrow path."""
    from sqlspec.typing import PYARROW_INSTALLED

    sync_driver = cast("SyncContractDriver", driver)
    items = _pc_table(case, "items")
    _sync_drop_table(sync_driver, items)
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
    finally:
        _sync_drop_table(sync_driver, items)


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


def assert_sync_statement_input_contract(driver: object, case: DriverCase, input_case: StatementInputCase) -> None:
    """Assert sync drivers return equivalent rows for one statement input shape."""
    sync_driver = cast("SyncContractDriver", driver)
    _seed_sync(sync_driver, input_case.setup_rows, case.table)

    statement = input_case.statement_factory(case.table.name)
    result = _execute_sync(sync_driver, statement, input_case.parameters)
    assert_result_data(result, input_case.expected_data)

    repeat_result = _execute_sync(sync_driver, statement, input_case.parameters)
    assert_result_data(repeat_result, input_case.expected_data)


async def assert_async_statement_input_contract(
    driver: object, case: DriverCase, input_case: StatementInputCase
) -> None:
    """Assert async drivers return equivalent rows for one statement input shape."""
    async_driver = cast("AsyncContractDriver", driver)
    await _seed_async(async_driver, input_case.setup_rows, case.table)

    statement = input_case.statement_factory(case.table.name)
    result = await _execute_async(async_driver, statement, input_case.parameters)
    assert_result_data(result, input_case.expected_data)

    repeat_result = await _execute_async(async_driver, statement, input_case.parameters)
    assert_result_data(repeat_result, input_case.expected_data)


def assert_sync_parameter_contract(driver: object, case: DriverCase, parameter_case: ParameterProfileCase) -> None:
    """Assert sync drivers bind one parameter profile case correctly."""
    sync_driver = cast("SyncContractDriver", driver)
    _seed_sync(sync_driver, parameter_case.setup_rows, case.table)

    result = _execute_sync(sync_driver, _with_table(parameter_case.statement, case.table), parameter_case.parameters)
    if parameter_case.expected_rows_affected is not None and _should_assert_execute_rows_affected(case):
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
    await _seed_async(async_driver, parameter_case.setup_rows, case.table)

    result = await _execute_async(
        async_driver, _with_table(parameter_case.statement, case.table), parameter_case.parameters
    )
    if parameter_case.expected_rows_affected is not None and _should_assert_execute_rows_affected(case):
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
    sync_driver = cast("SyncContractDriver", driver)
    _seed_sync(sync_driver, parameter_style_case.setup_rows, case.table)

    style_statement = _with_table(parameter_style_case.statement, case.table)
    if parameter_style_case.method == "execute_many":
        result = sync_driver.execute_many(style_statement, parameter_style_case.parameters)
    else:
        result = _execute_sync(sync_driver, style_statement, parameter_style_case.parameters)

    if parameter_style_case.expected_rows_affected is not None and (
        parameter_style_case.method == "execute_many" or _should_assert_execute_rows_affected(case)
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
    async_driver = cast("AsyncContractDriver", driver)
    await _seed_async(async_driver, parameter_style_case.setup_rows, case.table)

    style_statement = _with_table(parameter_style_case.statement, case.table)
    if parameter_style_case.method == "execute_many":
        result = await async_driver.execute_many(style_statement, parameter_style_case.parameters)
    else:
        result = await _execute_async(async_driver, style_statement, parameter_style_case.parameters)

    if parameter_style_case.expected_rows_affected is not None and (
        parameter_style_case.method == "execute_many" or _should_assert_execute_rows_affected(case)
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
    _seed_sync(sync_driver, (ContractRow("result1", 10), ContractRow("result2", 20), ContractRow("result3", 30)), table)

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
        async_driver, (ContractRow("result1", 10), ContractRow("result2", 20), ContractRow("result3", 30)), table
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
        INSERT INTO {table} (name, value, note) VALUES ('script1', 10, NULL);
        INSERT INTO {table} (name, value, note) VALUES ('script2', 20, NULL);
        UPDATE {table} SET value = 30 WHERE name = 'script1';
    """)
    assert_sql_result(result)
    assert result.operation_type == "SCRIPT"
    assert_result_data(
        sync_driver.execute(f"SELECT name, value FROM {table} WHERE name LIKE 'script%' ORDER BY name"),
        ({"name": "script1", "value": 30}, {"name": "script2", "value": 20}),
    )

    if "emulator-retries-invalid-sql" not in case.deviations:
        with pytest.raises(SQLParsingError):
            sync_driver.execute(f"SELCT * FROM {table}")
        with pytest.raises(SQLSpecError):
            sync_driver.execute("SELECT * FROM missing_contract_table")


async def assert_async_script_error_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers execute scripts and normalize generic SQL errors."""
    async_driver = cast("AsyncContractDriver", driver)
    table = case.table.name

    result = await async_driver.execute_script(f"""
        INSERT INTO {table} (name, value, note) VALUES ('script1', 10, NULL);
        INSERT INTO {table} (name, value, note) VALUES ('script2', 20, NULL);
        UPDATE {table} SET value = 30 WHERE name = 'script1';
    """)
    assert_sql_result(result)
    assert result.operation_type == "SCRIPT"
    assert_result_data(
        await async_driver.execute(f"SELECT name, value FROM {table} WHERE name LIKE 'script%' ORDER BY name"),
        ({"name": "script1", "value": 30}, {"name": "script2", "value": 20}),
    )

    if "emulator-retries-invalid-sql" not in case.deviations:
        with pytest.raises(SQLParsingError):
            await async_driver.execute(f"SELCT * FROM {table}")
        with pytest.raises(SQLSpecError):
            await async_driver.execute("SELECT * FROM missing_contract_table")


def _explain_skip_reason(case: DriverCase) -> str:
    if "explain-copy-incompatible" in case.deviations:
        return f"{case.adapter}-{case.dialect} EXPLAIN is incompatible with the driver's COPY result transfer"
    return f"{case.adapter} ({case.dialect}) has no verified EXPLAIN support"


def assert_sync_explain_contract(driver: object, case: DriverCase, explain_case: ExplainCase) -> None:
    """Assert sync drivers execute one EXPLAIN artifact and return plan rows."""
    if not case.supports_explain:
        pytest.skip(_explain_skip_reason(case))
    sync_driver = cast("SyncContractDriver", driver)
    result = assert_sql_result(sync_driver.execute(explain_case.build(case.table, case.dialect)))
    assert result.data is not None


async def assert_async_explain_contract(driver: object, case: DriverCase, explain_case: ExplainCase) -> None:
    """Assert async drivers execute one EXPLAIN artifact and return plan rows."""
    if not case.supports_explain:
        pytest.skip(_explain_skip_reason(case))
    async_driver = cast("AsyncContractDriver", driver)
    result = assert_sql_result(await async_driver.execute(explain_case.build(case.table, case.dialect)))
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
    _seed_sync(sync_driver, (ContractRow("a", 1), ContractRow("b", 2), ContractRow("c", 3)), table)

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


async def assert_async_arrow_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers return Arrow tables, batches, filtered, and empty results."""
    if not case.supports_arrow:
        pytest.skip(f"{case.adapter} has no verified Arrow support")
    import pyarrow as pa

    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    await _seed_async(async_driver, (ContractRow("a", 1), ContractRow("b", 2), ContractRow("c", 3)), table)

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


_ARROW_LARGE_ROW_COUNT = 1000


def assert_sync_arrow_extras_contract(driver: object, case: DriverCase) -> None:
    """Assert sync Arrow output preserves NULLs and scales to a large result set."""
    if not case.supports_arrow:
        pytest.skip(f"{case.adapter} has no verified Arrow support")
    import pyarrow as pa

    sync_driver = cast("SyncContractDriver", driver)
    table = case.table

    _seed_sync(sync_driver, (ContractRow("a", 1, None), ContractRow("b", 2, "noted")), table)
    null_result = sync_driver.select_to_arrow(table.select_ordered_sql)
    assert isinstance(null_result.data, pa.Table)
    assert null_result.data.column("note").to_pylist() == [None, "noted"]

    sync_driver.execute(table.delete_sql)
    sync_driver.commit()
    _seed_sync(sync_driver, tuple(ContractRow(f"n{i}", i) for i in range(1, _ARROW_LARGE_ROW_COUNT + 1)), table)
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

    await _seed_async(async_driver, (ContractRow("a", 1, None), ContractRow("b", 2, "noted")), table)
    null_result = await async_driver.select_to_arrow(table.select_ordered_sql)
    assert isinstance(null_result.data, pa.Table)
    assert null_result.data.column("note").to_pylist() == [None, "noted"]

    await async_driver.execute(table.delete_sql)
    await async_driver.commit()
    await _seed_async(async_driver, tuple(ContractRow(f"n{i}", i) for i in range(1, _ARROW_LARGE_ROW_COUNT + 1)), table)
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
    _seed_sync(sync_driver, (ContractRow("a", 1), ContractRow("b", 2)), table)

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
    await _seed_async(async_driver, (ContractRow("a", 1), ContractRow("b", 2)), table)

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


register_sync_extra_assertion("arrow_specifics:duckdb", ARROW_SPECIFICS_SCOPE, _duckdb_arrow_specifics)
register_sync_extra_assertion("arrow_specifics:postgres", ARROW_SPECIFICS_SCOPE, _postgres_arrow_specifics)
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
        _seed_sync(sync_driver, (ContractRow("alpha", 1, "first"), ContractRow("beta", 2, "second")), table)

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
        await _seed_async(async_driver, (ContractRow("alpha", 1, "first"), ContractRow("beta", 2, "second")), table)

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
