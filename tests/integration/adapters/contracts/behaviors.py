"""Public behavior helpers for adapter-local and central contract tests."""

import contextlib
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, cast

import pytest

from sqlspec import SQL, SQLResult, StatementStack, sql
from sqlspec.core.filters import InCollectionFilter, LimitOffsetFilter, OrderByFilter, SearchFilter
from sqlspec.exceptions import SQLParsingError, SQLSpecError
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


def assert_sync_storage_bridge_rustfs_contract(
    driver: object, case: DriverCase, rustfs_service: Any, rustfs_bucket_name: str
) -> None:
    """Assert sync drivers round-trip a SELECT through RustFS object storage."""
    if not case.supports_storage_bridge:
        pytest.skip(f"{case.adapter} has no verified storage-bridge support")
    from sqlspec.storage.registry import storage_registry
    from tests.fixtures.rustfs import rustfs_object_size
    from tests.integration.adapters._storage_bridge_helpers import register_rustfs_alias

    sync_driver = cast("SyncContractDriver", driver)
    table = case.table
    alias, prefix, destination, object_name = _storage_bridge_rustfs_names(case)

    storage_registry.clear()
    try:
        register_rustfs_alias(alias, rustfs_service, rustfs_bucket_name, prefix=prefix)
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
    from tests.integration.adapters._storage_bridge_helpers import register_rustfs_alias

    async_driver = cast("AsyncContractDriver", driver)
    table = case.table
    alias, prefix, destination, object_name = _storage_bridge_rustfs_names(case)

    storage_registry.clear()
    try:
        register_rustfs_alias(alias, rustfs_service, rustfs_bucket_name, prefix=prefix)
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
