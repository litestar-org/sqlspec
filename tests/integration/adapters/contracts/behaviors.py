"""Public behavior helpers for adapter-local and central contract tests."""

from typing import Any, Protocol, cast

import pytest

from sqlspec import SQLResult
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from tests.integration.adapters.contracts._assertions import assert_result_data, assert_sql_result
from tests.integration.adapters.contracts._cases import DriverCase
from tests.integration.adapters.contracts._inputs import ParameterProfileCase, ParameterStyleCase, StatementInputCase
from tests.integration.adapters.contracts._schema import DEFAULT_CONTRACT_TABLE, ContractRow, ContractTable


class SyncContractDriver(Protocol):
    """Sync driver surface used by adapter contract helpers."""

    def commit(self) -> None: ...

    def execute(self, statement: object, /, *parameters: object, **kwargs: Any) -> SQLResult: ...

    def execute_many(self, statement: object, parameters: object, /, **kwargs: Any) -> SQLResult: ...

    def execute_script(self, statement: object, /, *parameters: object, **kwargs: Any) -> SQLResult: ...

    def select_one(self, statement: object, /, *parameters: object, **kwargs: Any) -> dict[str, Any]: ...

    def select_one_or_none(self, statement: object, /, *parameters: object, **kwargs: Any) -> dict[str, Any] | None: ...

    def select_value(self, statement: object, /, *parameters: object, **kwargs: Any) -> object: ...


class AsyncContractDriver(Protocol):
    """Async driver surface used by adapter contract helpers."""

    async def commit(self) -> None: ...

    async def execute(self, statement: object, /, *parameters: object, **kwargs: Any) -> SQLResult: ...

    async def execute_many(self, statement: object, parameters: object, /, **kwargs: Any) -> SQLResult: ...

    async def execute_script(self, statement: object, /, *parameters: object, **kwargs: Any) -> SQLResult: ...

    async def select_one(self, statement: object, /, *parameters: object, **kwargs: Any) -> dict[str, Any]: ...

    async def select_one_or_none(
        self, statement: object, /, *parameters: object, **kwargs: Any
    ) -> dict[str, Any] | None: ...

    async def select_value(self, statement: object, /, *parameters: object, **kwargs: Any) -> object: ...


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


def assert_sync_statement_input_contract(driver: object, case: DriverCase, input_case: StatementInputCase) -> None:
    """Assert sync drivers return equivalent rows for one statement input shape."""
    sync_driver = cast("SyncContractDriver", driver)
    _seed_sync(sync_driver, input_case.setup_rows, case.table)

    statement = input_case.statement_factory()
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

    statement = input_case.statement_factory()
    result = await _execute_async(async_driver, statement, input_case.parameters)
    assert_result_data(result, input_case.expected_data)

    repeat_result = await _execute_async(async_driver, statement, input_case.parameters)
    assert_result_data(repeat_result, input_case.expected_data)


def assert_sync_parameter_contract(driver: object, case: DriverCase, parameter_case: ParameterProfileCase) -> None:
    """Assert sync drivers bind one parameter profile case correctly."""
    sync_driver = cast("SyncContractDriver", driver)
    _seed_sync(sync_driver, parameter_case.setup_rows, case.table)

    result = _execute_sync(sync_driver, parameter_case.statement, parameter_case.parameters)
    if parameter_case.expected_rows_affected is not None and _should_assert_execute_rows_affected(case):
        assert_sql_result(result, rows_affected=parameter_case.expected_rows_affected)
    if parameter_case.expected_result_data is not None:
        assert_result_data(result, parameter_case.expected_result_data)
    if parameter_case.verification_statement is not None and parameter_case.expected_verification_data is not None:
        verification = _execute_sync(
            sync_driver, parameter_case.verification_statement, parameter_case.verification_parameters
        )
        assert_result_data(verification, parameter_case.expected_verification_data)


async def assert_async_parameter_contract(
    driver: object, case: DriverCase, parameter_case: ParameterProfileCase
) -> None:
    """Assert async drivers bind one parameter profile case correctly."""
    async_driver = cast("AsyncContractDriver", driver)
    await _seed_async(async_driver, parameter_case.setup_rows, case.table)

    result = await _execute_async(async_driver, parameter_case.statement, parameter_case.parameters)
    if parameter_case.expected_rows_affected is not None and _should_assert_execute_rows_affected(case):
        assert_sql_result(result, rows_affected=parameter_case.expected_rows_affected)
    if parameter_case.expected_result_data is not None:
        assert_result_data(result, parameter_case.expected_result_data)
    if parameter_case.verification_statement is not None and parameter_case.expected_verification_data is not None:
        verification = await _execute_async(
            async_driver, parameter_case.verification_statement, parameter_case.verification_parameters
        )
        assert_result_data(verification, parameter_case.expected_verification_data)


def assert_sync_parameter_style_contract(
    driver: object, case: DriverCase, parameter_style_case: ParameterStyleCase
) -> None:
    """Assert sync drivers bind one parameter style case correctly."""
    sync_driver = cast("SyncContractDriver", driver)
    _seed_sync(sync_driver, parameter_style_case.setup_rows, case.table)

    if parameter_style_case.method == "execute_many":
        result = sync_driver.execute_many(parameter_style_case.statement, parameter_style_case.parameters)
    else:
        result = _execute_sync(sync_driver, parameter_style_case.statement, parameter_style_case.parameters)

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
            sync_driver, parameter_style_case.verification_statement, parameter_style_case.verification_parameters
        )
        assert_result_data(verification, parameter_style_case.expected_verification_data)


async def assert_async_parameter_style_contract(
    driver: object, case: DriverCase, parameter_style_case: ParameterStyleCase
) -> None:
    """Assert async drivers bind one parameter style case correctly."""
    async_driver = cast("AsyncContractDriver", driver)
    await _seed_async(async_driver, parameter_style_case.setup_rows, case.table)

    if parameter_style_case.method == "execute_many":
        result = await async_driver.execute_many(parameter_style_case.statement, parameter_style_case.parameters)
    else:
        result = await _execute_async(async_driver, parameter_style_case.statement, parameter_style_case.parameters)

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
            async_driver, parameter_style_case.verification_statement, parameter_style_case.verification_parameters
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

    result = sync_driver.execute_script("""
        INSERT INTO contract_items (name, value, note) VALUES ('script1', 10, NULL);
        INSERT INTO contract_items (name, value, note) VALUES ('script2', 20, NULL);
        UPDATE contract_items SET value = 30 WHERE name = 'script1';
    """)
    assert_sql_result(result)
    assert result.operation_type == "SCRIPT"
    assert_result_data(
        sync_driver.execute("SELECT name, value FROM contract_items WHERE name LIKE 'script%' ORDER BY name"),
        ({"name": "script1", "value": 30}, {"name": "script2", "value": 20}),
    )

    with pytest.raises(SQLParsingError):
        sync_driver.execute("SELCT * FROM contract_items")
    with pytest.raises(SQLSpecError):
        sync_driver.execute("SELECT * FROM missing_contract_table")


async def assert_async_script_error_contract(driver: object, case: DriverCase) -> None:
    """Assert async drivers execute scripts and normalize generic SQL errors."""
    async_driver = cast("AsyncContractDriver", driver)

    result = await async_driver.execute_script("""
        INSERT INTO contract_items (name, value, note) VALUES ('script1', 10, NULL);
        INSERT INTO contract_items (name, value, note) VALUES ('script2', 20, NULL);
        UPDATE contract_items SET value = 30 WHERE name = 'script1';
    """)
    assert_sql_result(result)
    assert result.operation_type == "SCRIPT"
    assert_result_data(
        await async_driver.execute("SELECT name, value FROM contract_items WHERE name LIKE 'script%' ORDER BY name"),
        ({"name": "script1", "value": 30}, {"name": "script2", "value": 20}),
    )

    with pytest.raises(SQLParsingError):
        await async_driver.execute("SELCT * FROM contract_items")
    with pytest.raises(SQLSpecError):
        await async_driver.execute("SELECT * FROM missing_contract_table")
