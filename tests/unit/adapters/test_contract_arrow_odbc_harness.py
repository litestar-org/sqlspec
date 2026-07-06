"""Unit coverage for arrow-odbc contract harness capability gates."""

from typing import Any, cast

import pytest

from tests.integration.adapters.contracts import behaviors
from tests.integration.adapters.contracts._cases import DriverCase, get_driver_case
from tests.integration.adapters.contracts._inputs import PARAMETER_STYLE_CASES
from tests.integration.adapters.contracts._schema import DEFAULT_CONTRACT_TABLE, ContractRow
from tests.integration.adapters.contracts.behaviors import SyncContractDriver


class _BulkOnlyDriver:
    def __init__(self) -> None:
        self.commits = 0
        self.execute_many_calls: list[tuple[object, object]] = []
        self.load_from_arrow_calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def execute_many(self, statement: object, parameters: object, /, **kwargs: Any) -> object:
        self.execute_many_calls.append((statement, parameters))
        msg = "execute_many should not be used for this case"
        raise AssertionError(msg)

    def load_from_arrow(self, table: str, source: Any, /, **kwargs: Any) -> object:
        self.load_from_arrow_calls.append((table, source.to_pydict(), kwargs))
        return object()

    def commit(self) -> None:
        self.commits += 1


def _bulk_only_case() -> DriverCase:
    return DriverCase(
        id="bulk-only-sync",
        fixture_name="contract_bulk_only_driver",
        adapter="arrow_odbc",
        dialect="mssql",
        mode="sync",
        supports_execute_many=False,
        supports_native_bulk_ingest=True,
    )


def test_arrow_odbc_sync_case_is_active_sql_server_bulk_only() -> None:
    """The arrow-odbc contract case is active against SQL Server without row execute_many."""
    case = get_driver_case("arrow-odbc-sync")

    assert case.integration_status == "active"
    assert case.fixture_name == "contract_arrow_odbc_mssql_driver"
    assert case.dialect == "mssql"
    assert case.supports_arrow
    assert case.supports_arrow_streaming
    assert case.supports_native_arrow
    assert case.supports_native_bulk_ingest
    assert not case.supports_execute_many
    assert not case.supports_load_from_records
    assert case.execute_rowcount_policy == "unavailable"
    assert not case.supports_stream_reopen_after_partial_iteration


def test_seed_sync_uses_arrow_bulk_ingest_when_execute_many_is_disabled() -> None:
    """Contract seeding uses native Arrow ingest for sync cases without execute_many."""
    driver = _BulkOnlyDriver()

    behaviors._seed_sync(  # pyright: ignore[reportPrivateUsage]
        cast("SyncContractDriver", driver),
        (ContractRow("alpha", 10, None), ContractRow("beta", 20, "note")),
        DEFAULT_CONTRACT_TABLE,
        _bulk_only_case(),
    )

    assert driver.execute_many_calls == []
    assert driver.commits == 1
    assert driver.load_from_arrow_calls == [
        ("contract_items", {"name": ["alpha", "beta"], "value": [10, 20], "note": [None, "note"]}, {"overwrite": True})
    ]


def test_parameter_style_execute_many_case_skips_when_execute_many_is_disabled() -> None:
    """Parameter-style cases that call execute_many skip for bulk-only sync drivers."""
    driver = _BulkOnlyDriver()
    parameter_style_case = next(case for case in PARAMETER_STYLE_CASES if case.method == "execute_many")

    with pytest.raises(pytest.skip.Exception):
        behaviors.assert_sync_parameter_style_contract(
            cast("SyncContractDriver", driver), _bulk_only_case(), parameter_style_case
        )
