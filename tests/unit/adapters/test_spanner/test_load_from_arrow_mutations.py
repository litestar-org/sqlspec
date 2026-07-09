"""Spanner load_from_arrow mutations transport (insert_or_update)."""

from typing import Any, cast

import pyarrow as pa
import pytest

import sqlspec.adapters.spanner.driver as spanner_driver
from sqlspec.adapters.spanner.driver import SpannerSyncDriver

CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": ["fixed"],
}


class _FakeTransaction:
    def __init__(self) -> None:
        self.insert_or_update_calls: list[tuple[str, list[str], list[list[Any]]]] = []
        self.execute_update_calls: list[str] = []
        self.committed = None

    def insert_or_update(self, table: str, columns: Any, values: Any) -> None:
        self.insert_or_update_calls.append((table, list(columns), [list(v) for v in values]))

    def execute_update(self, sql: str, params: Any = None, param_types: Any = None, **kwargs: Any) -> int:
        self.execute_update_calls.append(sql)
        return 0


@pytest.fixture
def mutations_driver(monkeypatch: pytest.MonkeyPatch) -> SpannerSyncDriver:
    monkeypatch.setattr(spanner_driver, "SpannerTransaction", _FakeTransaction)
    return SpannerSyncDriver(cast("Any", _FakeTransaction()), driver_features={"storage_capabilities": CAPABILITIES})


def test_load_from_arrow_uses_single_insert_or_update(mutations_driver: SpannerSyncDriver) -> None:
    txn = cast("_FakeTransaction", mutations_driver.connection)
    arrow_table = pa.table({"id": list(range(50)), "name": [f"n{i}" for i in range(50)]})

    result = mutations_driver.load_from_arrow("users", arrow_table)

    assert result.telemetry["rows_processed"] == 50
    assert len(txn.insert_or_update_calls) == 1
    table, columns, values = txn.insert_or_update_calls[0]
    assert table == "users"
    assert columns == ["id", "name"]
    assert len(values) == 50
    assert values[0] == [0, "n0"]


def test_load_from_arrow_chunks_above_cell_cap(mutations_driver: SpannerSyncDriver) -> None:
    txn = cast("_FakeTransaction", mutations_driver.connection)
    columns = {f"c{c}": list(range(8001)) for c in range(10)}
    arrow_table = pa.table(columns)

    mutations_driver.load_from_arrow("wide", arrow_table)

    assert len(txn.insert_or_update_calls) == 2
    assert len(txn.insert_or_update_calls[0][2]) == 8000
    assert len(txn.insert_or_update_calls[1][2]) == 1


def test_load_from_arrow_overwrite_deletes_then_mutates(mutations_driver: SpannerSyncDriver) -> None:
    txn = cast("_FakeTransaction", mutations_driver.connection)
    arrow_table = pa.table({"id": [1, 2], "name": ["a", "b"]})

    mutations_driver.load_from_arrow("users", arrow_table, overwrite=True)

    assert txn.execute_update_calls
    assert "DELETE FROM users WHERE TRUE" in txn.execute_update_calls[0]
    assert len(txn.insert_or_update_calls) == 1


def test_load_from_arrow_rerun_is_idempotent(mutations_driver: SpannerSyncDriver) -> None:
    txn = cast("_FakeTransaction", mutations_driver.connection)
    arrow_table = pa.table({"id": [1, 2], "name": ["a", "b"]})

    mutations_driver.load_from_arrow("users", arrow_table)
    mutations_driver.load_from_arrow("users", arrow_table)

    assert len(txn.insert_or_update_calls) == 2
