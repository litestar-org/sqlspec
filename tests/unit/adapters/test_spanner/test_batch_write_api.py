"""Spanner opt-in Batch Write API transport for load_from_arrow."""

from typing import Any, cast

import pyarrow as pa
import pytest
from typing_extensions import Self

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


class _FakeGroup:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[str], list[list[Any]]]] = []

    def insert_or_update(self, table: str, columns: Any, values: Any) -> None:
        self.calls.append((table, list(columns), [list(v) for v in values]))


class _FakeMutationGroups:
    def __init__(self) -> None:
        self.groups: list[_FakeGroup] = []
        self.batch_write_calls = 0
        self.entered = False
        self.exited = False

    def __enter__(self) -> Self:
        self.entered = True
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.exited = True

    def group(self) -> _FakeGroup:
        group = _FakeGroup()
        self.groups.append(group)
        return group

    def batch_write(self, request_options: Any = None, exclude_txn_from_change_streams: bool = False) -> list[Any]:
        self.batch_write_calls += 1
        return []


class _FakeDatabase:
    def __init__(self) -> None:
        self.mutation_groups_obj = _FakeMutationGroups()

    def mutation_groups(self) -> _FakeMutationGroups:
        return self.mutation_groups_obj


class _FakeSession:
    def __init__(self, database: _FakeDatabase) -> None:
        self._database = database


class _FakeBatchTransaction:
    def __init__(self) -> None:
        self.database = _FakeDatabase()
        self._session = _FakeSession(self.database)
        self.execute_update_calls: list[str] = []
        self.insert_or_update_calls: list[tuple[str, list[str], list[list[Any]]]] = []

    def execute_update(self, sql: str, params: Any = None, param_types: Any = None, **kwargs: Any) -> int:
        self.execute_update_calls.append(sql)
        return 0

    def insert_or_update(self, table: str, columns: Any, values: Any) -> None:
        self.insert_or_update_calls.append((table, list(columns), [list(v) for v in values]))


@pytest.fixture
def batch_write_driver(monkeypatch: pytest.MonkeyPatch) -> SpannerSyncDriver:
    monkeypatch.setattr(spanner_driver, "Transaction", _FakeBatchTransaction)
    return SpannerSyncDriver(
        cast("Any", _FakeBatchTransaction()),
        driver_features={"storage_capabilities": CAPABILITIES, "enable_batch_write_api": True},
    )


def test_batch_write_used_for_ingest(batch_write_driver: SpannerSyncDriver) -> None:
    conn = cast("_FakeBatchTransaction", batch_write_driver.connection)
    job = batch_write_driver.load_from_arrow("users", pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}))

    assert job.telemetry["rows_processed"] == 3
    groups = conn.database.mutation_groups_obj
    assert groups.entered is True
    assert groups.exited is True
    assert groups.batch_write_calls == 1
    assert len(groups.groups) == 1
    assert groups.groups[0].calls[0] == ("users", ["id", "name"], [[1, "a"], [2, "b"], [3, "c"]])
    assert conn.execute_update_calls == []


def test_batch_write_splits_before_crossing_mutation_group_cell_cap(batch_write_driver: SpannerSyncDriver) -> None:
    columns = ["a", "b", "c"]
    records = [(index, index + 1, index + 2) for index in range(26_667)]

    chunks = batch_write_driver._chunk_mutation_rows(columns, records)

    assert [len(chunk) for chunk in chunks] == [26_666, 1]


def test_batch_write_overwrite_uses_transactional_mutations(batch_write_driver: SpannerSyncDriver) -> None:
    conn = cast("_FakeBatchTransaction", batch_write_driver.connection)
    batch_write_driver.load_from_arrow("users", pa.table({"id": [1]}), overwrite=True)

    assert conn.execute_update_calls and "DELETE FROM users WHERE TRUE" in conn.execute_update_calls[0]
    assert conn.insert_or_update_calls == [("users", ["id"], [[1]])]
    assert conn.database.mutation_groups_obj.batch_write_calls == 0
