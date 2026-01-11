"""Storage bridge integration tests for SQLite adapter."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.sqlite import SqliteDriver

pytestmark = pytest.mark.xdist_group("sqlite")


def test_sqlite_load_from_arrow(sqlite_session: SqliteDriver) -> None:
    sqlite_session.execute("DROP TABLE IF EXISTS storage_bridge_sqlite")
    sqlite_session.execute("CREATE TABLE storage_bridge_sqlite (id INTEGER PRIMARY KEY, label TEXT)")

    arrow_table = pa.table({"id": [1, 2], "label": ["alpha", "beta"]})

    job = sqlite_session.load_from_arrow("storage_bridge_sqlite", arrow_table, overwrite=True)

    assert job.telemetry["destination"] == "storage_bridge_sqlite"
    assert job.telemetry["rows_processed"] == arrow_table.num_rows

    result = sqlite_session.execute("SELECT id, label FROM storage_bridge_sqlite ORDER BY id")
    assert result.data == [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}]


def test_sqlite_load_from_storage(sqlite_session: SqliteDriver, tmp_path: Path) -> None:
    sqlite_session.execute("DROP TABLE IF EXISTS storage_bridge_sqlite")
    sqlite_session.execute("CREATE TABLE storage_bridge_sqlite (id INTEGER PRIMARY KEY, label TEXT)")

    arrow_table = pa.table({"id": [10, 11], "label": ["gamma", "delta"]})
    destination = tmp_path / "sqlite-bridge.parquet"
    pq.write_table(arrow_table, destination)

    job = sqlite_session.load_from_storage(
        "storage_bridge_sqlite", str(destination), file_format="parquet", overwrite=True
    )

    assert job.telemetry["extra"]["source"]["destination"].endswith("sqlite-bridge.parquet")  # type: ignore[index]
    assert job.telemetry["extra"]["source"]["backend"]  # type: ignore[index]

    result = sqlite_session.execute("SELECT id, label FROM storage_bridge_sqlite ORDER BY id")
    assert result.data == [{"id": 10, "label": "gamma"}, {"id": 11, "label": "delta"}]
