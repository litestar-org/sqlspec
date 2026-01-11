"""Storage bridge integration tests for AioSQLite adapter."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteDriver

pytestmark = [pytest.mark.asyncio, pytest.mark.xdist_group("sqlite")]


async def test_aiosqlite_load_from_arrow(aiosqlite_session: AiosqliteDriver) -> None:
    await aiosqlite_session.execute("DROP TABLE IF EXISTS storage_bridge_aiosqlite")
    await aiosqlite_session.execute("CREATE TABLE storage_bridge_aiosqlite (id INTEGER PRIMARY KEY, label TEXT)")

    arrow_table = pa.table({"id": [1, 2], "label": ["north", "south"]})

    job = await aiosqlite_session.load_from_arrow("storage_bridge_aiosqlite", arrow_table, overwrite=True)

    assert job.telemetry["rows_processed"] == arrow_table.num_rows

    result = await aiosqlite_session.execute("SELECT id, label FROM storage_bridge_aiosqlite ORDER BY id")
    assert result.data == [{"id": 1, "label": "north"}, {"id": 2, "label": "south"}]


async def test_aiosqlite_load_from_storage(aiosqlite_session: AiosqliteDriver, tmp_path: Path) -> None:
    await aiosqlite_session.execute("DROP TABLE IF EXISTS storage_bridge_aiosqlite")
    await aiosqlite_session.execute("CREATE TABLE storage_bridge_aiosqlite (id INTEGER PRIMARY KEY, label TEXT)")

    arrow_table = pa.table({"id": [3, 4], "label": ["east", "west"]})
    destination = tmp_path / "aiosqlite-bridge.parquet"
    pq.write_table(arrow_table, destination)

    job = await aiosqlite_session.load_from_storage(
        "storage_bridge_aiosqlite", str(destination), file_format="parquet", overwrite=True
    )

    assert job.telemetry["extra"]["source"]["destination"].endswith("aiosqlite-bridge.parquet")  # type: ignore[index]
    assert job.telemetry["extra"]["source"]["backend"]  # type: ignore[index]

    result = await aiosqlite_session.execute("SELECT id, label FROM storage_bridge_aiosqlite ORDER BY id")
    assert result.data == [{"id": 3, "label": "east"}, {"id": 4, "label": "west"}]
