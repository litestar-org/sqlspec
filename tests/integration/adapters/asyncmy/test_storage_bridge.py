"""Adapter-specific storage bridge coverage for AsyncMy.

Shared load_from_arrow + local parquet load_from_storage round-trips live in
tests/integration/adapters/contracts/test_storage_bridge_contract.py. This file keeps the
MySQL-specific DECIMAL type-fidelity load_from_storage case.
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.asyncmy import AsyncmyDriver

pytestmark = [pytest.mark.xdist_group("mysql")]


async def test_asyncmy_load_from_storage_decimal(tmp_path: Path, asyncmy_driver: AsyncmyDriver) -> None:
    await asyncmy_driver.execute("DROP TABLE IF EXISTS storage_bridge_scores")
    await asyncmy_driver.execute("CREATE TABLE storage_bridge_scores (id INT PRIMARY KEY, score DECIMAL(5,2))")

    arrow_table = pa.table({"id": [5, 6], "score": [12.5, 99.1]})
    destination = tmp_path / "scores.parquet"
    pq.write_table(arrow_table, destination)

    job = await asyncmy_driver.load_from_storage(
        "storage_bridge_scores", str(destination), file_format="parquet", overwrite=True
    )

    assert job.telemetry["destination"] == "storage_bridge_scores"
    assert job.telemetry["extra"]["source"]["destination"].endswith("scores.parquet")  # type: ignore[index]
    assert job.telemetry["extra"]["source"]["backend"]  # type: ignore[index]

    rows = await asyncmy_driver.select("SELECT id, score FROM storage_bridge_scores ORDER BY id")
    assert len(rows) == 2
    assert rows[0]["id"] == 5
    assert float(rows[0]["score"]) == pytest.approx(12.5)
    assert rows[1]["id"] == 6
    assert float(rows[1]["score"]) == pytest.approx(99.1)

    await asyncmy_driver.execute("DROP TABLE IF EXISTS storage_bridge_scores")
