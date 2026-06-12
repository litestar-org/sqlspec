"""GizmoSQL Arrow and storage bridge integration tests for ADBC."""

from pathlib import Path

import pyarrow as pa
import pytest

from sqlspec.adapters.adbc import AdbcDriver
from sqlspec.storage.registry import storage_registry
from tests.integration.adapters.adbc.conftest import xfail_if_driver_missing

pytestmark = [pytest.mark.adbc, pytest.mark.xdist_group("gizmosql")]


def _seed_arrow_rows(session: AdbcDriver) -> None:
    rows = [(101, "arrow-one", 11), (102, "arrow-two", 22), (103, "arrow-three", 33)]
    session.execute_many("INSERT INTO test_table_adbc (id, name, value) VALUES (?, ?, ?)", rows)


@xfail_if_driver_missing
def test_gizmosql_select_to_arrow_table(adbc_gizmosql_session: AdbcDriver) -> None:
    """GizmoSQL should expose FlightSQL results through sqlspec's Arrow API."""
    _seed_arrow_rows(adbc_gizmosql_session)

    result = adbc_gizmosql_session.select_to_arrow(
        "SELECT id, name, value FROM test_table_adbc WHERE id >= ? ORDER BY id", (101,)
    )
    table = result.get_data()

    assert result.rows_affected == 3
    assert result.column_names == ["id", "name", "value"]
    assert table.to_pylist() == [
        {"id": 101, "name": "arrow-one", "value": 11},
        {"id": 102, "name": "arrow-two", "value": 22},
        {"id": 103, "name": "arrow-three", "value": 33},
    ]


@xfail_if_driver_missing
def test_gizmosql_select_to_arrow_record_batches(adbc_gizmosql_session: AdbcDriver) -> None:
    """GizmoSQL Arrow results should support batch-shaped returns."""
    _seed_arrow_rows(adbc_gizmosql_session)

    result = adbc_gizmosql_session.select_to_arrow(
        "SELECT id, name FROM test_table_adbc WHERE id >= ? ORDER BY id", (101,), return_format="batches", batch_size=2
    )
    batches = result.get_data()

    assert len(batches) >= 1
    assert sum(batch.num_rows for batch in batches) == 3
    assert batches[0].schema.names == ["id", "name"]


@xfail_if_driver_missing
def test_gizmosql_load_from_arrow_round_trip(adbc_gizmosql_session: AdbcDriver) -> None:
    """GizmoSQL should ingest Arrow tables through the ADBC storage bridge."""
    arrow_table = pa.table({
        "id": [201, 202, 203],
        "label": ["ingest-one", "ingest-two", "ingest-three"],
        "amount": [1.5, 2.5, 3.5],
    })

    job = adbc_gizmosql_session.load_from_arrow("gizmosql_arrow_ingest_adbc", arrow_table, overwrite=True)

    assert job.telemetry["rows_processed"] == 3
    result = adbc_gizmosql_session.execute("SELECT id, label, amount FROM gizmosql_arrow_ingest_adbc ORDER BY id")
    assert result.get_data() == [
        {"id": 201, "label": "ingest-one", "amount": 1.5},
        {"id": 202, "label": "ingest-two", "amount": 2.5},
        {"id": 203, "label": "ingest-three", "amount": 3.5},
    ]


@xfail_if_driver_missing
def test_gizmosql_storage_bridge_round_trip(tmp_path: Path, adbc_gizmosql_session: AdbcDriver) -> None:
    """GizmoSQL should export to local Parquet and load it back through ADBC."""
    _seed_arrow_rows(adbc_gizmosql_session)
    alias = "gizmosql_storage_bridge_local"
    storage_registry.register_alias(alias, f"file://{tmp_path}", backend="local")
    destination = f"alias://{alias}/gizmosql_storage_bridge.parquet"

    try:
        export_job = adbc_gizmosql_session.select_to_storage(
            "SELECT id, name, value FROM test_table_adbc WHERE id >= ? ORDER BY id",
            destination,
            (101,),
            format_hint="parquet",
        )
        assert export_job.telemetry["rows_processed"] == 3
        assert (tmp_path / "gizmosql_storage_bridge.parquet").exists()

        load_job = adbc_gizmosql_session.load_from_storage(
            "gizmosql_storage_bridge_target_adbc", destination, file_format="parquet", overwrite=True
        )
        assert load_job.telemetry["rows_processed"] == 3

        result = adbc_gizmosql_session.execute(
            "SELECT id, name, value FROM gizmosql_storage_bridge_target_adbc ORDER BY id"
        )
        assert result.get_data() == [
            {"id": 101, "name": "arrow-one", "value": 11},
            {"id": 102, "name": "arrow-two", "value": 22},
            {"id": 103, "name": "arrow-three", "value": 33},
        ]
    finally:
        storage_registry.clear()
