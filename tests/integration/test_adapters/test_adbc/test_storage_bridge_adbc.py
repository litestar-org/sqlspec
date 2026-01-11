"""Storage bridge integration tests for the ADBC adapter."""

from pathlib import Path

import pytest

from sqlspec.adapters.adbc import AdbcDriver
from sqlspec.storage.registry import storage_registry

pytestmark = [pytest.mark.xdist_group("storage"), pytest.mark.postgres]


def _prepare_tables(session: AdbcDriver, source: str, target: str) -> None:
    session.execute_script(
        f"""
        DROP TABLE IF EXISTS {target};
        DROP TABLE IF EXISTS {source};
        CREATE TABLE {source} (
            id INT PRIMARY KEY,
            label TEXT NOT NULL
        );
        CREATE TABLE {target} (
            id INT PRIMARY KEY,
            label TEXT NOT NULL
        );
        """
    )


def _seed_source(session: AdbcDriver, source: str) -> None:
    session.execute(f"INSERT INTO {source} (id, label) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")


@pytest.mark.usefixtures("adbc_postgresql_session")
def test_adbc_postgres_storage_bridge_round_trip(tmp_path: Path, adbc_postgresql_session: AdbcDriver) -> None:
    source_table = "storage_bridge_adbc_source"
    target_table = "storage_bridge_adbc_target"
    alias = "adbc_storage_bridge_local"
    storage_registry.register_alias(alias, f"file://{tmp_path}", backend="local")
    destination = f"alias://{alias}/adbc_storage_bridge.parquet"

    session = adbc_postgresql_session
    _prepare_tables(session, source_table, target_table)
    _seed_source(session, source_table)

    export_job = session.select_to_storage(
        f"SELECT id, label FROM {source_table} ORDER BY id", destination, format_hint="parquet"
    )
    assert export_job.telemetry["rows_processed"] == 3
    destination_path = tmp_path / "adbc_storage_bridge.parquet"
    assert destination_path.exists()

    load_job = session.load_from_storage(target_table, destination, file_format="parquet", overwrite=True)
    assert load_job.telemetry["rows_processed"] == 3
    assert load_job.telemetry["destination"] == target_table

    result = session.execute(f"SELECT id, label FROM {target_table} ORDER BY id")
    assert [(row["id"], row["label"]) for row in result] == [(1, "alpha"), (2, "beta"), (3, "gamma")]

    session.execute_script(f"DROP TABLE IF EXISTS {target_table}; DROP TABLE IF EXISTS {source_table};")
    storage_registry.clear()
