"""Live checks that concurrent migrators serialize through the lease lock."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pytest_databases.docker.cockroachdb import CockroachDBService
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgSyncConfig
from sqlspec.adapters.oracledb import OracleSyncConfig
from sqlspec.migrations.commands import SyncMigrationCommands

pytestmark = pytest.mark.xdist_group("lease_lock")

MIGRATION = '''"""Create a table once."""


def up(context=None):
    return ["CREATE TABLE {table} (id INTEGER)"]


def down(context=None):
    return ["DROP TABLE {table}"]
'''


def _write_migration(tmp_path: Path, table: str) -> Path:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_create.py").write_text(MIGRATION.format(table=table), encoding="utf-8")
    return migrations_dir


def _upgrade_concurrently(make_config: Any, migrations_dir: Path, version_table: str) -> list[str]:
    def run(_: int) -> str:
        config = make_config({
            "script_location": str(migrations_dir),
            "version_table_name": version_table,
            "enabled": True,
        })
        try:
            SyncMigrationCommands(config).upgrade(echo=False)
        finally:
            config.close_pool()
        return "ok"

    with ThreadPoolExecutor(max_workers=2) as pool:
        return list(pool.map(run, range(2)))


def test_cockroach_concurrent_upgrades_apply_once(tmp_path: Path, cockroachdb_service: CockroachDBService) -> None:
    suffix = uuid4().hex[:8]
    table, version_table = f"lease_items_{suffix}", f"lease_versions_{suffix}"
    migrations_dir = _write_migration(tmp_path, table)
    sslmode = cockroachdb_service.driver_opts.get("sslmode", "disable")
    conninfo = (
        f"host={cockroachdb_service.host} port={cockroachdb_service.port} "
        f"user=root dbname={cockroachdb_service.database} sslmode={sslmode}"
    )

    def make_config(migration_config: dict[str, Any]) -> CockroachPsycopgSyncConfig:
        return CockroachPsycopgSyncConfig(connection_config={"conninfo": conninfo}, migration_config=migration_config)

    assert _upgrade_concurrently(make_config, migrations_dir, version_table) == ["ok", "ok"]

    verifier = make_config({})
    try:
        with verifier.provide_session() as session:
            rows = session.select(f"SELECT version_num FROM {version_table}")
    finally:
        verifier.close_pool()
    assert [row["version_num"] for row in rows] == ["0001"]


def test_oracle_concurrent_upgrades_apply_once(tmp_path: Path, oracle_23ai_service: OracleService) -> None:
    suffix = uuid4().hex[:8]
    table, version_table = f"LEASE_ITEMS_{suffix}", f"lease_versions_{suffix}"
    migrations_dir = _write_migration(tmp_path, table)

    def make_config(migration_config: dict[str, Any]) -> OracleSyncConfig:
        return OracleSyncConfig(
            connection_config={
                "host": oracle_23ai_service.host,
                "port": oracle_23ai_service.port,
                "service_name": oracle_23ai_service.service_name,
                "user": oracle_23ai_service.user,
                "password": oracle_23ai_service.password,
            },
            migration_config=migration_config,
        )

    assert _upgrade_concurrently(make_config, migrations_dir, version_table) == ["ok", "ok"]

    verifier = make_config({})
    try:
        with verifier.provide_session() as session:
            rows = session.select(f"SELECT version_num FROM {version_table}")
    finally:
        verifier.close_pool()
    assert [next(iter(row.values())) for row in rows] == ["0001"]
