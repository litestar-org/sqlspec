"""GizmoSQL data dictionary and migration integration tests for ADBC."""

from pathlib import Path
from typing import Any, cast

import pytest

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.data_dictionary import VersionInfo
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands, create_migration_commands
from tests.integration.fixtures.adbc import xfail_if_driver_missing

pytestmark = [pytest.mark.adbc, pytest.mark.xdist_group("gizmosql")]


def _get_objects_dict_list(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [cast("dict[str, Any]", item) for item in value if isinstance(item, dict)]


def _find_get_objects_table(rows: list[dict[str, Any]], table_name: str) -> dict[str, Any] | None:
    for catalog in rows:
        for db_schema in _get_objects_dict_list(catalog.get("catalog_db_schemas")):
            for table in _get_objects_dict_list(db_schema.get("db_schema_tables")):
                if table.get("table_name") == table_name:
                    return table
    return None


@xfail_if_driver_missing
def test_gizmosql_data_dictionary_tables_columns_and_indexes(adbc_gizmosql_session: AdbcDriver) -> None:
    """GizmoSQL should expose DuckDB table and column metadata through the ADBC dictionary."""
    adbc_gizmosql_session.execute_script(
        """
            DROP TABLE IF EXISTS gizmosql_dd_child_adbc;
            DROP TABLE IF EXISTS gizmosql_dd_parent_adbc;
            CREATE TABLE gizmosql_dd_parent_adbc (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE gizmosql_dd_child_adbc (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL,
                amount DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (parent_id) REFERENCES gizmosql_dd_parent_adbc (id)
            );
        """
    )
    adbc_gizmosql_session.commit()

    dictionary = adbc_gizmosql_session.data_dictionary
    version = dictionary.get_version(adbc_gizmosql_session)
    assert isinstance(version, VersionInfo)
    assert version.major >= 1

    schema_rows = adbc_gizmosql_session.execute(
        """
            SELECT DISTINCT table_schema
            FROM information_schema.tables
            WHERE table_name IN ('gizmosql_dd_parent_adbc', 'gizmosql_dd_child_adbc')
            ORDER BY table_schema
        """
    ).get_data()
    assert schema_rows
    schema_name = schema_rows[0]["table_schema"]

    raw_objects = (
        adbc_gizmosql_session.connection
        .adbc_get_objects(depth="all", db_schema_filter=schema_name, table_name_filter="gizmosql_dd_child_adbc")
        .read_all()
        .to_pylist()
    )
    assert isinstance(raw_objects, list)
    raw_child = _find_get_objects_table(raw_objects, "gizmosql_dd_child_adbc")
    assert raw_child is not None
    assert {column["column_name"] for column in raw_child.get("table_columns") or []} == {
        "id",
        "parent_id",
        "amount",
        "created_at",
    }
    constraints = raw_child.get("table_constraints") or []
    if not constraints:
        pytest.xfail("GizmoSQL FlightSQL GetObjects did not return constraints; SQLSpec must use central fallback")
    assert any(str(constraint.get("constraint_type")).upper() == "FOREIGN KEY" for constraint in constraints)

    table_names = {table["table_name"] for table in dictionary.get_tables(adbc_gizmosql_session, schema=schema_name)}
    assert {"gizmosql_dd_parent_adbc", "gizmosql_dd_child_adbc"}.issubset(table_names)

    columns = dictionary.get_columns(adbc_gizmosql_session, table="gizmosql_dd_child_adbc", schema=schema_name)
    columns_by_name = {column["column_name"]: column for column in columns}
    assert set(columns_by_name) == {"id", "parent_id", "amount", "created_at"}
    assert columns_by_name["id"]["data_type"].upper() == "INTEGER"
    assert columns_by_name["amount"]["data_type"].upper() == "DOUBLE"
    assert columns_by_name["parent_id"]["is_nullable"] == "NO"

    foreign_keys = dictionary.get_foreign_keys(
        adbc_gizmosql_session, table="gizmosql_dd_child_adbc", schema=schema_name
    )
    assert any(
        key.column_name == "parent_id" and key.referenced_table == "gizmosql_dd_parent_adbc" for key in foreign_keys
    )

    indexes = dictionary.get_indexes(adbc_gizmosql_session, table="gizmosql_dd_child_adbc", schema=schema_name)
    assert indexes == []


@xfail_if_driver_missing
def test_gizmosql_migration_smoke(tmp_path: Path, adbc_gizmosql_connection_config: dict[str, Any]) -> None:
    """GizmoSQL should run sqlspec migrations and no-op repeated upgrades."""
    migration_dir = tmp_path / "gizmosql_migrations"
    version_table = "gizmosql_migrations_adbc"
    migrated_table = "gizmosql_migrated_items_adbc"
    config = AdbcConfig(
        connection_config=dict(adbc_gizmosql_connection_config),
        migration_config={"script_location": str(migration_dir), "version_table_name": version_table},
    )
    commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(config)

    try:
        with config.provide_session() as driver:
            driver.execute(f"DROP TABLE IF EXISTS {migrated_table}")
            driver.execute(f"DROP TABLE IF EXISTS {version_table}")

        commands.init(str(migration_dir), package=True)
        migration_content = f'''"""Create GizmoSQL migration smoke table."""


def up():
    """Create and seed migration smoke table."""
    return [
        """CREATE TABLE {migrated_table} (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )""",
        "INSERT INTO {migrated_table} (id, name) VALUES (1, 'created-by-migration')",
    ]


def down():
    """Drop migration smoke table."""
    return ["DROP TABLE IF EXISTS {migrated_table}"]
'''
        (migration_dir / "0001_create_gizmosql_items.py").write_text(migration_content)

        commands.upgrade()
        commands.upgrade()

        with config.provide_session() as driver:
            data = driver.execute(f"SELECT id, name FROM {migrated_table} ORDER BY id").get_data()
            assert data == [{"id": 1, "name": "created-by-migration"}]

            versions = driver.execute(f"SELECT version_num FROM {version_table} ORDER BY version_num").get_data()
            assert versions == [{"version_num": "0001"}]
    finally:
        with config.provide_session() as driver:
            driver.execute(f"DROP TABLE IF EXISTS {migrated_table}")
            driver.execute(f"DROP TABLE IF EXISTS {version_table}")
        config.close_pool()
