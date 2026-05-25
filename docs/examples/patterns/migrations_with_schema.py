from pathlib import Path

__all__ = ("test_migrations_with_schema",)


def test_migrations_with_schema(tmp_path: Path) -> None:
    # start-example
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.migrations.commands import SyncMigrationCommands

    migration_dir = tmp_path / "migrations"
    db_path = tmp_path / "app.duckdb"

    config = DuckDBConfig(
        connection_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": "schema_versions",
            "default_schema": "app_schema",
            "version_table_schema": "admin_schema",
        },
    )

    try:
        with config.provide_session() as session:
            session.execute("CREATE SCHEMA app_schema")
            session.execute("CREATE SCHEMA admin_schema")

        commands = SyncMigrationCommands(config)
        commands.init(str(migration_dir), package=True)

        (migration_dir / "0001_create_users.py").write_text(
            '''"""Create users."""


def up():
    """Create an unqualified table in app_schema."""
    return ["CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)"]


def down():
    """Drop the unqualified table from app_schema."""
    return ["DROP TABLE IF EXISTS users"]
'''
        )

        commands.upgrade()

        with config.provide_session() as session:
            users_table = session.select_value(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
                """,
                ("app_schema", "users"),
            )
            tracker_table = session.select_value(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
                """,
                ("admin_schema", "schema_versions"),
            )

        assert users_table == "users"
        assert tracker_table == "schema_versions"
    finally:
        if config.connection_instance:
            config.close_pool()
    # end-example
