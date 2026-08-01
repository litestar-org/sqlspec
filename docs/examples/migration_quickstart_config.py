from sqlspec.adapters.sqlite import SqliteConfig

database_config = SqliteConfig(
    bind_key="app",
    connection_config={"database": "app.db"},
    migration_config={"script_location": "migrations", "version_table_name": "schema_versions"},
)
