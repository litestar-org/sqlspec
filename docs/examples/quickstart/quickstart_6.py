from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig

db_manager = SQLSpec()

# Register multiple databases
sqlite_db = db_manager.add_config(SqliteConfig(pool_config={"database": "app.db"}))
duckdb_db = db_manager.add_config(DuckDBConfig(pool_config={"database": "analytics.duckdb"}))

# Use different databases
with db_manager.provide_session(sqlite_db) as sqlite_session:
    users = sqlite_session.select("SELECT * FROM users")

with db_manager.provide_session(duckdb_db) as duckdb_session:
    analytics = duckdb_session.select("SELECT * FROM events")
