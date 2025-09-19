"""Example demonstrating how to use Litestar extension migrations with SQLSpec.

This example shows how to configure SQLSpec to include Litestar's session table
migrations, which will create dialect-specific tables when you run migrations.
"""

from pathlib import Path

from litestar import Litestar

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.extensions.litestar.plugin import SQLSpec
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import MigrationCommands

# Configure database with extension migrations enabled
db_config = SqliteConfig(
    pool_config={"database": "app.db"},
    migration_config={
        "script_location": "migrations",
        "version_table_name": "ddl_migrations",
        # Enable Litestar extension migrations
        "include_extensions": ["litestar"],
    },
)

# Create SQLSpec plugin with session store
sqlspec_plugin = SQLSpec(db_config)

# Configure session store to use the database
session_store = SQLSpecSessionStore(
    config=db_config,
    table_name="litestar_sessions",  # Matches migration table name
)

# Create Litestar app with SQLSpec and sessions
app = Litestar(plugins=[sqlspec_plugin], stores={"sessions": session_store})


def run_migrations() -> None:
    """Run database migrations including extension migrations.

    This will:
    1. Create your project's migrations (from migrations/ directory)
    2. Create Litestar extension migrations (session table with dialect-specific types)
    """
    commands = MigrationCommands(db_config)

    # Initialize migrations directory if it doesn't exist
    migrations_dir = Path("migrations")
    if not migrations_dir.exists():
        commands.init("migrations")

    # Run all migrations including extension migrations
    # The session table will be created with:
    # - JSONB for PostgreSQL
    # - JSON for MySQL/MariaDB
    # - TEXT for SQLite
    commands.upgrade()

    # Check current version
    current = commands.current(verbose=True)
    print(f"Current migration version: {current}")


if __name__ == "__main__":
    # Run migrations before starting the app
    run_migrations()

    # Start the application
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
