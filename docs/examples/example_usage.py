#!/usr/bin/env python3
# ruff: noqa: T201, EXE001, ERA001
"""Example demonstrating the improved SQLSpec adapter architecture.

This example shows:
1. TypedDict configuration for better maintainability
2. DictRow as the default row type
3. Statement configuration on drivers
4. Simplified 2-file adapter structure
"""

import asyncio
from typing import TYPE_CHECKING

from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgConnectionConfig, PsycopgPoolConfig
from sqlspec.adapters.sqlite.config import SqliteConfig, SqliteConnectionConfig
from sqlspec.base import SQLSpec
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    from sqlspec.statement.result import SelectResult

__all__ = ("main",)


async def main() -> None:
    """Demonstrate the improved adapter architecture."""

    # Initialize SQLSpec registry
    spec = SQLSpec()

    # 1. TypedDict Configuration Example - SQLite (simple, no pooling)
    print("=== TypedDict Configuration Example ===")

    # Define connection config using TypedDict
    sqlite_conn_config: SqliteConnectionConfig = {
        "database": ":memory:",
        "timeout": 30.0,
        "check_same_thread": False,
    }

    # Create config with statement configuration and default DictRow
    sqlite_config = SqliteConfig(
        connection_config=sqlite_conn_config,
        statement_config=SQLConfig(
            enable_parsing=True,
            enable_validation=True,
        ),
        default_row_type=DictRow,  # Explicit DictRow (this is the default)
    )

    # Register configuration
    spec.add_config(sqlite_config)

    # Use the configuration (SQLite is sync, not async)
    with spec.provide_session(SqliteConfig) as session:
        # Create table
        session.execute("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)")

        # Insert data - note how DictRow is used automatically
        session.execute_many(
            "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
            [(1, "Alice", "alice@example.com"), (2, "Bob", "bob@example.com")],
        )

        # Query data - returns DictRow automatically
        result: SelectResult[DictRow] = session.execute("SELECT * FROM users")
        print(f"SQLite Results (DictRow): {result.rows}")
        print(f"Column names: {result.column_names}")

    print()

    # 2. Async PostgreSQL with Connection Pooling
    print("=== Async PostgreSQL with Pooling ===")

    # Define pool config using TypedDict
    pg_pool_config: PsycopgPoolConfig = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_pass",
        "dbname": "test_db",
        "min_size": 2,
        "max_size": 10,
        "timeout": 30.0,
    }

    # Optional separate connection config (can be included in pool_config)
    pg_conn_config: PsycopgConnectionConfig = {
        "application_name": "sqlspec_example",
        "connect_timeout": 10.0,
    }

    # Create async config with pool
    psycopg_config = PsycopgAsyncConfig(
        pool_config=pg_pool_config,
        connection_config=pg_conn_config,  # Optional, can be None
        statement_config=SQLConfig(
            enable_parsing=True,
            enable_validation=True,
            enable_transformations=True,
        ),
        default_row_type=DictRow,  # DictRow is configured at the cursor level
    )

    print(f"PostgreSQL Config - Connection Dict: {psycopg_config.connection_config_dict}")
    print(f"Default row type: {psycopg_config.default_row_type}")
    print(f"Statement config: {psycopg_config.statement_config}")
    print()

    # 3. Demonstrate TypedDict advantages
    print("=== TypedDict Advantages ===")

    # Type safety with TypedDict - this would cause a type error if uncommented:
    # bad_config: SqliteConnectionConfig = {
    #     "database": "/path/to/db.sqlite",
    #     "invalid_key": "value",  # This would cause a type error!
    # }

    # Easy configuration merging
    base_sqlite_config: SqliteConnectionConfig = {
        "database": ":memory:",
        "timeout": 30.0,
    }

    # Override specific settings
    production_config: SqliteConnectionConfig = {
        **base_sqlite_config,
        "database": "/var/lib/app/production.db",
        "timeout": 60.0,
    }

    print(f"Base config: {base_sqlite_config}")
    print(f"Production config: {production_config}")
    print()

    # 4. Default Row Type Configuration
    print("=== Default Row Type Benefits ===")

    # The default_row_type is used to configure cursors directly
    # For psycopg: config_dict["row_factory"] = DictRow
    # For asyncpg: Records are converted to dict automatically
    # For sqlite: connection.row_factory = sqlite3.Row

    config_with_custom_row_type = SqliteConfig(
        connection_config={"database": ":memory:"},
        default_row_type=DictRow,  # Could be TupleRow or other types
    )

    print(f"Custom row type: {config_with_custom_row_type.default_row_type}")
    print("This configures the cursor/connection to return the specified row type")
    print()

    print("=== Summary ===")
    print("✅ All adapters use TypedDict for configuration")
    print("✅ All adapters reduced to 2 files: config.py and driver.py")
    print("✅ DictRow is the default row type, configurable per adapter")
    print("✅ Statement configuration available on all drivers")
    print("✅ Clean, consistent API across all database adapters")


if __name__ == "__main__":
    asyncio.run(main())
