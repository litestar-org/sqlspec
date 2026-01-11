"""Mock adapter for SQLSpec.

This adapter provides mock database drivers that use SQLite :memory: as the
execution backend while accepting SQL written in other dialects (Postgres,
MySQL, Oracle, etc.). SQL is transpiled to SQLite syntax before execution
using sqlglot.

Key Features:
    - Write SQL in your target dialect (Postgres, MySQL, Oracle, SQLite)
    - SQL is transpiled to SQLite before execution
    - Fast execution using SQLite :memory: database
    - Same API as real database adapters
    - Both sync and async drivers available
    - Initial SQL support for test fixtures

Example:
    >>> from sqlspec.adapters.mock import MockSyncConfig
    >>>
    >>> # Create config with Postgres dialect
    >>> config = MockSyncConfig(target_dialect="postgres")
    >>>
    >>> # Use Postgres syntax - it will be transpiled to SQLite
    >>> with config.provide_session() as session:
    ...     session.execute(\"\"\"
    ...         CREATE TABLE users (
    ...             id SERIAL PRIMARY KEY,
    ...             name VARCHAR(100)
    ...         )
    ...     \"\"\")
    ...     session.execute("INSERT INTO users (name) VALUES ($1)", "Alice")
    ...     user = session.select_one("SELECT * FROM users WHERE name = $1", "Alice")
    ...     print(user["name"])
    Alice

With Test Fixtures:
    >>> config = MockSyncConfig(
    ...     target_dialect="postgres",
    ...     initial_sql=[
    ...         "CREATE TABLE users (id INT, name TEXT, role TEXT)",
    ...         "INSERT INTO users VALUES (1, 'Alice', 'admin')",
    ...         "INSERT INTO users VALUES (2, 'Bob', 'user')",
    ...     ],
    ... )
    >>>
    >>> with config.provide_session() as session:
    ...     admins = session.select(
    ...         "SELECT * FROM users WHERE role = $1", "admin"
    ...     )
    ...     print(len(admins))
    1
"""

from sqlspec.adapters.mock._typing import MockAsyncSessionContext, MockConnection, MockSyncSessionContext
from sqlspec.adapters.mock.config import MockAsyncConfig, MockConnectionParams, MockDriverFeatures, MockSyncConfig
from sqlspec.adapters.mock.data_dictionary import MockAsyncDataDictionary, MockDataDictionary
from sqlspec.adapters.mock.driver import MockAsyncDriver, MockCursor, MockExceptionHandler, MockSyncDriver

__all__ = (
    "MockAsyncConfig",
    "MockAsyncDataDictionary",
    "MockAsyncDriver",
    "MockAsyncSessionContext",
    "MockConnection",
    "MockConnectionParams",
    "MockCursor",
    "MockDataDictionary",
    "MockDriverFeatures",
    "MockExceptionHandler",
    "MockSyncConfig",
    "MockSyncDriver",
    "MockSyncSessionContext",
)
