"""Structure isolated database fixtures and seed helpers for integration tests."""

import tempfile
from pathlib import Path
from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("create_test_spec", "main", "seed_database")


# --8<-- [start:example]
def create_test_spec(db_path: Path) -> tuple[SQLSpec, SqliteConfig]:
    """Create an isolated SQLSpec instance and SQLite configuration."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))
    return spec, config


def seed_database(spec: SQLSpec, config: SqliteConfig) -> None:
    """Create schema and populate seed rows for integration verification."""
    with spec.provide_session(config) as session:
        session.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                active BOOLEAN
            )
            """
        )
        session.execute_many(
            """
            INSERT INTO users (name, active)
            VALUES (?, ?)
            """,
            [("Alice", True), ("Bob", False), ("Charlie", True)],
        )


# --8<-- [end:example]


def main() -> dict[str, Any]:
    """Provision, seed, and query an isolated test database inside a temporary directory."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        spec, config = create_test_spec(Path(tmp_dir) / "test.db")
        seed_database(spec, config)

        with spec.provide_session(config) as session:
            active_count = session.select_value(
                """
                SELECT COUNT(*)
                FROM users
                WHERE active = 1
                """
            )
            assert active_count == 2

            user = session.select_one(
                """
                SELECT name
                FROM users
                WHERE id = ?
                """,
                1,
            )
            assert user["name"] == "Alice"
            return user


if __name__ == "__main__":
    print(main())
