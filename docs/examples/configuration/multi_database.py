"""Register multiple database configurations and load shared SQL files on one SQLSpec instance."""

import tempfile
from pathlib import Path
from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.observability import ObservabilityConfig, SamplingConfig

__all__ = ("main",)


def main() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Configure independent primary and analytics SQLite databases and run queries on each."""
    spec = SQLSpec(observability_config=ObservabilityConfig(sampling=SamplingConfig(sample_rate=0.1), print_sql=False))

    with tempfile.TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir)
        # --8<-- [start:example]
        primary = spec.add_config(SqliteConfig(connection_config={"database": str(base_dir / "primary.db")}))
        analytics = spec.add_config(SqliteConfig(connection_config={"database": str(base_dir / "analytics.db")}))

        sql_dir = base_dir / "sql"
        sql_dir.mkdir()
        (sql_dir / "queries.sql").write_text("-- name: list_users\nSELECT id, name FROM users ORDER BY id;\n")
        spec.load_sql_files(sql_dir)

        with spec.provide_session(primary) as session:
            session.execute(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
                """
            )
            session.execute("INSERT INTO users (name) VALUES ('Alice')")
            users = session.execute(spec.get_sql("list_users")).all()

        with spec.provide_session(analytics) as session:
            session.execute(
                """
                CREATE TABLE events (
                    id INTEGER PRIMARY KEY,
                    event TEXT
                )
                """
            )
            session.execute("INSERT INTO events (event) VALUES ('page_view')")
            events = session.execute("SELECT id, event FROM events").all()
        # --8<-- [end:example]

    assert users == [{"id": 1, "name": "Alice"}]
    assert events == [{"id": 1, "event": "page_view"}]
    return users, events


if __name__ == "__main__":
    print(main())
