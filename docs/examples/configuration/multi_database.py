"""Register multiple database configurations and load shared SQL files on one SQLSpec instance."""

import tempfile
from pathlib import Path
from typing import Any

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.observability import ObservabilityConfig, SamplingConfig


def run_multi_database(base_dir: Path, sql_dir: Path) -> None:
    """Configure independent primary and analytics SQLite databases."""
    sampling = SamplingConfig(sample_rate=0.1)
    obs = ObservabilityConfig(sampling=sampling, print_sql=False)
    spec = SQLSpec(observability_config=obs)

    primary_cfg = {"database": str(base_dir / "primary.db")}
    analytics_cfg = {"database": str(base_dir / "analytics.db")}
    primary_db = SqliteConfig(bind_key="primary", connection_config=primary_cfg)
    analytics_db = SqliteConfig(bind_key="analytics", connection_config=analytics_cfg)
    primary = spec.add_config(primary_db)
    analytics = spec.add_config(analytics_db)
    spec.load_sql_files(sql_dir)

    with spec.provide_session(primary) as session:
        session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute("INSERT INTO users (name) VALUES ('Alice')")
        users = session.execute(spec.get_sql("list_users")).all()
        assert users == [{"id": 1, "name": "Alice"}]

    with spec.provide_session(analytics) as session:
        session.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, event TEXT)")
        session.execute("INSERT INTO events (event) VALUES ('page_view')")
        events = session.execute("SELECT id, event FROM events").all()
        assert events == [{"id": 1, "event": "page_view"}]


# --8<-- [end:example]

__all__ = ("main", "run_multi_database")


def main() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Configure independent primary and analytics SQLite databases and run queries on each."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir)
        sql_dir = base_dir / "sql"
        sql_dir.mkdir()
        (sql_dir / "queries.sql").write_text("-- name: list_users\nSELECT id, name FROM users ORDER BY id;\n")
        run_multi_database(base_dir, sql_dir)
    return [{"id": 1, "name": "Alice"}], [{"id": 1, "event": "page_view"}]


if __name__ == "__main__":
    print(main())
