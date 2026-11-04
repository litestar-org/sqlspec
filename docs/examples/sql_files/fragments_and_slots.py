"""Reuse a SQL fragment and fill named query slots with bound predicates."""

from pathlib import Path
from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import SQL

__all__ = ("main",)


def main() -> list[dict[str, Any]]:
    """Execute default slots, override their SQL, and bind a required predicate."""
    sql_file = Path(__file__).with_suffix(".sql")
    # --8<-- [start:example]
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    spec.load_sql_files(sql_file)
    try:
        with spec.provide_session(config) as session:
            session.execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT, workspace_id INTEGER)")
            session.execute_many(
                "INSERT INTO teams VALUES (:id, :name, :workspace_id)",
                [
                    {"id": 1, "name": "Litestar", "workspace_id": 42},
                    {"id": 2, "name": "SQLSpec", "workspace_id": 42},
                    {"id": 3, "name": "Other", "workspace_id": 99},
                ],
            )
            teams = session.select(spec.get_sql("list_teams"), workspace_id=42)
            ordered = session.select(spec.get_sql("list_teams", ordering="t.name DESC"), workspace_id=42)

            predicate = SQL("t.name = :team_name", team_name="SQLSpec")
            query = spec.get_sql("list_teams", predicates=predicate, ordering="t.name DESC")
            selected = session.select(query, workspace_id=42)
            count = session.select_value(spec.get_sql("count_teams", predicates=predicate), workspace_id=42)
    finally:
        config.close_pool()
    # --8<-- [end:example]

    assert teams == [{"id": 1, "name": "Litestar"}, {"id": 2, "name": "SQLSpec"}]
    assert ordered == [{"id": 2, "name": "SQLSpec"}, {"id": 1, "name": "Litestar"}]
    assert selected == [{"id": 2, "name": "SQLSpec"}]
    assert count == 1
    return selected


if __name__ == "__main__":
    print(main())
