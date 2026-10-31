"""Validate required and optional named parameters declared in SQL file headers."""

import tempfile
from pathlib import Path
from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import SQLSpecError

__all__ = ("main",)

TEAMS_SQL = """-- name: get_team_by_name
-- param: name str  The team name to look up
SELECT id, name
FROM teams
WHERE name = :name;

-- name: list_teams
-- param: name str?  Optional team name filter
SELECT id, name
FROM teams
WHERE (:name IS NULL OR name = :name)
ORDER BY id;
"""


def main() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Load declared parameter metadata from a SQL file and verify automatic validation."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with tempfile.TemporaryDirectory() as tmp_dir:
        sql_file = Path(tmp_dir) / "teams.sql"
        sql_file.write_text(TEAMS_SQL, encoding="utf-8")
        spec.load_sql_files(sql_file)

    # --8<-- [start:example]
    declarations = spec.get_query_parameters("get_team_by_name")
    query = spec.get_sql("get_team_by_name")

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute("INSERT INTO teams (name) VALUES ('Litestar'), ('SQLSpec')")

        row = session.execute(query, name="SQLSpec").one()
        optional_rows = session.execute(spec.get_sql("list_teams")).all()
    # --8<-- [end:example]
    assert query.declared_parameters == declarations

    with spec.provide_session(config) as session:
        missing_error = ""
        try:
            session.execute(spec.get_sql("get_team_by_name"))
        except SQLSpecError as exc:
            missing_error = str(exc)

    assert row["name"] == "SQLSpec"
    assert [team["name"] for team in optional_rows] == ["Litestar", "SQLSpec"]
    assert "name" in missing_error
    return row, optional_rows


if __name__ == "__main__":
    print(main())
