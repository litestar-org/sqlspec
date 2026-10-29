"""Load named SQL queries from disk with SQLSpec.load_sql_files."""

import tempfile
from pathlib import Path
from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> list[dict[str, Any]]:
    """Write a named SQL file, load it into SQLSpec, and execute the named query."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with tempfile.TemporaryDirectory() as tmp_dir:
        sql_file = Path(tmp_dir) / "queries.sql"
        sql_file.write_text("-- name: list_teams\nSELECT id, name FROM teams ORDER BY id;\n", encoding="utf-8")
        spec.load_sql_files(sql_file)

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute("INSERT INTO teams (name) VALUES ('Litestar'), ('SQLSpec')")
        rows = session.execute(spec.get_sql("list_teams")).all()
        assert rows == [{"id": 1, "name": "Litestar"}, {"id": 2, "name": "SQLSpec"}]
        return rows


if __name__ == "__main__":
    print(main())
