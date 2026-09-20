from __future__ import annotations

from pathlib import Path

__all__ = ("test_query_modifiers",)


def test_query_modifiers(tmp_path: Path) -> None:
    """Demonstrate query builder ordering, limits, offsets, and row locking."""
    # start-example
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "modifiers.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists users (id integer primary key, name text, status text)")
        session.execute(
            "insert into users (name, status) values ('Ada', 'active'), ('Bob', 'inactive'), ('Charlie', 'active')"
        )

        query = (
            sql.select("id", "name").from_("users").where_eq("status", "active").order_by("name").limit(10).offset(0)
        )
        result = session.execute(query)
        print(result.all())

        pg_query = (
            sql
            .select("id", "name", dialect="postgres")
            .from_("users")
            .where_eq("status", "active")
            .for_update(skip_locked=True)
        )
        pg_sql = pg_query.to_sql()
    # end-example

    assert result.all() == [{"id": 1, "name": "Ada"}, {"id": 3, "name": "Charlie"}]
    assert "FOR UPDATE SKIP LOCKED" in pg_sql
