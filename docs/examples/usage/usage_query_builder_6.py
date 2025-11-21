from pathlib import Path
from typing import Any

__all__ = ("test_example_6",)


def test_example_6(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example6.db"
    config = SqliteConfig(
        pool_config={
            "database": database.name,
            "timeout": 5.0,
            "check_same_thread": False,
            "cached_statements": 100,
            "uri": False,
        }
    )
    with db.provide_session(config) as session:
        session.execute(
            """CREATE TABLE if not exists users(id int primary key, name text, created_at timestamp, status text)"""
        )
        # start-example
        # ORDER BY
        query = sql.select("*").from_("users").order_by("created_at DESC")
        session.execute(query)

        # Multiple order columns
        query = sql.select("*").from_("users").order_by("status ASC", "created_at DESC")
        session.execute(query)

        # LIMIT and OFFSET
        query = sql.select("*").from_("users").limit(10).offset(20)
        session.execute(query)

        # Pagination helper
        def paginate(page: int = 1, per_page: int = 20) -> Any:
            offset = (page - 1) * per_page
            return sql.select("*").from_("users").order_by("id").limit(per_page).offset(offset)

        session.execute(paginate(2, 10))
        # end-example
