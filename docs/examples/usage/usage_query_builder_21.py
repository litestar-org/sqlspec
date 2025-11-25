from pathlib import Path

__all__ = ("test_example_21",)


def test_example_21(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example21.db"
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
            """CREATE TABLE if not exists users(id integer primary key autoincrement, name text, status text)"""
        )
        # start-example
        # Use raw SQL for CASE expression for SQLite compatibility
        case_expr = "CASE WHEN status = 'active' THEN 'Active User' WHEN status = 'pending' THEN 'Pending Approval' ELSE 'Inactive' END"
        query = sql.select("id", "name", f"{case_expr} as status_label").from_("users")
        session.execute(query)
        # end-example
