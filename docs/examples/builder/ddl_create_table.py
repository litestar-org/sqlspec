"""Build and execute portable DDL statements and record-inferred schemas."""

from typing import Any

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.builder import CreateTable

__all__ = ("main",)


def main() -> list[dict[str, Any]]:
    """Create tables and indexes in SQLite with DDL builders and schema inference."""
    # --8<-- [start:example]
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        users_ddl = (
            sql
            .create_table("users")
            .if_not_exists()
            .column("id", "INTEGER", primary_key=True)
            .column("email", "TEXT", not_null=True, unique=True)
        )
        idx_ddl = sql.create_index("idx_users_email").on_table("users").columns("email")
        events_ddl = CreateTable.from_records("events", [{"id": 1, "event_type": "signup"}], primary_key="id")
        session.execute(users_ddl)
        session.execute(idx_ddl)
        session.execute(events_ddl)
        session.execute(sql.insert("events").columns("id", "event_type").values(1, "signup"))
        rows = session.execute(sql.select("id", "event_type").from_("events")).all()
        assert rows == [{"id": 1, "event_type": "signup"}]
    # --8<-- [end:example]
    return rows


if __name__ == "__main__":
    print(main())
