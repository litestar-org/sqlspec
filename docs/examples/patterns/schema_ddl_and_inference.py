"""Extract topological schema DDL, profile table columns, and infer typed DDL from data."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.builder import CreateTable

__all__ = ("main",)


def main() -> tuple[str, int, str, str]:
    """Introspect DDL, transpile schema DDL to PostgreSQL, and infer table DDL from rows."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    try:
        with spec.provide_session(config) as session:
            session.execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)")
            session.execute(
                "CREATE TABLE members (id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id), email TEXT NOT NULL)"
            )
            session.execute("CREATE TABLE raw_events (event_id TEXT, user_count TEXT, created_on TEXT)")
            session.execute_many(
                "INSERT INTO raw_events (event_id, user_count, created_on) VALUES (?, ?, ?)",
                [("evt-1", "12", "2026-01-15"), ("evt-2", "48", "2026-01-16")],
            )

            # --8<-- [start:example]
            dd = session.data_dictionary
            teams_ddl = dd.get_ddl(session, "teams").ddl or ""
            schema_items = dd.get_schema_ddl(session, target_dialect="postgres").items
            profiles = dd.get_table_profile(session, "raw_events")
            inferred_ddl = dd.infer_table_ddl_from_data(session, "raw_events", target_table="events").ddl or ""

            records_ddl = CreateTable.from_records(
                "audit_events", [{"id": 1, "actor": "system", "active": True}], primary_key="id", dialect="postgres"
            ).to_sql()
            # --8<-- [end:example]

        assert "teams" in teams_ddl
        assert len(schema_items) >= 2
        assert len(profiles) == 3
        assert "events" in inferred_ddl
        assert "audit_events" in records_ddl
        return teams_ddl, len(schema_items), inferred_ddl, records_ddl
    finally:
        config.close_pool()


if __name__ == "__main__":
    print(main())
