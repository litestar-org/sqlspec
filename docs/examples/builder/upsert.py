"""Perform ON CONFLICT DO NOTHING and ON CONFLICT DO UPDATE upserts with the SQL builder."""

from typing import Any

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> dict[str, Any]:
    """Insert a default setting and upsert a replacement value on conflict."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute(
            """
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

        insert_ignore = (
            sql.insert("settings").columns("key", "value").values("theme", "dark").on_conflict("key").do_nothing()
        )
        session.execute(insert_ignore)

        # --8<-- [start:example]
        upsert = (
            sql
            .insert("settings")
            .columns("key", "value")
            .values("theme", "light")
            .on_conflict("key")
            .do_update(value="light")
        )
        session.execute(upsert)
        # --8<-- [end:example]

        row = session.select_one(
            """
            SELECT value
            FROM settings
            WHERE key = 'theme'
            """
        )
        assert row["value"] == "light"
        return row


if __name__ == "__main__":
    print(main())
