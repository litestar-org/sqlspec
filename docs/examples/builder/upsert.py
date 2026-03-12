from __future__ import annotations

from pathlib import Path

__all__ = ("test_upsert",)


def test_upsert(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "upsert.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table settings (  key text primary key,  value text not null)")

        # ON CONFLICT DO NOTHING - skip if key exists
        insert_ignore = (
            sql.insert("settings").columns("key", "value").values("theme", "dark").on_conflict("key").do_nothing()
        )
        session.execute(insert_ignore)

        # ON CONFLICT DO UPDATE - upsert pattern
        upsert = (
            sql
            .insert("settings")
            .columns("key", "value")
            .values("theme", "light")
            .on_conflict("key")
            .do_update(value="light")
        )
        session.execute(upsert)

        result = session.select_one("select value from settings where key = 'theme'")
        print(result)  # {"value": "light"}
    # end-example

    assert result["value"] == "light"
