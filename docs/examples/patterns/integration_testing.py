from __future__ import annotations

from pathlib import Path

__all__ = ("test_integration_testing",)


def test_integration_testing(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    def create_test_spec(db_path: Path) -> tuple[SQLSpec, SqliteConfig]:
        """Factory for test database setup."""
        spec = SQLSpec()
        config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))
        return spec, config

    def seed_database(spec: SQLSpec, config: SqliteConfig) -> None:
        """Populate test fixtures."""
        with spec.provide_session(config) as session:
            session.execute("create table users (id integer primary key, name text, active boolean)")
            session.execute_many(
                "insert into users (name, active) values (?, ?)", [("Alice", True), ("Bob", False), ("Charlie", True)]
            )

    # In your test
    spec, config = create_test_spec(tmp_path / "test.db")
    seed_database(spec, config)

    with spec.provide_session(config) as session:
        active_count = session.select_value("select count(*) from users where active = 1")
        assert active_count == 2

        user = session.select_one("select name from users where id = ?", 1)
        assert user["name"] == "Alice"
    # end-example
