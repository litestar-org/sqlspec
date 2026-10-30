"""Configure a synchronous SQLite adapter and run a health-check query."""

from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> dict[str, Any]:
    """Create a table in an in-memory SQLite database and count its rows."""
    # --8<-- [start:example]
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE health (id INTEGER PRIMARY KEY)")
        row = session.select_one(
            """
            SELECT COUNT(*) AS total
            FROM health
            """
        )
        # --8<-- [end:example]
        assert row == {"total": 0}
        return row


if __name__ == "__main__":
    print(main())
