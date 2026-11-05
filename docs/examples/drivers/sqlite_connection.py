"""Configure a synchronous SQLite adapter and run a health-check query."""

from typing import Any

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

spec = SQLSpec()
config = spec.add_config(
    SqliteConfig(
        connection_config={
            "database": ":memory:",
            "pool_recycle_seconds": 3600,
            "health_check_interval": 30.0,
        },
        driver_features={
            "pragmas": {
                "journal_mode": "WAL",
                "synchronous": "NORMAL",
                "foreign_keys": "ON",
                "busy_timeout": 5000,
            }
        },
    )
)

with spec.provide_session(config) as session:
    session.execute("CREATE TABLE health (id INTEGER PRIMARY KEY)")
    row = session.select_one("SELECT COUNT(*) AS total FROM health")
# --8<-- [end:example]

__all__ = ("main",)


def main() -> dict[str, Any]:
    """Create a table in an in-memory SQLite database and count its rows."""
    assert row == {"total": 0}
    return row


if __name__ == "__main__":
    print(main())
