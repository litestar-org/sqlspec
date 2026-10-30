"""Attach a custom StatementConfig to a database adapter configuration."""

from typing import Any

from sqlspec import SQLSpec, StatementConfig
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> list[dict[str, Any]]:
    """Register a SqliteConfig with custom statement validation settings."""
    statement_config = StatementConfig(enable_validation=False)
    spec = SQLSpec()
    primary = spec.add_config(
        SqliteConfig(connection_config={"database": ":memory:"}, statement_config=statement_config)
    )

    with spec.provide_session(primary) as session:
        session.execute("CREATE TABLE health (id INTEGER PRIMARY KEY, ok BOOLEAN)")
        rows = session.select("SELECT id, ok FROM health")
        assert rows == []
        return rows


if __name__ == "__main__":
    print(main())
