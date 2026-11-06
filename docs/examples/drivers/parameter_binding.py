"""Bind named and positional parameters consistently across SQLSpec drivers."""

from typing import Any

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

spec = SQLSpec()
config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

with spec.provide_session(config) as session:
    named = session.select_one(
        """
        SELECT :status AS status, :status AS status_copy
        """,
        status="active",
    )
    positional = session.select_one(
        """
        SELECT ? AS value
        """,
        42,
    )
# --8<-- [end:example]

__all__ = ("main",)


def main() -> dict[str, Any]:
    """Execute named and positional parameter bindings in SQLite."""
    assert named == {"status": "active", "status_copy": "active"}
    assert positional == {"value": 42}
    return positional


if __name__ == "__main__":
    print(main())
