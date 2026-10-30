"""Stream query results in bounded chunks with session.select_stream."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> int:
    """Stream 250 rows in 100-row chunks and return the accumulated sum."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute(
            """
            CREATE TABLE readings (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )
            """
        )
        session.execute_many("INSERT INTO readings (value) VALUES (?)", [(i,) for i in range(250)])

        # --8<-- [start:example]
        total = 0
        with session.select_stream(
            """
            SELECT value
            FROM readings
            ORDER BY id
            """,
            chunk_size=100,
        ) as stream:
            for row in stream:
                total += int(row["value"])
        # --8<-- [end:example]

    assert total == sum(range(250))
    return total


if __name__ == "__main__":
    print(main())
