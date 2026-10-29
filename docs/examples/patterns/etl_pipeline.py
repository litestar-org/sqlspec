"""Extract completed orders from a source database and load revenue totals into a target database."""

import tempfile
from pathlib import Path

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> float:
    """Run a two-database SQLite ETL pipeline inside a temporary directory."""
    spec = SQLSpec()

    with tempfile.TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir)
        source_config = spec.add_config(SqliteConfig(connection_config={"database": str(base_dir / "source.db")}))
        target_config = spec.add_config(SqliteConfig(connection_config={"database": str(base_dir / "target.db")}))

        with spec.provide_session(source_config) as session:
            session.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, status TEXT)")
            session.execute_many(
                "INSERT INTO orders (amount, status) VALUES (?, ?)",
                [(100.0, "complete"), (50.0, "pending"), (200.0, "complete")],
            )

        with spec.provide_session(source_config) as session:
            completed = session.select("SELECT id, amount FROM orders WHERE status = ?", "complete")

        with spec.provide_session(target_config) as session:
            session.execute("CREATE TABLE revenue (order_id INTEGER, amount REAL)")
            session.execute_many(
                "INSERT INTO revenue (order_id, amount) VALUES (?, ?)",
                [(row["id"], row["amount"]) for row in completed],
            )
            total = float(session.select_value("SELECT SUM(amount) FROM revenue"))

    assert total == 300.0
    return total


if __name__ == "__main__":
    print(f"Total revenue: {main()}")
