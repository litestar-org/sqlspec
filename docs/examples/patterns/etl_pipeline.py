"""Extract completed orders from a source database and load revenue totals into a target database."""

import tempfile
from pathlib import Path

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig


def run_etl_pipeline(base_dir: Path) -> float:
    """Extract completed orders and batch-load revenue totals."""
    spec = SQLSpec()
    src_cfg = {"database": str(base_dir / "source.db")}
    dst_cfg = {"database": str(base_dir / "target.db")}
    source_config = spec.add_config(SqliteConfig(connection_config=src_cfg))
    target_config = spec.add_config(SqliteConfig(connection_config=dst_cfg))

    with spec.provide_session(source_config) as session:
        session.execute(
            """
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                amount REAL,
                status TEXT
            )
            """
        )
        rows = [(100.0, "complete"), (50.0, "pending"), (200.0, "complete")]
        session.execute_many(
            """
            INSERT INTO orders (amount, status)
            VALUES (?, ?)
            """,
            rows,
        )
        completed = session.select(
            """
            SELECT id, amount
            FROM orders
            WHERE status = ?
            """,
            "complete",
        )

    with spec.provide_session(target_config) as session:
        session.execute(
            """
            CREATE TABLE revenue (
                order_id INTEGER,
                amount REAL
            )
            """
        )
        session.execute_many(
            """
            INSERT INTO revenue (order_id, amount)
            VALUES (?, ?)
            """,
            [(row["id"], row["amount"]) for row in completed],
        )
        total = float(session.select_value("SELECT SUM(amount) FROM revenue"))

    return total


# --8<-- [end:example]

__all__ = ("main", "run_etl_pipeline")


def main() -> float:
    """Run a two-database SQLite ETL pipeline inside a temporary directory."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        total = run_etl_pipeline(Path(tmp_dir))
    assert total == 300.0
    return total


if __name__ == "__main__":
    print(f"Total revenue: {main()}")
