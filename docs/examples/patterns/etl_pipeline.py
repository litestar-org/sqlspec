from __future__ import annotations

from pathlib import Path

__all__ = ("test_etl_pipeline",)


def test_etl_pipeline(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    # Simulate an ETL pipeline with two SQLite databases
    spec = SQLSpec()
    source_config = spec.add_config(
        SqliteConfig(connection_config={"database": str(tmp_path / "source.db")})
    )
    target_config = spec.add_config(
        SqliteConfig(connection_config={"database": str(tmp_path / "target.db")})
    )

    # Step 1: Seed source data
    with spec.provide_session(source_config) as session:
        session.execute("create table orders (id integer primary key, amount real, status text)")
        session.execute_many(
            "insert into orders (amount, status) values (?, ?)",
            [(100.0, "complete"), (50.0, "pending"), (200.0, "complete")],
        )

    # Step 2: Extract from source
    with spec.provide_session(source_config) as session:
        completed = session.select(
            "select id, amount from orders where status = ?", "complete"
        )

    # Step 3: Load into target
    with spec.provide_session(target_config) as session:
        session.execute("create table revenue (order_id integer, amount real)")
        session.execute_many(
            "insert into revenue (order_id, amount) values (?, ?)",
            [(row["id"], row["amount"]) for row in completed],
        )
        total = session.select_value("select sum(amount) from revenue")
        print(f"Total revenue: {total}")  # Total revenue: 300.0
    # end-example

    assert total == 300.0
