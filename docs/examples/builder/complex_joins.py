from __future__ import annotations

from pathlib import Path

__all__ = ("test_builder_complex_joins",)


def test_builder_complex_joins(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "builder_joins.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists customers (id integer primary key, name text)")
        session.execute("create table if not exists orders (id integer primary key, customer_id int)")
        session.execute("insert into customers (name) values ('Ada')")
        session.execute("insert into orders (customer_id) values (1)")

        query = (
            sql
            .select("orders.id", "customers.name")
            .from_("orders")
            .join("customers", "orders.customer_id = customers.id")
        )
        result = session.execute(query)
        print(result.all())
    # end-example

    assert result.all() == [{"id": 1, "name": "Ada"}]
