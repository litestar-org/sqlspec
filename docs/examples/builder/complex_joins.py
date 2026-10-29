"""Execute a joined SELECT query built with the fluent SQL query builder."""

from typing import Any

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> list[dict[str, Any]]:
    """Run an inner join between orders and customers in SQLite."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)")
        session.execute("INSERT INTO customers (name) VALUES ('Ada')")
        session.execute("INSERT INTO orders (customer_id) VALUES (1)")

        query = (
            sql
            .select("orders.id", "customers.name")
            .from_("orders")
            .join("customers", "orders.customer_id = customers.id")
        )
        rows = session.execute(query).all()
        assert rows == [{"id": 1, "name": "Ada"}]
        return rows


if __name__ == "__main__":
    print(main())
