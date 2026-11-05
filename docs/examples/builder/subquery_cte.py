"""Execute a parameterized CTE query built with the fluent SQL query builder."""

from typing import Any

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> list[dict[str, Any]]:
    """Aggregate high-value orders per customer with a Common Table Expression."""
    # --8<-- [start:example]
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE orders (customer_id INTEGER, amount INTEGER)")
        session.execute_many(
            "INSERT INTO orders (customer_id, amount) VALUES (?, ?)", [(1, 50), (1, 150), (1, 200), (2, 80)]
        )

        high_value = sql.select("customer_id", "amount").from_("orders").where_gte("amount", 100)
        summary = (
            sql
            .select("customer_id", sql.count().as_("order_count"))
            .with_("high_value", high_value)
            .from_("high_value")
            .group_by("customer_id")
        )
        rows = session.execute(summary).all()
        assert rows == [{"customer_id": 1, "order_count": 2}]
    # --8<-- [end:example]
    return rows


if __name__ == "__main__":
    print(main())
