__all__ = ("test_upgrade_returns_list", "upgrade")


# start-example
def upgrade() -> list[str]:
    """Apply migration in multiple steps."""
    return [
        "CREATE TABLE products (id SERIAL PRIMARY KEY);",
        "CREATE TABLE orders (id SERIAL PRIMARY KEY, product_id INTEGER);",
        "CREATE INDEX idx_orders_product ON orders(product_id);",
    ]


# end-example


def test_upgrade_returns_list() -> None:
    stmts = upgrade()
    assert isinstance(stmts, list)
    assert any("products" in s for s in stmts)
    assert any("orders" in s for s in stmts)
