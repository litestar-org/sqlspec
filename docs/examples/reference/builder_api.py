"""Construct a filtered and paginated SELECT statement with the fluent sql factory."""

from sqlspec import sql

__all__ = ("main",)


def main() -> str:
    """Build a SELECT query and render its SQL string."""
    # --8<-- [start:example]
    query = sql.select("id", "name").from_("users").where_eq("status", "active").limit(10).offset(0)
    rendered = query.to_sql()
    # --8<-- [end:example]
    assert "SELECT" in rendered.upper()
    return rendered


if __name__ == "__main__":
    print(main())
