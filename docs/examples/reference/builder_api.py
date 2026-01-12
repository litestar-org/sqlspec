from __future__ import annotations

__all__ = ("test_builder_api",)


def test_builder_api() -> None:
    # start-example
    from sqlspec import sql

    query = sql.select("id", "name").from_("users").where_eq("status", "active").limit(10).offset(0)
    # end-example

    assert "select" in query.sql.lower()
