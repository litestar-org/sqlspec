from __future__ import annotations

__all__ = ("test_base_api",)


def test_base_api() -> None:
    # start-example
    from sqlspec import SQLSpec

    spec = SQLSpec()
    spec.add_named_sql("health_check", "select 1 as ok")
    query = spec.get_sql("health_check")
    # end-example

    assert query.raw_sql.startswith("select")
