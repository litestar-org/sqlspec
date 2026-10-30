"""Register and retrieve named SQL statements on the SQLSpec registry."""

from sqlspec import SQL, SQLSpec

__all__ = ("main",)


def main() -> SQL:
    """Register a named SQL query and retrieve its compiled SQL wrapper."""
    # --8<-- [start:example]
    spec = SQLSpec()
    spec.add_named_sql("health_check", "SELECT 1 AS ok")
    query = spec.get_sql("health_check")
    # --8<-- [end:example]
    assert query.raw_sql == "SELECT 1 AS ok"
    return query


if __name__ == "__main__":
    print(main().raw_sql)
