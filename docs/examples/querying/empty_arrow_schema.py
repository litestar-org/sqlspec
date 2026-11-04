"""Preserve a typed Arrow schema when a query returns no rows."""

import pyarrow as pa

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> dict[str, str]:
    """Query an empty table and retain its explicit Arrow column types."""
    # --8<-- [start:example]
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    schema = pa.schema([("host", pa.string()), ("cpu_pct", pa.float64())])
    try:
        with spec.provide_session(config) as session:
            session.execute("CREATE TABLE metrics (host TEXT, cpu_pct REAL)")
            query = "SELECT host, cpu_pct FROM metrics WHERE 1 = 0"
            empty_result = session.select_to_arrow(query, arrow_schema=schema)
            empty_table = empty_result.data
    finally:
        config.close_pool()
    # --8<-- [end:example]

    assert empty_table.num_rows == 0
    assert empty_table.schema == schema
    return {field.name: str(field.type) for field in empty_table.schema}


if __name__ == "__main__":
    print(main())
