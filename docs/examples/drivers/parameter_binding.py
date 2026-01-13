from __future__ import annotations

from pathlib import Path

__all__ = ("test_parameter_binding",)


def test_parameter_binding(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "params.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("select :status as status, :status as status_copy", {"status": "active"})
        result = session.execute("select ? as value", (42,))
        print(result.one())
    # end-example

    assert result.one() == {"value": 42}
