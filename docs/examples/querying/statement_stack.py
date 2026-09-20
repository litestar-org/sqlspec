from __future__ import annotations

__all__ = ("test_statement_stack",)


def test_statement_stack() -> None:
    # start-example
    from sqlspec import SQLResult, SQLSpec, StatementStack
    from sqlspec.adapters.sqlite import SqliteConfig

    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    stack = (
        StatementStack()
        .push_execute("create table teams (id integer primary key, name text)")
        .push_execute_many("insert into teams (name) values (:name)", [{"name": "Litestar"}, {"name": "SQLSpec"}])
        .push_execute("select id, name from teams order by id")
    )

    with spec.provide_session(config) as session:
        results = session.execute_stack(stack)
        final_result = results[-1].result
        assert isinstance(final_result, SQLResult)
        rows = final_result.all()
    # end-example

    assert rows == [{"id": 1, "name": "Litestar"}, {"id": 2, "name": "SQLSpec"}]
