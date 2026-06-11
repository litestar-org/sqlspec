"""Example: streaming query results in bounded chunks with ``select_stream``."""

__all__ = ("test_streaming_results",)


def test_streaming_results() -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("create table readings (id integer primary key, value integer)")
        session.execute_many("insert into readings (value) values (?)", [(i,) for i in range(250)])

        total = 0
        with session.select_stream("select value from readings order by id", chunk_size=100) as stream:
            for row in stream:
                total += row["value"]
    # end-example

    assert total == sum(range(250))
