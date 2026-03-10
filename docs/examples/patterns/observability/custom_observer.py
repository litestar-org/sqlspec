from __future__ import annotations

__all__ = ("test_custom_observer",)


def test_custom_observer() -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.observability import ObservabilityConfig, StatementEvent

    # Collect events for testing or custom processing
    captured_events: list[StatementEvent] = []

    def my_observer(event: StatementEvent) -> None:
        """Custom observer that captures SQL events."""
        captured_events.append(event)
        if event.duration_s > 1.0:
            print(f"SLOW QUERY ({event.duration_s:.2f}s): {event.sql[:80]}")

    # Wire the observer into the config
    observability = ObservabilityConfig(statement_observers=(my_observer,), print_sql=False)

    spec = SQLSpec(observability_config=observability)
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("create table items (id integer primary key, name text)")
        session.execute("insert into items (name) values ('Widget')")
        session.select("select * from items")

    # Observer received events for each execution
    print(f"Captured {len(captured_events)} events")
    for event in captured_events:
        print(f"  {event.operation} - {event.driver} - {event.duration_s:.4f}s")
    # end-example

    assert len(captured_events) >= 3
    assert all(isinstance(e, StatementEvent) for e in captured_events)
