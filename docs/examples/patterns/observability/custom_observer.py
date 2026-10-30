"""Attach a custom StatementEvent observer callback to ObservabilityConfig."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.observability import ObservabilityConfig, StatementEvent

__all__ = ("main",)


def main() -> list[StatementEvent]:
    """Capture per-statement telemetry events across DDL, DML, and SELECT operations."""
    captured_events: list[StatementEvent] = []

    # --8<-- [start:example]
    def observe_statement(event: StatementEvent) -> None:
        captured_events.append(event)

    observability = ObservabilityConfig(statement_observers=(observe_statement,), print_sql=False)
    spec = SQLSpec(observability_config=observability)
    # --8<-- [end:example]
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute("INSERT INTO items (name) VALUES ('Widget')")
        session.select("SELECT id, name FROM items")

    assert len(captured_events) >= 3
    assert all(isinstance(event, StatementEvent) for event in captured_events)
    return captured_events


if __name__ == "__main__":
    events = main()
    for item in events:
        print(f"{item.operation} - {item.driver} - {item.duration_s:.4f}s")
