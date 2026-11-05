"""Attach a custom StatementEvent observer callback to ObservabilityConfig."""

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.observability import ObservabilityConfig, StatementEvent

captured_events: list[StatementEvent] = []


def observe_statement(event: StatementEvent) -> None:
    """Record each StatementEvent emitted by the SQLSpec runtime."""
    captured_events.append(event)


observers = (observe_statement,)
observability = ObservabilityConfig(statement_observers=observers, print_sql=False)
spec = SQLSpec(observability_config=observability)
config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

with spec.provide_session(config) as session:
    session.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    session.execute("INSERT INTO items (name) VALUES ('Widget')")
    session.select("SELECT id, name FROM items")
# --8<-- [end:example]

__all__ = ("captured_events", "main", "observe_statement")


def main() -> list[StatementEvent]:
    """Capture per-statement telemetry events across DDL, DML, and SELECT operations."""
    assert len(captured_events) >= 3
    assert all(isinstance(event, StatementEvent) for event in captured_events)
    return captured_events


if __name__ == "__main__":
    events = main()
    for item in events:
        print(f"{item.operation} - {item.driver} - {item.duration_s:.4f}s")
