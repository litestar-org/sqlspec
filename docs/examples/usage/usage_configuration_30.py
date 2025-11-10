"""Telemetry snapshot example."""

__all__ = ("test_telemetry_snapshot",)


def test_telemetry_snapshot() -> None:
    """Demonstrate SQLSpec.telemetry_snapshot()."""
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with db_manager.provide_session(db) as session:
        session.execute("SELECT 1")

    snapshot = db_manager.telemetry_snapshot()
    # end-example
    assert "SqliteConfig.lifecycle.query_start" in snapshot
    _ = snapshot.get("storage_bridge.bytes_written", 0)
