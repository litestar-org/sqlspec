"""Synchronous routing behavior through real SQLite sessions."""

import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest

from sqlspec import sql
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import SQL, StatementConfig, StatementStack
from sqlspec.driver._routing import SyncRoutingSession
from sqlspec.exceptions import ImproperConfigurationError, RoutingContextError, SQLSpecError, TransactionError
from sqlspec.utils.routing import RoutingContext, RoutingEntry


def seed_routing_databases(tmp_path: Path) -> tuple[str, str]:
    databases = (str(tmp_path / "primary.db"), str(tmp_path / "replica.db"))
    for database, origin in zip(databases, ("primary", "replica"), strict=True):
        with sqlite3.connect(database) as connection:
            connection.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, origin TEXT)")
            connection.executemany("INSERT INTO notes VALUES (?, ?)", [(1, origin), (2, origin), (3, origin)])
    return databases


@pytest.fixture
def routing_config(tmp_path: Path) -> Iterator[SqliteConfig]:
    primary, replica = seed_routing_databases(tmp_path)
    config = SqliteConfig(connection_config={"database": primary}, replicas=[{"database": replica}], bind_key="shop")
    try:
        yield config
    finally:
        config.close_pool()


def test_idle_facade_and_closed_session(routing_config: SqliteConfig) -> None:
    with SyncRoutingSession(routing_config) as session:
        assert session.primary_session is None
        assert session.replica_session is None
        session.commit()
        session.rollback()
    with pytest.raises(SQLSpecError, match="closed"):
        session.select("SELECT 1")


def test_reads_replica_then_write_pins_primary(routing_config: SqliteConfig) -> None:
    with RoutingContext.scope(), routing_config.provide_routing_session() as session:
        assert session.select_value("SELECT origin FROM notes WHERE id=:id", id=1) == "replica"
        assert session.primary_session is None
        session.execute("INSERT INTO notes VALUES (:id,:origin)", {"id": 4, "origin": "new"})
        session.commit()
        assert session.pinned_to_primary
        assert cast(RoutingEntry, RoutingContext.get_entry("shop")).pinned_by_write
        assert session.fetch_one("SELECT origin FROM notes WHERE id=1") == {"origin": "primary"}
        with RoutingContext.replica("shop"):
            assert session.fetch_value("SELECT origin FROM notes WHERE id=1") == "replica"


@pytest.mark.parametrize(
    "operation", ["execute", "execute_many", "execute_script", "begin", "savepoint", "load", "indexes"]
)
def test_replica_scope_rejects_primary_operations_before_checkout(routing_config: SqliteConfig, operation: str) -> None:
    with SyncRoutingSession(routing_config) as session, RoutingContext.replica(), pytest.raises(RoutingContextError):
        if operation == "execute":
            session.execute("DELETE FROM notes")
        elif operation == "execute_many":
            session.execute_many("INSERT INTO notes VALUES (?,?)", [(4, "new")])
        elif operation == "execute_script":
            session.execute_script("SELECT 1")
        elif operation == "begin":
            session.begin()
        elif operation == "savepoint":
            session.create_savepoint("test")
        elif operation == "load":
            session.load_from_records("notes", [(4, "new")], columns=["id", "origin"])
        else:
            with session.without_indexes("notes"):
                pass
    assert session.primary_session is None
    assert session.replica_session is None


def test_primary_transaction_rejects_conflicting_replica_scope(routing_config: SqliteConfig) -> None:
    with SyncRoutingSession(routing_config) as session:
        with session.transaction():
            assert session.in_transaction()
            primary = session.primary_session
            with RoutingContext.replica(), pytest.raises(RoutingContextError):
                session.select_value("SELECT 1")
            assert session.primary_session is primary
            assert session.replica_session is None
            session.execute("INSERT INTO notes VALUES (4,'committed')")
        assert not session.in_transaction()
        assert session.select_value("SELECT origin FROM notes WHERE id=4") == "committed"


def test_no_replicas_reuses_single_checkout(tmp_path: Path) -> None:
    primary, _ = seed_routing_databases(tmp_path)
    config = SqliteConfig(connection_config={"database": primary})
    try:
        with SyncRoutingSession(config) as session:
            assert session.select_value("SELECT origin FROM notes WHERE id=1") == "primary"
            driver = session.primary_session
            session.execute("INSERT INTO notes VALUES (4,'new')")
            assert session.primary_session is driver
            assert session.replica_session is None
    finally:
        config.close_pool()


def test_stream_schema_aliases_builder_and_stack_routing(routing_config: SqliteConfig) -> None:
    @dataclass
    class Note:
        origin: str

    with SyncRoutingSession(routing_config) as session:
        with session.fetch_stream("SELECT origin FROM notes ORDER BY id", schema_type=Note, chunk_size=2) as stream:
            assert list(stream) == [Note("replica")] * 3
        assert session.primary_session is None
        assert session.select_one_or_none("SELECT id FROM notes WHERE id=99") is None
        assert session.fetch_value_or_none("SELECT id FROM notes WHERE id=99") is None
        assert session.select_with_total("SELECT id FROM notes")[1] == 3
        builder = sql.select("origin").from_("notes").where_eq("id", 1)
        assert session.select_value(builder) == "replica"
        assert session.select_value(SQL("SELECT :value", {"value": 7})) == 7
        pure = StatementStack().push_execute("SELECT origin FROM notes WHERE id=1")
        assert len(session.execute_stack(pure)) == 1
        assert session.primary_session is None
        mixed = pure.push_execute("INSERT INTO notes VALUES (4,'new')")
        assert len(session.execute_stack(mixed)) == 2
        assert session.pinned_to_primary


def test_savepoints_and_primary_rollback_on_error(routing_config: SqliteConfig) -> None:
    with SyncRoutingSession(routing_config) as session:
        with pytest.raises(TransactionError):
            session.release_savepoint("missing")
        with pytest.raises(ValueError, match="abort"), session.transaction():
            session.execute("INSERT INTO notes VALUES (4,'rolledback')")
            session.create_savepoint("before")
            session.execute("INSERT INTO notes VALUES (5,'rolledback')")
            session.rollback_to_savepoint("before")
            session.release_savepoint("before")
            raise ValueError("abort")
    with routing_config.provide_session() as primary:
        assert primary.select_value("SELECT count(*) FROM notes") == 3


def test_disabled_parsing_and_override_force_primary(routing_config: SqliteConfig) -> None:
    with SyncRoutingSession(routing_config) as session:
        assert (
            session.select_value(
                "SELECT origin FROM notes WHERE id=1", statement_config=StatementConfig(enable_parsing=False)
            )
            == "primary"
        )
        assert session.pinned_to_primary
    with pytest.raises(ImproperConfigurationError):
        SyncRoutingSession(routing_config, tenant="tenant", bypass_tenant=True)


def test_registry_primary_scope_helpers_and_schema_mapping(routing_config: SqliteConfig) -> None:
    from sqlspec.base import SQLSpec

    @dataclass
    class Note:
        origin: str

    registry = SQLSpec()
    config = registry.add_config(routing_config)
    with RoutingContext.primary(), registry.provide_routing_session(config) as session:
        assert session.statement_config is config.statement_config
        assert session.driver_features == config.driver_features
        assert session.observability is config.get_observability_runtime()
        assert session.prepare_statement("SELECT 1").is_read_only()
        assert session.split_script_statements("SELECT 1; SELECT 2", session.statement_config) == [
            "SELECT 1;",
            "SELECT 2",
        ]
        assert session.convert_to_dialect("SELECT 1", "postgres", pretty=False) == "SELECT 1"
        assert session.to_schema({"origin": "primary"}, schema_type=Note) == Note("primary")
        assert session.select_one("SELECT origin FROM notes WHERE id=1", schema_type=Note) == Note("primary")
        assert session.select_one_or_none("SELECT origin FROM notes WHERE id=1", schema_type=Note) == Note("primary")
        assert session.select_value_or_none("SELECT id FROM notes WHERE id=1", value_type=int) == 1
        assert "notes" in [table["table_name"] for table in session.data_dictionary.get_tables(session.primary_session)]
        session.execute_many("INSERT INTO notes VALUES (?,?)", [(4, "new"), (5, "new")])
        session.execute_script("INSERT INTO notes VALUES (6,'new');")
        session.commit()
        assert session.select_value("SELECT count(*) FROM notes") == 6
        session.close()


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("policy", ["causal", "staleness"])
async def test_nested_read_policy_revalidates_cached_replica(tmp_path: Path, async_mode: bool, policy: str) -> None:
    import sqlite3
    from typing import Any

    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.config import ReadReplicaGroup
    from sqlspec.core.routing import ReplicaSpec

    databases = [tmp_path / "writer.db", tmp_path / "old.db", tmp_path / "fresh.db"]
    for version, database in enumerate(databases):
        with sqlite3.connect(database) as connection:
            connection.execute("CREATE TABLE progress (version INTEGER)")
            connection.execute("INSERT INTO progress VALUES (?)", (version,))
    config_type: Any = AiosqliteConfig if async_mode else SqliteConfig
    old = config_type(connection_config={"database": str(databases[1])})
    fresh = config_type(connection_config={"database": str(databases[2])})
    checked: list[str] = []

    def sync_caught_up(session: Any, token: str) -> bool:
        checked.append(token)
        return int(session.select_value("SELECT version FROM progress")) >= int(token)

    async def async_caught_up(session: Any, token: str) -> bool:
        checked.append(token)
        return int(await session.select_value("SELECT version FROM progress")) >= int(token)

    group: ReadReplicaGroup[Any] = ReadReplicaGroup(
        [ReplicaSpec(old, max_staleness_seconds=10), ReplicaSpec(fresh, max_staleness_seconds=1)],
        causal_validator=async_caught_up if async_mode else sync_caught_up,
        fallback_to_primary=False,
    )
    primary = config_type(
        connection_config={"database": str(databases[0])}, bind_key="progress", replicas={"default": group}
    )
    initial: dict[str, Any] = {"causal_token": "1"} if policy == "causal" else {"max_staleness_seconds": 10.0}
    stronger: dict[str, Any] = {"causal_token": "2"} if policy == "causal" else {"max_staleness_seconds": 1.0}
    with RoutingContext.scope():
        if async_mode:
            async with primary.provide_routing_session() as session:
                with RoutingContext.replica("progress", **initial):
                    assert await session.select_value("SELECT version FROM progress") == 1
                with RoutingContext.replica("progress", **stronger):
                    assert await session.select_value("SELECT version FROM progress") == 2
        else:
            with primary.provide_routing_session() as session:
                with RoutingContext.replica("progress", **initial):
                    assert session.select_value("SELECT version FROM progress") == 1
                with RoutingContext.replica("progress", **stronger):
                    assert session.select_value("SELECT version FROM progress") == 2
    if policy == "causal":
        assert "2" in checked
    if async_mode:
        await primary.close_pool()
    else:
        primary.close_pool()


@pytest.mark.parametrize("async_mode", [False, True])
async def test_managed_primary_fallback_is_reused_for_subsequent_write(tmp_path: Path, async_mode: bool) -> None:
    import asyncio
    from typing import Any

    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    connections: list[Any] = []

    def connection_created(connection: Any) -> None:
        connections.append(connection)

    async def async_connection_created(connection: Any) -> None:
        connections.append(connection)

    config_type: Any = AiosqliteConfig if async_mode else SqliteConfig

    def reader_unavailable(_connection: Any) -> None:
        msg = "reader initialization is offline"
        raise OSError(msg)

    reader = config_type(
        connection_config={"database": str(tmp_path / "reader.db")},
        driver_features={"on_connection_create": reader_unavailable},
    )
    connection_config = {"database": str(tmp_path / "writer.db"), **({"pool_size": 1} if async_mode else {})}
    primary = config_type(
        connection_config=connection_config,
        replicas=[reader],
        driver_features={"on_connection_create": async_connection_created if async_mode else connection_created},
    )
    if async_mode:
        async with primary.provide_routing_session() as session:
            assert await session.select_value("SELECT 1") == 1
            fallback = session.replica_session
            assert session.primary_session is fallback
            await asyncio.wait_for(session.execute("CREATE TABLE items (id INTEGER)"), timeout=2)
            assert session.primary_session is fallback
        await primary.close_pool()
    else:
        with primary.provide_routing_session() as session:
            assert session.select_value("SELECT 1") == 1
            fallback = session.replica_session
            assert session.primary_session is fallback
            session.execute("CREATE TABLE items (id INTEGER)")
            assert session.primary_session is fallback
        primary.close_pool()
    assert len(connections) == 1
