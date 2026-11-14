"""Aiosqlite ADK adapter-local configuration tests."""

import weakref
from datetime import UTC, datetime, timedelta
from typing import Any, Self, get_type_hints
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from sqlspec.adapters.aiosqlite.adk import AiosqliteADKConfig, AiosqliteADKMemoryStore, AiosqliteADKStore
from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.aiosqlite.core import end_transaction
from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk import StoredEvent, StoredMemory


class AsyncRecordingConnection:
    """Record SQL executed by async ADK PRAGMA setup."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    async def execute(self, statement: str) -> None:
        self.statements.append(statement)


def test_aiosqlite_adk_config_extends_shared_config_and_exports_sqlite_knobs() -> None:
    """AiosqliteADKConfig should inherit shared ADK settings and add SQLite-specific knobs."""
    annotations = get_type_hints(AiosqliteADKConfig, include_extras=True)

    assert set(ADKConfig.__annotations__) <= set(annotations)
    assert {"pragma_overrides", "fts_tokenize", "fts_detail"} <= set(annotations)


async def test_aiosqlite_adk_defaults_preserve_existing_pragma_profile_and_ignore_driver_features() -> None:
    """Default ADK PRAGMAs should stay fixed and not read global driver_features."""
    config = AiosqliteConfig(driver_features={"pragma_overrides": {"cache_size": -32000}})
    store = AiosqliteADKStore(config)
    connection = AsyncRecordingConnection()

    await store._apply_pragmas(connection)

    assert connection.statements == [
        "PRAGMA foreign_keys = ON",
        "PRAGMA cache_size = -64000",
        "PRAGMA mmap_size = 30000000",
        "PRAGMA journal_size_limit = 67108864",
    ]


async def test_aiosqlite_adk_pragma_overrides_apply_after_defaults() -> None:
    """Adapter-local ADK PRAGMA overrides should render through SQLite validation and apply last."""
    config = AiosqliteConfig(
        extension_config={
            "adk": {"pragma_overrides": {"cache_size": -32000, "foreign_keys": False, "journal_mode": "WAL"}}
        }
    )
    store = AiosqliteADKStore(config)
    connection = AsyncRecordingConnection()

    await store._apply_pragmas(connection)

    assert connection.statements[-3:] == [
        "PRAGMA cache_size = -32000",
        "PRAGMA foreign_keys = 0",
        "PRAGMA journal_mode = WAL",
    ]


def test_aiosqlite_adk_pragma_overrides_reuse_aiosqlite_validation() -> None:
    """ADK PRAGMA overrides should reject unsafe names before SQL rendering."""
    with pytest.raises(ImproperConfigurationError, match="PRAGMA name"):
        AiosqliteADKStore(
            AiosqliteConfig(extension_config={"adk": {"pragma_overrides": {"cache_size; DROP TABLE x": -32000}}})
        )


async def test_aiosqlite_adk_fts5_options_render_only_when_fts_enabled() -> None:
    """FTS5 tokenizer/detail options should be emitted only for opt-in FTS DDL."""
    default_store = AiosqliteADKMemoryStore(AiosqliteConfig())
    configured_store = AiosqliteADKMemoryStore(
        AiosqliteConfig(
            extension_config={
                "adk": {"memory_use_fts": True, "fts_tokenize": "porter unicode61", "fts_detail": "column"}
            }
        )
    )

    assert "tokenize" not in await default_store._memory_table_ddl()
    sql = await configured_store._memory_table_ddl()
    assert "tokenize = 'porter unicode61'" in sql
    assert "detail = column" in sql


class _SessionListCursor:
    def __init__(self) -> None:
        self.rows: list[Any] = []

    async def fetchall(self) -> "list[Any]":
        return self.rows


class _SessionListConnection:
    """Record the session-list query and its bound parameters."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, sql: str, params: "tuple[Any, ...] | None" = None) -> _SessionListCursor:
        if params is not None:
            self.calls.append((sql, params))
        return _SessionListCursor()

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _session_store_with_connection() -> "tuple[AiosqliteADKStore, _SessionListConnection]":
    conn = _SessionListConnection()
    config = MagicMock()
    config.extension_config = {"adk": {}}
    config.provide_connection = lambda *_a, **_k: conn
    return AiosqliteADKStore(config), conn


async def test_aiosqlite_list_sessions_binds_order_and_page() -> None:
    """Explicit ordering renders inline while page bounds bind as qmark parameters."""
    store, conn = _session_store_with_connection()

    await store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = conn.calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = ? AND user_id = ? "
        "ORDER BY create_time ASC, id ASC LIMIT ? OFFSET ?"
    )
    assert params == ("app", "u1", 10, 20)


async def test_aiosqlite_list_sessions_defaults_to_recent_first_without_a_page() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, conn = _session_store_with_connection()

    await store.list_sessions("app")

    sql, params = conn.calls[0]
    assert _normalized(sql).endswith("WHERE app_name = ? ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


async def test_aiosqlite_list_sessions_orders_page_after_scope_parameters() -> None:
    """Page values bind after the scope parameters that are actually present."""
    store, conn = _session_store_with_connection()

    await store.list_sessions("app", limit=5)

    sql, params = conn.calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC LIMIT ? OFFSET ?")
    assert params == ("app", 5, 0)


async def test_aiosqlite_list_sessions_zero_limit_never_opens_a_connection() -> None:
    """A zero limit short-circuits before any database work."""
    store, conn = _session_store_with_connection()

    assert await store.list_sessions("app", limit=0) == []
    assert conn.calls == []


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"order_by": "id"}, id="unknown-order-column"),
        pytest.param({"limit": -1}, id="negative-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"offset": 5}, id="unbounded-offset"),
    ],
)
async def test_aiosqlite_list_sessions_rejects_invalid_options_before_connecting(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, conn = _session_store_with_connection()

    with pytest.raises(ValueError):
        await store.list_sessions("app", **options)

    assert conn.calls == []


async def test_aiosqlite_adk_recycled_connection_reruns_pragma_configuration() -> None:
    """Verify WeakSet tracking allows recycled connections to re-run PRAGMAs."""
    config = AiosqliteConfig()
    store = AiosqliteADKStore(config)
    assert isinstance(store._configured_connections, weakref.WeakSet)

    conn1 = AsyncRecordingConnection()
    await store._ensure_pragmas(conn1)
    assert len(conn1.statements) == 4

    await store._ensure_pragmas(conn1)
    assert len(conn1.statements) == 4

    conn2 = AsyncRecordingConnection()
    await store._ensure_pragmas(conn2)
    assert len(conn2.statements) == 4


async def test_aiosqlite_adk_get_events_with_limit_binds_parameter() -> None:
    """Verify get_events parameterizes LIMIT instead of string interpolation."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    store = AiosqliteADKStore(config)
    calls: list[tuple[str, tuple[Any, ...]]] = []

    class _MockEventsCursor:
        async def fetchall(self) -> list[Any]:
            return []

    class _MockEventsConnection:
        async def execute(self, sql: str, params: Any = None) -> _MockEventsCursor:
            calls.append((sql, params))
            return _MockEventsCursor()

    class _AsyncContextManager:
        async def __aenter__(self) -> Any:
            return _MockEventsConnection()

        async def __aexit__(self, *_: Any) -> None:
            return None

    config.provide_connection = MagicMock(return_value=_AsyncContextManager())  # type: ignore[method-assign]

    events = await store.get_events("app", "user", "session", limit=10)
    assert events == []
    assert len(calls) >= 1
    sql, params = calls[-1]
    assert "LIMIT ?" in sql
    assert 10 in params


async def test_aiosqlite_adk_memory_store_persists_and_retrieves_float32_embeddings() -> None:
    """Verify memory entries persist and retrieve float32 embeddings."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    store = AiosqliteADKMemoryStore(config)
    try:
        await store.create_tables()

        now = datetime.now(UTC)
        embedding = [0.1, 0.2, 0.3, 0.4]
        record = StoredMemory(
            id=str(uuid4()),
            session_id="s1",
            app_name="app",
            user_id="u1",
            scope="user",
            event_id="evt-1",
            author="user",
            timestamp=now,
            content_json={"text": "vector search test"},
            content_text="vector search test",
            metadata_json=None,
            inserted_at=now,
            embedding=embedding,
        )
        inserted = await store.insert_memory_entries([record])
        assert inserted == 1

        results = await store.search_entries(query="vector", app_name="app", user_id="u1", embedding=embedding)
        assert len(results) == 1
        assert results[0]["event_id"] == "evt-1"
        assert results[0]["embedding"] is not None
        assert len(results[0]["embedding"]) == 4
        assert results[0]["embedding"] == pytest.approx(embedding, abs=1e-6)
    finally:
        await config.close_pool()


@pytest.mark.parametrize(
    ("renew_for", "expect_renewed"),
    [
        pytest.param(None, False, id="none"),
        pytest.param(0, False, id="zero-int"),
        pytest.param(-5, False, id="negative-int"),
        pytest.param(timedelta(seconds=0), False, id="zero-timedelta"),
        pytest.param(timedelta(milliseconds=500), True, id="subsecond-timedelta"),
        pytest.param(30, True, id="positive-int"),
    ],
)
async def test_aiosqlite_adk_get_session_renews_only_for_positive_duration(
    renew_for: "int | timedelta | None", expect_renewed: bool
) -> None:
    """get_session touches update_time only when renew_for is positive."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    store = AiosqliteADKStore(config)
    try:
        await store.create_tables()
        await store.create_session("s1", "app-a", "user-1", {"k": "v"})
        async with config.provide_connection() as conn:
            cur = await conn.execute("UPDATE adk_session SET update_time = ? WHERE id = ?", (2451545.0, "s1"))
            await cur.close()
            await end_transaction(conn, commit=True)

        baseline = await store.get_session("app-a", "user-1", "s1")
        assert baseline is not None

        fetched = await store.get_session("app-a", "user-1", "s1", renew_for=renew_for)
        assert fetched is not None
        if expect_renewed:
            assert fetched["update_time"] > baseline["update_time"]
        else:
            assert fetched["update_time"] == baseline["update_time"]
    finally:
        await config.close_pool()


@pytest.mark.parametrize(
    ("event_app", "event_user", "event_session", "expect_succeed"),
    [
        pytest.param("app-a", "user-1", "s1", True, id="owner"),
        pytest.param("app-b", "user-1", "s1", False, id="wrong-app"),
        pytest.param("app-a", "user-2", "s1", False, id="wrong-user"),
        pytest.param("app-a", "user-1", "missing", False, id="missing-session"),
    ],
)
async def test_aiosqlite_adk_append_event_enforces_session_ownership(
    event_app: str, event_user: str, event_session: str, expect_succeed: bool
) -> None:
    """append_event writes only when (session_id, app_name, user_id) matches an existing session."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    store = AiosqliteADKStore(config)
    try:
        await store.create_tables()
        await store.create_session("s1", "app-a", "user-1", {})
        event = StoredEvent(
            id="evt-1",
            app_name=event_app,
            user_id=event_user,
            session_id=event_session,
            invocation_id="inv-1",
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            event_data={"role": "user"},
        )

        if expect_succeed:
            await store.append_event(event)
            events = await store.get_events("app-a", "user-1", "s1")
            assert [item["id"] for item in events] == ["evt-1"]
        else:
            with pytest.raises(ValueError, match="not found"):
                await store.append_event(event)
            async with config.provide_connection() as conn:
                cur = await conn.execute(f"SELECT COUNT(*) FROM {store.events_table}")
                row = await cur.fetchone()
                await cur.close()
            assert row is not None
            assert row[0] == 0
    finally:
        await config.close_pool()


@pytest.mark.parametrize(
    ("record_app", "record_user", "record_session", "arg_session", "expect_succeed"),
    [
        pytest.param("app-a", "user-1", "s1", "s1", True, id="matching-owner"),
        pytest.param("app-b", "user-1", "s1", "s1", False, id="mismatched-app"),
        pytest.param("app-a", "user-2", "s1", "s1", False, id="mismatched-user"),
        pytest.param("app-a", "user-1", "s2", "s1", False, id="mismatched-session"),
        pytest.param("app-a", "user-1", "missing", "missing", False, id="missing-session"),
    ],
)
async def test_aiosqlite_adk_append_event_and_update_state_validates_scope_and_ownership(
    record_app: str, record_user: str, record_session: str, arg_session: str, expect_succeed: bool
) -> None:
    """append_event_and_update_state rejects mismatched records and missing sessions without inserting events."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    store = AiosqliteADKStore(config)
    try:
        await store.create_tables()
        await store.create_session("s1", "app-a", "user-1", {"step": 0})
        event = StoredEvent(
            id="evt-atomic",
            app_name=record_app,
            user_id=record_user,
            session_id=record_session,
            invocation_id="inv-1",
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            event_data={"role": "model"},
        )

        if expect_succeed:
            updated = await store.append_event_and_update_state(event, "app-a", "user-1", arg_session, {"step": 1})
            assert updated["state"] == {"step": 1}
            assert [item["id"] for item in await store.get_events("app-a", "user-1", "s1")] == ["evt-atomic"]
        else:
            with pytest.raises(ValueError):
                await store.append_event_and_update_state(event, "app-a", "user-1", arg_session, {"step": 1})
            async with config.provide_connection() as conn:
                cur = await conn.execute(f"SELECT COUNT(*) FROM {store.events_table}")
                row = await cur.fetchone()
                await cur.close()
            assert row is not None
            assert row[0] == 0
    finally:
        await config.close_pool()


async def test_aiosqlite_adk_get_events_joins_session_owner_columns() -> None:
    """get_events omits event rows whose owner columns differ from the owning session row."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    store = AiosqliteADKStore(config)
    try:
        await store.create_tables()
        await store.create_session("s1", "app-a", "user-1", {})
        await store.append_event(
            StoredEvent(
                id="evt-owned",
                app_name="app-a",
                user_id="user-1",
                session_id="s1",
                invocation_id="inv-1",
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                event_data={"ok": True},
            )
        )
        async with config.provide_connection() as conn:
            cur = await conn.execute(
                f"INSERT INTO {store.events_table} (id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("evt-foreign", "app-b", "user-2", "s1", "inv-2", 2461042.0, "{}"),
            )
            await cur.close()
            await end_transaction(conn, commit=True)

        assert [item["id"] for item in await store.get_events("app-a", "user-1", "s1")] == ["evt-owned"]
        assert await store.get_events("app-b", "user-2", "s1") == []
    finally:
        await config.close_pool()


@pytest.mark.parametrize(
    ("second_app", "second_user", "expected_second_inserted", "expected_total"),
    [
        pytest.param("app-a", "user-1", 0, 1, id="same-owner-duplicate"),
        pytest.param("app-b", "user-1", 1, 2, id="different-app-same-event-id"),
        pytest.param("app-a", "user-2", 1, 2, id="different-user-same-event-id"),
    ],
)
async def test_aiosqlite_adk_memory_deduplicates_per_app_user_event_id(
    second_app: str, second_user: str, expected_second_inserted: int, expected_total: int
) -> None:
    """Memory inserts deduplicate on (app_name, user_id, event_id) rather than event_id alone."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    store = AiosqliteADKMemoryStore(config)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        await store.create_tables()
        first = StoredMemory(
            id="m-1",
            session_id="s1",
            app_name="app-a",
            user_id="user-1",
            scope="user",
            event_id="shared-evt",
            author="user",
            timestamp=now,
            content_json={"text": "alpha"},
            content_text="alpha",
            metadata_json=None,
            inserted_at=now,
            embedding=None,
        )
        second = StoredMemory(
            id="m-2",
            session_id="s2",
            app_name=second_app,
            user_id=second_user,
            scope="user",
            event_id="shared-evt",
            author="user",
            timestamp=now,
            content_json={"text": "alpha"},
            content_text="alpha",
            metadata_json=None,
            inserted_at=now,
            embedding=None,
        )

        assert await store.insert_memory_entries([first]) == 1
        assert await store.insert_memory_entries([second]) == expected_second_inserted
        async with config.provide_connection() as conn:
            cur = await conn.execute(f"SELECT COUNT(*) FROM {store.memory_table}")
            row = await cur.fetchone()
            await cur.close()
        assert row is not None
        assert row[0] == expected_total
    finally:
        await config.close_pool()


@pytest.mark.parametrize("use_fts", [False, True], ids=["simple", "fts5"])
@pytest.mark.parametrize(
    ("filter_app", "filter_user", "expected_deleted", "expected_remaining"),
    [
        pytest.param(None, None, 3, [], id="session-only"),
        pytest.param("app-a", None, 2, ["m-3"], id="app-scoped"),
        pytest.param("app-a", "user-1", 1, ["m-2", "m-3"], id="app-and-user-scoped"),
    ],
)
async def test_aiosqlite_adk_delete_entries_by_session_scopes_and_syncs_fts(
    use_fts: bool,
    filter_app: "str | None",
    filter_user: "str | None",
    expected_deleted: int,
    expected_remaining: list[str],
) -> None:
    """Scoped delete_entries_by_session removes only matching rows and keeps FTS5 content synchronized."""
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"adk": {"memory_use_fts": use_fts}}
    )
    store = AiosqliteADKMemoryStore(config)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        await store.create_tables()
        entries = [
            StoredMemory(
                id="m-1",
                session_id="shared-sess",
                app_name="app-a",
                user_id="user-1",
                scope="user",
                event_id="evt-1",
                author="user",
                timestamp=now,
                content_json={"text": "quartztoken"},
                content_text="quartztoken",
                metadata_json=None,
                inserted_at=now,
                embedding=None,
            ),
            StoredMemory(
                id="m-2",
                session_id="shared-sess",
                app_name="app-a",
                user_id="user-2",
                scope="user",
                event_id="evt-2",
                author="user",
                timestamp=now,
                content_json={"text": "quartztoken"},
                content_text="quartztoken",
                metadata_json=None,
                inserted_at=now,
                embedding=None,
            ),
            StoredMemory(
                id="m-3",
                session_id="shared-sess",
                app_name="app-b",
                user_id="user-1",
                scope="user",
                event_id="evt-3",
                author="user",
                timestamp=now,
                content_json={"text": "quartztoken"},
                content_text="quartztoken",
                metadata_json=None,
                inserted_at=now,
                embedding=None,
            ),
        ]
        assert await store.insert_memory_entries(entries) == 3

        deleted = await store.delete_entries_by_session("shared-sess", app_name=filter_app, user_id=filter_user)
        assert deleted == expected_deleted

        async with config.provide_connection() as conn:
            cur = await conn.execute(f"SELECT id FROM {store.memory_table} ORDER BY id ASC")
            remaining_ids = [row[0] for row in await cur.fetchall()]
            await cur.close()
        assert remaining_ids == expected_remaining

        app_a_u1 = [r["id"] for r in await store.search_entries("quartztoken", "app-a", "user-1")]
        app_a_u2 = [r["id"] for r in await store.search_entries("quartztoken", "app-a", "user-2")]
        app_b_u1 = [r["id"] for r in await store.search_entries("quartztoken", "app-b", "user-1")]
        assert app_a_u1 == (["m-1"] if "m-1" in expected_remaining else [])
        assert app_a_u2 == (["m-2"] if "m-2" in expected_remaining else [])
        assert app_b_u1 == (["m-3"] if "m-3" in expected_remaining else [])
    finally:
        await config.close_pool()
