"""Unified MySQL ADK store integration tests across all MySQL driver modes."""

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pymysql
import pytest

from sqlspec.utils.module_loader import import_optional_attr
from tests.integration.fixtures.mysql import _build_mysql_adk_config_and_store, _close_mysql_config

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from pytest_databases.docker.mysql import MySQLService

    from sqlspec.adapters.aiomysql.adk import AiomysqlADKMemoryStore
    from sqlspec.adapters.asyncmy.adk import AsyncmyADKMemoryStore
    from sqlspec.adapters.mysqlconnector.adk import MysqlConnectorAsyncADKMemoryStore, MysqlConnectorSyncADKMemoryStore
    from sqlspec.adapters.pymysql.adk import PyMysqlADKMemoryStore
    from sqlspec.extensions.adk._types import StoredEvent
    from sqlspec.extensions.adk.memory._types import StoredMemory
    from tests.integration.fixtures.mysql import MysqlAdkStoreHarness
else:
    StoredEvent = import_optional_attr("sqlspec.extensions.adk._types", "StoredEvent")
    StoredMemory = import_optional_attr("sqlspec.extensions.adk.memory._types", "StoredMemory")
    AiomysqlADKMemoryStore = import_optional_attr("sqlspec.adapters.aiomysql.adk", "AiomysqlADKMemoryStore")
    AsyncmyADKMemoryStore = import_optional_attr("sqlspec.adapters.asyncmy.adk", "AsyncmyADKMemoryStore")
    MysqlConnectorAsyncADKMemoryStore = import_optional_attr(
        "sqlspec.adapters.mysqlconnector.adk", "MysqlConnectorAsyncADKMemoryStore"
    )
    MysqlConnectorSyncADKMemoryStore = import_optional_attr(
        "sqlspec.adapters.mysqlconnector.adk", "MysqlConnectorSyncADKMemoryStore"
    )
    PyMysqlADKMemoryStore = import_optional_attr("sqlspec.adapters.pymysql.adk", "PyMysqlADKMemoryStore")

pytest.importorskip("google.adk")

pytestmark = [pytest.mark.mysql, pytest.mark.integration, pytest.mark.xdist_group("mysql")]

_MEMORY_STORES: dict[str, Any] = {
    "aiomysql": AiomysqlADKMemoryStore,
    "asyncmy": AsyncmyADKMemoryStore,
    "mysqlconnector-async": MysqlConnectorAsyncADKMemoryStore,
    "mysqlconnector-sync": MysqlConnectorSyncADKMemoryStore,
    "pymysql": PyMysqlADKMemoryStore,
}

_APP_TABLES: dict[str, Any] = {
    "session_table": "sqlspec_adk_app.t",
    "events_table": "sqlspec_adk_app.e",
    "app_state_table": "sqlspec_adk_app.a",
    "user_state_table": "sqlspec_adk_app.u",
    "metadata_table": "sqlspec_adk_app.d",
}


async def _call(harness: "MysqlAdkStoreHarness", fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Invoke a store method, awaiting the result when the harness is async."""
    if harness.is_async:
        return await fn(*args, **kwargs)
    return fn(*args, **kwargs)


@pytest.fixture(scope="module")
def mysql_qualified_databases(mysql_service: "MySQLService", mysql_root_password: str) -> "Generator[None, None, None]":
    """Create test databases for qualified table tests and drop them on teardown."""
    conn = pymysql.connect(
        host=mysql_service.host, port=mysql_service.port, user="root", password=mysql_root_password, autocommit=True
    )
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE IF NOT EXISTS sqlspec_adk_app")
        cur.execute("CREATE DATABASE IF NOT EXISTS sqlspec_adk_other")
        cur.execute(f"GRANT ALL ON sqlspec_adk_app.* TO '{mysql_service.user}'@'%'")
        cur.execute(f"GRANT ALL ON sqlspec_adk_other.* TO '{mysql_service.user}'@'%'")
    yield
    with conn.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS sqlspec_adk_other")
        cur.execute("DROP DATABASE IF EXISTS sqlspec_adk_app")
    conn.close()


@pytest.fixture
async def clean_qualified_tables(
    mysql_qualified_databases: None, mysql_adk_store_harness: "MysqlAdkStoreHarness"
) -> "AsyncGenerator[None, None]":
    """Ensure clean table state before and after each qualified table test."""

    async def _drop_all() -> None:
        for sql in (
            "DROP TABLE IF EXISTS sqlspec_adk_other.e",
            "DROP TABLE IF EXISTS sqlspec_adk_app.e",
            "DROP TABLE IF EXISTS sqlspec_adk_app.t",
            "DROP TABLE IF EXISTS sqlspec_adk_app.a",
            "DROP TABLE IF EXISTS sqlspec_adk_app.u",
            "DROP TABLE IF EXISTS sqlspec_adk_app.d",
            "DROP TABLE IF EXISTS sqlspec_adk_app.m",
        ):
            await mysql_adk_store_harness.execute_sql(mysql_adk_store_harness.config, sql)

    await _drop_all()
    yield
    await _drop_all()


async def test_mysql_adk_json_and_timestamp6_column_types(mysql_adk_store_harness: "MysqlAdkStoreHarness") -> None:
    """Verify MySQL ADK tables use native JSON columns and TIMESTAMP(6) microsecond columns."""
    session_columns = await mysql_adk_store_harness.select_rows(
        mysql_adk_store_harness.config,
        """
        SELECT
            COLUMN_NAME AS column_name,
            DATA_TYPE AS data_type,
            COLUMN_TYPE AS column_type,
            DATETIME_PRECISION AS datetime_precision
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION
        """,
        table_name=mysql_adk_store_harness.session_table,
    )
    by_session_col = {str(row["column_name"]): row for row in session_columns}
    assert str(by_session_col["state"]["data_type"]).lower() == "json"
    assert "timestamp(6)" in str(by_session_col["create_time"]["column_type"]).lower()
    assert int(by_session_col["create_time"]["datetime_precision"]) == 6
    assert "timestamp(6)" in str(by_session_col["update_time"]["column_type"]).lower()
    assert int(by_session_col["update_time"]["datetime_precision"]) == 6

    event_columns = await mysql_adk_store_harness.select_rows(
        mysql_adk_store_harness.config,
        """
        SELECT
            COLUMN_NAME AS column_name,
            DATA_TYPE AS data_type,
            COLUMN_TYPE AS column_type,
            DATETIME_PRECISION AS datetime_precision
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION
        """,
        table_name=mysql_adk_store_harness.events_table,
    )
    by_event_col = {str(row["column_name"]): row for row in event_columns}
    assert {"session_id", "invocation_id", "timestamp", "event_data"}.issubset(by_event_col)
    assert str(by_event_col["event_data"]["data_type"]).lower() == "json"
    assert "timestamp(6)" in str(by_event_col["timestamp"]["column_type"]).lower()
    assert int(by_event_col["timestamp"]["datetime_precision"]) == 6


async def test_mysql_adk_microsecond_timestamp_roundtrip(mysql_adk_store_harness: "MysqlAdkStoreHarness") -> None:
    """Verify TIMESTAMP(6) preserves sub-second microsecond precision on sessions and events."""
    session_id = "session-micro-001"
    app_name = "test-app"
    user_id = "user-micro-001"

    created = await mysql_adk_store_harness.create_session(
        mysql_adk_store_harness.store, session_id, app_name, user_id, {"precision": "microsecond"}
    )
    assert isinstance(created["create_time"], datetime)

    event_time = datetime(2026, 1, 15, 12, 34, 56, 123456, tzinfo=UTC)
    event: StoredEvent = {
        "id": "event-micro-001",
        "app_name": app_name,
        "user_id": user_id,
        "session_id": session_id,
        "invocation_id": "inv-micro-001",
        "timestamp": event_time,
        "event_data": {"app_name": app_name, "author": "system"},
    }
    await mysql_adk_store_harness.append_event(mysql_adk_store_harness.store, event)

    events = await mysql_adk_store_harness.get_events(mysql_adk_store_harness.store, app_name, user_id, session_id)
    assert len(events) == 1
    assert events[0]["timestamp"].microsecond == 123456


async def test_mysql_adk_owner_id_fk_constraint_isolation_and_cascade(
    mysql_adk_store_harness: "MysqlAdkStoreHarness",
) -> None:
    """Verify owner_id_column DDL, FK referential integrity, multi-tenant isolation, and ON DELETE CASCADE."""
    fk_store = mysql_adk_store_harness.fk_store
    fk_config = mysql_adk_store_harness.fk_config
    assert fk_store.owner_id_column_name == "tenant_id"
    assert fk_store.owner_id_column_ddl is not None
    assert "tenant_id" in fk_store.owner_id_column_ddl

    tenant_col_rows = await mysql_adk_store_harness.select_rows(
        fk_config,
        """
        SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
          AND COLUMN_NAME = 'tenant_id'
        """,
        table_name=mysql_adk_store_harness.fk_session_table,
    )
    assert len(tenant_col_rows) == 1
    assert str(tenant_col_rows[0]["data_type"]).lower() == "bigint"

    app_name = "test-app"
    user_id = "user-shared"
    await mysql_adk_store_harness.create_session(
        fk_store, "tenant1-s1", app_name, user_id, {"tenant": "one"}, owner_id=1
    )
    await mysql_adk_store_harness.create_session(
        fk_store, "tenant1-s2", app_name, user_id, {"tenant": "one"}, owner_id=1
    )
    await mysql_adk_store_harness.create_session(
        fk_store, "tenant2-s1", app_name, user_id, {"tenant": "two"}, owner_id=2
    )

    with pytest.raises(Exception) as exc_info:
        await mysql_adk_store_harness.create_session(
            fk_store, "invalid-fk", app_name, user_id, {"tenant": "invalid"}, owner_id=999
        )
    assert any(token in str(exc_info.value).lower() for token in ("foreign key", "1452", "constraint"))

    tenant1_rows = await mysql_adk_store_harness.select_rows(
        fk_config,
        f"SELECT id AS id FROM {mysql_adk_store_harness.fk_session_table} WHERE tenant_id = :tenant_id ORDER BY id",
        tenant_id=1,
    )
    assert [row["id"] for row in tenant1_rows] == ["tenant1-s1", "tenant1-s2"]

    tenant2_rows = await mysql_adk_store_harness.select_rows(
        fk_config,
        f"SELECT id AS id FROM {mysql_adk_store_harness.fk_session_table} WHERE tenant_id = :tenant_id ORDER BY id",
        tenant_id=2,
    )
    assert [row["id"] for row in tenant2_rows] == ["tenant2-s1"]

    await mysql_adk_store_harness.execute_sql(
        fk_config, f"DELETE FROM {mysql_adk_store_harness.tenants_table} WHERE id = 1"
    )
    assert await mysql_adk_store_harness.get_session(fk_store, app_name, user_id, "tenant1-s1") is None
    assert await mysql_adk_store_harness.get_session(fk_store, app_name, user_id, "tenant1-s2") is None
    assert await mysql_adk_store_harness.get_session(fk_store, app_name, user_id, "tenant2-s1") is not None


async def test_mysql_adk_qualified_tables_land_in_database(
    mysql_adk_store_harness: "MysqlAdkStoreHarness", mysql_service: "MySQLService", clean_qualified_tables: None
) -> None:
    """Verify schema-qualified tables land in target database and derive bare index names."""
    is_async, config, store = _build_mysql_adk_config_and_store(
        mysql_service, mysql_adk_store_harness.driver_mode, _APP_TABLES
    )
    try:
        await _call(mysql_adk_store_harness, store.create_tables)
        await _call(mysql_adk_store_harness, store.create_tables)

        app_tables = await mysql_adk_store_harness.select_rows(
            config,
            """
            SELECT TABLE_NAME AS table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'sqlspec_adk_app'
            """,
        )
        app_names = {row["table_name"] for row in app_tables}
        assert {"t", "e", "a", "u", "d"}.issubset(app_names)

        curr_tables = await mysql_adk_store_harness.select_rows(
            config,
            """
            SELECT TABLE_NAME AS table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
            """,
        )
        curr_names = {row["table_name"] for row in curr_tables}
        assert curr_names.isdisjoint({"t", "e", "a", "u", "d"})

        indexes = await mysql_adk_store_harness.select_rows(
            config,
            """
            SELECT INDEX_NAME AS index_name
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = 'sqlspec_adk_app'
              AND TABLE_NAME = 't'
            """,
        )
        index_names = {row["index_name"] for row in indexes}
        assert "idx_t_app_user" in index_names
    finally:
        await _close_mysql_config(config, is_async=is_async)


async def test_mysql_adk_qualified_session_round_trip(
    mysql_adk_store_harness: "MysqlAdkStoreHarness", mysql_service: "MySQLService", clean_qualified_tables: None
) -> None:
    """Verify session create, update state, renew, list, and delete on qualified tables."""
    is_async, config, store = _build_mysql_adk_config_and_store(
        mysql_service, mysql_adk_store_harness.driver_mode, _APP_TABLES
    )
    try:
        await _call(mysql_adk_store_harness, store.create_tables)
        created = await _call(mysql_adk_store_harness, store.create_session, "s1", "app", "user", {"n": 1})
        assert created["id"] == "s1"

        await _call(mysql_adk_store_harness, store.update_session_state, "app", "user", "s1", {"n": 2})

        session = await _call(mysql_adk_store_harness, store.get_session, "app", "user", "s1", renew_for=60)
        assert session is not None
        assert session["state"] == {"n": 2}

        sessions = await _call(mysql_adk_store_harness, store.list_sessions, "app")
        session_ids = [s["id"] for s in sessions]
        assert "s1" in session_ids

        await _call(mysql_adk_store_harness, store.delete_session, "app", "user", "s1")
        assert await _call(mysql_adk_store_harness, store.get_session, "app", "user", "s1") is None
    finally:
        await _close_mysql_config(config, is_async=is_async)


async def test_mysql_adk_qualified_event_round_trip(
    mysql_adk_store_harness: "MysqlAdkStoreHarness", mysql_service: "MySQLService", clean_qualified_tables: None
) -> None:
    """Verify append event, append and update state, list events, and delete expired on qualified tables."""
    is_async, config, store = _build_mysql_adk_config_and_store(
        mysql_service, mysql_adk_store_harness.driver_mode, _APP_TABLES
    )
    try:
        await _call(mysql_adk_store_harness, store.create_tables)
        await _call(mysql_adk_store_harness, store.create_session, "s1", "app", "user", {"n": 1})

        event1: StoredEvent = {
            "id": "e1",
            "app_name": "app",
            "user_id": "user",
            "session_id": "s1",
            "invocation_id": "inv1",
            "timestamp": datetime(2026, 1, 1, 10, 0, 0, tzinfo=UTC),
            "event_data": {"msg": "first"},
        }
        await _call(mysql_adk_store_harness, store.append_event, event1)

        event2: StoredEvent = {
            "id": "e2",
            "app_name": "app",
            "user_id": "user",
            "session_id": "s1",
            "invocation_id": "inv2",
            "timestamp": datetime(2026, 1, 1, 11, 0, 0, tzinfo=UTC),
            "event_data": {"msg": "second"},
        }
        await _call(mysql_adk_store_harness, store.append_event_and_update_state, event2, "app", "user", "s1", {"n": 3})

        events = await _call(mysql_adk_store_harness, store.get_events, "app", "user", "s1")
        assert len(events) == 2
        assert events[0]["id"] == "e1"
        assert events[1]["id"] == "e2"

        session = await _call(mysql_adk_store_harness, store.get_session, "app", "user", "s1")
        assert session is not None
        assert session["state"] == {"n": 3}

        future = datetime(2030, 1, 1, 0, 0, 0, tzinfo=UTC)
        deleted = await _call(mysql_adk_store_harness, store.delete_expired_events, future)
        assert deleted == 2

        remaining = await _call(mysql_adk_store_harness, store.get_events, "app", "user", "s1")
        assert remaining == []
    finally:
        await _close_mysql_config(config, is_async=is_async)


async def test_mysql_adk_qualified_state_round_trip(
    mysql_adk_store_harness: "MysqlAdkStoreHarness", mysql_service: "MySQLService", clean_qualified_tables: None
) -> None:
    """Verify app state, user state, metadata upsert and retention cleanup on qualified tables."""
    is_async, config, store = _build_mysql_adk_config_and_store(
        mysql_service, mysql_adk_store_harness.driver_mode, _APP_TABLES
    )
    try:
        await _call(mysql_adk_store_harness, store.create_tables)
        await _call(mysql_adk_store_harness, store.create_session, "s1", "app", "user", {"init": True})

        await _call(mysql_adk_store_harness, store.upsert_app_state, "app", {"k": 1})
        await _call(mysql_adk_store_harness, store.upsert_app_state, "app", {"k": 2})

        await _call(mysql_adk_store_harness, store.upsert_user_state, "app", "user", {"k": 1})
        await _call(mysql_adk_store_harness, store.upsert_user_state, "app", "user", {"k": 2})

        await _call(mysql_adk_store_harness, store.set_metadata, "schema", "v1")

        app_state = await _call(mysql_adk_store_harness, store.get_app_state, "app")
        user_state = await _call(mysql_adk_store_harness, store.get_user_state, "app", "user")
        meta_val = await _call(mysql_adk_store_harness, store.get_metadata, "schema")

        assert app_state == {"k": 2}
        assert user_state == {"k": 2}
        assert meta_val == "v1"

        future = datetime(2030, 1, 1, 0, 0, 0, tzinfo=UTC)
        await _call(mysql_adk_store_harness, store.delete_idle_user_states, future)
        await _call(mysql_adk_store_harness, store.delete_idle_sessions, future)

        assert await _call(mysql_adk_store_harness, store.get_user_state, "app", "user") is None
        assert await _call(mysql_adk_store_harness, store.get_session, "app", "user", "s1") is None
    finally:
        await _close_mysql_config(config, is_async=is_async)


async def test_mysql_adk_qualified_memory_round_trip(
    mysql_adk_store_harness: "MysqlAdkStoreHarness", mysql_service: "MySQLService", clean_qualified_tables: None
) -> None:
    """Verify qualified memory table creation, deduplication, search, session deletion, and drop."""
    mem_config = {"memory_table": "sqlspec_adk_app.m", "memory_use_fts": True}
    is_async, config, _ = _build_mysql_adk_config_and_store(
        mysql_service, mysql_adk_store_harness.driver_mode, mem_config
    )
    store_cls = _MEMORY_STORES[mysql_adk_store_harness.driver_mode]
    store = store_cls(config)
    try:
        await _call(mysql_adk_store_harness, store.create_tables)

        now = datetime.now(tz=UTC)
        entry: StoredMemory = {
            "id": "m1",
            "session_id": "s1",
            "app_name": "app",
            "user_id": "user",
            "scope": "user",
            "event_id": "ev1",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "hello memory"},
            "content_text": "hello memory",
            "metadata_json": {"tag": "test"},
            "inserted_at": now,
            "embedding": None,
        }

        inserted1 = await _call(mysql_adk_store_harness, store.insert_memory_entries, [entry])
        assert inserted1 == 1

        inserted2 = await _call(mysql_adk_store_harness, store.insert_memory_entries, [entry])
        if mysql_adk_store_harness.driver_mode in ("aiomysql", "asyncmy"):
            assert inserted2 == 1
        else:
            assert inserted2 == 0

        found = await _call(mysql_adk_store_harness, store.search_entries, "hello", "app", "user")
        assert len(found) == 1
        assert found[0]["id"] == "m1"

        deleted = await _call(mysql_adk_store_harness, store.delete_entries_by_session, "s1")
        assert deleted == 1

        entry2: StoredMemory = {
            "id": "m2",
            "session_id": "s1",
            "app_name": "app",
            "user_id": "user",
            "scope": "user",
            "event_id": "ev2",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "hello memory 2"},
            "content_text": "hello memory 2",
            "metadata_json": {"tag": "test"},
            "inserted_at": now,
            "embedding": None,
        }
        reinserted = await _call(mysql_adk_store_harness, store.insert_memory_entries, [entry2])
        assert reinserted == 1

        older_deleted = await _call(mysql_adk_store_harness, store.delete_entries_older_than, 0)
        assert older_deleted >= 1

        await _call(mysql_adk_store_harness, store.drop_tables)

        app_tables = await mysql_adk_store_harness.select_rows(
            config,
            """
            SELECT TABLE_NAME AS table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'sqlspec_adk_app'
              AND TABLE_NAME = 'm'
            """,
        )
        assert len(app_tables) == 0
    finally:
        await _close_mysql_config(config, is_async=is_async)


async def test_mysql_adk_split_database_round_trip(
    mysql_adk_store_harness: "MysqlAdkStoreHarness", mysql_service: "MySQLService", clean_qualified_tables: None
) -> None:
    """Verify foreign key cascading across split databases when deleting a session."""
    split_tables: dict[str, Any] = {
        "session_table": "sqlspec_adk_app.t",
        "events_table": "sqlspec_adk_other.e",
        "app_state_table": "sqlspec_adk_app.a",
        "user_state_table": "sqlspec_adk_app.u",
        "metadata_table": "sqlspec_adk_app.d",
    }
    is_async, config, store = _build_mysql_adk_config_and_store(
        mysql_service, mysql_adk_store_harness.driver_mode, split_tables
    )
    try:
        await _call(mysql_adk_store_harness, store.create_tables)

        app_tables = await mysql_adk_store_harness.select_rows(
            config,
            """
            SELECT TABLE_NAME AS table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'sqlspec_adk_app'
              AND TABLE_NAME = 't'
            """,
        )
        assert len(app_tables) == 1

        other_tables = await mysql_adk_store_harness.select_rows(
            config,
            """
            SELECT TABLE_NAME AS table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'sqlspec_adk_other'
              AND TABLE_NAME = 'e'
            """,
        )
        assert len(other_tables) == 1

        await _call(mysql_adk_store_harness, store.create_session, "s1", "app", "user", {"split": True})
        event: StoredEvent = {
            "id": "e_split_1",
            "app_name": "app",
            "user_id": "user",
            "session_id": "s1",
            "invocation_id": "inv_split",
            "timestamp": datetime.now(tz=UTC),
            "event_data": {"split": True},
        }
        await _call(mysql_adk_store_harness, store.append_event, event)

        events_before = await _call(mysql_adk_store_harness, store.get_events, "app", "user", "s1")
        assert len(events_before) == 1

        await _call(mysql_adk_store_harness, store.delete_session, "app", "user", "s1")

        events_after = await _call(mysql_adk_store_harness, store.get_events, "app", "user", "s1")
        assert len(events_after) == 0
    finally:
        await _close_mysql_config(config, is_async=is_async)


async def test_mysql_adk_qualified_tables_missing_database(
    mysql_adk_store_harness: "MysqlAdkStoreHarness", mysql_service: "MySQLService"
) -> None:
    """Verify referencing a non-existent database fails with MySQL error 1049."""
    missing_tables: dict[str, Any] = {
        "session_table": "sqlspec_adk_missing.t",
        "events_table": "sqlspec_adk_missing.e",
        "app_state_table": "sqlspec_adk_missing.a",
        "user_state_table": "sqlspec_adk_missing.u",
        "metadata_table": "sqlspec_adk_missing.d",
    }
    is_async, config, store = _build_mysql_adk_config_and_store(
        mysql_service, mysql_adk_store_harness.driver_mode, missing_tables
    )
    try:
        with pytest.raises(Exception) as exc_info:
            await _call(mysql_adk_store_harness, store.create_tables)
        err_msg = str(exc_info.value).lower()
        assert "1049" in err_msg or "unknown database" in err_msg

        curr_tables = await mysql_adk_store_harness.select_rows(
            mysql_adk_store_harness.config,
            """
            SELECT TABLE_NAME AS table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 't'
            """,
        )
        assert len(curr_tables) == 0
    finally:
        await _close_mysql_config(config, is_async=is_async)
