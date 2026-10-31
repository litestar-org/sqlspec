"""Unified MySQL ADK store integration tests across all MySQL driver modes."""

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from sqlspec.exceptions import IntegrityError
from sqlspec.utils.module_loader import import_optional_attr

if TYPE_CHECKING:
    from sqlspec.extensions.adk._types import StoredEvent
    from tests.integration.fixtures.mysql import MysqlAdkStoreHarness
else:
    StoredEvent = import_optional_attr("sqlspec.extensions.adk._types", "StoredEvent")

pytest.importorskip("google.adk")

pytestmark = [pytest.mark.mysql, pytest.mark.integration, pytest.mark.xdist_group("mysql")]


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

    with pytest.raises(IntegrityError):
        await mysql_adk_store_harness.create_session(
            fk_store, "invalid-fk", app_name, user_id, {"tenant": "invalid"}, owner_id=999
        )

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
