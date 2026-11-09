"""SAP SQL Anywhere integration tests for extension schema reconciliation."""

from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("sqlanydb", reason="SAP SQL Anywhere driver sqlanydb is not installed")

from sqlspec.adapters.sqlanywhere import SqlanywhereConfig
from sqlspec.config import ExtensionConfigs
from sqlspec.migrations.schema import metadata_has_index

pytestmark = [pytest.mark.sqlanywhere, pytest.mark.integration, pytest.mark.xdist_group("sqlanywhere")]


def test_sqlanywhere_reconcile_restores_dropped_adk_index_idempotently(
    sqlanywhere_connection_config: dict[str, Any],
) -> None:
    """Dropping an ADK companion index and running migrate_up() twice restores the index idempotently."""
    suffix = uuid4().hex[:8]
    litestar_table = f"sa_ls_{suffix}"
    events_table = f"sa_ev_{suffix}"
    session_table = f"sa_adk_s_{suffix}"
    adk_events_table = f"sa_adk_e_{suffix}"
    app_state_table = f"sa_adk_a_{suffix}"
    user_state_table = f"sa_adk_u_{suffix}"
    metadata_table = f"sa_adk_m_{suffix}"
    memory_table = f"sa_adk_mem_{suffix}"
    ledger_table = f"sa_mig_{suffix}"
    dropped_index = f"idx_{session_table}_app_user"

    ext_config: ExtensionConfigs = {
        "litestar": {"session_table": litestar_table, "migration_mode": "reconcile"},
        "events": {"queue_table": events_table, "migration_mode": "reconcile"},
        "adk": {
            "migration_mode": "reconcile",
            "enable_memory": True,
            "session_table": session_table,
            "events_table": adk_events_table,
            "app_state_table": app_state_table,
            "user_state_table": user_state_table,
            "metadata_table": metadata_table,
            "memory_table": memory_table,
        },
    }
    config = SqlanywhereConfig(
        connection_config=dict(sqlanywhere_connection_config),
        migration_config={"version_table_name": ledger_table},
        extension_config=ext_config,
    )

    try:
        config.migrate_up()
        with config.provide_session() as driver:
            indexes = driver.data_dictionary.get_indexes(driver, table=session_table)
            assert metadata_has_index(indexes, session_table, dropped_index) is True
            driver.execute_script(f'DROP INDEX IF EXISTS "{dropped_index}"')
            driver.commit()
            after_drop = driver.data_dictionary.get_indexes(driver, table=session_table)
            assert metadata_has_index(after_drop, session_table, dropped_index) is False

        config.migrate_up()
        config.migrate_up()

        with config.provide_session() as driver:
            restored = driver.data_dictionary.get_indexes(driver, table=session_table)
            assert metadata_has_index(restored, session_table, dropped_index) is True
    finally:
        with config.provide_session() as driver:
            for tbl in (
                memory_table,
                metadata_table,
                user_state_table,
                app_state_table,
                adk_events_table,
                session_table,
                events_table,
                litestar_table,
                ledger_table,
            ):
                driver.execute_script(f'DROP TABLE IF EXISTS "DBA"."{tbl}"')
            driver.commit()
        config.close_pool()
