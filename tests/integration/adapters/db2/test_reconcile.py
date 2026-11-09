"""Integration tests for IBM Db2 built-in extension schema reconciliation."""

from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("ibm_db")
pytest.importorskip("ibm_db_dbi")

from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2SyncConfig
from sqlspec.migrations.schema import metadata_has_index

pytestmark = [pytest.mark.db2, pytest.mark.integration, pytest.mark.xdist_group("db2")]


def _resolve_db2_connection_config(request: pytest.FixtureRequest) -> dict[str, Any]:
    """Resolve live Db2 connection parameters or skip when the host kernel cannot start Db2."""
    try:
        return dict(request.getfixturevalue("db2_connection_config"))
    except pytest.skip.Exception:
        raise
    except Exception as exc:
        pytest.skip(f"Db2 service container unavailable on host: {exc}")


def _extension_config(prefix: str) -> tuple[dict[str, Any], tuple[str, ...], str]:
    litestar_table = f"{prefix}_ls_sess"
    events_table = f"{prefix}_evt_q"
    adk_session = f"{prefix}_adk_sess"
    adk_event = f"{prefix}_adk_evt"
    adk_app_state = f"{prefix}_adk_app"
    adk_user_state = f"{prefix}_adk_usr"
    adk_meta = f"{prefix}_adk_meta"
    adk_memory = f"{prefix}_adk_mem"
    ext = {
        "litestar": {"migration_mode": "reconcile", "session_table": litestar_table},
        "events": {"migration_mode": "reconcile", "queue_table": events_table},
        "adk": {
            "migration_mode": "reconcile",
            "enable_memory": True,
            "session_table": adk_session,
            "events_table": adk_event,
            "app_state_table": adk_app_state,
            "user_state_table": adk_user_state,
            "metadata_table": adk_meta,
            "memory_table": adk_memory,
        },
    }
    tables = (adk_memory, adk_meta, adk_user_state, adk_app_state, adk_event, adk_session, events_table, litestar_table)
    dropped_index = f"idx_{adk_session}_update_time"
    return ext, tables, dropped_index


def test_db2_sync_reconcile_converges_and_repairs_dropped_adk_index(request: pytest.FixtureRequest) -> None:
    """Db2SyncConfig migrate_up() reconciles litestar, events, and adk without ledger rows and repairs dropped indexes."""
    conn_cfg = _resolve_db2_connection_config(request)
    prefix = f"db2_rec_s_{uuid4().hex[:6]}"
    ext, tables, dropped_index = _extension_config(prefix)
    adk_session = ext["adk"]["session_table"]
    config = Db2SyncConfig(connection_config=conn_cfg, extension_config=ext)
    try:
        config.migrate_up()
        with config.provide_session() as driver:
            existing_tables = {row["table_name"].casefold() for row in driver.data_dictionary.get_tables(driver)}
            assert {name.casefold() for name in tables}.issubset(existing_tables)
            assert "ddl_migrations" not in existing_tables
            indexes = driver.data_dictionary.get_indexes(driver, table=adk_session)
            assert metadata_has_index(indexes, adk_session, dropped_index)
            driver.execute(f"DROP INDEX {dropped_index}")
            driver.commit()
            after_drop = driver.data_dictionary.get_indexes(driver, table=adk_session)
            assert not metadata_has_index(after_drop, adk_session, dropped_index)

        config.migrate_up()
        config.migrate_up()
        with config.provide_session() as driver:
            restored = driver.data_dictionary.get_indexes(driver, table=adk_session)
            assert metadata_has_index(restored, adk_session, dropped_index)
            final_tables = {row["table_name"].casefold() for row in driver.data_dictionary.get_tables(driver)}
            assert "ddl_migrations" not in final_tables
    finally:
        with config.provide_session() as cleanup:
            cleanup.rollback()
            for table in tables:
                try:
                    cleanup.execute(f"DROP TABLE {table}")
                    cleanup.commit()
                except Exception:
                    cleanup.rollback()
        config.close_pool()


@pytest.mark.anyio
async def test_db2_async_reconcile_converges_and_repairs_dropped_adk_index(request: pytest.FixtureRequest) -> None:
    """Db2AsyncConfig migrate_up() reconciles litestar, events, and adk without ledger rows and repairs dropped indexes."""
    conn_cfg = _resolve_db2_connection_config(request)
    prefix = f"db2_rec_a_{uuid4().hex[:6]}"
    ext, tables, dropped_index = _extension_config(prefix)
    adk_session = ext["adk"]["session_table"]
    config = Db2AsyncConfig(connection_config=conn_cfg, extension_config=ext)
    try:
        await config.migrate_up()
        async with config.provide_session() as driver:
            existing_tables = {row["table_name"].casefold() for row in await driver.data_dictionary.get_tables(driver)}
            assert {name.casefold() for name in tables}.issubset(existing_tables)
            assert "ddl_migrations" not in existing_tables
            indexes = await driver.data_dictionary.get_indexes(driver, table=adk_session)
            assert metadata_has_index(indexes, adk_session, dropped_index)
            await driver.execute(f"DROP INDEX {dropped_index}")
            await driver.commit()

        await config.migrate_up()
        await config.migrate_up()
        async with config.provide_session() as driver:
            restored = await driver.data_dictionary.get_indexes(driver, table=adk_session)
            assert metadata_has_index(restored, adk_session, dropped_index)
            final_tables = {row["table_name"].casefold() for row in await driver.data_dictionary.get_tables(driver)}
            assert "ddl_migrations" not in final_tables
    finally:
        async with config.provide_session() as cleanup:
            await cleanup.rollback()
            for table in tables:
                try:
                    await cleanup.execute(f"DROP TABLE {table}")
                    await cleanup.commit()
                except Exception:
                    await cleanup.rollback()
        await config.close_pool()
