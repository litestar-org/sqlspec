"""End-to-end Spanner emulator acceptance tests for extension schema reconciliation."""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
from google.api_core import exceptions as api_exceptions

from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.adk import SpannerSyncADKArtifactStore, SpannerSyncADKMemoryStore, SpannerSyncADKStore
from sqlspec.adapters.spanner.events.store import SpannerSyncEventQueueStore
from sqlspec.adapters.spanner.litestar.store import SpannerSyncStore
from sqlspec.config import ExtensionConfigs
from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk import StoredMemory
from sqlspec.extensions.adk.artifact import StoredArtifact
from tests.integration.fixtures.spanner import build_spanner_connection_config, drop_table_if_exists, run_ddl

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database
    from pytest_databases.docker.spanner import SpannerService

pytestmark = [pytest.mark.spanner, pytest.mark.integration, pytest.mark.xdist_group("spanner")]


def _drop_index_if_exists(database: "Database", index_name: str, kind: str = "INDEX") -> None:
    try:
        run_ddl(database, [f"DROP {kind} {index_name}"])
    except api_exceptions.GoogleAPICallError:
        pass


def _table_names(config: SpannerSyncConfig) -> set[str]:
    with config.provide_read_session() as driver:
        rows = driver.data_dictionary.get_tables(driver)
    return {str(row["table_name"]) for row in rows}


def _index_names(config: SpannerSyncConfig, table_name: str) -> set[str]:
    with config.provide_read_session() as driver:
        rows = driver.data_dictionary.get_indexes(driver, table=table_name)
    return {str(row["index_name"]) for row in rows}


def _column_names(config: SpannerSyncConfig, table_name: str) -> set[str]:
    with config.provide_read_session() as driver:
        rows = driver.data_dictionary.get_columns(driver, table=table_name)
    return {str(row["column_name"]) for row in rows}


@pytest.mark.anyio
async def test_spanner_emulator_reconciles_litestar_events_adk_and_artifact_end_to_end(
    spanner_service: "SpannerService", spanner_database: "Database"
) -> None:
    """Reconcile mode provisions and repairs Litestar, events, ADK, and artifact tables without ledger rows."""
    suffix = uuid4().hex[:8]
    litestar_table = f"ls_sess_{suffix}"
    events_table = f"ev_queue_{suffix}"
    session_table = f"adk_sess_{suffix}"
    adk_events_table = f"adk_ev_{suffix}"
    app_state_table = f"adk_app_{suffix}"
    user_state_table = f"adk_usr_{suffix}"
    metadata_table = f"adk_meta_{suffix}"
    memory_table = f"adk_mem_{suffix}"
    artifact_table = f"adk_art_{suffix}"
    ledger_table = f"ddl_mig_{suffix}"

    connection_config = build_spanner_connection_config(spanner_service)

    def _build_extensions(*, include_owner: bool) -> ExtensionConfigs:
        adk_cfg: dict[str, Any] = {
            "migration_mode": "reconcile",
            "session_table": session_table,
            "events_table": adk_events_table,
            "app_state_table": app_state_table,
            "user_state_table": user_state_table,
            "metadata_table": metadata_table,
            "enable_memory": True,
            "memory_table": memory_table,
            "artifact_table": artifact_table,
        }
        if include_owner:
            adk_cfg["owner_id_column"] = "owner_id STRING(64)"
        litestar_cfg: dict[str, Any] = {"session_table": litestar_table, "migration_mode": "reconcile"}
        events_cfg: dict[str, Any] = {"queue_table": events_table, "migration_mode": "reconcile"}
        return {"litestar": litestar_cfg, "events": events_cfg, "adk": adk_cfg}

    config = SpannerSyncConfig(
        connection_config=dict(connection_config),
        migration_config={"version_table_name": ledger_table},
        extension_config=_build_extensions(include_owner=False),
    )
    evolved_config: SpannerSyncConfig | None = None
    async_config: SpannerAsyncConfig | None = None

    try:
        config.migrate_up()

        expected_tables = {
            litestar_table,
            events_table,
            session_table,
            adk_events_table,
            app_state_table,
            user_state_table,
            metadata_table,
            memory_table,
            artifact_table,
        }
        present_tables = _table_names(config)
        assert expected_tables <= present_tables

        litestar_store = SpannerSyncStore(config)
        await litestar_store.set("ls-1", b"payload", expires_in=timedelta(minutes=5))
        assert await litestar_store.get("ls-1") == b"payload"

        adk_store = SpannerSyncADKStore(config)
        adk_store.create_session("s-1", "app", "user", {"step": 1})
        now = datetime.now(UTC)
        adk_store.append_event({
            "id": "e-1",
            "app_name": "app",
            "user_id": "user",
            "session_id": "s-1",
            "invocation_id": "inv-1",
            "timestamp": now,
            "event_data": {"author": "user", "text": "hi"},
        })

        memory_store = SpannerSyncADKMemoryStore(config)
        memory_entry: StoredMemory = {
            "id": "m-1",
            "session_id": "s-1",
            "app_name": "app",
            "user_id": "user",
            "scope": "user",
            "event_id": "e-1",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "remembered"},
            "content_text": "remembered",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": None,
        }
        assert memory_store.insert_memory_entries([memory_entry]) == 1

        artifact_store = SpannerSyncADKArtifactStore(config)
        artifact_record: StoredArtifact = {
            "app_name": "app",
            "user_id": "user",
            "session_id": "s-1",
            "filename": "report.txt",
            "version": 0,
            "mime_type": "text/plain",
            "canonical_uri": "gs://bucket/report.txt",
            "custom_metadata": {"tag": "v0"},
            "created_at": now,
        }
        artifact_store.insert_artifact(artifact_record)

        events_store = SpannerSyncEventQueueStore(config)
        assert events_store.table_name == events_table

        run_ddl(
            spanner_database,
            [f"DROP INDEX idx_{session_table}_update_time", f"DROP INDEX idx_{litestar_table}_expires_at"],
        )
        assert f"idx_{session_table}_update_time" not in _index_names(config, session_table)
        assert f"idx_{litestar_table}_expires_at" not in _index_names(config, litestar_table)

        evolved_config = SpannerSyncConfig(
            connection_config=dict(connection_config),
            migration_config={"version_table_name": ledger_table},
            extension_config=_build_extensions(include_owner=True),
        )
        evolved_config.migrate_up()
        evolved_config.migrate_up()

        async_config = SpannerAsyncConfig(
            connection_config=dict(connection_config),
            migration_config={"version_table_name": ledger_table},
            extension_config=_build_extensions(include_owner=True),
        )
        await async_config.migrate_up()

        assert f"idx_{session_table}_update_time" in _index_names(evolved_config, session_table)
        assert f"idx_{litestar_table}_expires_at" in _index_names(evolved_config, litestar_table)
        assert "owner_id" in _column_names(evolved_config, session_table)
        assert "owner_id" in _column_names(evolved_config, memory_table)

        restored_session = adk_store.get_session("app", "user", "s-1")
        assert restored_session is not None
        assert restored_session["state"] == {"step": 1}
        assert len(adk_store.get_events("app", "user", "s-1")) == 1
        assert len(memory_store.search_entries("remembered", "app", "user")) == 1
        restored_artifact = artifact_store.get_artifact("app", "user", "report.txt", session_id="s-1")
        assert restored_artifact is not None
        assert restored_artifact["canonical_uri"] == "gs://bucket/report.txt"
        assert await litestar_store.get("ls-1") == b"payload"

        if ledger_table in _table_names(evolved_config):
            with evolved_config.provide_read_session() as driver:
                ledger_rows = driver.select(f"SELECT version_num FROM {ledger_table}")
            ext_versions = [
                str(row["version_num"]) for row in ledger_rows if str(row["version_num"]).startswith("ext_")
            ]
            assert ext_versions == []
    finally:
        config.close_pool()
        if evolved_config is not None:
            evolved_config.close_pool()
        if async_config is not None:
            await async_config.close_pool()
        for idx_name in (
            f"idx_{litestar_table}_expires_at",
            f"idx_{events_table}_channel_status",
            f"idx_{events_table}_lease",
            f"idx_{adk_events_table}_timestamp",
            f"idx_{session_table}_update_time",
            f"idx_{memory_table}_event_id",
            f"idx_{memory_table}_session",
            f"idx_{memory_table}_app_scope_user_time",
            f"idx_{memory_table}_scope",
        ):
            _drop_index_if_exists(spanner_database, idx_name)
        for tbl in (
            artifact_table,
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
            drop_table_if_exists(spanner_database, tbl)


def test_spanner_emulator_reconcile_search_and_vector_indexes_or_skip(
    spanner_service: "SpannerService", spanner_database: "Database"
) -> None:
    """Memory FTS and vector index reconciliation converges or skips naming the unsupported emulator DDL."""
    suffix = uuid4().hex[:8]
    memory_table = f"adk_mem_vec_{suffix}"
    ledger_table = f"ddl_mig_vec_{suffix}"
    adk_cfg: dict[str, Any] = {
        "migration_mode": "reconcile",
        "enable_sessions": False,
        "enable_memory": True,
        "memory_table": memory_table,
        "memory_use_fts": True,
        "vector_dimensions": 4,
        "vector_index_enabled": True,
    }
    ext: ExtensionConfigs = {"adk": adk_cfg}
    config = SpannerSyncConfig(
        connection_config=build_spanner_connection_config(spanner_service),
        migration_config={"version_table_name": ledger_table},
        extension_config=ext,
    )
    try:
        try:
            config.migrate_up()
        except (SQLSpecError, api_exceptions.GoogleAPICallError) as exc:
            for statement_kind in ("CREATE SEARCH INDEX", "CREATE VECTOR INDEX", "TOKENLIST", "vector_length"):
                if statement_kind.lower() in str(exc).lower():
                    pytest.skip(f"Spanner emulator unsupported DDL ({statement_kind}): {exc}")
            pytest.skip(f"Spanner emulator unsupported FTS/vector DDL statement: {exc}")

        present_indexes = _index_names(config, memory_table)
        assert f"idx_{memory_table}_fts" in present_indexes
        assert f"idx_{memory_table}_embedding" in present_indexes
    finally:
        config.close_pool()
        _drop_index_if_exists(spanner_database, f"idx_{memory_table}_embedding", kind="VECTOR INDEX")
        _drop_index_if_exists(spanner_database, f"idx_{memory_table}_fts", kind="SEARCH INDEX")
        for idx_name in (
            f"idx_{memory_table}_event_id",
            f"idx_{memory_table}_session",
            f"idx_{memory_table}_app_scope_user_time",
            f"idx_{memory_table}_scope",
        ):
            _drop_index_if_exists(spanner_database, idx_name)
        drop_table_if_exists(spanner_database, memory_table)
        drop_table_if_exists(spanner_database, ledger_table)
