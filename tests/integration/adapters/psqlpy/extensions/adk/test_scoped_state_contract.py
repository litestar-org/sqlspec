"""Cross-adapter ADK scoped-state contract for psqlpy."""

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psqlpy.adk import PsqlpyADKStore

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService
from tests.integration.adapters._adk_contract_helpers import (
    assert_session_atomic_scoped_write_contract,
    assert_session_empty_state_roundtrip,
    assert_session_event_cleanup_contract,
    assert_session_event_store_contract,
    assert_session_get_session_renewal_contract,
    assert_session_scoped_state_contract,
    assert_session_sibling_app_isolation,
    assert_session_sibling_user_isolation,
    assert_session_table_lifecycle_contract,
    assert_session_temp_state_not_persisted,
)

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.postgres, pytest.mark.integration]


@pytest.fixture(scope="session")
async def psqlpy_adk_store(postgres_service: "PostgresService") -> "AsyncGenerator[PsqlpyADKStore, None]":
    dsn = (
        f"postgres://{postgres_service.user}:{postgres_service.password}@"
        f"{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )
    config = PsqlpyConfig(
        connection_config={"dsn": dsn, "max_db_pool_size": 5},
        extension_config={"adk": {"session_table": "adk_session_psqlpy", "events_table": "adk_event_psqlpy"}},
    )
    store = PsqlpyADKStore(config)
    try:
        await store.create_tables()
        yield store
    finally:
        try:
            await store.drop_tables()
        except Exception:
            pass
        if config.connection_instance is not None:
            config.connection_instance.close()
            config.connection_instance = None


async def test_psqlpy_session_event_store_contract(psqlpy_adk_store: PsqlpyADKStore) -> None:
    await assert_session_event_store_contract(psqlpy_adk_store, marker="psqlpy")


async def test_psqlpy_session_event_cleanup_contract(psqlpy_adk_store: PsqlpyADKStore) -> None:
    await assert_session_event_cleanup_contract(psqlpy_adk_store, marker="psqlpy")


async def test_psqlpy_session_get_session_renewal_contract(psqlpy_adk_store: PsqlpyADKStore) -> None:
    await assert_session_get_session_renewal_contract(psqlpy_adk_store, marker="psqlpy")


async def test_psqlpy_session_scoped_state_contract(psqlpy_adk_store: PsqlpyADKStore) -> None:
    await assert_session_scoped_state_contract(psqlpy_adk_store, marker="psqlpy")


async def test_psqlpy_session_atomic_scoped_write_contract(psqlpy_adk_store: PsqlpyADKStore) -> None:
    await assert_session_atomic_scoped_write_contract(psqlpy_adk_store, marker="psqlpy")


async def test_psqlpy_session_temp_state_not_persisted(psqlpy_adk_store: PsqlpyADKStore) -> None:
    await assert_session_temp_state_not_persisted(psqlpy_adk_store, marker="psqlpy")


async def test_psqlpy_session_empty_state_roundtrip(psqlpy_adk_store: PsqlpyADKStore) -> None:
    await assert_session_empty_state_roundtrip(psqlpy_adk_store, marker="psqlpy")


async def test_psqlpy_session_sibling_app_isolation(psqlpy_adk_store: PsqlpyADKStore) -> None:
    await assert_session_sibling_app_isolation(psqlpy_adk_store, marker="psqlpy")


async def test_psqlpy_session_sibling_user_isolation(psqlpy_adk_store: PsqlpyADKStore) -> None:
    await assert_session_sibling_user_isolation(psqlpy_adk_store, marker="psqlpy")


@pytest.mark.xfail(
    reason="sqlspec-8cyp: PsqlpyADKStore.get_session does not catch UndefinedTable; tracked separately", strict=False
)
async def test_psqlpy_session_table_lifecycle_contract(psqlpy_adk_store: PsqlpyADKStore) -> None:
    await assert_session_table_lifecycle_contract(psqlpy_adk_store, marker="psqlpy")
