"""Cross-adapter ADK scoped-state contract for OracleDB."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.oracledb import OracleAsyncConfig
from sqlspec.adapters.oracledb.adk import OracleAsyncADKStore
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

pytestmark = [pytest.mark.xdist_group("oracle"), pytest.mark.oracledb, pytest.mark.integration]


@pytest.fixture(scope="session")
async def oracle_adk_store(oracle_async_config: "OracleAsyncConfig") -> "AsyncGenerator[OracleAsyncADKStore, None]":
    store = OracleAsyncADKStore(oracle_async_config)
    try:
        await store.drop_tables()
    except Exception:
        pass
    await store.create_tables()
    try:
        yield store
    finally:
        try:
            await store.drop_tables()
        except Exception:
            pass


async def test_oracledb_session_event_store_contract(oracle_adk_store: OracleAsyncADKStore) -> None:
    await assert_session_event_store_contract(oracle_adk_store, marker="oracledb")


async def test_oracledb_session_event_cleanup_contract(oracle_adk_store: OracleAsyncADKStore) -> None:
    await assert_session_event_cleanup_contract(oracle_adk_store, marker="oracledb")


async def test_oracledb_session_get_session_renewal_contract(oracle_adk_store: OracleAsyncADKStore) -> None:
    await assert_session_get_session_renewal_contract(oracle_adk_store, marker="oracledb")


async def test_oracledb_session_table_lifecycle_contract(oracle_adk_store: OracleAsyncADKStore) -> None:
    await assert_session_table_lifecycle_contract(oracle_adk_store, marker="oracledb")


async def test_oracledb_session_scoped_state_contract(oracle_adk_store: OracleAsyncADKStore) -> None:
    await assert_session_scoped_state_contract(oracle_adk_store, marker="oracledb")


async def test_oracledb_session_atomic_scoped_write_contract(oracle_adk_store: OracleAsyncADKStore) -> None:
    await assert_session_atomic_scoped_write_contract(oracle_adk_store, marker="oracledb")


async def test_oracledb_session_temp_state_not_persisted(oracle_adk_store: OracleAsyncADKStore) -> None:
    await assert_session_temp_state_not_persisted(oracle_adk_store, marker="oracledb")


async def test_oracledb_session_empty_state_roundtrip(oracle_adk_store: OracleAsyncADKStore) -> None:
    await assert_session_empty_state_roundtrip(oracle_adk_store, marker="oracledb")


async def test_oracledb_session_sibling_app_isolation(oracle_adk_store: OracleAsyncADKStore) -> None:
    await assert_session_sibling_app_isolation(oracle_adk_store, marker="oracledb")


async def test_oracledb_session_sibling_user_isolation(oracle_adk_store: OracleAsyncADKStore) -> None:
    await assert_session_sibling_user_isolation(oracle_adk_store, marker="oracledb")
