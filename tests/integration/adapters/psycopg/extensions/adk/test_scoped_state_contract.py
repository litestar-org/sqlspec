"""Cross-adapter ADK scoped-state contract for psycopg."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.psycopg import PsycopgAsyncConfig
from sqlspec.adapters.psycopg.adk import PsycopgAsyncADKStore
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

pytestmark = [
    pytest.mark.xdist_group("postgres"),
    pytest.mark.psycopg,
    pytest.mark.integration,
    pytest.mark.xfail(
        reason="sqlspec-xqnf: PsycopgAsyncADKStore read paths return tuples instead of dicts; tracked separately",
        strict=False,
    ),
]


@pytest.fixture(scope="session")
async def psycopg_adk_store(psycopg_async_config: "PsycopgAsyncConfig") -> "AsyncGenerator[PsycopgAsyncADKStore, None]":
    store = PsycopgAsyncADKStore(psycopg_async_config)
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


async def test_psycopg_session_event_store_contract(psycopg_adk_store: PsycopgAsyncADKStore) -> None:
    await assert_session_event_store_contract(psycopg_adk_store, marker="psycopg")


async def test_psycopg_session_event_cleanup_contract(psycopg_adk_store: PsycopgAsyncADKStore) -> None:
    await assert_session_event_cleanup_contract(psycopg_adk_store, marker="psycopg")


async def test_psycopg_session_get_session_renewal_contract(psycopg_adk_store: PsycopgAsyncADKStore) -> None:
    await assert_session_get_session_renewal_contract(psycopg_adk_store, marker="psycopg")


async def test_psycopg_session_table_lifecycle_contract(psycopg_adk_store: PsycopgAsyncADKStore) -> None:
    await assert_session_table_lifecycle_contract(psycopg_adk_store, marker="psycopg")


async def test_psycopg_session_scoped_state_contract(psycopg_adk_store: PsycopgAsyncADKStore) -> None:
    await assert_session_scoped_state_contract(psycopg_adk_store, marker="psycopg")


async def test_psycopg_session_atomic_scoped_write_contract(psycopg_adk_store: PsycopgAsyncADKStore) -> None:
    await assert_session_atomic_scoped_write_contract(psycopg_adk_store, marker="psycopg")


async def test_psycopg_session_temp_state_not_persisted(psycopg_adk_store: PsycopgAsyncADKStore) -> None:
    await assert_session_temp_state_not_persisted(psycopg_adk_store, marker="psycopg")


async def test_psycopg_session_empty_state_roundtrip(psycopg_adk_store: PsycopgAsyncADKStore) -> None:
    await assert_session_empty_state_roundtrip(psycopg_adk_store, marker="psycopg")


async def test_psycopg_session_sibling_app_isolation(psycopg_adk_store: PsycopgAsyncADKStore) -> None:
    await assert_session_sibling_app_isolation(psycopg_adk_store, marker="psycopg")


async def test_psycopg_session_sibling_user_isolation(psycopg_adk_store: PsycopgAsyncADKStore) -> None:
    await assert_session_sibling_user_isolation(psycopg_adk_store, marker="psycopg")
