"""Cross-adapter ADK scoped-state contract for cockroach_psycopg (async)."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig
from sqlspec.adapters.cockroach_psycopg.adk import CockroachPsycopgAsyncADKStore
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

pytestmark = [pytest.mark.xdist_group("cockroachdb"), pytest.mark.cockroachdb, pytest.mark.integration]


@pytest.fixture(scope="session")
async def cockroach_psycopg_adk_store(
    cockroach_async_config: "CockroachPsycopgAsyncConfig",
) -> "AsyncGenerator[CockroachPsycopgAsyncADKStore, None]":
    store = CockroachPsycopgAsyncADKStore(cockroach_async_config)
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


async def test_cockroach_psycopg_session_event_store_contract(
    cockroach_psycopg_adk_store: CockroachPsycopgAsyncADKStore,
) -> None:
    await assert_session_event_store_contract(cockroach_psycopg_adk_store, marker="cockroach-psycopg")


async def test_cockroach_psycopg_session_event_cleanup_contract(
    cockroach_psycopg_adk_store: CockroachPsycopgAsyncADKStore,
) -> None:
    await assert_session_event_cleanup_contract(cockroach_psycopg_adk_store, marker="cockroach-psycopg")


async def test_cockroach_psycopg_session_get_session_renewal_contract(
    cockroach_psycopg_adk_store: CockroachPsycopgAsyncADKStore,
) -> None:
    await assert_session_get_session_renewal_contract(cockroach_psycopg_adk_store, marker="cockroach-psycopg")


async def test_cockroach_psycopg_session_table_lifecycle_contract(
    cockroach_psycopg_adk_store: CockroachPsycopgAsyncADKStore,
) -> None:
    await assert_session_table_lifecycle_contract(cockroach_psycopg_adk_store, marker="cockroach-psycopg")


async def test_cockroach_psycopg_session_scoped_state_contract(
    cockroach_psycopg_adk_store: CockroachPsycopgAsyncADKStore,
) -> None:
    await assert_session_scoped_state_contract(cockroach_psycopg_adk_store, marker="cockroach-psycopg")


async def test_cockroach_psycopg_session_atomic_scoped_write_contract(
    cockroach_psycopg_adk_store: CockroachPsycopgAsyncADKStore,
) -> None:
    await assert_session_atomic_scoped_write_contract(cockroach_psycopg_adk_store, marker="cockroach-psycopg")


async def test_cockroach_psycopg_session_temp_state_not_persisted(
    cockroach_psycopg_adk_store: CockroachPsycopgAsyncADKStore,
) -> None:
    await assert_session_temp_state_not_persisted(cockroach_psycopg_adk_store, marker="cockroach-psycopg")


async def test_cockroach_psycopg_session_empty_state_roundtrip(
    cockroach_psycopg_adk_store: CockroachPsycopgAsyncADKStore,
) -> None:
    await assert_session_empty_state_roundtrip(cockroach_psycopg_adk_store, marker="cockroach-psycopg")


async def test_cockroach_psycopg_session_sibling_app_isolation(
    cockroach_psycopg_adk_store: CockroachPsycopgAsyncADKStore,
) -> None:
    await assert_session_sibling_app_isolation(cockroach_psycopg_adk_store, marker="cockroach-psycopg")


async def test_cockroach_psycopg_session_sibling_user_isolation(
    cockroach_psycopg_adk_store: CockroachPsycopgAsyncADKStore,
) -> None:
    await assert_session_sibling_user_isolation(cockroach_psycopg_adk_store, marker="cockroach-psycopg")
