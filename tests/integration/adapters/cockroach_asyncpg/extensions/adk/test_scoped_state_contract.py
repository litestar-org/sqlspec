"""Cross-adapter ADK scoped-state contract for cockroach_asyncpg."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
from sqlspec.adapters.cockroach_asyncpg.adk import CockroachAsyncpgADKStore
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
async def cockroach_asyncpg_adk_store(
    cockroach_asyncpg_config: "CockroachAsyncpgConfig",
) -> "AsyncGenerator[CockroachAsyncpgADKStore, None]":
    store = CockroachAsyncpgADKStore(cockroach_asyncpg_config)
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


async def test_cockroach_asyncpg_session_event_store_contract(
    cockroach_asyncpg_adk_store: CockroachAsyncpgADKStore,
) -> None:
    await assert_session_event_store_contract(cockroach_asyncpg_adk_store, marker="cockroach-asyncpg")


async def test_cockroach_asyncpg_session_event_cleanup_contract(
    cockroach_asyncpg_adk_store: CockroachAsyncpgADKStore,
) -> None:
    await assert_session_event_cleanup_contract(cockroach_asyncpg_adk_store, marker="cockroach-asyncpg")


async def test_cockroach_asyncpg_session_get_session_renewal_contract(
    cockroach_asyncpg_adk_store: CockroachAsyncpgADKStore,
) -> None:
    await assert_session_get_session_renewal_contract(cockroach_asyncpg_adk_store, marker="cockroach-asyncpg")


async def test_cockroach_asyncpg_session_table_lifecycle_contract(
    cockroach_asyncpg_adk_store: CockroachAsyncpgADKStore,
) -> None:
    await assert_session_table_lifecycle_contract(cockroach_asyncpg_adk_store, marker="cockroach-asyncpg")


@pytest.mark.xfail(
    reason="sqlspec-7rbl: cockroach_asyncpg multi-statement tx hits multiple_active_portals limitation; tracked separately",
    strict=False,
)
async def test_cockroach_asyncpg_session_scoped_state_contract(
    cockroach_asyncpg_adk_store: CockroachAsyncpgADKStore,
) -> None:
    await assert_session_scoped_state_contract(cockroach_asyncpg_adk_store, marker="cockroach-asyncpg")


@pytest.mark.xfail(
    reason="sqlspec-7rbl: cockroach_asyncpg multi-statement tx hits multiple_active_portals limitation; tracked separately",
    strict=False,
)
async def test_cockroach_asyncpg_session_atomic_scoped_write_contract(
    cockroach_asyncpg_adk_store: CockroachAsyncpgADKStore,
) -> None:
    await assert_session_atomic_scoped_write_contract(cockroach_asyncpg_adk_store, marker="cockroach-asyncpg")


async def test_cockroach_asyncpg_session_temp_state_not_persisted(
    cockroach_asyncpg_adk_store: CockroachAsyncpgADKStore,
) -> None:
    await assert_session_temp_state_not_persisted(cockroach_asyncpg_adk_store, marker="cockroach-asyncpg")


async def test_cockroach_asyncpg_session_empty_state_roundtrip(
    cockroach_asyncpg_adk_store: CockroachAsyncpgADKStore,
) -> None:
    await assert_session_empty_state_roundtrip(cockroach_asyncpg_adk_store, marker="cockroach-asyncpg")


@pytest.mark.xfail(
    reason="sqlspec-7rbl: cockroach_asyncpg multi-statement tx hits multiple_active_portals limitation; tracked separately",
    strict=False,
)
async def test_cockroach_asyncpg_session_sibling_app_isolation(
    cockroach_asyncpg_adk_store: CockroachAsyncpgADKStore,
) -> None:
    await assert_session_sibling_app_isolation(cockroach_asyncpg_adk_store, marker="cockroach-asyncpg")


@pytest.mark.xfail(
    reason="sqlspec-7rbl: cockroach_asyncpg multi-statement tx hits multiple_active_portals limitation; tracked separately",
    strict=False,
)
async def test_cockroach_asyncpg_session_sibling_user_isolation(
    cockroach_asyncpg_adk_store: CockroachAsyncpgADKStore,
) -> None:
    await assert_session_sibling_user_isolation(cockroach_asyncpg_adk_store, marker="cockroach-asyncpg")
