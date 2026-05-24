"""Cross-adapter ADK scoped-state contract for pymysql."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.pymysql import PyMysqlConfig
from sqlspec.adapters.pymysql.adk import PyMysqlADKStore
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

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.pymysql, pytest.mark.integration]


@pytest.fixture(scope="session")
async def pymysql_adk_store(pymysql_config: "PyMysqlConfig") -> "AsyncGenerator[PyMysqlADKStore, None]":
    store = PyMysqlADKStore(pymysql_config)
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


async def test_pymysql_session_event_store_contract(pymysql_adk_store: PyMysqlADKStore) -> None:
    await assert_session_event_store_contract(pymysql_adk_store, marker="pymysql")


async def test_pymysql_session_event_cleanup_contract(pymysql_adk_store: PyMysqlADKStore) -> None:
    await assert_session_event_cleanup_contract(pymysql_adk_store, marker="pymysql")


async def test_pymysql_session_get_session_renewal_contract(pymysql_adk_store: PyMysqlADKStore) -> None:
    await assert_session_get_session_renewal_contract(pymysql_adk_store, marker="pymysql")


async def test_pymysql_session_table_lifecycle_contract(pymysql_adk_store: PyMysqlADKStore) -> None:
    await assert_session_table_lifecycle_contract(pymysql_adk_store, marker="pymysql")


async def test_pymysql_session_scoped_state_contract(pymysql_adk_store: PyMysqlADKStore) -> None:
    await assert_session_scoped_state_contract(pymysql_adk_store, marker="pymysql")


async def test_pymysql_session_atomic_scoped_write_contract(pymysql_adk_store: PyMysqlADKStore) -> None:
    await assert_session_atomic_scoped_write_contract(pymysql_adk_store, marker="pymysql")


async def test_pymysql_session_temp_state_not_persisted(pymysql_adk_store: PyMysqlADKStore) -> None:
    await assert_session_temp_state_not_persisted(pymysql_adk_store, marker="pymysql")


async def test_pymysql_session_empty_state_roundtrip(pymysql_adk_store: PyMysqlADKStore) -> None:
    await assert_session_empty_state_roundtrip(pymysql_adk_store, marker="pymysql")


async def test_pymysql_session_sibling_app_isolation(pymysql_adk_store: PyMysqlADKStore) -> None:
    await assert_session_sibling_app_isolation(pymysql_adk_store, marker="pymysql")


async def test_pymysql_session_sibling_user_isolation(pymysql_adk_store: PyMysqlADKStore) -> None:
    await assert_session_sibling_user_isolation(pymysql_adk_store, marker="pymysql")
