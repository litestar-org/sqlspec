"""Cross-adapter ADK scoped-state contract for BigQuery (analytics-replica path).

BigQuery is analytics-replica only; this contract suite skips assertions that
require synchronous OLTP semantics (table lifecycle DROP statements that hang on
the goccy/bigquery-emulator, affected-row counts that BigQuery does not expose).
"""

from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.bigquery.adk import BigQueryADKStore
from tests.integration.adapters._adk_contract_helpers import (
    assert_session_atomic_scoped_write_contract,
    assert_session_empty_state_roundtrip,
    assert_session_event_store_contract,
    assert_session_get_session_renewal_contract,
    assert_session_scoped_state_contract,
    assert_session_sibling_app_isolation,
    assert_session_sibling_user_isolation,
    assert_session_temp_state_not_persisted,
)

if TYPE_CHECKING:
    from pytest_databases.docker.bigquery import BigQueryService

    from sqlspec.adapters.bigquery.config import BigQueryConfig

pytestmark = [pytest.mark.xdist_group("bigquery"), pytest.mark.bigquery, pytest.mark.integration]


@pytest.fixture(scope="session")
async def bigquery_adk_store(
    native_bigquery_service: "BigQueryService", bigquery_config: "BigQueryConfig"
) -> "AsyncGenerator[BigQueryADKStore, None]":
    _ = native_bigquery_service
    store = BigQueryADKStore(bigquery_config)
    await store.create_tables()
    yield store


@pytest.fixture(autouse=True)
def _bigquery_adk_cleanup(bigquery_adk_store: BigQueryADKStore) -> "Generator[None, None, None]":
    yield
    store = bigquery_adk_store
    for table in (
        store._events_table,  # pyright: ignore[reportPrivateUsage]
        store._user_state_table,  # pyright: ignore[reportPrivateUsage]
        store._app_state_table,  # pyright: ignore[reportPrivateUsage]
        store._session_table,  # pyright: ignore[reportPrivateUsage]
    ):
        store._run_query(f"DELETE FROM {store._qualified(table)} WHERE TRUE")  # pyright: ignore[reportPrivateUsage]


async def test_bigquery_session_event_store_contract(bigquery_adk_store: BigQueryADKStore) -> None:
    await assert_session_event_store_contract(bigquery_adk_store, marker="bigquery")


async def test_bigquery_session_get_session_renewal_contract(bigquery_adk_store: BigQueryADKStore) -> None:
    await assert_session_get_session_renewal_contract(bigquery_adk_store, marker="bigquery")


async def test_bigquery_session_scoped_state_contract(bigquery_adk_store: BigQueryADKStore) -> None:
    await assert_session_scoped_state_contract(bigquery_adk_store, marker="bigquery")


async def test_bigquery_session_atomic_scoped_write_contract(bigquery_adk_store: BigQueryADKStore) -> None:
    await assert_session_atomic_scoped_write_contract(bigquery_adk_store, marker="bigquery")


async def test_bigquery_session_temp_state_not_persisted(bigquery_adk_store: BigQueryADKStore) -> None:
    await assert_session_temp_state_not_persisted(bigquery_adk_store, marker="bigquery")


async def test_bigquery_session_empty_state_roundtrip(bigquery_adk_store: BigQueryADKStore) -> None:
    await assert_session_empty_state_roundtrip(bigquery_adk_store, marker="bigquery")


async def test_bigquery_session_sibling_app_isolation(bigquery_adk_store: BigQueryADKStore) -> None:
    await assert_session_sibling_app_isolation(bigquery_adk_store, marker="bigquery")


async def test_bigquery_session_sibling_user_isolation(bigquery_adk_store: BigQueryADKStore) -> None:
    await assert_session_sibling_user_isolation(bigquery_adk_store, marker="bigquery")
