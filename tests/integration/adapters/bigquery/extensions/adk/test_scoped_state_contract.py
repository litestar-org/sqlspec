"""Cross-adapter ADK scoped-state contract for BigQuery (analytics-replica path).

BigQuery is analytics-replica only; this contract suite skips assertions that
require synchronous OLTP semantics (table lifecycle DROP statements that hang on
the goccy/bigquery-emulator, affected-row counts that BigQuery does not expose).
"""

from collections.abc import AsyncGenerator
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
    from sqlspec.adapters.bigquery.config import BigQueryConfig

pytestmark = [pytest.mark.xdist_group("bigquery"), pytest.mark.bigquery, pytest.mark.integration]


@pytest.fixture
async def bigquery_adk_store(bigquery_config: "BigQueryConfig") -> "AsyncGenerator[BigQueryADKStore, None]":
    store = BigQueryADKStore(bigquery_config)
    await store.create_tables()
    yield store


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
