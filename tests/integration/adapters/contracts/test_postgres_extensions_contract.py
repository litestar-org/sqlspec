"""Shared PostgreSQL extension contracts for pgvector and ParadeDB."""

import pytest

from tests.integration.adapters.contracts._postgres_extension_cases import (
    PostgresExtensionCaseContext,
    async_postgres_extension_params_with,
    sync_postgres_extension_params_with,
)
from tests.integration.adapters.contracts.postgres_extension_behaviors import (
    assert_async_paradedb_pgvector_contract,
    assert_async_paradedb_search_contract,
    assert_async_pgvector_contract,
    assert_async_postgres_extension_detection_contract,
    assert_sync_paradedb_pgvector_contract,
    assert_sync_paradedb_search_contract,
    assert_sync_pgvector_contract,
    assert_sync_postgres_extension_detection_contract,
)


@pytest.mark.parametrize("sync_postgres_extension_case", sync_postgres_extension_params_with("pgvector"), indirect=True)
def test_sync_pgvector_detection_contract(sync_postgres_extension_case: PostgresExtensionCaseContext) -> None:
    """Sync pgvector configs detect the extension and promote the first session dialect."""
    assert_sync_postgres_extension_detection_contract(
        sync_postgres_extension_case.config, sync_postgres_extension_case.driver, sync_postgres_extension_case.case
    )


@pytest.mark.parametrize(
    "async_postgres_extension_case", async_postgres_extension_params_with("pgvector"), indirect=True
)
async def test_async_pgvector_detection_contract(async_postgres_extension_case: PostgresExtensionCaseContext) -> None:
    """Async pgvector configs detect the extension and promote the first session dialect."""
    await assert_async_postgres_extension_detection_contract(
        async_postgres_extension_case.config, async_postgres_extension_case.driver, async_postgres_extension_case.case
    )


@pytest.mark.parametrize("sync_postgres_extension_case", sync_postgres_extension_params_with("pgvector"), indirect=True)
def test_sync_pgvector_contract(sync_postgres_extension_case: PostgresExtensionCaseContext) -> None:
    """Sync pgvector drivers execute distance operators and vector builder expressions."""
    assert_sync_pgvector_contract(sync_postgres_extension_case.driver, sync_postgres_extension_case.case)


@pytest.mark.parametrize(
    "async_postgres_extension_case", async_postgres_extension_params_with("pgvector"), indirect=True
)
async def test_async_pgvector_contract(async_postgres_extension_case: PostgresExtensionCaseContext) -> None:
    """Async pgvector drivers execute distance operators and vector builder expressions."""
    await assert_async_pgvector_contract(async_postgres_extension_case.driver, async_postgres_extension_case.case)


@pytest.mark.parametrize("sync_postgres_extension_case", sync_postgres_extension_params_with("paradedb"), indirect=True)
def test_sync_paradedb_detection_contract(sync_postgres_extension_case: PostgresExtensionCaseContext) -> None:
    """Sync ParadeDB configs detect pgvector plus pg_search and promote the first session dialect."""
    assert_sync_postgres_extension_detection_contract(
        sync_postgres_extension_case.config, sync_postgres_extension_case.driver, sync_postgres_extension_case.case
    )


@pytest.mark.parametrize(
    "async_postgres_extension_case", async_postgres_extension_params_with("paradedb"), indirect=True
)
async def test_async_paradedb_detection_contract(async_postgres_extension_case: PostgresExtensionCaseContext) -> None:
    """Async ParadeDB configs detect pgvector plus pg_search and promote the first session dialect."""
    await assert_async_postgres_extension_detection_contract(
        async_postgres_extension_case.config, async_postgres_extension_case.driver, async_postgres_extension_case.case
    )


@pytest.mark.parametrize("sync_postgres_extension_case", sync_postgres_extension_params_with("paradedb"), indirect=True)
def test_sync_paradedb_search_contract(sync_postgres_extension_case: PostgresExtensionCaseContext) -> None:
    """Sync ParadeDB drivers execute BM25 and pdb query helper searches."""
    assert_sync_paradedb_search_contract(sync_postgres_extension_case.driver, sync_postgres_extension_case.case)


@pytest.mark.parametrize(
    "async_postgres_extension_case", async_postgres_extension_params_with("paradedb"), indirect=True
)
async def test_async_paradedb_search_contract(async_postgres_extension_case: PostgresExtensionCaseContext) -> None:
    """Async ParadeDB drivers execute BM25 and pdb query helper searches."""
    await assert_async_paradedb_search_contract(
        async_postgres_extension_case.driver, async_postgres_extension_case.case
    )


@pytest.mark.parametrize("sync_postgres_extension_case", sync_postgres_extension_params_with("paradedb"), indirect=True)
def test_sync_paradedb_pgvector_contract(sync_postgres_extension_case: PostgresExtensionCaseContext) -> None:
    """Sync ParadeDB drivers keep pgvector distance operators available."""
    assert_sync_paradedb_pgvector_contract(sync_postgres_extension_case.driver, sync_postgres_extension_case.case)


@pytest.mark.parametrize(
    "async_postgres_extension_case", async_postgres_extension_params_with("paradedb"), indirect=True
)
async def test_async_paradedb_pgvector_contract(async_postgres_extension_case: PostgresExtensionCaseContext) -> None:
    """Async ParadeDB drivers keep pgvector distance operators available."""
    await assert_async_paradedb_pgvector_contract(
        async_postgres_extension_case.driver, async_postgres_extension_case.case
    )
