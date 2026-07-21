"""Shared migration-lifecycle contracts."""

from pathlib import Path

import pytest

from tests.integration.adapters._shared._migration_cases import (
    MigrationCaseContext,
    async_migration_params_with,
    sync_migration_params_with,
)
from tests.integration.adapters._shared.migration_behaviors import (
    assert_async_migration_current_contract,
    assert_async_migration_default_schema_contract,
    assert_async_migration_error_handling_contract,
    assert_async_migration_full_workflow_contract,
    assert_async_migration_missing_schema_contract,
    assert_async_migration_multi_schema_contract,
    assert_async_migration_multi_statement_contract,
    assert_async_migration_multiple_contract,
    assert_async_migration_non_transactional_default_schema_contract,
    assert_async_migration_version_table_schema_contract,
    assert_sync_migration_current_contract,
    assert_sync_migration_default_schema_contract,
    assert_sync_migration_error_handling_contract,
    assert_sync_migration_full_workflow_contract,
    assert_sync_migration_missing_schema_contract,
    assert_sync_migration_multi_schema_contract,
    assert_sync_migration_multi_statement_contract,
    assert_sync_migration_multiple_contract,
    assert_sync_migration_version_table_schema_contract,
)


def test_sync_migration_full_workflow_contract(sync_migration_case: MigrationCaseContext, tmp_path: Path) -> None:
    """Sync migrations init, upgrade, and downgrade a single revision."""
    assert_sync_migration_full_workflow_contract(sync_migration_case.make_config, sync_migration_case.case, tmp_path)


async def test_async_migration_full_workflow_contract(
    async_migration_case: MigrationCaseContext, tmp_path: Path
) -> None:
    """Async migrations init, upgrade, and downgrade a single revision."""
    await assert_async_migration_full_workflow_contract(
        async_migration_case.make_config, async_migration_case.case, tmp_path
    )


def test_sync_migration_multiple_contract(sync_migration_case: MigrationCaseContext, tmp_path: Path) -> None:
    """Sync migrations apply two revisions and downgrade them stepwise."""
    assert_sync_migration_multiple_contract(sync_migration_case.make_config, sync_migration_case.case, tmp_path)


async def test_async_migration_multiple_contract(async_migration_case: MigrationCaseContext, tmp_path: Path) -> None:
    """Async migrations apply two revisions and downgrade them stepwise."""
    await assert_async_migration_multiple_contract(
        async_migration_case.make_config, async_migration_case.case, tmp_path
    )


def test_sync_migration_current_contract(sync_migration_case: MigrationCaseContext, tmp_path: Path) -> None:
    """Sync current command reports the applied revision."""
    assert_sync_migration_current_contract(sync_migration_case.make_config, sync_migration_case.case, tmp_path)


async def test_async_migration_current_contract(async_migration_case: MigrationCaseContext, tmp_path: Path) -> None:
    """Async current command reports the applied revision."""
    await assert_async_migration_current_contract(async_migration_case.make_config, async_migration_case.case, tmp_path)


def test_sync_migration_error_handling_contract(sync_migration_case: MigrationCaseContext, tmp_path: Path) -> None:
    """Sync upgrade of an invalid migration records no applied version."""
    assert_sync_migration_error_handling_contract(sync_migration_case.make_config, sync_migration_case.case, tmp_path)


async def test_async_migration_error_handling_contract(
    async_migration_case: MigrationCaseContext, tmp_path: Path
) -> None:
    """Async upgrade of an invalid migration records no applied version."""
    await assert_async_migration_error_handling_contract(
        async_migration_case.make_config, async_migration_case.case, tmp_path
    )


def test_sync_migration_multi_statement_contract(sync_migration_case: MigrationCaseContext, tmp_path: Path) -> None:
    """Sync migrations apply every statement in a multi-statement revision."""
    assert_sync_migration_multi_statement_contract(sync_migration_case.make_config, sync_migration_case.case, tmp_path)


async def test_async_migration_multi_statement_contract(
    async_migration_case: MigrationCaseContext, tmp_path: Path
) -> None:
    """Async migrations apply every statement in a multi-statement revision."""
    await assert_async_migration_multi_statement_contract(
        async_migration_case.make_config, async_migration_case.case, tmp_path
    )


@pytest.mark.parametrize("sync_migration_case", sync_migration_params_with("supports_default_schema"), indirect=True)
def test_sync_migration_default_schema_contract(sync_migration_case: MigrationCaseContext, tmp_path: Path) -> None:
    """Sync migrations run unqualified DDL in the configured default schema."""
    assert_sync_migration_default_schema_contract(sync_migration_case.make_config, sync_migration_case.case, tmp_path)


@pytest.mark.parametrize("async_migration_case", async_migration_params_with("supports_default_schema"), indirect=True)
async def test_async_migration_default_schema_contract(
    async_migration_case: MigrationCaseContext, tmp_path: Path
) -> None:
    """Async migrations run unqualified DDL in the configured default schema."""
    await assert_async_migration_default_schema_contract(
        async_migration_case.make_config, async_migration_case.case, tmp_path
    )


@pytest.mark.parametrize(
    "sync_migration_case", sync_migration_params_with("supports_multi_schema_migrations"), indirect=True
)
def test_sync_migration_multi_schema_contract(sync_migration_case: MigrationCaseContext, tmp_path: Path) -> None:
    """Sync migrations can separate migrated DDL and tracker schemas."""
    assert_sync_migration_multi_schema_contract(sync_migration_case.make_config, sync_migration_case.case, tmp_path)


@pytest.mark.parametrize(
    "async_migration_case", async_migration_params_with("supports_multi_schema_migrations"), indirect=True
)
async def test_async_migration_multi_schema_contract(
    async_migration_case: MigrationCaseContext, tmp_path: Path
) -> None:
    """Async migrations can separate migrated DDL and tracker schemas."""
    await assert_async_migration_multi_schema_contract(
        async_migration_case.make_config, async_migration_case.case, tmp_path
    )


@pytest.mark.parametrize(
    "sync_migration_case", sync_migration_params_with("supports_multi_schema_migrations"), indirect=True
)
def test_sync_migration_version_table_schema_contract(
    sync_migration_case: MigrationCaseContext, tmp_path: Path
) -> None:
    """Sync migrations can place only the tracker table in a configured schema."""
    assert_sync_migration_version_table_schema_contract(
        sync_migration_case.make_config, sync_migration_case.case, tmp_path
    )


@pytest.mark.parametrize(
    "async_migration_case", async_migration_params_with("supports_multi_schema_migrations"), indirect=True
)
async def test_async_migration_version_table_schema_contract(
    async_migration_case: MigrationCaseContext, tmp_path: Path
) -> None:
    """Async migrations can place only the tracker table in a configured schema."""
    await assert_async_migration_version_table_schema_contract(
        async_migration_case.make_config, async_migration_case.case, tmp_path
    )


@pytest.mark.parametrize(
    "sync_migration_case", sync_migration_params_with("supports_missing_schema_validation"), indirect=True
)
def test_sync_migration_missing_schema_contract(sync_migration_case: MigrationCaseContext, tmp_path: Path) -> None:
    """Sync migrations fail before touching public schema when the configured schema is absent."""
    assert_sync_migration_missing_schema_contract(sync_migration_case.make_config, sync_migration_case.case, tmp_path)


@pytest.mark.parametrize(
    "async_migration_case", async_migration_params_with("supports_missing_schema_validation"), indirect=True
)
async def test_async_migration_missing_schema_contract(
    async_migration_case: MigrationCaseContext, tmp_path: Path
) -> None:
    """Async migrations fail before touching public schema when the configured schema is absent."""
    await assert_async_migration_missing_schema_contract(
        async_migration_case.make_config, async_migration_case.case, tmp_path
    )


@pytest.mark.parametrize(
    "async_migration_case", async_migration_params_with("supports_non_transactional_default_schema"), indirect=True
)
async def test_async_migration_non_transactional_default_schema_contract(
    async_migration_case: MigrationCaseContext, tmp_path: Path
) -> None:
    """Async non-transactional SQL migrations honor the configured default schema."""
    await assert_async_migration_non_transactional_default_schema_contract(
        async_migration_case.make_config, async_migration_case.case, tmp_path
    )
