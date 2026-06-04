"""Shared migration-lifecycle contracts."""

from pathlib import Path

from tests.integration.adapters.contracts._migration_cases import MigrationCaseContext
from tests.integration.adapters.contracts.migration_behaviors import (
    assert_async_migration_current_contract,
    assert_async_migration_error_handling_contract,
    assert_async_migration_full_workflow_contract,
    assert_async_migration_multi_statement_contract,
    assert_async_migration_multiple_contract,
    assert_sync_migration_current_contract,
    assert_sync_migration_error_handling_contract,
    assert_sync_migration_full_workflow_contract,
    assert_sync_migration_multi_statement_contract,
    assert_sync_migration_multiple_contract,
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
    await assert_async_migration_current_contract(
        async_migration_case.make_config, async_migration_case.case, tmp_path
    )


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
