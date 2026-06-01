"""Contract tests for migration command signatures."""

import inspect

from sqlspec.migrations.base import BaseMigrationCommands
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands


def test_base_revision_file_type_default_matches_concrete_commands() -> None:
    base_param = inspect.signature(BaseMigrationCommands.revision).parameters["file_type"]
    sync_param = inspect.signature(SyncMigrationCommands.revision).parameters["file_type"]
    async_param = inspect.signature(AsyncMigrationCommands.revision).parameters["file_type"]

    assert base_param.default is None
    assert sync_param.default is None
    assert async_param.default is None


def test_upgrade_documents_config_auto_sync_as_unoverridable() -> None:
    sync_source = inspect.getsource(SyncMigrationCommands.upgrade)
    async_source = inspect.getsource(AsyncMigrationCommands.upgrade)

    assert "config auto_sync=False cannot be overridden by the call-site flag" in sync_source
    assert "config auto_sync=False cannot be overridden by the call-site flag" in async_source
