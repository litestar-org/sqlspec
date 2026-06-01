"""Regression tests for assert-to-exception runtime guards."""

from pathlib import Path
from unittest.mock import patch

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.migrations.commands import SyncMigrationCommands


def test_get_observability_runtime_raises_runtime_error_not_assertion_error() -> None:
    """A broken attach_observability implementation should hit RuntimeError guard."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config._observability_runtime = None

    with (
        patch.object(config, "attach_observability", return_value=None),
        pytest.raises(RuntimeError, match="ObservabilityRuntime was not set"),
    ):
        config.get_observability_runtime()


def test_no_pool_sync_config_init_migrations_uses_default_directory(tmp_path: Path) -> None:
    """init_migrations derives directory from migration_config when argument is None."""
    migration_dir = tmp_path / "migrations"
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(SyncMigrationCommands, "init", return_value=None) as init:
        config.init_migrations()

    init.assert_called_once_with(str(migration_dir), True)


def test_default_serializer_raises_runtime_error_if_fallback_does_not_set_serializer(monkeypatch) -> None:
    """get_default_serializer should use RuntimeError instead of assert."""
    import sqlspec.utils.serializers._json as json_module

    monkeypatch.setattr(json_module, "_default_serializer", None)
    monkeypatch.setattr(json_module, "MSGSPEC_INSTALLED", False)
    monkeypatch.setattr(json_module, "ORJSON_INSTALLED", False)
    monkeypatch.setattr(json_module, "StandardLibSerializer", lambda: None)

    with pytest.raises(RuntimeError, match="No JSON serializer available"):
        json_module.get_default_serializer()
