"""Regression tests for pooled-config unexpected-keyword-argument validation.

The pooled base configs historically accepted an unbounded ``**kwargs`` that was
captured in the signature and silently discarded, so a typo'd keyword argument
produced no error. These tests pin the validate-and-raise behavior: any keyword
argument the pooled constructors do not recognize must raise ``TypeError``.
"""

from typing import Any

import pytest
from click.testing import CliRunner

from sqlspec.cli import add_migration_commands, get_sqlspec_group
from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig


class _MiniSyncPooledConfig(SyncDatabaseConfig[Any, Any, Any]):
    """Minimal concrete sync pooled config for construction tests."""

    def _create_pool(self) -> Any:
        return None

    def _close_pool(self) -> None:
        return None


class _MiniAsyncPooledConfig(AsyncDatabaseConfig[Any, Any, Any]):
    """Minimal concrete async pooled config for construction tests."""

    async def _create_pool(self) -> Any:
        return None

    async def _close_pool(self) -> None:
        return None


class _MiniNoPoolSyncConfig(NoPoolSyncConfig[Any, Any]):
    """Minimal concrete no-pool sync config for construction tests."""


class _MiniNoPoolAsyncConfig(NoPoolAsyncConfig[Any, Any]):
    """Minimal concrete no-pool async config for construction tests."""


def test_sync_pooled_config_rejects_unexpected_kwarg() -> None:
    """Sync pooled config raises TypeError on an unexpected keyword argument."""
    with pytest.raises(TypeError, match="unexpected_option"):
        _MiniSyncPooledConfig(unexpected_option=123)


def test_async_pooled_config_rejects_unexpected_kwarg() -> None:
    """Async pooled config raises TypeError on an unexpected keyword argument."""
    with pytest.raises(TypeError, match="unexpected_option"):
        _MiniAsyncPooledConfig(unexpected_option=123)


def test_no_pool_configs_reject_unexpected_kwarg() -> None:
    """NoPoolSyncConfig and NoPoolAsyncConfig raise TypeError on an unexpected keyword argument."""
    with pytest.raises(TypeError, match="unexpected_option"):
        _MiniNoPoolSyncConfig(unexpected_option=123)
    with pytest.raises(TypeError, match="unexpected_option"):
        _MiniNoPoolAsyncConfig(unexpected_option=123)


def test_distinct_uninitialized_configs_do_not_compare_equal() -> None:
    """Distinct config instances with uninitialized pools must not compare equal."""
    cfg1 = _MiniSyncPooledConfig(connection_config={"database": "a.db"})
    cfg2 = _MiniSyncPooledConfig(connection_config={"database": "b.db"})
    assert cfg1 != cfg2
    assert len({cfg1, cfg2}) == 2


def test_cli_preserves_multiple_unnamed_configs(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI must preserve multiple unnamed configs with bind_key=None."""
    cfg1 = _MiniSyncPooledConfig(connection_config={"database": "a.db"})
    cfg2 = _MiniSyncPooledConfig(connection_config={"database": "b.db"})
    monkeypatch.setattr("sqlspec.cli.resolve_config_sync", lambda _path: [cfg1, cfg2])
    group = add_migration_commands(get_sqlspec_group())
    runner = CliRunner()
    result = runner.invoke(group, ["--config", "dummy:configs", "--validate-config", "show-config"])
    assert "Successfully loaded 2 config(s)" in result.output


def test_cli_multi_config_operation_exits_nonzero_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI migration operation across multiple configs must exit non-zero when a config fails."""
    cfg1 = _MiniSyncPooledConfig(
        bind_key="failing_db",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "migrations"},
    )
    cfg2 = _MiniSyncPooledConfig(
        bind_key="second_db",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "migrations"},
    )
    monkeypatch.setattr("sqlspec.cli.resolve_config_sync", lambda _path: [cfg1, cfg2])

    class _BoomCommands:
        def current(self, verbose: bool = False) -> None:
            msg = "migration boom"
            raise RuntimeError(msg)

    monkeypatch.setattr("sqlspec.migrations.commands.create_migration_commands", lambda **_kw: _BoomCommands())
    group = add_migration_commands(get_sqlspec_group())
    runner = CliRunner()
    result = runner.invoke(group, ["--config", "dummy:configs", "show-current-revision"])
    assert result.exit_code != 0
    assert "failing_db" in result.output


def test_sync_pooled_config_accepts_known_kwargs() -> None:
    """Sync pooled config still constructs when only known keyword arguments are passed."""
    config = _MiniSyncPooledConfig(bind_key="primary", connection_config={"database": ":memory:"})
    assert config.bind_key == "primary"
    assert config.connection_config == {"database": ":memory:"}


def test_async_pooled_config_accepts_known_kwargs() -> None:
    """Async pooled config still constructs when only known keyword arguments are passed."""
    config = _MiniAsyncPooledConfig(bind_key="primary", connection_config={"database": ":memory:"})
    assert config.bind_key == "primary"
    assert config.connection_config == {"database": ":memory:"}
