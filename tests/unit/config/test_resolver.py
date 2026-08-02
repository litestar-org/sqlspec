"""Tests for configuration resolver functionality."""

import uuid
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock, NonCallableMock, patch

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.exceptions import ConfigResolverError, ImproperConfigurationError
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.config_tools import _is_valid_config, resolve_config_async, resolve_config_sync


def _create_mock_config(
    database_url: str = "sqlite:///test.db", bind_key: str = "test", migration_config: dict[str, Any] | None = None
) -> NonCallableMock:
    """Create a non-callable mock config with required attributes.

    Using NonCallableMock is critical because the config resolver checks
    `callable(config_obj)` to determine if it should invoke the config.
    Regular Mock objects are callable by default, which causes them to be
    called and return a NEW Mock without our configured attributes.
    """
    mock_config: NonCallableMock = NonCallableMock()
    mock_config.database_url = database_url
    mock_config.bind_key = bind_key
    mock_config.migration_config = migration_config if migration_config is not None else {}
    return mock_config


async def test_resolve_direct_config_instance() -> None:
    """Test resolving a direct config instance."""
    mock_config = _create_mock_config()
    with patch("sqlspec.utils.config_tools.import_string", return_value=mock_config):
        result = await resolve_config_async("myapp.config.database_config")
        assert hasattr(result, "database_url")
        assert hasattr(result, "bind_key")
        assert hasattr(result, "migration_config")


async def test_resolve_config_async_accepts_colon_path() -> None:
    """Test resolving a config from a module:attribute path."""
    mock_config = _create_mock_config()
    with patch("sqlspec.utils.config_tools.import_string", return_value=mock_config) as import_mock:
        result = await resolve_config_async("myapp.config:database_config")

    assert result is mock_config
    import_mock.assert_called_once_with("myapp.config.database_config")


def test_resolve_config_sync_accepts_colon_path() -> None:
    """Test resolving a config from a module:attribute path."""
    mock_config = _create_mock_config()
    with patch("sqlspec.utils.config_tools.import_string", return_value=mock_config) as import_mock:
        result = resolve_config_sync("myapp.config:database_config")

    assert result is mock_config
    import_mock.assert_called_once_with("myapp.config.database_config")


@pytest.mark.parametrize("config_path", ["myapp:config:extra", ":database_config", "myapp.config:"])
def test_resolve_config_rejects_malformed_reference(config_path: str) -> None:
    """Test that a reference using ':' incorrectly reports the accepted syntax."""
    with pytest.raises(ConfigResolverError, match="is not a valid reference"):
        resolve_config_sync(config_path)


def test_resolve_config_rejects_module_and_names_its_configs() -> None:
    """Test that pointing at a module reports the module:attribute references it exports."""
    module = ModuleType("myapp.database")
    module.database_config = _create_mock_config()  # type: ignore[attr-defined]
    module.other_config = _create_mock_config(bind_key="other")  # type: ignore[attr-defined]
    module.not_a_config = "sqlite:///test.db"  # type: ignore[attr-defined]

    with patch("sqlspec.utils.config_tools.import_string", return_value=module):
        with pytest.raises(ConfigResolverError) as exc_info:
            resolve_config_sync("myapp.database")

    message = str(exc_info.value)
    assert "names a module, not a database configuration" in message
    assert "'myapp.database:database_config'" in message
    assert "'myapp.database:other_config'" in message
    assert "not_a_config" not in message


def test_resolve_config_rejects_module_without_configs() -> None:
    """Test that a module exporting no config explains what to point at instead."""
    module = ModuleType("myapp.empty")

    with patch("sqlspec.utils.config_tools.import_string", return_value=module):
        with pytest.raises(ConfigResolverError, match="exports no database configuration"):
            resolve_config_sync("myapp.empty")


def test_resolve_config_unwraps_nested_config_holder() -> None:
    """Test that a wrapper exposing .config resolves to the config it holds."""
    nested = _create_mock_config()

    class _Holder:
        config = nested

    with patch("sqlspec.utils.config_tools.import_string", return_value=_Holder()):
        result = resolve_config_sync("myapp.config.plugin")

    assert result is nested


async def test_resolve_config_list() -> None:
    """Test resolving a list of config instances."""
    mock_config1 = _create_mock_config(database_url="sqlite:///test1.db", bind_key="test1")
    mock_config2 = _create_mock_config(database_url="sqlite:///test2.db", bind_key="test2")
    config_list = [mock_config1, mock_config2]
    with patch("sqlspec.utils.config_tools.import_string", return_value=config_list):
        result = await resolve_config_async("myapp.config.database_configs")
        assert result == config_list
        assert isinstance(result, list) and len(result) == 2


async def test_resolve_sync_callable_config() -> None:
    """Test resolving a synchronous callable that returns config."""
    mock_config = _create_mock_config()

    def get_config() -> NonCallableMock:
        return mock_config

    with patch("sqlspec.utils.config_tools.import_string", return_value=get_config):
        result = await resolve_config_async("myapp.config.get_database_config")
        assert result is mock_config


async def test_resolve_async_callable_config() -> None:
    """Test resolving an asynchronous callable that returns config."""
    mock_config = _create_mock_config()

    async def get_config() -> NonCallableMock:
        return mock_config

    with patch("sqlspec.utils.config_tools.import_string", return_value=get_config):
        result = await resolve_config_async("myapp.config.async_get_database_config")
        assert result is mock_config


async def test_resolve_sync_callable_config_list() -> None:
    """Test resolving a sync callable that returns config list."""
    mock_config = _create_mock_config()

    def get_configs() -> list[NonCallableMock]:
        return [mock_config]

    with patch("sqlspec.utils.config_tools.import_string", return_value=get_configs):
        result = await resolve_config_async("myapp.config.get_database_configs")
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] is mock_config


async def test_import_error_handling() -> None:
    """Test proper handling of import errors."""
    with patch("sqlspec.utils.config_tools.import_string", side_effect=ImportError("Module not found")):
        with pytest.raises(ConfigResolverError, match="Failed to import config from path"):
            await resolve_config_async("nonexistent.config")


async def test_callable_execution_error() -> None:
    """Test handling of errors during callable execution."""

    def failing_config() -> None:
        raise ValueError("Config generation failed")

    with patch("sqlspec.utils.config_tools.import_string", return_value=failing_config):
        with pytest.raises(ConfigResolverError, match="Failed to execute callable config"):
            await resolve_config_async("myapp.config.failing_config")


async def test_none_result_validation() -> None:
    """Test validation when config resolves to None."""

    def none_config() -> None:
        return None

    with patch("sqlspec.utils.config_tools.import_string", return_value=none_config):
        with pytest.raises(ConfigResolverError, match="resolved to None"):
            await resolve_config_async("myapp.config.none_config")


async def test_empty_list_validation() -> None:
    """Test validation when config resolves to empty list."""

    def empty_list_config() -> list[Any]:
        return []

    with patch("sqlspec.utils.config_tools.import_string", return_value=empty_list_config):
        with pytest.raises(ConfigResolverError, match="resolved to empty list"):
            await resolve_config_async("myapp.config.empty_list_config")


async def test_invalid_config_type_validation() -> None:
    """Test validation when config is invalid type."""

    def invalid_config() -> str:
        return "not a config"

    with patch("sqlspec.utils.config_tools.import_string", return_value=invalid_config):
        with pytest.raises(ConfigResolverError, match="returned invalid type"):
            await resolve_config_async("myapp.config.invalid_config")


async def test_invalid_config_in_list_validation() -> None:
    """Test validation when list contains invalid config."""
    mock_valid_config = _create_mock_config()

    def mixed_config_list() -> list[Any]:
        return [mock_valid_config, "invalid_config"]

    with patch("sqlspec.utils.config_tools.import_string", return_value=mixed_config_list):
        with pytest.raises(ConfigResolverError, match="returned invalid config at index"):
            await resolve_config_async("myapp.config.mixed_configs")


async def test_config_validation_attributes() -> None:
    """Test that config validation checks for required attributes."""

    class IncompleteConfig:
        def __init__(self) -> None:
            self.bind_key = "test"
            self.migration_config: dict[str, Any] = {}

    def incomplete_config() -> IncompleteConfig:
        return IncompleteConfig()

    with patch("sqlspec.utils.config_tools.import_string", return_value=incomplete_config):
        with pytest.raises(ConfigResolverError, match="returned invalid type"):
            await resolve_config_async("myapp.config.incomplete_config")


async def test_config_class_rejected() -> None:
    """Test that config classes (not instances) are rejected.

    Note: This test directly validates that _is_valid_config rejects classes.
    When using resolve_config_*, classes are callable and get instantiated,
    so they don't reach direct validation as classes.
    """

    class MockConfigClass:
        """Mock config class to simulate config classes being passed."""

        database_url = "sqlite:///test.db"
        bind_key = "test"
        migration_config: dict[str, Any] = {}

    assert isinstance(MockConfigClass, type), "Should be a class"
    assert not _is_valid_config(MockConfigClass), "Classes should be rejected"
    instance = MockConfigClass()
    assert not isinstance(instance, type), "Should be an instance"
    assert _is_valid_config(instance), "Instances should be accepted"


async def test_config_class_in_list_rejected() -> None:
    """Test that config classes in a list are rejected."""
    mock_instance = Mock()
    mock_instance.database_url = "sqlite:///test.db"
    mock_instance.bind_key = "test"
    mock_instance.migration_config = {}

    class MockConfigClass:
        """Mock config class."""

        database_url = "sqlite:///test.db"
        bind_key = "test"
        migration_config: dict[str, Any] = {}

    def mixed_list() -> list[Any]:
        return [mock_instance, MockConfigClass]

    with patch("sqlspec.utils.config_tools.import_string", return_value=mixed_list):
        with pytest.raises(ConfigResolverError, match="returned invalid config at index"):
            await resolve_config_async("myapp.config.mixed_list")


async def test_config_instance_accepted() -> None:
    """Test that config instances (not classes) are accepted."""

    class MockConfigClass:
        """Mock config class."""

        def __init__(self) -> None:
            self.database_url = "sqlite:///test.db"
            self.bind_key = "test"
            self.migration_config: dict[str, Any] = {}

    mock_instance = MockConfigClass()
    with patch("sqlspec.utils.config_tools.import_string", return_value=mock_instance):
        result = await resolve_config_async("myapp.config.config_instance")
        assert hasattr(result, "database_url")
        assert hasattr(result, "bind_key")
        assert hasattr(result, "migration_config")


def test_resolve_config_sync_wrapper() -> None:
    """Test that the sync wrapper works correctly."""
    mock_config = _create_mock_config()
    with patch("sqlspec.utils.config_tools.import_string", return_value=mock_config):
        result = resolve_config_sync("myapp.config.database_config")
        assert hasattr(result, "database_url")
        assert hasattr(result, "bind_key")
        assert hasattr(result, "migration_config")


def test_resolve_config_sync_callable() -> None:
    """Test sync wrapper with callable config."""
    mock_config = _create_mock_config()

    def get_config() -> NonCallableMock:
        return mock_config

    with patch("sqlspec.utils.config_tools.import_string", return_value=get_config):
        result = resolve_config_sync("myapp.config.get_database_config")
        assert result is mock_config


def test_assert_guards_get_observability_runtime_raises_runtime_error_not_assertion_error() -> None:
    """A broken attach_observability implementation should hit RuntimeError guard."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config._observability_runtime = None
    with (
        patch.object(config, "attach_observability", return_value=None),
        pytest.raises(RuntimeError, match="ObservabilityRuntime was not set"),
    ):
        config.get_observability_runtime()


def test_assert_guards_no_pool_sync_config_init_migrations_uses_default_directory(tmp_path: Path) -> None:
    """init_migrations derives directory from migration_config when argument is None."""
    migration_dir = tmp_path / "migrations"
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        migration_config={"script_location": str(migration_dir)},
    )
    with patch.object(SyncMigrationCommands, "init", return_value=None) as init:
        config.init_migrations()
    init.assert_called_once_with(str(migration_dir), True)


def test_assert_guards_default_serializer_raises_runtime_error_if_fallback_does_not_set_serializer(monkeypatch) -> None:
    """get_default_serializer should use RuntimeError instead of assert."""
    import sqlspec.utils.serializers._json as json_module

    monkeypatch.setattr(json_module, "_default_serializer", None)
    monkeypatch.setattr(json_module, "MSGSPEC_INSTALLED", False)
    monkeypatch.setattr(json_module, "ORJSON_INSTALLED", False)
    monkeypatch.setattr(json_module, "StandardLibSerializer", lambda: None)
    with pytest.raises(RuntimeError, match="No JSON serializer available"):
        json_module.get_default_serializer()


def test_import_string_preserves_sqlspec_errors_from_the_imported_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A SQLSpec error raised while importing a config module keeps its type and message."""
    from sqlspec.utils.module_loader import import_string

    module_name = f"resolver_error_module_{uuid.uuid4().hex}"
    (tmp_path / f"{module_name}.py").write_text(
        "from sqlspec.exceptions import ImproperConfigurationError\n\nraise ImproperConfigurationError('bad key')\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(ImproperConfigurationError, match="bad key"):
        import_string(f"{module_name}.database_config")


def test_import_string_reports_missing_dependency_as_import_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing dependency stays an ImportError so existing handlers keep working."""
    from sqlspec.utils.module_loader import import_string

    module_name = f"resolver_missing_dep_{uuid.uuid4().hex}"
    (tmp_path / f"{module_name}.py").write_text(
        "from sqlspec.exceptions import MissingDependencyError\n\nraise MissingDependencyError('somepkg')\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(ImportError):
        import_string(f"{module_name}.database_config")
