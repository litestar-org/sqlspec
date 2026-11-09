# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for MigrationRunner functionality.

Tests for MigrationRunner core functionality including:
- Migration discovery and loading
- Migration execution coordination
- Upgrade and downgrade operations
- Migration metadata management
- Error handling and validation
"""

import asyncio
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple, cast
from unittest.mock import AsyncMock, Mock, call, patch

import pytest

from sqlspec.core import SQL
from sqlspec.exceptions import MigrationError
from sqlspec.loader import SQLFileLoader as CoreSQLFileLoader
from sqlspec.migrations import runner as runner_module
from sqlspec.migrations.base import LoadedMigrationMetadata
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.loaders import SQLFileLoader as MigrationSQLFileLoader
from sqlspec.migrations.runner import AsyncMigrationRunner, SyncMigrationRunner


class _RunnerConfig:
    supports_transactional_ddl = True
    supports_migration_schemas = True

    def __init__(self, migration_config: dict[str, Any]) -> None:
        self.migration_config = migration_config


class _AsyncMigrationLoader:
    async def get_up_sql(self, _file_path: Path) -> list[str]:
        return ["CREATE TABLE example (id INTEGER)"]

    async def get_down_sql(self, _file_path: Path) -> list[str]:
        return ["DROP TABLE example"]


def _migration(file_path: Path, loader: Any, schema: str | None = None) -> LoadedMigrationMetadata:
    return cast(
        "LoadedMigrationMetadata",
        {
            "version": "0001",
            "file_path": file_path,
            "loader": loader,
            "has_upgrade": True,
            "has_downgrade": True,
            "schema": schema,
        },
    )


def _sync_runner(tmp_path: Path, migration_config: dict[str, Any]) -> SyncMigrationRunner:
    context = MigrationContext(config=_RunnerConfig(migration_config))
    return SyncMigrationRunner(tmp_path, {}, context, {})


def _async_runner(tmp_path: Path, migration_config: dict[str, Any]) -> AsyncMigrationRunner:
    context = MigrationContext(config=_RunnerConfig(migration_config))
    return AsyncMigrationRunner(tmp_path, {}, context, {})


def create_test_migration_runner(migrations_path: Path = Path("/test")) -> SyncMigrationRunner:
    """Create a test migration runner implementation."""

    class StubMigrationRunner(SyncMigrationRunner):
        def __init__(self, migrations_path: Path) -> None:
            super().__init__(migrations_path)

        def get_migration_files(self) -> Any:
            pass

        def load_migration(self, file_path: Path, version: str | None = None) -> Any:
            _ = file_path, version
            pass

        def execute_upgrade(
            self,
            driver: Any,
            migration: LoadedMigrationMetadata,
            *,
            use_transaction: bool | None = None,
            on_success: Callable[[int], None] | None = None,
        ) -> Any:
            _ = driver, migration, use_transaction, on_success
            pass

        def execute_downgrade(
            self,
            driver: Any,
            migration: LoadedMigrationMetadata,
            *,
            use_transaction: bool | None = None,
            on_success: Callable[[int], None] | None = None,
        ) -> Any:
            _ = driver, migration, use_transaction, on_success
            pass

        def load_all_migrations(self) -> Any:
            pass

    return StubMigrationRunner(migrations_path)


def test_error_span_uses_one_duration_value(tmp_path: Path) -> None:
    """Span and log metadata should share one elapsed-time measurement."""
    runtime = Mock()
    runner = SyncMigrationRunner(tmp_path, runtime=runtime)
    driver = Mock()
    migration = {"version": "0001"}

    with (
        patch("sqlspec.migrations.runner.time.perf_counter", side_effect=[10.0, 11.0]),
        patch.object(runner, "_log_migration_event") as log_event,
    ):
        runner._finish_migration_span_error(  # pyright: ignore[reportPrivateUsage]
            Mock(),
            driver,
            cast("LoadedMigrationMetadata", migration),
            "upgrade",
            "migration.apply",
            9.0,
            RuntimeError("boom"),
        )

    span_duration = runtime.end_migration_span.call_args.kwargs["duration_ms"]
    log_duration = log_event.call_args.kwargs["duration_ms"]
    assert span_duration == log_duration


def create_migration_runner_with_sync_files(migrations_path: Path) -> SyncMigrationRunner:
    """Create a migration runner with sync file discovery."""

    class StubMigrationRunner(SyncMigrationRunner):
        def __init__(self, migrations_path: Path) -> None:
            super().__init__(migrations_path)

        def get_migration_files(self) -> Any:
            return super().get_migration_files()

        def load_migration(self, file_path: Path, version: str | None = None) -> Any:
            _ = file_path, version
            pass

        def execute_upgrade(
            self,
            driver: Any,
            migration: LoadedMigrationMetadata,
            *,
            use_transaction: bool | None = None,
            on_success: Callable[[int], None] | None = None,
        ) -> Any:
            _ = driver, migration, use_transaction, on_success
            pass

        def execute_downgrade(
            self,
            driver: Any,
            migration: LoadedMigrationMetadata,
            *,
            use_transaction: bool | None = None,
            on_success: Callable[[int], None] | None = None,
        ) -> Any:
            _ = driver, migration, use_transaction, on_success
            pass

        def load_all_migrations(self) -> Any:
            pass

    return StubMigrationRunner(migrations_path)


def create_migration_runner_with_metadata(migrations_path: Path) -> SyncMigrationRunner:
    """Create a migration runner with metadata loading."""

    class StubMigrationRunner(SyncMigrationRunner):
        def __init__(self, migrations_path: Path) -> None:
            super().__init__(migrations_path)

        def get_migration_files(self) -> Any:
            return super().get_migration_files()

        def load_migration(self, file_path: Path, version: str | None = None) -> Any:
            return super().load_migration(file_path, version)

        def execute_upgrade(
            self,
            driver: Any,
            migration: LoadedMigrationMetadata,
            *,
            use_transaction: bool | None = None,
            on_success: Callable[[int], None] | None = None,
        ) -> Any:
            _ = driver, migration, use_transaction, on_success
            pass

        def execute_downgrade(
            self,
            driver: Any,
            migration: LoadedMigrationMetadata,
            *,
            use_transaction: bool | None = None,
            on_success: Callable[[int], None] | None = None,
        ) -> Any:
            _ = driver, migration, use_transaction, on_success
            pass

        def load_all_migrations(self) -> Any:
            pass

    return StubMigrationRunner(migrations_path)


def _write_basic_sql(path: Path, version: str, body: str = "SELECT 1;") -> None:
    path.write_text(
        f"""
-- name: migrate-{version}-up
{body}

-- name: migrate-{version}-down
{body}
""".strip()
    )


def test_sync_execute_upgrade_sets_migration_schema_after_begin(tmp_path: Path) -> None:
    """Configured migration schemas should be applied inside the migration transaction."""
    migration_file = tmp_path / "0001_schema.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {"default_schema": "app_schema"})
    driver = Mock()

    runner.execute_upgrade(driver, _migration(migration_file, _AsyncMigrationLoader()), use_transaction=True)

    assert driver.mock_calls[:4] == [
        call.begin(),
        call.set_migration_session_schema("app_schema"),
        call.execute_script("CREATE TABLE example (id INTEGER)"),
        call.commit(),
    ]


def test_sync_execute_upgrade_skips_migration_schema_when_unset(tmp_path: Path) -> None:
    """Back-compat path should not call the schema hook when default_schema is unset."""
    migration_file = tmp_path / "0001_schema.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {})
    driver = Mock()

    runner.execute_upgrade(driver, _migration(migration_file, _AsyncMigrationLoader()), use_transaction=True)

    driver.set_migration_session_schema.assert_not_called()


def test_sync_execute_downgrade_sets_migration_schema_after_begin(tmp_path: Path) -> None:
    """Downgrades should use the same schema setup order as upgrades."""
    migration_file = tmp_path / "0001_schema.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {"default_schema": "app_schema"})
    driver = Mock()

    runner.execute_downgrade(driver, _migration(migration_file, _AsyncMigrationLoader()), use_transaction=True)

    assert driver.mock_calls[:4] == [
        call.begin(),
        call.set_migration_session_schema("app_schema"),
        call.execute_script("DROP TABLE example"),
        call.commit(),
    ]


def test_sync_execute_upgrade_sets_and_resets_non_transactional_schema(tmp_path: Path) -> None:
    """Non-transactional migrations need session-level schema setup and cleanup."""
    migration_file = tmp_path / "0001_schema.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {"default_schema": "app_schema"})
    driver = Mock()

    runner.execute_upgrade(driver, _migration(migration_file, _AsyncMigrationLoader()), use_transaction=False)

    assert driver.mock_calls[:3] == [
        call.set_migration_non_transactional_schema("app_schema"),
        call.execute_script("CREATE TABLE example (id INTEGER)"),
        call.reset_migration_session_schema(),
    ]
    driver.begin.assert_not_called()
    driver.set_migration_session_schema.assert_not_called()


def test_sync_execute_downgrade_sets_and_resets_non_transactional_schema(tmp_path: Path) -> None:
    """Non-transactional downgrades should clean up session-level schema state."""
    migration_file = tmp_path / "0001_schema.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {"default_schema": "app_schema"})
    driver = Mock()

    runner.execute_downgrade(driver, _migration(migration_file, _AsyncMigrationLoader()), use_transaction=False)

    assert driver.mock_calls[:3] == [
        call.set_migration_non_transactional_schema("app_schema"),
        call.execute_script("DROP TABLE example"),
        call.reset_migration_session_schema(),
    ]
    driver.begin.assert_not_called()
    driver.set_migration_session_schema.assert_not_called()


@pytest.mark.anyio
async def test_async_execute_upgrade_sets_migration_schema_after_begin(tmp_path: Path) -> None:
    """Async migrations should await the schema hook inside the transaction."""
    migration_file = tmp_path / "0001_schema.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _async_runner(tmp_path, {"default_schema": "app_schema"})
    driver = AsyncMock()

    await runner.execute_upgrade(driver, _migration(migration_file, _AsyncMigrationLoader()), use_transaction=True)

    assert driver.mock_calls[:4] == [
        call.begin(),
        call.set_migration_session_schema("app_schema"),
        call.execute_script("CREATE TABLE example (id INTEGER)"),
        call.commit(),
    ]


@pytest.mark.anyio
async def test_async_execute_upgrade_sets_and_resets_non_transactional_schema(tmp_path: Path) -> None:
    """Async non-transactional migrations need session-level schema setup and cleanup."""
    migration_file = tmp_path / "0001_schema.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _async_runner(tmp_path, {"default_schema": "app_schema"})
    driver = AsyncMock()

    await runner.execute_upgrade(driver, _migration(migration_file, _AsyncMigrationLoader()), use_transaction=False)

    assert driver.mock_calls[:3] == [
        call.set_migration_non_transactional_schema("app_schema"),
        call.execute_script("CREATE TABLE example (id INTEGER)"),
        call.reset_migration_session_schema(),
    ]
    driver.begin.assert_not_called()
    driver.set_migration_session_schema.assert_not_called()


@pytest.mark.anyio
async def test_async_execute_downgrade_sets_and_resets_non_transactional_schema(tmp_path: Path) -> None:
    """Async non-transactional downgrades should clean up session-level schema state."""
    migration_file = tmp_path / "0001_schema.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _async_runner(tmp_path, {"default_schema": "app_schema"})
    driver = AsyncMock()

    await runner.execute_downgrade(driver, _migration(migration_file, _AsyncMigrationLoader()), use_transaction=False)

    assert driver.mock_calls[:3] == [
        call.set_migration_non_transactional_schema("app_schema"),
        call.execute_script("DROP TABLE example"),
        call.reset_migration_session_schema(),
    ]
    driver.begin.assert_not_called()
    driver.set_migration_session_schema.assert_not_called()


@pytest.mark.anyio
async def test_async_execute_upgrade_skips_migration_schema_when_unset(tmp_path: Path) -> None:
    """Async back-compat path should not call the schema hook when default_schema is unset."""
    migration_file = tmp_path / "0001_schema.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _async_runner(tmp_path, {})
    driver = AsyncMock()

    await runner.execute_upgrade(driver, _migration(migration_file, _AsyncMigrationLoader()), use_transaction=True)

    driver.set_migration_session_schema.assert_not_called()


def test_load_migration_metadata_uses_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure metadata caching prevents redundant checksum calculations."""

    file_path = tmp_path / "0001_cached.sql"
    _write_basic_sql(file_path, "0001")
    runner = SyncMigrationRunner(tmp_path, {}, None, {})

    checksum_calls = 0
    original_checksum = runner_module.SyncMigrationRunner.calculate_checksum

    def _tracked_checksum(self: Any, content: str) -> str:
        nonlocal checksum_calls
        checksum_calls += 1
        return original_checksum(self, content)

    monkeypatch.setattr(runner_module.SyncMigrationRunner, "calculate_checksum", _tracked_checksum)

    runner.load_migration(file_path)
    runner.load_migration(file_path)

    assert checksum_calls == 1


def test_load_migration_metadata_invalidates_on_change(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Metadata cache invalidates when file content changes."""

    file_path = tmp_path / "0001_mutated.sql"
    _write_basic_sql(file_path, "0001")
    runner = SyncMigrationRunner(tmp_path, {}, None, {})

    checksum_calls = 0
    original_checksum = runner_module.SyncMigrationRunner.calculate_checksum

    def _tracked_checksum(self: Any, content: str) -> str:
        nonlocal checksum_calls
        checksum_calls += 1
        return original_checksum(self, content)

    monkeypatch.setattr(runner_module.SyncMigrationRunner, "calculate_checksum", _tracked_checksum)

    runner.load_migration(file_path)
    time.sleep(0.01)
    _write_basic_sql(file_path, "0001", body="SELECT 2;")
    runner.load_migration(file_path)

    assert checksum_calls == 2


def test_migration_runner_initialization() -> None:
    """Test basic MigrationRunner initialization."""
    migrations_path = Path("/test/migrations")
    runner = create_test_migration_runner()
    runner.migrations_path = migrations_path

    assert runner.migrations_path == migrations_path
    assert runner.loader is not None
    assert runner.project_root is None


def test_migration_runner_with_project_root() -> None:
    """Test MigrationRunner with project root set."""
    migrations_path = Path("/test/migrations")
    project_root = Path("/test/project")

    runner = create_test_migration_runner()
    runner.migrations_path = migrations_path
    runner.project_root = project_root

    assert runner.migrations_path == migrations_path
    assert runner.project_root == project_root


def test_get_migration_files_sorting(tmp_path: Path) -> None:
    """Test that migration files are properly sorted by version."""
    (tmp_path / "0003_add_indexes.sql").write_text("-- Migration 3")
    (tmp_path / "0001_initial.sql").write_text("-- Migration 1")
    (tmp_path / "0010_final_touches.sql").write_text("-- Migration 10")
    (tmp_path / "0002_add_users.sql").write_text("-- Migration 2")

    runner = create_migration_runner_with_sync_files(tmp_path)
    files = runner.get_migration_files()

    expected_order = ["0001", "0002", "0003", "0010"]
    actual_order = [version for version, _ in files]

    assert actual_order == expected_order


def test_get_migration_files_mixed_extensions(tmp_path: Path) -> None:
    """Test migration file discovery with mixed SQL and Python files."""
    (tmp_path / "0001_schema.sql").write_text("-- SQL Migration")
    (tmp_path / "0002_data.py").write_text("# Data migration")
    (tmp_path / "0003_more_schema.sql").write_text("-- Another SQL Migration")
    (tmp_path / "README.md").write_text("# README")

    runner = create_migration_runner_with_sync_files(tmp_path)
    files = runner.get_migration_files()

    assert len(files) == 3
    assert files[0][0] == "0001"
    assert files[1][0] == "0002"
    assert files[2][0] == "0003"

    assert files[0][1].suffix == ".sql"
    assert files[1][1].suffix == ".py"
    assert files[2][1].suffix == ".sql"


def test_load_migration_metadata_integration(tmp_path: Path) -> None:
    """Test full migration metadata loading process."""
    migration_file = tmp_path / "0001_create_users.sql"
    migration_content = """
-- name: migrate-0001-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- name: migrate-0001-down
DROP TABLE users;
"""
    migration_file.write_text(migration_content)

    runner = create_migration_runner_with_metadata(tmp_path)

    with (
        patch.object(type(runner.loader), "clear_cache"),
        patch.object(type(runner.loader), "load_sql"),
        patch.object(type(runner.loader), "has_query", side_effect=lambda q: True),
    ):
        with patch("sqlspec.migrations.runner.get_migration_loader") as mock_get_loader:
            mock_loader = Mock()
            mock_loader.validate_migration_file = Mock()
            mock_get_loader.return_value = mock_loader

            metadata = runner.load_migration(migration_file)

        assert metadata["version"] == "0001"
        assert metadata["description"] == "create_users"
        assert metadata["file_path"] == migration_file
        assert metadata["has_upgrade"] is True
        assert metadata["has_downgrade"] is True
        assert isinstance(metadata["checksum"], str)
        assert len(metadata["checksum"]) == 32
        assert "loader" in metadata


def test_load_migration_metadata_prefers_sql_description(tmp_path: Path) -> None:
    migration_file = tmp_path / "0001_custom.sql"
    migration_file.write_text(
        """
-- SQLSpec Migration
-- Description: Custom summary
-- Author: Example

-- name: migrate-0001-up
SELECT 1;
"""
    )

    runner = create_migration_runner_with_metadata(tmp_path)
    metadata = runner.load_migration(migration_file)

    assert metadata["description"] == "Custom summary"


def test_load_migration_metadata_prefers_python_docstring(tmp_path: Path) -> None:
    migration_file = tmp_path / "0002_feature.py"
    migration_file.write_text('"""Description: Add feature"""\n\ndef up():\n    return "SELECT 1"\n')

    runner = create_migration_runner_with_metadata(tmp_path)

    with (
        patch("sqlspec.migrations.runner.get_migration_loader") as mock_get_loader,
        patch("sqlspec.migrations.runner._load_migration_sql") as mock_loader_sql,
    ):
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_loader.get_up_sql = Mock(return_value=["SELECT 1"])
        mock_loader.get_down_sql = Mock(return_value=None)
        mock_get_loader.return_value = mock_loader
        mock_loader_sql.return_value = True

        metadata = runner.load_migration(migration_file)

    assert metadata["description"] == "Add feature"


def test_sync_loads_sql_from_registered_extension_directory(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    extension_dir = tmp_path / "widgets"
    extension_dir.mkdir()
    migration_file = extension_dir / "0001_create_widgets.sql"
    _write_basic_sql(migration_file, "0001", "CREATE TABLE widgets (id INTEGER PRIMARY KEY);")
    runner = SyncMigrationRunner(migrations_dir, {"widgets": extension_dir})

    version, discovered_file = runner.get_migration_files()[0]
    migration = runner.load_migration(discovered_file, version)

    assert migration["version"] == "ext_widgets_0001"
    assert migration["has_upgrade"] is True
    assert migration["has_downgrade"] is True
    assert runner._migration_sql(migration, "up") == [  # pyright: ignore[reportPrivateUsage]
        "CREATE TABLE widgets (id INTEGER PRIMARY KEY);"
    ]


@pytest.mark.anyio
async def test_async_loads_sql_from_registered_extension_directory(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    extension_dir = tmp_path / "widgets"
    extension_dir.mkdir()
    migration_file = extension_dir / "0001_create_widgets.sql"
    _write_basic_sql(migration_file, "0001", "CREATE TABLE widgets (id INTEGER PRIMARY KEY);")
    runner = AsyncMigrationRunner(migrations_dir, {"widgets": extension_dir})

    version, discovered_file = (await runner.get_migration_files())[0]
    migration = await runner.load_migration(discovered_file, version)

    assert migration["version"] == "ext_widgets_0001"
    assert migration["has_upgrade"] is True
    assert migration["has_downgrade"] is True
    assert await runner._migration_sql(migration, "up") == [  # pyright: ignore[reportPrivateUsage]
        "CREATE TABLE widgets (id INTEGER PRIMARY KEY);"
    ]


def test_sync_load_all_migrations_isolates_registered_extension_queries(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    alpha_dir = tmp_path / "alpha"
    alpha_dir.mkdir()
    beta_dir = tmp_path / "beta"
    beta_dir.mkdir()
    _write_basic_sql(alpha_dir / "0001_create_alpha.sql", "0001", "SELECT 'alpha';")
    _write_basic_sql(beta_dir / "0001_create_beta.sql", "0001", "SELECT 'beta';")
    runner = SyncMigrationRunner(migrations_dir, {"alpha": alpha_dir, "beta": beta_dir})

    queries = runner.load_all_migrations()

    assert queries["migrate-ext_alpha_0001-up"].raw_sql == "SELECT 'alpha';"
    assert queries["migrate-ext_beta_0001-up"].raw_sql == "SELECT 'beta';"


@pytest.mark.anyio
async def test_async_load_all_migrations_isolates_registered_extension_queries(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    alpha_dir = tmp_path / "alpha"
    alpha_dir.mkdir()
    beta_dir = tmp_path / "beta"
    beta_dir.mkdir()
    _write_basic_sql(alpha_dir / "0001_create_alpha.sql", "0001", "SELECT 'alpha';")
    _write_basic_sql(beta_dir / "0001_create_beta.sql", "0001", "SELECT 'beta';")
    runner = AsyncMigrationRunner(migrations_dir, {"alpha": alpha_dir, "beta": beta_dir})

    queries = await runner.load_all_migrations()

    assert queries["migrate-ext_alpha_0001-up"].raw_sql == "SELECT 'alpha';"
    assert queries["migrate-ext_beta_0001-up"].raw_sql == "SELECT 'beta';"


def test_sync_load_all_migrations_skips_load_sql_when_query_is_loaded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    migration_file = tmp_path / "0001_init.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {})
    load_calls: list[Path] = []

    monkeypatch.setattr(runner, "get_migration_files", lambda: [("0001", migration_file)])
    monkeypatch.setattr(type(runner.loader), "has_query", lambda _loader, _query: True)
    monkeypatch.setattr(type(runner.loader), "load_sql", lambda _loader, file_path: load_calls.append(file_path))
    monkeypatch.setattr(type(runner.loader), "list_queries", lambda _loader: ["migrate-0001-up"])
    monkeypatch.setattr(type(runner.loader), "get_sql", lambda _loader, _query: SQL("SELECT 1"))

    queries = runner.load_all_migrations()

    assert load_calls == []
    assert "migrate-0001-up" in queries


def test_sync_load_all_migrations_loads_sql_once_for_new_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    migration_file = tmp_path / "0001_init.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {})
    load_calls: list[Path] = []

    monkeypatch.setattr(runner, "get_migration_files", lambda: [("0001", migration_file)])
    monkeypatch.setattr(type(runner.loader), "has_query", lambda _loader, _query: False)
    monkeypatch.setattr(type(runner.loader), "load_sql", lambda _loader, file_path: load_calls.append(file_path))
    monkeypatch.setattr(type(runner.loader), "list_queries", lambda _loader: ["migrate-0001-up"])
    monkeypatch.setattr(type(runner.loader), "get_sql", lambda _loader, _query: SQL("SELECT 1"))

    runner.load_all_migrations()

    assert load_calls == [migration_file]


def test_sync_load_all_migrations_only_loads_missing_sql_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    first_file = tmp_path / "0001_init.sql"
    second_file = tmp_path / "0002_more.sql"
    _write_basic_sql(first_file, "0001")
    _write_basic_sql(second_file, "0002")
    runner = _sync_runner(tmp_path, {})
    load_calls: list[Path] = []

    monkeypatch.setattr(runner, "get_migration_files", lambda: [("0001", first_file), ("0002", second_file)])
    monkeypatch.setattr(type(runner.loader), "has_query", lambda _loader, query: query == "migrate-0001-up")
    monkeypatch.setattr(type(runner.loader), "load_sql", lambda _loader, file_path: load_calls.append(file_path))
    monkeypatch.setattr(type(runner.loader), "list_queries", lambda _loader: ["migrate-0001-up", "migrate-0002-up"])
    monkeypatch.setattr(type(runner.loader), "get_sql", lambda _loader, _query: SQL("SELECT 1"))

    runner.load_all_migrations()

    assert load_calls == [second_file]


@pytest.mark.anyio
async def test_async_load_all_migrations_skips_load_sql_when_query_is_loaded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    migration_file = tmp_path / "0001_init.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _async_runner(tmp_path, {})
    load_calls: list[Path] = []

    async def get_files() -> list[tuple[str, Path]]:
        return [("0001", migration_file)]

    monkeypatch.setattr(runner, "get_migration_files", get_files)
    monkeypatch.setattr(type(runner.loader), "has_query", lambda _loader, _query: True)
    monkeypatch.setattr(type(runner.loader), "load_sql", lambda _loader, file_path: load_calls.append(file_path))
    monkeypatch.setattr(type(runner.loader), "list_queries", lambda _loader: ["migrate-0001-up"])
    monkeypatch.setattr(type(runner.loader), "get_sql", lambda _loader, _query: SQL("SELECT 1"))

    queries = await runner.load_all_migrations()

    assert load_calls == []
    assert "migrate-0001-up" in queries


@pytest.mark.anyio
async def test_async_load_all_migrations_loads_sql_once_for_new_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    migration_file = tmp_path / "0001_init.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _async_runner(tmp_path, {})
    load_calls: list[Path] = []

    async def get_files() -> list[tuple[str, Path]]:
        return [("0001", migration_file)]

    monkeypatch.setattr(runner, "get_migration_files", get_files)
    monkeypatch.setattr(type(runner.loader), "has_query", lambda _loader, _query: False)
    monkeypatch.setattr(type(runner.loader), "load_sql", lambda _loader, file_path: load_calls.append(file_path))
    monkeypatch.setattr(type(runner.loader), "list_queries", lambda _loader: ["migrate-0001-up"])
    monkeypatch.setattr(type(runner.loader), "get_sql", lambda _loader, _query: SQL("SELECT 1"))

    await runner.load_all_migrations()

    assert load_calls == [migration_file]


def test_load_migration_metadata_python_file(tmp_path: Path) -> None:
    """Test metadata loading for Python migration files."""
    migration_file = tmp_path / "0001_data_migration.py"
    python_content = '''
def up():
    """Upgrade migration."""
    return [
        "INSERT INTO users (name, email) VALUES ('admin', 'admin@example.com')",
        "UPDATE settings SET initialized = true"
    ]

def down():
    """Downgrade migration."""
    return [
        "UPDATE settings SET initialized = false",
        "DELETE FROM users WHERE name = 'admin'"
    ]
'''
    migration_file.write_text(python_content)

    runner = create_migration_runner_with_metadata(tmp_path)

    with (
        patch("sqlspec.migrations.runner.get_migration_loader") as mock_get_loader,
        patch("sqlspec.migrations.runner._load_migration_sql") as mock_loader_sql,
    ):
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_loader.get_up_sql = Mock()
        mock_loader.get_down_sql = Mock()
        mock_get_loader.return_value = mock_loader

        mock_loader_sql.return_value = True

        metadata = runner.load_migration(migration_file)

    assert metadata["version"] == "0001"
    assert metadata["description"] == "data_migration"
    assert metadata["has_upgrade"] is True
    assert metadata["has_downgrade"] is True


def test_migration_sql_upgrade_success() -> None:
    """Test successful upgrade SQL generation."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(get_up_sql=AsyncMock(return_value=["CREATE TABLE test (id INTEGER PRIMARY KEY);"])),
    }

    result = runner._migration_sql(cast("LoadedMigrationMetadata", migration), "up")

    assert result is not None
    assert isinstance(result, list)
    assert result == ["CREATE TABLE test (id INTEGER PRIMARY KEY);"]


def test_migration_sql_downgrade_success() -> None:
    """Test successful downgrade SQL generation."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": True,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(get_down_sql=AsyncMock(return_value=["DROP TABLE test;"])),
    }

    result = runner._migration_sql(cast("LoadedMigrationMetadata", migration), "down")

    assert result is not None
    assert isinstance(result, list)
    assert result == ["DROP TABLE test;"]


def test_migration_sql_no_downgrade_warning() -> None:
    """Test warning when no downgrade is available."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    runner._log_migration_event = Mock()  # type: ignore[method-assign]

    result = runner._migration_sql(cast("LoadedMigrationMetadata", migration), "down")

    assert result is None
    runner._log_migration_event.assert_called_once()  # type: ignore[attr-defined]


def test_migration_sql_no_upgrade_error() -> None:
    """Test error when no upgrade is available."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": False,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with pytest.raises(ValueError) as exc_info:
        runner._migration_sql(cast("LoadedMigrationMetadata", migration), "up")

    assert "Migration 0001 has no upgrade query" in str(exc_info.value)


def test_migration_sql_loader_exception_upgrade() -> None:
    """Test handling of loader exceptions during upgrade SQL generation."""
    runner = create_test_migration_runner()
    loader = Mock()
    loader.get_up_sql.side_effect = Exception("Loader failed to parse migration")

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": loader,
    }

    with pytest.raises(ValueError) as exc_info:
        runner._migration_sql(cast("LoadedMigrationMetadata", migration), "up")

    assert "Failed to load upgrade for migration 0001" in str(exc_info.value)


def test_migration_sql_loader_exception_downgrade() -> None:
    """Test handling of loader exceptions during downgrade SQL generation."""
    runner = create_test_migration_runner()
    loader = Mock()
    loader.get_down_sql.side_effect = Exception("Downgrade loader failed")

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": True,
        "file_path": Path("/test/0001_test.sql"),
        "loader": loader,
    }

    runner._log_migration_event = Mock()  # type: ignore[method-assign]

    result = runner._migration_sql(cast("LoadedMigrationMetadata", migration), "down")

    assert result is None
    runner._log_migration_event.assert_called_once()  # type: ignore[attr-defined]


def test_migration_sql_empty_statements() -> None:
    """Test handling when migration loader returns empty statements."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(get_up_sql=AsyncMock(return_value=[])),
    }

    result = runner._migration_sql(cast("LoadedMigrationMetadata", migration), "up")

    assert result == []


def test_migration_sql_none_statements() -> None:
    """Test handling when migration loader returns None."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(get_up_sql=AsyncMock(return_value=None)),
    }

    result = runner._migration_sql(cast("LoadedMigrationMetadata", migration), "up")

    assert result is None


def test_invalid_migration_version_handling(tmp_path: Path) -> None:
    """Test handling of invalid migration version formats."""
    invalid_file = tmp_path / "invalid_version_format.sql"
    invalid_file.write_text("CREATE TABLE test (id INTEGER);")

    runner = create_migration_runner_with_sync_files(tmp_path)
    files = runner.get_migration_files()

    assert len(files) == 0


def test_corrupted_migration_file_handling(tmp_path: Path) -> None:
    """Test handling of corrupted migration files."""
    corrupted_file = tmp_path / "0001_corrupted.sql"
    corrupted_file.write_text("This is not a valid migration file content")

    runner = create_migration_runner_with_metadata(tmp_path)

    with patch("sqlspec.migrations.runner.get_migration_loader") as mock_get_loader:
        mock_loader = Mock()
        mock_loader.validate_migration_file.side_effect = Exception("Validation failed")
        mock_get_loader.return_value = mock_loader

        with pytest.raises(Exception):
            runner.load_migration(corrupted_file)


def test_missing_migrations_directory() -> None:
    """Test handling when migrations directory is missing."""
    nonexistent_path = Path("/nonexistent/migrations/directory")
    runner = create_migration_runner_with_sync_files(nonexistent_path)

    files = runner.get_migration_files()
    assert files == []


def test_large_migration_file_handling(tmp_path: Path) -> None:
    """Test handling of large migration files."""
    large_file = tmp_path / "0001_large_migration.sql"

    large_content_parts = [
        """
-- name: migrate-0001-up
CREATE TABLE large_table (
    id INTEGER PRIMARY KEY,
    data TEXT
);
"""
    ]

    large_content_parts.extend(f"INSERT INTO large_table (data) VALUES ('data_{i:04d}');" for i in range(1000))

    large_content_parts.append("""
-- name: migrate-0001-down
DROP TABLE large_table;
""")

    large_content = "\n".join(large_content_parts)
    large_file.write_text(large_content)

    runner = create_migration_runner_with_metadata(tmp_path)

    with (
        patch.object(type(runner.loader), "clear_cache"),
        patch.object(type(runner.loader), "load_sql"),
        patch.object(type(runner.loader), "has_query", return_value=True),
    ):
        with patch("sqlspec.migrations.runner.get_migration_loader") as mock_get_loader:
            mock_loader = Mock()
            mock_loader.validate_migration_file = Mock()
            mock_get_loader.return_value = mock_loader

            metadata = runner.load_migration(large_file)

            assert metadata["version"] == "0001"
            assert metadata["description"] == "large_migration"
            assert len(metadata["checksum"]) == 32


def test_many_migration_files_performance(tmp_path: Path) -> None:
    """Test performance with many migration files."""
    for i in range(100):
        migration_file = tmp_path / f"{i + 1:04d}_migration_{i}.sql"
        migration_file.write_text(f"""
-- name: migrate-{i + 1:04d}-up
CREATE TABLE test_table_{i} (id INTEGER PRIMARY KEY);

-- name: migrate-{i + 1:04d}-down
DROP TABLE test_table_{i};
""")

    runner = create_migration_runner_with_sync_files(tmp_path)

    files = runner.get_migration_files()

    assert len(files) == 100

    for i, (version, _) in enumerate(files):
        expected_version = f"{i + 1:04d}"
        assert version == expected_version


def test_sql_loader_caches_files(tmp_path: Path) -> None:
    """Test that SQL migration files leverage CoreSQLFileLoader caching.

    Verifies fix for bug #118 - duplicate SQL loading during migrations.
    The SQLFileLoader should NOT call clear_cache() before operations,
    allowing CoreSQLFileLoader's internal caching to work properly.
    """
    import asyncio

    migration_file = tmp_path / "0001_test_migration.sql"
    migration_content = """
-- name: migrate-0001-up
CREATE TABLE test (id INTEGER PRIMARY KEY);

-- name: migrate-0001-down
DROP TABLE test;
"""
    migration_file.write_text(migration_content)

    sql_loader = MigrationSQLFileLoader()

    async def test_operations() -> None:
        sql_loader.validate_migration_file(migration_file)
        path_str = str(migration_file)
        assert path_str in sql_loader.sql_loader._files
        assert sql_loader.sql_loader.has_query("migrate-0001-up")
        assert sql_loader.sql_loader.has_query("migrate-0001-down")

        await sql_loader.get_up_sql(migration_file)
        assert path_str in sql_loader.sql_loader._files

        await sql_loader.get_down_sql(migration_file)
        assert path_str in sql_loader.sql_loader._files

    asyncio.run(test_operations())


def test_no_duplicate_loading_during_migration_execution(tmp_path: Path) -> None:
    """Test that SQL files are loaded exactly once during migration execution.

    Verifies fix for issue #118 - validates that running a migration loads
    the SQL file only once, not multiple times. Checks that the file is in
    the loader's cache after validation and remains there throughout the workflow.
    """
    import asyncio

    migration_file = tmp_path / "0001_create_users.sql"
    migration_content = """
-- name: migrate-0001-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL
);

-- name: migrate-0001-down
DROP TABLE users;
"""
    migration_file.write_text(migration_content)

    sql_loader = MigrationSQLFileLoader()

    async def test_migration_workflow() -> None:
        sql_loader.validate_migration_file(migration_file)

        path_str = str(migration_file)
        assert path_str in sql_loader.sql_loader._files, "File should be loaded after validation"
        assert sql_loader.sql_loader.has_query("migrate-0001-up")
        assert sql_loader.sql_loader.has_query("migrate-0001-down")

        file_count_after_validation = len(sql_loader.sql_loader._files)

        await sql_loader.get_up_sql(migration_file)
        file_count_after_up = len(sql_loader.sql_loader._files)
        assert file_count_after_validation == file_count_after_up, "get_up_sql should not load additional files"

        await sql_loader.get_down_sql(migration_file)
        file_count_after_down = len(sql_loader.sql_loader._files)
        assert file_count_after_up == file_count_after_down, "get_down_sql should not load additional files"

    asyncio.run(test_migration_workflow())


def test_sql_file_loader_counter_accuracy_single_file(tmp_path: Path) -> None:
    """Test SQLFileLoader caching behavior for single file loading.

    Verifies fix for issue #118 (Solution 2) - ensures that load_sql()
    properly caches files. First call should load and parse the file,
    second call should return immediately from cache without reparsing.
    """

    test_file = tmp_path / "test_queries.sql"
    test_content = """
-- name: get_user
SELECT * FROM users WHERE id = :id;

-- name: list_users
SELECT * FROM users;

-- name: delete_user
DELETE FROM users WHERE id = :id;
"""
    test_file.write_text(test_content)

    loader = CoreSQLFileLoader()

    loader.load_sql(test_file)
    path_str = str(test_file)
    assert path_str in loader._files, "First load should add file to cache"
    assert len(loader._queries) == 3, "First load should parse 3 queries"

    query_count_before_reload = len(loader._queries)
    file_count_before_reload = len(loader._files)

    loader.load_sql(test_file)

    assert len(loader._queries) == query_count_before_reload, "Second load should not add new queries (cached)"
    assert len(loader._files) == file_count_before_reload, "Second load should not add new files (cached)"


def test_sql_file_loader_counter_accuracy_directory(tmp_path: Path) -> None:
    """Test SQLFileLoader caching behavior for directory loading.

    Verifies that _load_directory() properly caches files and doesn't
    reload them on subsequent calls.
    """

    file1 = tmp_path / "queries1.sql"
    file1.write_text("""
-- name: query1
SELECT 1;
""")

    file2 = tmp_path / "queries2.sql"
    file2.write_text("""
-- name: query2
SELECT 2;
""")

    loader = CoreSQLFileLoader()

    loader.load_sql(tmp_path)
    assert len(loader._files) == 2, "First load should add 2 files to cache"
    assert len(loader._queries) == 2, "First load should parse 2 queries"

    query_count_before_reload = len(loader._queries)
    file_count_before_reload = len(loader._files)

    loader.load_sql(tmp_path)

    assert len(loader._queries) == query_count_before_reload, "Second load should not add new queries (all cached)"
    assert len(loader._files) == file_count_before_reload, "Second load should not add new files (all cached)"


def test_migration_workflow_single_load_design(tmp_path: Path) -> None:
    """Test that migration workflow respects single-load design.

    Verifies fix for issue #118 (Solution 1) - confirms that:
    1. validate_migration_file() loads the file and parses queries
    2. get_up_sql() retrieves queries WITHOUT reloading the file
    3. get_down_sql() retrieves queries WITHOUT reloading the file

    All three operations should use the same cached file.
    """
    import asyncio

    migration_file = tmp_path / "0001_test.sql"
    migration_content = """
-- name: migrate-0001-up
CREATE TABLE test_table (id INTEGER);

-- name: migrate-0001-down
DROP TABLE test_table;
"""
    migration_file.write_text(migration_content)

    sql_loader = MigrationSQLFileLoader()

    async def test_workflow() -> None:
        sql_loader.validate_migration_file(migration_file)

        path_str = str(migration_file)
        assert path_str in sql_loader.sql_loader._files, "File should be loaded after validation"
        assert sql_loader.sql_loader.has_query("migrate-0001-up")
        assert sql_loader.sql_loader.has_query("migrate-0001-down")

        file_count_before_up = len(sql_loader.sql_loader._files)
        up_sql = await sql_loader.get_up_sql(migration_file)
        file_count_after_up = len(sql_loader.sql_loader._files)

        assert file_count_before_up == file_count_after_up, "get_up_sql() should not load additional files"
        assert len(up_sql) == 1
        assert "CREATE TABLE test_table" in up_sql[0]

        file_count_before_down = len(sql_loader.sql_loader._files)
        down_sql = await sql_loader.get_down_sql(migration_file)
        file_count_after_down = len(sql_loader.sql_loader._files)

        assert file_count_before_down == file_count_after_down, "get_down_sql() should not load additional files"
        assert down_sql is not None
        assert len(down_sql) == 1
        assert "DROP TABLE test_table" in down_sql[0]

    asyncio.run(test_workflow())


def test_migration_loader_does_not_reload_on_get_sql_calls(tmp_path: Path) -> None:
    """Test that get_up_sql and get_down_sql do not trigger file reloads.

    Verifies that after validate_migration_file() loads the file,
    subsequent calls to get_up_sql() and get_down_sql() retrieve
    the cached queries without calling load_sql() again.
    """
    import asyncio

    migration_file = tmp_path / "0001_schema.sql"
    migration_content = """
-- name: migrate-0001-up
CREATE TABLE products (id INTEGER, name TEXT);

-- name: migrate-0001-down
DROP TABLE products;
"""
    migration_file.write_text(migration_content)

    sql_loader = MigrationSQLFileLoader()

    call_counts = {"load_sql": 0}
    original_load_sql = CoreSQLFileLoader.load_sql

    def counting_load_sql(self: CoreSQLFileLoader, *args: Any, **kwargs: Any) -> None:
        call_counts["load_sql"] += 1
        return original_load_sql(self, *args, **kwargs)

    with patch.object(CoreSQLFileLoader, "load_sql", counting_load_sql):

        async def test_no_reload() -> None:
            sql_loader.validate_migration_file(migration_file)
            assert call_counts["load_sql"] == 1, "validate_migration_file should call load_sql exactly once"

            await sql_loader.get_up_sql(migration_file)
            assert call_counts["load_sql"] == 1, "get_up_sql should NOT call load_sql (should use cache)"

            await sql_loader.get_down_sql(migration_file)
            assert call_counts["load_sql"] == 1, "get_down_sql should NOT call load_sql (should use cache)"

        asyncio.run(test_no_reload())


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("ext_litestar_0001_init.sql", "ext_litestar_0001"),
        ("ext_litestar_queues_0001_init.sql", "ext_litestar_queues_0001"),
        ("ext_litestar_queues_0002_add_index.sql", "ext_litestar_queues_0002"),
        ("ext_adk_20251011120000_seed.sql", "ext_adk_20251011120000"),
        ("0001_core.sql", "0001"),
        ("20251011120000_core.sql", "20251011120000"),
    ],
)
def test_extract_version_handles_multi_underscore_extension_names(tmp_path: Path, filename: str, expected: str) -> None:
    """Extension names containing underscores keep their numeric version."""
    runner = SyncMigrationRunner(tmp_path)

    assert runner._extract_version(filename) == expected


def test_extract_version_result_parses_for_multi_underscore_extension(tmp_path: Path) -> None:
    """The extracted version parses rather than falling back to raw string sorting."""
    from sqlspec.migrations.version import parse_version

    runner = SyncMigrationRunner(tmp_path)

    version = runner._extract_version("ext_litestar_queues_0001_init.sql")
    assert version is not None
    parsed = parse_version(version)
    assert parsed.extension == "litestar_queues"
    assert parsed.sequence == 1


def test_migration_context_resolves_multi_underscore_extension_settings(tmp_path: Path) -> None:
    """Per-migration context carries the settings of an underscore-named extension."""
    ext_dir = tmp_path / "vendor"
    ext_dir.mkdir()
    settings = {"table_name": "queue_tasks"}
    runner = SyncMigrationRunner(
        tmp_path, {"litestar_queues": ext_dir}, MigrationContext(dialect="sqlite"), {"litestar_queues": settings}
    )

    context = runner._migration_context(ext_dir / "ext_litestar_queues_0001_init.sql")

    assert context is not None
    assert context.extension_config == settings


class _EmptyMigrationLoader:
    async def get_up_sql(self, _file_path: Path) -> list[str]:
        return []

    async def get_down_sql(self, _file_path: Path) -> list[str]:
        return []


def _write_python_migration(path: Path, body: str) -> None:
    path.write_text(body.strip() + "\n")


def test_finalize_migration_sql_distinguishes_absent_from_empty(tmp_path: Path) -> None:
    """None means the direction is absent; an empty list is a real no-op."""
    runner = _sync_runner(tmp_path, {})

    assert runner._finalize_migration_sql(None) is None
    assert runner._finalize_migration_sql([]) == []
    assert runner._finalize_migration_sql(["SELECT 1"]) == ["SELECT 1"]


def test_sync_execute_upgrade_records_empty_statement_list(tmp_path: Path) -> None:
    """An upgrade returning no statements still reports success so it can be recorded."""
    migration_file = tmp_path / "0001_noop.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {})
    driver = Mock()
    on_success = Mock()

    _, execution_time = runner.execute_upgrade(
        driver, _migration(migration_file, _EmptyMigrationLoader()), use_transaction=True, on_success=on_success
    )

    driver.execute_script.assert_not_called()
    on_success.assert_called_once()
    recorded_time = on_success.call_args.args[0]
    assert isinstance(recorded_time, int)
    assert recorded_time >= 0
    assert execution_time >= 0


def test_sync_execute_downgrade_records_empty_statement_list(tmp_path: Path) -> None:
    """A present-but-empty downgrade still fires the tracking callback."""
    migration_file = tmp_path / "0001_noop.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {})
    driver = Mock()
    on_success = Mock()

    _, execution_time = runner.execute_downgrade(
        driver, _migration(migration_file, _EmptyMigrationLoader()), use_transaction=True, on_success=on_success
    )

    driver.execute_script.assert_not_called()
    on_success.assert_called_once()
    assert on_success.call_args.args[0] >= 0
    assert execution_time >= 0


def test_sync_execute_downgrade_skips_absent_direction(tmp_path: Path) -> None:
    """A migration flagged without a downgrade must not be treated as a no-op."""
    migration_file = tmp_path / "0001_noop.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {})
    driver = Mock()
    on_success = Mock()
    migration = _migration(migration_file, _EmptyMigrationLoader())
    migration["has_downgrade"] = False

    result = runner.execute_downgrade(driver, migration, use_transaction=True, on_success=on_success)

    assert result == (None, 0)
    on_success.assert_not_called()
    driver.execute_script.assert_not_called()


@pytest.mark.anyio
async def test_async_execute_upgrade_records_empty_statement_list(tmp_path: Path) -> None:
    """Async upgrades returning no statements still report success."""
    migration_file = tmp_path / "0001_noop.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _async_runner(tmp_path, {})
    driver = AsyncMock()
    on_success = AsyncMock()

    _, execution_time = await runner.execute_upgrade(
        driver, _migration(migration_file, _EmptyMigrationLoader()), use_transaction=True, on_success=on_success
    )

    driver.execute_script.assert_not_called()
    on_success.assert_awaited_once()
    assert on_success.call_args.args[0] >= 0
    assert execution_time >= 0


@pytest.mark.anyio
async def test_async_execute_downgrade_records_empty_statement_list(tmp_path: Path) -> None:
    """Async present-but-empty downgrades still fire the tracking callback."""
    migration_file = tmp_path / "0001_noop.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _async_runner(tmp_path, {})
    driver = AsyncMock()
    on_success = AsyncMock()

    _, execution_time = await runner.execute_downgrade(
        driver, _migration(migration_file, _EmptyMigrationLoader()), use_transaction=True, on_success=on_success
    )

    driver.execute_script.assert_not_called()
    on_success.assert_awaited_once()
    assert on_success.call_args.args[0] >= 0
    assert execution_time >= 0


@pytest.mark.anyio
async def test_async_execute_downgrade_skips_absent_direction(tmp_path: Path) -> None:
    """Async downgrades on a flagged-absent direction return the missing sentinel."""
    migration_file = tmp_path / "0001_noop.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _async_runner(tmp_path, {})
    driver = AsyncMock()
    on_success = AsyncMock()
    migration = _migration(migration_file, _EmptyMigrationLoader())
    migration["has_downgrade"] = False

    result = await runner.execute_downgrade(driver, migration, use_transaction=True, on_success=on_success)

    assert result == (None, 0)
    on_success.assert_not_awaited()
    driver.execute_script.assert_not_called()


def test_load_migration_flags_empty_python_downgrade_as_present(tmp_path: Path) -> None:
    """A Python down() returning an empty list is a reversible no-op."""
    migration_file = tmp_path / "0001_empty_down.py"
    _write_python_migration(
        migration_file,
        """
def up() -> list[str]:
    return ["CREATE TABLE example (id INTEGER)"]


def down() -> list[str]:
    return []
""",
    )
    runner = _sync_runner(tmp_path, {})

    migration = runner.load_migration(migration_file)

    assert migration["has_downgrade"] is True


def test_load_migration_flags_missing_python_downgrade_as_absent(tmp_path: Path) -> None:
    """A Python migration without down() stays irreversible."""
    migration_file = tmp_path / "0001_no_down.py"
    _write_python_migration(
        migration_file,
        """
def up() -> list[str]:
    return ["CREATE TABLE example (id INTEGER)"]
""",
    )
    runner = _sync_runner(tmp_path, {})

    migration = runner.load_migration(migration_file)

    assert migration["has_downgrade"] is False


def test_load_migration_flags_missing_sql_downgrade_as_absent(tmp_path: Path) -> None:
    """A SQL migration without a down block stays irreversible."""
    migration_file = tmp_path / "0001_up_only.sql"
    migration_file.write_text("-- name: migrate-0001-up\nSELECT 1;\n")
    runner = _sync_runner(tmp_path, {})

    migration = runner.load_migration(migration_file)

    assert migration["has_upgrade"] is True
    assert migration["has_downgrade"] is False


@pytest.mark.anyio
async def test_async_load_migration_flags_empty_python_downgrade_as_present(tmp_path: Path) -> None:
    """The async runner agrees that an empty down() is a present direction."""
    migration_file = tmp_path / "0001_empty_down.py"
    _write_python_migration(
        migration_file,
        """
def up() -> list[str]:
    return ["CREATE TABLE example (id INTEGER)"]


def down() -> list[str]:
    return []
""",
    )
    runner = _async_runner(tmp_path, {})

    migration = await runner.load_migration(migration_file)

    assert migration["has_downgrade"] is True


def test_load_metadata_parses_schema_directive(tmp_path: Path) -> None:
    """Load metadata should extract the schema directive from SQL comments."""
    migration_file = tmp_path / "0001_directive.sql"
    migration_file.write_text(
        """-- schema: custom_schema
-- transactional: true
-- name: migrate-0001-up
CREATE TABLE foo (id INT);
""".strip()
    )
    runner = _sync_runner(tmp_path, {})
    metadata = runner._load_metadata(migration_file)
    assert metadata["schema"] == "custom_schema"


def test_load_metadata_leaves_schema_none_without_directive(tmp_path: Path) -> None:
    """Load metadata should have schema as None when no directive is present."""
    migration_file = tmp_path / "0001_no_directive.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {})
    metadata = runner._load_metadata(migration_file)
    assert metadata["schema"] is None


def test_load_metadata_rejects_invalid_schema_directive(tmp_path: Path) -> None:
    """Load metadata should raise MigrationError for invalid schema names."""
    migration_file = tmp_path / "0001_bad_schema.sql"
    migration_file.write_text(
        """-- schema: bad-schema;
-- name: migrate-0001-up
CREATE TABLE foo (id INT);
""".strip()
    )
    runner = _sync_runner(tmp_path, {})
    with pytest.raises(MigrationError, match="Invalid schema directive"):
        runner._load_metadata(migration_file)


def test_sync_execute_upgrade_prefers_migration_schema_directive(tmp_path: Path) -> None:
    """Migration schema directive overrides configured default schema."""
    migration_file = tmp_path / "0001_override.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {"default_schema": "configured_schema"})
    driver = Mock()
    driver.has_schema.return_value = True

    migration = _migration(migration_file, _AsyncMigrationLoader(), schema="directive_schema")
    runner.execute_upgrade(driver, migration, use_transaction=True)

    driver.has_schema.assert_called_once_with("directive_schema")
    driver.set_migration_session_schema.assert_called_once_with("directive_schema")


def test_sync_execute_upgrade_resets_schema_after_commit(tmp_path: Path) -> None:
    """Migration schema session should be reset after transaction commit."""
    migration_file = tmp_path / "0001_schema.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {"default_schema": "app_schema"})
    driver = Mock()
    driver.has_schema.return_value = True

    migration = _migration(migration_file, _AsyncMigrationLoader(), schema="tenant")
    runner.execute_upgrade(driver, migration, use_transaction=True)

    commit_index = driver.mock_calls.index(call.commit())
    reset_index = driver.mock_calls.index(call.reset_migration_session_schema())
    assert reset_index > commit_index


def test_sync_execute_upgrade_rejects_missing_directive_schema(tmp_path: Path) -> None:
    """Execution fails when directive schema does not exist in database."""
    migration_file = tmp_path / "0001_missing.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _sync_runner(tmp_path, {})
    driver = Mock()
    driver.has_schema.return_value = False

    migration = _migration(migration_file, _AsyncMigrationLoader(), schema="missing_schema")
    with pytest.raises(MigrationError, match="Migration schema 'missing_schema' does not exist"):
        runner.execute_upgrade(driver, migration, use_transaction=True)

    driver.has_schema.assert_called_once_with("missing_schema")


def test_sync_execute_upgrade_rejects_directive_when_adapter_lacks_support(tmp_path: Path) -> None:
    """Execution fails when adapter does not support migration schemas."""
    migration_file = tmp_path / "0001_unsupported.sql"
    _write_basic_sql(migration_file, "0001")
    config = _RunnerConfig({})
    config.supports_migration_schemas = False
    context = MigrationContext(config=config)
    runner = SyncMigrationRunner(tmp_path, {}, context, {})
    driver = Mock()

    migration = _migration(migration_file, _AsyncMigrationLoader(), schema="directive_schema")
    with pytest.raises(MigrationError, match="does not support migration schema directives"):
        runner.execute_upgrade(driver, migration, use_transaction=True)


_MIGRATION_STATEMENT = "CREATE TABLE example (id INTEGER)"


class _FailureCase(NamedTuple):
    use_transaction: bool
    migration_config: dict[str, Any]
    side_effects: dict[str, Any]
    expected_calls: list[Any]


_TRANSACTION_FAILURE_CALLS = [
    call.begin(),
    call.set_migration_session_schema("app_schema"),
    call.execute_script(_MIGRATION_STATEMENT),
    call.rollback(),
    call.reset_migration_session_schema(),
]

_FAILURE_CASES = {
    "transaction_statement_fails": _FailureCase(
        True, {"default_schema": "app_schema"}, {"execute_script": ValueError("boom")}, _TRANSACTION_FAILURE_CALLS
    ),
    "rollback_fails": _FailureCase(
        True,
        {"default_schema": "app_schema"},
        {"execute_script": ValueError("boom"), "rollback": RuntimeError("rollback failed")},
        _TRANSACTION_FAILURE_CALLS,
    ),
    "reset_fails": _FailureCase(
        True,
        {"default_schema": "app_schema"},
        {"execute_script": ValueError("boom"), "reset_migration_session_schema": RuntimeError("reset failed")},
        _TRANSACTION_FAILURE_CALLS,
    ),
    "commit_fails": _FailureCase(
        True,
        {"default_schema": "app_schema"},
        {"commit": ValueError("boom")},
        [
            call.begin(),
            call.set_migration_session_schema("app_schema"),
            call.execute_script(_MIGRATION_STATEMENT),
            call.commit(),
            call.rollback(),
            call.reset_migration_session_schema(),
        ],
    ),
    "no_schema": _FailureCase(
        True,
        {},
        {"execute_script": ValueError("boom")},
        [call.begin(), call.execute_script(_MIGRATION_STATEMENT), call.rollback()],
    ),
    "non_transactional_statement_fails": _FailureCase(
        False,
        {"default_schema": "app_schema"},
        {"execute_script": ValueError("boom")},
        [
            call.set_migration_non_transactional_schema("app_schema"),
            call.execute_script(_MIGRATION_STATEMENT),
            call.reset_migration_session_schema(),
        ],
    ),
    "non_transactional_connection_aborted": _FailureCase(
        False,
        {"default_schema": "app_schema"},
        {"execute_script": ValueError("boom"), "reset_migration_session_schema": [RuntimeError("aborted"), None]},
        [
            call.set_migration_non_transactional_schema("app_schema"),
            call.execute_script(_MIGRATION_STATEMENT),
            call.reset_migration_session_schema(),
            call.rollback(),
            call.reset_migration_session_schema(),
        ],
    ),
    "non_transactional_no_schema": _FailureCase(
        False, {}, {"execute_script": ValueError("boom")}, [call.execute_script(_MIGRATION_STATEMENT)]
    ),
}


def _apply_side_effects(driver: Mock, side_effects: dict[str, Any]) -> None:
    for method_name, side_effect in side_effects.items():
        getattr(driver, method_name).side_effect = side_effect


def _failing_migration(tmp_path: Path) -> LoadedMigrationMetadata:
    migration_file = tmp_path / "0001_fails.sql"
    _write_basic_sql(migration_file, "0001")
    return _migration(migration_file, _AsyncMigrationLoader())


class _RunnerMode:
    """Build the sync or async runner, driver, and callback a shared test body exercises."""

    def __init__(self, is_async: bool) -> None:
        self.is_async = is_async

    def runner(self, tmp_path: Path, context_or_config: "MigrationContext | dict[str, Any]") -> Any:
        context = (
            context_or_config
            if isinstance(context_or_config, MigrationContext)
            else MigrationContext(config=_RunnerConfig(context_or_config))
        )
        runner_class = AsyncMigrationRunner if self.is_async else SyncMigrationRunner
        return runner_class(tmp_path, {}, context, {})

    def driver(self) -> Mock:
        return AsyncMock() if self.is_async else Mock()

    def callback(self, **kwargs: Any) -> Mock:
        return AsyncMock(**kwargs) if self.is_async else Mock(**kwargs)

    async def call(self, function: "Callable[..., Any]", *args: Any, **kwargs: Any) -> Any:
        result = function(*args, **kwargs)
        return await result if self.is_async else result


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def runner_mode(request: pytest.FixtureRequest) -> _RunnerMode:
    return _RunnerMode(bool(request.param))


@pytest.mark.anyio
@pytest.mark.parametrize("case", _FAILURE_CASES.values(), ids=_FAILURE_CASES.keys())
async def test_failure_recovery_order_keeps_original_error(
    tmp_path: Path, runner_mode: _RunnerMode, case: _FailureCase
) -> None:
    """A failed migration rolls back, resets, and raises the migration's own error."""
    runner = runner_mode.runner(tmp_path, case.migration_config)
    driver = runner_mode.driver()
    _apply_side_effects(driver, case.side_effects)

    with pytest.raises(ValueError, match="boom"):
        await runner_mode.call(
            runner.execute_upgrade, driver, _failing_migration(tmp_path), use_transaction=case.use_transaction
        )

    assert driver.mock_calls == case.expected_calls


@pytest.mark.anyio
async def test_missing_directive_schema_skips_begin_and_rollback(tmp_path: Path, runner_mode: _RunnerMode) -> None:
    """A missing directive schema raises before a transaction exists, so nothing is rolled back."""
    runner = runner_mode.runner(tmp_path, {})
    driver = runner_mode.driver()
    driver.has_schema.return_value = False
    migration = _migration(tmp_path / "0001_missing.sql", _AsyncMigrationLoader(), schema="missing_schema")

    with pytest.raises(MigrationError, match="Migration schema 'missing_schema' does not exist"):
        await runner_mode.call(runner.execute_upgrade, driver, migration, use_transaction=True)

    assert driver.mock_calls == [call.has_schema("missing_schema")]


@pytest.mark.anyio
async def test_begin_failure_runs_no_recovery(tmp_path: Path, runner_mode: _RunnerMode) -> None:
    """A failed begin raises unchanged because the runner owns no transaction yet."""
    runner = runner_mode.runner(tmp_path, {"default_schema": "app_schema"})
    driver = runner_mode.driver()
    driver.begin.side_effect = ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await runner_mode.call(runner.execute_upgrade, driver, _failing_migration(tmp_path), use_transaction=True)

    assert driver.mock_calls == [call.begin()]


@pytest.mark.anyio
async def test_on_success_failure_rolls_back_without_second_reset(tmp_path: Path, runner_mode: _RunnerMode) -> None:
    """A failed ledger callback rolls back, and the schema it already reset is not reset again."""
    runner = runner_mode.runner(tmp_path, {"default_schema": "app_schema"})
    driver = runner_mode.driver()

    with pytest.raises(ValueError, match="boom"):
        await runner_mode.call(
            runner.execute_upgrade,
            driver,
            _failing_migration(tmp_path),
            use_transaction=True,
            on_success=runner_mode.callback(side_effect=ValueError("boom")),
        )

    assert driver.reset_migration_session_schema.call_count == 1
    assert driver.mock_calls[-2:] == [call.reset_migration_session_schema(), call.rollback()]


@pytest.mark.anyio
async def test_reset_failure_before_on_success_retries_after_rollback(tmp_path: Path, runner_mode: _RunnerMode) -> None:
    """A reset that fails before the ledger callback is retried after rollback, and its error surfaces."""
    runner = runner_mode.runner(tmp_path, {"default_schema": "app_schema"})
    driver = runner_mode.driver()
    driver.reset_migration_session_schema.side_effect = [RuntimeError("reset failed"), None]
    on_success = runner_mode.callback()

    with pytest.raises(RuntimeError, match="reset failed"):
        await runner_mode.call(
            runner.execute_upgrade, driver, _failing_migration(tmp_path), use_transaction=True, on_success=on_success
        )

    on_success.assert_not_called()
    assert driver.mock_calls[-3:] == [
        call.reset_migration_session_schema(),
        call.rollback(),
        call.reset_migration_session_schema(),
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("use_transaction", [True, False], ids=["transactional", "non_transactional"])
async def test_post_success_reset_failure_is_raised(
    tmp_path: Path, runner_mode: _RunnerMode, use_transaction: bool
) -> None:
    """A reset failing after the migration finished stays visible to the caller."""
    runner = runner_mode.runner(tmp_path, {"default_schema": "app_schema"})
    driver = runner_mode.driver()
    driver.reset_migration_session_schema.side_effect = RuntimeError("reset failed")

    with pytest.raises(RuntimeError, match="reset failed"):
        await runner_mode.call(
            runner.execute_upgrade, driver, _failing_migration(tmp_path), use_transaction=use_transaction
        )

    driver.rollback.assert_not_called()


@pytest.mark.anyio
async def test_async_cancellation_runs_recovery(tmp_path: Path) -> None:
    """Task cancellation during a migration still rolls back and resets before propagating."""
    runner = _async_runner(tmp_path, {"default_schema": "app_schema"})
    driver = AsyncMock()
    driver.execute_script.side_effect = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await runner.execute_upgrade(driver, _failing_migration(tmp_path), use_transaction=True)

    assert driver.mock_calls[-2:] == [call.rollback(), call.reset_migration_session_schema()]


@pytest.mark.anyio
async def test_downgrade_failure_rolls_back_before_reset(tmp_path: Path, runner_mode: _RunnerMode) -> None:
    """A failed downgrade follows the same recovery order as an upgrade."""
    runner = runner_mode.runner(tmp_path, {"default_schema": "app_schema"})
    driver = runner_mode.driver()
    driver.execute_script.side_effect = ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await runner_mode.call(runner.execute_downgrade, driver, _failing_migration(tmp_path), use_transaction=True)

    assert driver.mock_calls == [
        call.begin(),
        call.set_migration_session_schema("app_schema"),
        call.execute_script("DROP TABLE example"),
        call.rollback(),
        call.reset_migration_session_schema(),
    ]


def test_failed_recovery_step_logs_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """A failed recovery step is logged as a warning, even with migration logging off."""
    runner = _sync_runner(tmp_path, {"default_schema": "app_schema"})
    driver = Mock()
    driver.execute_script.side_effect = ValueError("boom")
    driver.rollback.side_effect = RuntimeError("rollback failed")

    with caplog.at_level("WARNING", logger="sqlspec.migrations.runner"), pytest.raises(ValueError, match="boom"):
        runner.execute_upgrade(driver, _failing_migration(tmp_path), use_transaction=True)

    assert [record.getMessage() for record in caplog.records] == ["migration.recovery.failed"]


@pytest.mark.anyio
async def test_execute_upgrade_rejects_default_schema_when_adapter_lacks_support(
    tmp_path: Path, runner_mode: _RunnerMode
) -> None:
    """A configured default schema raises before any driver call on an unsupported adapter."""
    config = _RunnerConfig({"default_schema": "app_schema"})
    config.supports_migration_schemas = False
    runner = runner_mode.runner(tmp_path, MigrationContext(config=config))
    driver = runner_mode.driver()

    with pytest.raises(MigrationError, match="does not support migration default schemas"):
        await runner_mode.call(runner.execute_upgrade, driver, _failing_migration(tmp_path), use_transaction=True)

    driver.begin.assert_not_called()
    driver.set_migration_session_schema.assert_not_called()


@pytest.mark.anyio
async def test_async_execute_downgrade_prefers_migration_schema_directive(tmp_path: Path) -> None:
    """Async downgrade prioritizes migration schema directive over default schema."""
    migration_file = tmp_path / "0001_override.sql"
    _write_basic_sql(migration_file, "0001")
    runner = _async_runner(tmp_path, {"default_schema": "configured_schema"})
    driver = AsyncMock()
    driver.has_schema.return_value = True

    migration = _migration(migration_file, _AsyncMigrationLoader(), schema="directive_schema")
    await runner.execute_downgrade(driver, migration, use_transaction=True)

    driver.has_schema.assert_awaited_once_with("directive_schema")
    driver.set_migration_session_schema.assert_called_once_with("directive_schema")


class _NonTransactionalDdlConfig(_RunnerConfig):
    supports_transactional_ddl = False


@pytest.mark.parametrize(
    ("directive", "expected"), [(True, True), (False, False)], ids=["explicit-true", "explicit-false"]
)
def test_explicit_transactional_directive_overrides_capability(tmp_path: Path, directive: bool, expected: bool) -> None:
    """A migration that names its transaction mode wins over the backend capability flag."""
    runner = _sync_runner(tmp_path, {})
    config = _NonTransactionalDdlConfig({})
    migration = cast("LoadedMigrationMetadata", {**_migration(tmp_path / "0001.sql", None), "transactional": directive})

    assert runner.should_use_transaction(migration, cast("Any", config)) is expected


def test_capability_still_decides_without_a_directive(tmp_path: Path) -> None:
    """With no per-migration directive the backend capability flag governs."""
    runner = _sync_runner(tmp_path, {})
    migration = _migration(tmp_path / "0001.sql", None)

    assert runner.should_use_transaction(migration, cast("Any", _NonTransactionalDdlConfig({}))) is False
    assert runner.should_use_transaction(migration, cast("Any", _RunnerConfig({}))) is True


def test_sync_execute_reconciliation_passes_isolated_context(tmp_path: Path) -> None:
    """Sync callback receives an isolated MigrationContext with deep-copied extension_config."""
    import json

    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    log_file = tmp_path / "context_log.json"
    log_file.write_text("[]", encoding="utf-8")
    reconcile_path = tmp_path / "reconcile.py"
    reconcile_path.write_text(
        f"""
import json
from pathlib import Path

def reconcile(context):
    log_path = Path({str(log_file)!r})
    entries = json.loads(log_path.read_text(encoding="utf-8"))
    entries.append({{
        "has_driver": context.driver is not None,
        "dialect": context.dialect,
        "nested_k": context.extension_config["nested"]["k"],
        "metadata_keys": sorted(context.metadata.keys()),
    }})
    log_path.write_text(json.dumps(entries), encoding="utf-8")
    context.extension_config["nested"]["k"] = "mutated"
    context.metadata["tmp"] = 1
""",
        encoding="utf-8",
    )

    config = SqliteConfig(connection_config={"database": ":memory:"})
    ext_configs: dict[str, dict[str, Any]] = {"litestar": {"nested": {"k": "original"}}}
    runner = SyncMigrationRunner(tmp_path, {}, MigrationContext.from_config(config), ext_configs)
    descriptor = ExtensionReconcilerDescriptor(name="litestar", module_path=reconcile_path)

    with config.provide_session() as driver:
        elapsed_first = runner.execute_reconciliation(driver, descriptor)
        elapsed_second = runner.execute_reconciliation(driver, descriptor)

    assert isinstance(elapsed_first, int) and elapsed_first >= 0
    assert isinstance(elapsed_second, int) and elapsed_second >= 0
    assert ext_configs["litestar"]["nested"]["k"] == "original"
    recorded = json.loads(log_file.read_text(encoding="utf-8"))
    assert recorded == [
        {"has_driver": True, "dialect": "sqlite", "nested_k": "original", "metadata_keys": []},
        {"has_driver": True, "dialect": "sqlite", "nested_k": "original", "metadata_keys": []},
    ]


@pytest.mark.anyio
async def test_async_execute_reconciliation_supports_async_and_sync_callbacks(tmp_path: Path) -> None:
    """AsyncMigrationRunner supports both async def and plain def reconcile callbacks."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    async_path = tmp_path / "async_reconcile.py"
    async_path.write_text(
        """
async def reconcile(context):
    await context.driver.execute("CREATE TABLE async_cb (id INTEGER PRIMARY KEY)")
""",
        encoding="utf-8",
    )
    sync_path = tmp_path / "sync_reconcile.py"
    sync_path.write_text(
        """
def reconcile(context):
    context.metadata["ran_sync"] = True
""",
        encoding="utf-8",
    )

    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "async_cb.db")})
    runner = AsyncMigrationRunner(tmp_path, {}, MigrationContext.from_config(config), {"adk": {}, "events": {}})

    async with config.provide_session() as driver:
        elapsed_async = await runner.execute_reconciliation(
            driver, ExtensionReconcilerDescriptor(name="adk", module_path=async_path)
        )
        elapsed_sync = await runner.execute_reconciliation(
            driver, ExtensionReconcilerDescriptor(name="events", module_path=sync_path)
        )
        tables = await driver.select("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'async_cb'")

    await config.close_pool()
    assert isinstance(elapsed_async, int) and elapsed_async >= 0
    assert isinstance(elapsed_sync, int) and elapsed_sync >= 0
    assert len(tables) == 1


def test_sync_execute_reconciliation_rejects_async_callback_without_loop_or_warning(tmp_path: Path) -> None:
    """SyncMigrationRunner closes unawaited coroutines and raises MigrationError without spawning a loop."""
    import gc
    import warnings

    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    reconcile_path = tmp_path / "reconcile.py"
    reconcile_path.write_text("async def reconcile(context):\n    return None\n", encoding="utf-8")

    runner = _sync_runner(tmp_path, {})
    driver = Mock()
    descriptor = ExtensionReconcilerDescriptor(name="litestar", module_path=reconcile_path)

    with (
        warnings.catch_warnings(record=True) as caught,
        patch.object(asyncio, "new_event_loop", side_effect=AssertionError("unexpected new_event_loop")),
        patch.object(asyncio, "run", side_effect=AssertionError("unexpected asyncio.run")),
    ):
        warnings.simplefilter("always")
        with pytest.raises(MigrationError) as exc_info:
            runner.execute_reconciliation(driver, descriptor)
        gc.collect()

    message = str(exc_info.value)
    assert "litestar" in message
    assert str(reconcile_path) in message
    runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    assert runtime_warnings == []


@pytest.mark.anyio
async def test_execute_reconciliation_rejects_non_none_return_without_payload_leak(tmp_path: Path) -> None:
    """Sync and async runners reject non-None return values without interpolating secret payloads."""
    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    sync_path = tmp_path / "sync_ret.py"
    sync_path.write_text('def reconcile(context):\n    return ["CREATE TABLE t (id INT)"]\n', encoding="utf-8")
    async_path = tmp_path / "async_ret.py"
    async_path.write_text('async def reconcile(context):\n    return {"pw": "topsecret_value"}\n', encoding="utf-8")

    sync_runner = _sync_runner(tmp_path, {})
    with pytest.raises(MigrationError) as sync_exc:
        sync_runner.execute_reconciliation(
            Mock(), ExtensionReconcilerDescriptor(name="litestar", module_path=sync_path)
        )
    assert "litestar" in str(sync_exc.value)
    assert str(sync_path) in str(sync_exc.value)
    assert "list" in str(sync_exc.value)

    async_runner = _async_runner(tmp_path, {})
    with pytest.raises(MigrationError) as async_exc:
        await async_runner.execute_reconciliation(
            AsyncMock(), ExtensionReconcilerDescriptor(name="adk", module_path=async_path)
        )
    assert "adk" in str(async_exc.value)
    assert str(async_path) in str(async_exc.value)
    assert "dict" in str(async_exc.value)
    assert "topsecret_value" not in str(async_exc.value)


@pytest.mark.parametrize(
    ("case_id", "source", "expected_cause"),
    [
        ("missing_file", None, OSError),
        ("syntax_error", "def reconcile(:\n", SyntaxError),
        ("missing_symbol", "VALUE = 1\n", None),
        ("non_callable", 'reconcile = "not_callable"\n', None),
    ],
)
def test_load_reconcile_callable_rejects_invalid_modules_and_cleans_sys_modules(
    tmp_path: Path, case_id: str, source: str | None, expected_cause: type[BaseException] | None
) -> None:
    """Invalid reconcile modules raise MigrationError and leave zero temporary sys.modules entries."""
    import sys

    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    reconcile_path = tmp_path / f"{case_id}_reconcile.py"
    if source is not None:
        reconcile_path.write_text(source, encoding="utf-8")

    runner = _sync_runner(tmp_path, {})
    descriptor = ExtensionReconcilerDescriptor(name="events", module_path=reconcile_path)

    with pytest.raises(MigrationError) as exc_info:
        runner.execute_reconciliation(Mock(), descriptor)

    assert "events" in str(exc_info.value)
    assert str(reconcile_path) in str(exc_info.value)
    if expected_cause is not None:
        assert isinstance(exc_info.value.__cause__, expected_cause)
    leaked = [name for name in sys.modules if name.startswith("_sqlspec_reconcile_")]
    assert leaked == []


def test_execute_reconciliation_reloads_fresh_source_with_preserved_mtime_and_size(tmp_path: Path) -> None:
    """Rewriting reconcile.py with identical byte length and mtime executes the updated code."""
    import os

    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    out_file = tmp_path / "version.txt"
    reconcile_path = tmp_path / "reconcile.py"
    source_v1 = f'from pathlib import Path\ndef reconcile(context):\n    Path({str(out_file)!r}).write_text("v1")\n'
    source_v2 = f'from pathlib import Path\ndef reconcile(context):\n    Path({str(out_file)!r}).write_text("v2")\n'
    assert len(source_v1.encode("utf-8")) == len(source_v2.encode("utf-8"))

    reconcile_path.write_text(source_v1, encoding="utf-8")
    stat_before = reconcile_path.stat()

    runner = _sync_runner(tmp_path, {})
    descriptor = ExtensionReconcilerDescriptor(name="litestar", module_path=reconcile_path)
    runner.execute_reconciliation(Mock(), descriptor)
    assert out_file.read_text(encoding="utf-8") == "v1"

    reconcile_path.write_text(source_v2, encoding="utf-8")
    os.utime(reconcile_path, ns=(stat_before.st_atime_ns, stat_before.st_mtime_ns))

    runner.execute_reconciliation(Mock(), descriptor)
    assert out_file.read_text(encoding="utf-8") == "v2"


@pytest.mark.anyio
async def test_execute_reconciliation_resolves_project_root_and_honors_configured_root(tmp_path: Path) -> None:
    """Reconcilers resolve project_root via marker search or migration_config and restore sys.path."""
    import sys

    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.migrations.base import ExtensionReconcilerDescriptor
    from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

    auto_root = tmp_path / "auto_project"
    nested_dir = auto_root / "pkg" / "migrations"
    nested_dir.mkdir(parents=True)
    (auto_root / "pyproject.toml").write_text("[project]\nname = 'demo'\n", encoding="utf-8")
    (auto_root / "sibling_auto_helper.py").write_text("MARKER = 'auto_ok'\n", encoding="utf-8")
    auto_reconcile = nested_dir / "reconcile.py"
    auto_reconcile.write_text(
        """
import sys
import sibling_auto_helper

def reconcile(context):
    assert "None" not in sys.path
    assert sibling_auto_helper.MARKER == "auto_ok"
""",
        encoding="utf-8",
    )

    runner = _sync_runner(tmp_path, {})
    assert runner.project_root is None
    runner.execute_reconciliation(Mock(), ExtensionReconcilerDescriptor(name="litestar", module_path=auto_reconcile))
    assert str(auto_root) not in sys.path
    assert "None" not in sys.path
    sys.modules.pop("sibling_auto_helper", None)

    configured_root = tmp_path / "configured_root"
    configured_root.mkdir()
    (configured_root / "configured_helper.py").write_text("MARKER = 'configured_ok'\n", encoding="utf-8")
    separate_dir = tmp_path / "separate_reconcilers"
    separate_dir.mkdir()
    configured_reconcile = separate_dir / "reconcile.py"
    configured_reconcile.write_text(
        """
import sys
import configured_helper

def reconcile(context):
    assert "None" not in sys.path
    assert configured_helper.MARKER == "configured_ok"
""",
        encoding="utf-8",
    )

    sync_cfg = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path / "migs"), "project_root": str(configured_root)},
    )
    sync_cmds = SyncMigrationCommands(sync_cfg)
    assert sync_cmds.runner.project_root == configured_root
    with sync_cfg.provide_session() as sync_driver:
        sync_cmds.runner.execute_reconciliation(
            sync_driver, ExtensionReconcilerDescriptor(name="litestar", module_path=configured_reconcile)
        )
    assert str(configured_root) not in sys.path
    sys.modules.pop("configured_helper", None)

    async_cfg = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path / "migs"), "project_root": str(configured_root)},
    )
    async_cmds = AsyncMigrationCommands(async_cfg)
    assert async_cmds.runner.project_root == configured_root
    async with async_cfg.provide_session() as async_driver:
        await async_cmds.runner.execute_reconciliation(
            async_driver, ExtensionReconcilerDescriptor(name="litestar", module_path=configured_reconcile)
        )
    await async_cfg.close_pool()
    assert str(configured_root) not in sys.path
    sys.modules.pop("configured_helper", None)


@pytest.mark.anyio
async def test_execute_reconciliation_exception_wrapping_passthrough_and_cancellation(tmp_path: Path) -> None:
    """Non-MigrationError exceptions sanitize messages and chain __cause__; MigrationError and CancelledError behave per contract."""
    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    secret_path = tmp_path / "secret_err.py"
    secret_path.write_text('def reconcile(context):\n    raise ValueError("secret_dsn_password")\n', encoding="utf-8")

    guard_path = tmp_path / "guard_err.py"
    guard_path.write_text(
        'from sqlspec.exceptions import MigrationError\ndef reconcile(context):\n    raise MigrationError("explicit guard")\n',
        encoding="utf-8",
    )

    cancel_path = tmp_path / "cancel_err.py"
    cancel_path.write_text(
        "import asyncio\nasync def reconcile(context):\n    raise asyncio.CancelledError()\n", encoding="utf-8"
    )

    sync_runner = _sync_runner(tmp_path, {})
    with pytest.raises(MigrationError) as secret_exc:
        sync_runner.execute_reconciliation(Mock(), ExtensionReconcilerDescriptor(name="adk", module_path=secret_path))
    assert "adk" in str(secret_exc.value)
    assert str(secret_path) in str(secret_exc.value)
    assert "ValueError" in str(secret_exc.value)
    assert "secret_dsn_password" not in str(secret_exc.value)
    assert isinstance(secret_exc.value.__cause__, ValueError)

    with pytest.raises(MigrationError, match="explicit guard"):
        sync_runner.execute_reconciliation(Mock(), ExtensionReconcilerDescriptor(name="adk", module_path=guard_path))

    async_runner = _async_runner(tmp_path, {"default_schema": "app"})
    async_driver = AsyncMock()
    with pytest.raises(asyncio.CancelledError):
        await async_runner.execute_reconciliation(
            async_driver, ExtensionReconcilerDescriptor(name="adk", module_path=cancel_path)
        )
    assert async_driver.mock_calls == [
        call.begin(),
        call.set_migration_session_schema("app"),
        call.rollback(),
        call.reset_migration_session_schema(),
    ]


@pytest.mark.anyio
async def test_execute_reconciliation_transactional_call_order_and_failure_recovery(
    tmp_path: Path, runner_mode: _RunnerMode
) -> None:
    """Transactional reconcile commits before reset_schema on success and rolls back before reset_schema on failure."""
    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    ok_path = tmp_path / "ok_reconcile.py"
    ok_path.write_text('def reconcile(context):\n    context.driver.record_event("callback")\n', encoding="utf-8")
    fail_path = tmp_path / "fail_reconcile.py"
    fail_path.write_text(
        'def reconcile(context):\n    context.driver.record_event("callback")\n    raise RuntimeError("boom")\n',
        encoding="utf-8",
    )

    runner = runner_mode.runner(tmp_path, {"default_schema": "app"})
    events_ok: list[str] = []
    driver_ok = runner_mode.driver()
    driver_ok.begin.side_effect = lambda: events_ok.append("begin")
    driver_ok.set_migration_session_schema.side_effect = lambda s: events_ok.append(f"set_schema:{s}")
    driver_ok.record_event = Mock(side_effect=events_ok.append)
    driver_ok.commit.side_effect = lambda: events_ok.append("commit")
    driver_ok.reset_migration_session_schema.side_effect = lambda: events_ok.append("reset_schema")

    await runner_mode.call(
        runner.execute_reconciliation, driver_ok, ExtensionReconcilerDescriptor(name="litestar", module_path=ok_path)
    )
    assert events_ok == ["begin", "set_schema:app", "callback", "commit", "reset_schema"]
    driver_ok.rollback.assert_not_called()

    events_fail: list[str] = []
    driver_fail = runner_mode.driver()
    driver_fail.begin.side_effect = lambda: events_fail.append("begin")
    driver_fail.set_migration_session_schema.side_effect = lambda s: events_fail.append(f"set_schema:{s}")
    driver_fail.record_event = Mock(side_effect=events_fail.append)
    driver_fail.rollback.side_effect = lambda: events_fail.append("rollback")
    driver_fail.reset_migration_session_schema.side_effect = lambda: events_fail.append("reset_schema")

    with pytest.raises(MigrationError) as exc_info:
        await runner_mode.call(
            runner.execute_reconciliation,
            driver_fail,
            ExtensionReconcilerDescriptor(name="litestar", module_path=fail_path),
        )
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert events_fail == ["begin", "set_schema:app", "callback", "rollback", "reset_schema"]
    driver_fail.commit.assert_not_called()


@pytest.mark.anyio
async def test_execute_reconciliation_secondary_recovery_failure_preserves_primary_cause(
    tmp_path: Path, runner_mode: _RunnerMode
) -> None:
    """Rollback or schema-reset errors during recovery never mask the primary callback error."""
    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    fail_path = tmp_path / "primary_fail.py"
    fail_path.write_text('def reconcile(context):\n    raise ValueError("primary_err")\n', encoding="utf-8")

    runner = runner_mode.runner(tmp_path, {"default_schema": "app"})
    driver = runner_mode.driver()
    driver.rollback.side_effect = RuntimeError("conn lost")
    driver.reset_migration_session_schema.side_effect = RuntimeError("conn lost")

    with pytest.raises(MigrationError) as exc_info:
        await runner_mode.call(
            runner.execute_reconciliation, driver, ExtensionReconcilerDescriptor(name="events", module_path=fail_path)
        )
    assert isinstance(exc_info.value.__cause__, ValueError)


@pytest.mark.anyio
async def test_execute_reconciliation_rejects_unsupported_default_schema_before_begin(
    tmp_path: Path, runner_mode: _RunnerMode
) -> None:
    """Unsupported default_schema fails before driver.begin(), schema switching, or callback loading."""
    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    config = _RunnerConfig({"default_schema": "app"})
    config.supports_migration_schemas = False
    runner = runner_mode.runner(tmp_path, MigrationContext(config=config))
    driver = runner_mode.driver()
    missing_path = tmp_path / "does_not_exist.py"

    with pytest.raises(MigrationError, match="does not support migration default schemas"):
        await runner_mode.call(
            runner.execute_reconciliation,
            driver,
            ExtensionReconcilerDescriptor(name="litestar", module_path=missing_path),
        )
    driver.begin.assert_not_called()
    driver.set_migration_session_schema.assert_not_called()
    driver.set_migration_non_transactional_schema.assert_not_called()


@pytest.mark.anyio
async def test_execute_reconciliation_nontransactional_mock_and_duckdb(
    tmp_path: Path, runner_mode: _RunnerMode
) -> None:
    """Non-transactional mode skips begin/commit/rollback on stateless/DuckDB drivers while persisting DDL+DML."""
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    mock_path = tmp_path / "nontx_mock.py"
    mock_path.write_text('def reconcile(context):\n    context.driver.record_event("callback")\n', encoding="utf-8")

    runner = runner_mode.runner(tmp_path, {"transactional": False, "default_schema": "app"})
    events: list[str] = []
    driver = runner_mode.driver()
    driver.set_migration_non_transactional_schema.side_effect = lambda s: events.append(f"set_non_tx_schema:{s}")
    driver.record_event = Mock(side_effect=events.append)
    driver.reset_migration_session_schema.side_effect = lambda: events.append("reset_schema")

    await runner_mode.call(
        runner.execute_reconciliation, driver, ExtensionReconcilerDescriptor(name="litestar", module_path=mock_path)
    )
    assert events == ["set_non_tx_schema:app", "callback", "reset_schema"]
    driver.begin.assert_not_called()
    driver.commit.assert_not_called()
    driver.rollback.assert_not_called()

    if not runner_mode.is_async:
        duck_db_file = tmp_path / "reconcile_duck.ddb"
        duck_path = tmp_path / "duck_reconcile.py"
        duck_path.write_text(
            """
def reconcile(context):
    context.driver.execute("CREATE TABLE IF NOT EXISTS duck_items (id INTEGER, name VARCHAR)")
    context.driver.execute("INSERT INTO duck_items VALUES (1, 'persisted')")
""",
            encoding="utf-8",
        )
        duck_cfg = DuckDBConfig(
            connection_config={"database": str(duck_db_file)}, migration_config={"transactional": False}
        )
        duck_runner = SyncMigrationRunner(tmp_path, {}, MigrationContext.from_config(duck_cfg), {"events": {}})
        with duck_cfg.provide_session() as duck_driver:
            duck_runner.execute_reconciliation(
                duck_driver, ExtensionReconcilerDescriptor(name="events", module_path=duck_path)
            )
        with duck_cfg.provide_session() as duck_driver:
            rows = duck_driver.select("SELECT id, name FROM duck_items")
        duck_cfg.close_pool()
        assert rows == [{"id": 1, "name": "persisted"}]


@pytest.mark.anyio
async def test_execute_reconciliation_nontransactional_implicit_tx_and_psycopg_fallback(
    tmp_path: Path, runner_mode: _RunnerMode
) -> None:
    """Non-transactional mode commits active implicit DBAPI transactions and falls back to rollback+reset on INERROR."""
    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    ok_path = tmp_path / "implicit_ok.py"
    ok_path.write_text('def reconcile(context):\n    context.driver.record_event("callback")\n', encoding="utf-8")
    fail_path = tmp_path / "implicit_fail.py"
    fail_path.write_text(
        'def reconcile(context):\n    context.driver.record_event("callback")\n    raise RuntimeError("aborted")\n',
        encoding="utf-8",
    )

    runner_no_schema = runner_mode.runner(tmp_path, {"transactional": False})
    driver_implicit = runner_mode.driver()
    driver_implicit._connection_in_transaction = lambda: True
    driver_implicit.record_event = Mock()

    await runner_mode.call(
        runner_no_schema.execute_reconciliation,
        driver_implicit,
        ExtensionReconcilerDescriptor(name="adk", module_path=ok_path),
    )
    driver_implicit.begin.assert_not_called()
    driver_implicit.commit.assert_called_once()
    driver_implicit.rollback.assert_not_called()

    driver_implicit_fail = runner_mode.driver()
    driver_implicit_fail._connection_in_transaction = lambda: True
    driver_implicit_fail.record_event = Mock()
    with pytest.raises(MigrationError):
        await runner_mode.call(
            runner_no_schema.execute_reconciliation,
            driver_implicit_fail,
            ExtensionReconcilerDescriptor(name="adk", module_path=fail_path),
        )
    driver_implicit_fail.rollback.assert_called_once()

    runner_with_schema = runner_mode.runner(tmp_path, {"transactional": False, "default_schema": "app"})
    fallback_events: list[str] = []
    driver_psycopg = runner_mode.driver()
    driver_psycopg.set_migration_non_transactional_schema.side_effect = lambda s: fallback_events.append(
        f"set_non_tx_schema:{s}"
    )
    driver_psycopg.record_event = Mock(side_effect=fallback_events.append)
    reset_attempts = 0

    def _reset_side_effect() -> None:
        nonlocal reset_attempts
        reset_attempts += 1
        if reset_attempts == 1:
            fallback_events.append("reset_schema(fail)")
            msg = "current transaction is aborted"
            raise RuntimeError(msg)
        fallback_events.append("reset_schema(ok)")

    driver_psycopg.reset_migration_session_schema.side_effect = _reset_side_effect
    driver_psycopg.rollback.side_effect = lambda: fallback_events.append("rollback")

    with pytest.raises(MigrationError):
        await runner_mode.call(
            runner_with_schema.execute_reconciliation,
            driver_psycopg,
            ExtensionReconcilerDescriptor(name="adk", module_path=fail_path),
        )
    assert fallback_events == [
        "set_non_tx_schema:app",
        "callback",
        "reset_schema(fail)",
        "rollback",
        "reset_schema(ok)",
    ]


def test_execute_reconciliation_sqlite_atomic_rollback_and_zero_ledger_writes(tmp_path: Path) -> None:
    """Failed transactional reconcile rolls back atomically on SQLite and successful runs never create ddl_migrations."""
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.migrations.base import ExtensionReconcilerDescriptor

    db_path = tmp_path / "atomic_sqlite.db"
    cfg = SqliteConfig(connection_config={"database": str(db_path)})
    with cfg.provide_session() as driver:
        driver.execute("CREATE TABLE seed_rows (id INTEGER PRIMARY KEY, label TEXT)")
        driver.commit()

    fail_reconcile = tmp_path / "fail_sqlite.py"
    fail_reconcile.write_text(
        """
def reconcile(context):
    context.driver.execute("INSERT INTO seed_rows (id, label) VALUES (1, 'rolled_back')")
    raise RuntimeError("boom")
""",
        encoding="utf-8",
    )
    ok_reconcile = tmp_path / "ok_sqlite.py"
    ok_reconcile.write_text(
        """
def reconcile(context):
    context.driver.execute("CREATE TABLE ext_reconciled (id INTEGER PRIMARY KEY)")
""",
        encoding="utf-8",
    )

    runner = SyncMigrationRunner(tmp_path, {}, MigrationContext.from_config(cfg), {"litestar": {}})
    with cfg.provide_session() as driver, pytest.raises(MigrationError):
        runner.execute_reconciliation(
            driver, ExtensionReconcilerDescriptor(name="litestar", module_path=fail_reconcile)
        )

    with cfg.provide_session() as driver:
        assert driver.select("SELECT id FROM seed_rows") == []
        runner.execute_reconciliation(driver, ExtensionReconcilerDescriptor(name="litestar", module_path=ok_reconcile))

    with cfg.provide_session() as driver:
        tables = {row["name"] for row in driver.select("SELECT name FROM sqlite_master WHERE type = 'table'")}

    cfg.close_pool()
    assert "ext_reconciled" in tables
    assert "ddl_migrations" not in tables


def test_execute_reconciliation_records_observability_metrics(tmp_path: Path) -> None:
    """ObservabilityRuntime records invocations, applied, duration_ms, and errors for reconcile operations."""
    from sqlspec.migrations.base import ExtensionReconcilerDescriptor
    from sqlspec.observability import ObservabilityRuntime

    ok_path = tmp_path / "obs_ok.py"
    ok_path.write_text("def reconcile(context):\n    return None\n", encoding="utf-8")
    fail_path = tmp_path / "obs_fail.py"
    fail_path.write_text('def reconcile(context):\n    raise RuntimeError("fail")\n', encoding="utf-8")

    runtime = ObservabilityRuntime(config_name="test_cfg")
    runner = SyncMigrationRunner(
        tmp_path, {}, MigrationContext(config=_RunnerConfig({})), {"litestar": {}}, runtime=runtime
    )

    runner.execute_reconciliation(Mock(), ExtensionReconcilerDescriptor(name="litestar", module_path=ok_path))
    with pytest.raises(MigrationError):
        runner.execute_reconciliation(Mock(), ExtensionReconcilerDescriptor(name="litestar", module_path=fail_path))

    snapshot = runtime.metrics_snapshot()
    assert snapshot["test_cfg.migrations.reconcile.invocations"] == 2.0
    assert snapshot["test_cfg.migrations.reconcile.applied"] == 1.0
    assert snapshot["test_cfg.migrations.reconcile.errors"] == 1.0
    assert "test_cfg.migrations.reconcile.duration_ms" in snapshot
