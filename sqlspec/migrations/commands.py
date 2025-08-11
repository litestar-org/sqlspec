"""Migration command implementations for SQLSpec.

This module provides the main command interface for database migrations.
"""

from typing import TYPE_CHECKING, Any, Union, cast

from rich.console import Console
from rich.table import Table

from sqlspec._sql import sql
from sqlspec.migrations.base import BaseMigrationCommands
from sqlspec.migrations.runner import AsyncMigrationRunner, SyncMigrationRunner
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker
from sqlspec.migrations.utils import create_migration_file
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import await_

if TYPE_CHECKING:
    from sqlspec.config import AsyncConfigT, SyncConfigT

__all__ = ("AsyncMigrationCommands", "MigrationCommands", "SyncMigrationCommands")

logger = get_logger("migrations.commands")
console = Console()


class SyncMigrationCommands(BaseMigrationCommands["SyncConfigT", Any]):
    """SQLSpec native migration commands."""

    def __init__(self, config: "SyncConfigT") -> None:
        """Initialize migration commands.

        Args:
            config: The SQLSpec configuration.
        """
        super().__init__(config)
        self.tracker = SyncMigrationTracker(self.version_table)
        self.runner = SyncMigrationRunner(self.migrations_path)

    def init(self, directory: str, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in.
            package: Whether to create __init__.py file.
        """
        self.init_directory(directory, package)

    def current(self, verbose: bool = False) -> None:
        """Show current migration version.

        Args:
            verbose: Whether to show detailed migration history.
        """
        with self.config.provide_session() as driver:
            self.tracker.ensure_tracking_table(driver)

            current = self.tracker.get_current_version(driver)
            if not current:
                console.print("[yellow]No migrations applied yet[/]")
                return

            console.print(f"[green]Current version:[/] {current}")

            if verbose:
                applied = self.tracker.get_applied_migrations(driver)

                table = Table(title="Applied Migrations")
                table.add_column("Version", style="cyan")
                table.add_column("Description")
                table.add_column("Applied At")
                table.add_column("Time (ms)", justify="right")
                table.add_column("Applied By")

                for migration in applied:
                    table.add_row(
                        migration["version_num"],
                        migration.get("description", ""),
                        str(migration.get("applied_at", "")),
                        str(migration.get("execution_time_ms", "")),
                        migration.get("applied_by", ""),
                    )

                console.print(table)

    def upgrade(self, revision: str = "head") -> None:
        """Upgrade to a target revision.

        Args:
            revision: Target revision or "head" for latest.
        """
        with self.config.provide_session() as driver:
            self.tracker.ensure_tracking_table(driver)

            current = self.tracker.get_current_version(driver)
            all_migrations = self.runner.get_migration_files()
            pending = []
            for version, file_path in all_migrations:
                if (current is None or version > current) and (revision == "head" or version <= revision):
                    pending.append((version, file_path))

            if not pending:
                console.print("[green]Already at latest version[/]")
                return

            console.print(f"[yellow]Found {len(pending)} pending migrations[/]")

            for version, file_path in pending:
                migration = self.runner.load_migration(file_path)

                console.print(f"\n[cyan]Applying {version}:[/] {migration['description']}")

                try:
                    _, execution_time = self.runner.execute_upgrade(driver, migration)
                    self.tracker.record_migration(
                        driver, migration["version"], migration["description"], execution_time, migration["checksum"]
                    )
                    console.print(f"[green]✓ Applied in {execution_time}ms[/]")

                except Exception as e:
                    console.print(f"[red]✗ Failed: {e}[/]")
                    raise

    def downgrade(self, revision: str = "-1") -> None:
        """Downgrade to a target revision.

        Args:
            revision: Target revision or "-1" for one step back.
        """
        with self.config.provide_session() as driver:
            self.tracker.ensure_tracking_table(driver)
            applied = self.tracker.get_applied_migrations(driver)
            if not applied:
                console.print("[yellow]No migrations to downgrade[/]")
                return
            to_revert = []
            if revision == "-1":
                to_revert = [applied[-1]]
            else:
                for migration in reversed(applied):
                    if migration["version_num"] > revision:
                        to_revert.append(migration)

            if not to_revert:
                console.print("[yellow]Nothing to downgrade[/]")
                return

            console.print(f"[yellow]Reverting {len(to_revert)} migrations[/]")
            all_files = dict(self.runner.get_migration_files())
            for migration_record in to_revert:
                version = migration_record["version_num"]
                if version not in all_files:
                    console.print(f"[red]Migration file not found for {version}[/]")
                    continue
                migration = self.runner.load_migration(all_files[version])
                console.print(f"\n[cyan]Reverting {version}:[/] {migration['description']}")
                try:
                    _, execution_time = self.runner.execute_downgrade(driver, migration)
                    self.tracker.remove_migration(driver, version)
                    console.print(f"[green]✓ Reverted in {execution_time}ms[/]")
                except Exception as e:
                    console.print(f"[red]✗ Failed: {e}[/]")
                    raise

    def stamp(self, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        with self.config.provide_session() as driver:
            self.tracker.ensure_tracking_table(driver)
            all_migrations = dict(self.runner.get_migration_files())
            if revision not in all_migrations:
                console.print(f"[red]Unknown revision: {revision}[/]")
                return
            clear_sql = sql.delete().from_(self.tracker.version_table)
            driver.execute(clear_sql)
            self.tracker.record_migration(driver, revision, f"Stamped to {revision}", 0, "manual-stamp")
            console.print(f"[green]Database stamped at revision {revision}[/]")

    def revision(self, message: str, file_type: str = "sql") -> None:
        """Create a new migration file.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
        """
        existing = self.runner.get_migration_files()
        next_num = int(existing[-1][0]) + 1 if existing else 1
        next_version = str(next_num).zfill(4)
        file_path = create_migration_file(self.migrations_path, next_version, message, file_type)
        console.print(f"[green]Created migration:[/] {file_path}")


class AsyncMigrationCommands(BaseMigrationCommands["AsyncConfigT", Any]):
    """SQLSpec native migration commands."""

    def __init__(self, sqlspec_config: "AsyncConfigT") -> None:
        """Initialize migration commands.

        Args:
            sqlspec_config: The SQLSpec configuration.
        """
        super().__init__(sqlspec_config)
        self.tracker = AsyncMigrationTracker(self.version_table)
        self.runner = AsyncMigrationRunner(self.migrations_path)

    async def init(self, directory: str, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory path for migrations.
            package: Whether to create __init__.py in the directory.
        """
        self.init_directory(directory, package)

    async def current(self, verbose: bool = False) -> None:
        """Show current migration version.

        Args:
            verbose: Whether to show detailed migration history.
        """
        async with self.config.provide_session() as driver:
            await self.tracker.ensure_tracking_table(driver)

            current = await self.tracker.get_current_version(driver)
            if not current:
                console.print("[yellow]No migrations applied yet[/]")
                return

            console.print(f"[green]Current version:[/] {current}")
            if verbose:
                applied = await self.tracker.get_applied_migrations(driver)
                table = Table(title="Applied Migrations")
                table.add_column("Version", style="cyan")
                table.add_column("Description")
                table.add_column("Applied At")
                table.add_column("Time (ms)", justify="right")
                table.add_column("Applied By")
                for migration in applied:
                    table.add_row(
                        migration["version_num"],
                        migration.get("description", ""),
                        str(migration.get("applied_at", "")),
                        str(migration.get("execution_time_ms", "")),
                        migration.get("applied_by", ""),
                    )
                console.print(table)

    async def upgrade(self, revision: str = "head") -> None:
        """Upgrade to a target revision.

        Args:
            revision: Target revision or "head" for latest.
        """
        async with self.config.provide_session() as driver:
            await self.tracker.ensure_tracking_table(driver)

            current = await self.tracker.get_current_version(driver)
            all_migrations = await self.runner.get_migration_files()
            pending = []
            for version, file_path in all_migrations:
                if (current is None or version > current) and (revision == "head" or version <= revision):
                    pending.append((version, file_path))
            if not pending:
                console.print("[green]Already at latest version[/]")
                return
            console.print(f"[yellow]Found {len(pending)} pending migrations[/]")
            for version, file_path in pending:
                migration = await self.runner.load_migration(file_path)
                console.print(f"\n[cyan]Applying {version}:[/] {migration['description']}")
                try:
                    _, execution_time = await self.runner.execute_upgrade(driver, migration)
                    await self.tracker.record_migration(
                        driver, migration["version"], migration["description"], execution_time, migration["checksum"]
                    )
                    console.print(f"[green]✓ Applied in {execution_time}ms[/]")
                except Exception as e:
                    console.print(f"[red]✗ Failed: {e}[/]")
                    raise

    async def downgrade(self, revision: str = "-1") -> None:
        """Downgrade to a target revision.

        Args:
            revision: Target revision or "-1" for one step back.
        """
        async with self.config.provide_session() as driver:
            await self.tracker.ensure_tracking_table(driver)

            applied = await self.tracker.get_applied_migrations(driver)
            if not applied:
                console.print("[yellow]No migrations to downgrade[/]")
                return
            to_revert = []
            if revision == "-1":
                to_revert = [applied[-1]]
            else:
                for migration in reversed(applied):
                    if migration["version_num"] > revision:
                        to_revert.append(migration)
            if not to_revert:
                console.print("[yellow]Nothing to downgrade[/]")
                return

            console.print(f"[yellow]Reverting {len(to_revert)} migrations[/]")
            all_files = dict(await self.runner.get_migration_files())
            for migration_record in to_revert:
                version = migration_record["version_num"]
                if version not in all_files:
                    console.print(f"[red]Migration file not found for {version}[/]")
                    continue

                migration = await self.runner.load_migration(all_files[version])
                console.print(f"\n[cyan]Reverting {version}:[/] {migration['description']}")

                try:
                    _, execution_time = await self.runner.execute_downgrade(driver, migration)
                    await self.tracker.remove_migration(driver, version)
                    console.print(f"[green]✓ Reverted in {execution_time}ms[/]")
                except Exception as e:
                    console.print(f"[red]✗ Failed: {e}[/]")
                    raise

    async def stamp(self, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        async with self.config.provide_session() as driver:
            await self.tracker.ensure_tracking_table(driver)

            all_migrations = dict(await self.runner.get_migration_files())
            if revision not in all_migrations:
                console.print(f"[red]Unknown revision: {revision}[/]")
                return

            clear_sql = sql.delete().from_(self.tracker.version_table)
            await driver.execute(clear_sql)
            await self.tracker.record_migration(driver, revision, f"Stamped to {revision}", 0, "manual-stamp")
            console.print(f"[green]Database stamped at revision {revision}[/]")

    async def revision(self, message: str, file_type: str = "sql") -> None:
        """Create a new migration file.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
        """
        existing = await self.runner.get_migration_files()
        next_num = int(existing[-1][0]) + 1 if existing else 1
        next_version = str(next_num).zfill(4)
        file_path = create_migration_file(self.migrations_path, next_version, message, file_type)
        console.print(f"[green]Created migration:[/] {file_path}")


class MigrationCommands:
    """Unified migration commands that adapt to sync/async configs."""

    def __init__(self, config: "Union[SyncConfigT, AsyncConfigT]") -> None:
        """Initialize migration commands with sync/async implementation.

        Args:
            config: The SQLSpec configuration.
        """
        if config.is_async:
            self._impl: Union[AsyncMigrationCommands[Any], SyncMigrationCommands[Any]] = AsyncMigrationCommands(
                cast("AsyncConfigT", config)
            )
        else:
            self._impl = SyncMigrationCommands(cast("SyncConfigT", config))
        self._is_async = config.is_async

    def init(self, directory: str, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in.
            package: Whether to create __init__.py file.
        """
        if self._is_async:
            await_(cast("AsyncMigrationCommands[Any]", self._impl).init)(directory, package=package)
        else:
            cast("SyncMigrationCommands[Any]", self._impl).init(directory, package=package)

    def current(self, verbose: bool = False) -> None:
        """Show current migration version.

        Args:
            verbose: Whether to show detailed migration history.
        """
        if self._is_async:
            await_(cast("AsyncMigrationCommands[Any]", self._impl).current, raise_sync_error=False)(verbose=verbose)
        else:
            cast("SyncMigrationCommands[Any]", self._impl).current(verbose=verbose)

    def upgrade(self, revision: str = "head") -> None:
        """Upgrade to a target revision.

        Args:
            revision: Target revision or "head" for latest.
        """
        if self._is_async:
            await_(cast("AsyncMigrationCommands[Any]", self._impl).upgrade, raise_sync_error=False)(revision=revision)
        else:
            cast("SyncMigrationCommands[Any]", self._impl).upgrade(revision=revision)

    def downgrade(self, revision: str = "-1") -> None:
        """Downgrade to a target revision.

        Args:
            revision: Target revision or "-1" for one step back.
        """
        if self._is_async:
            await_(cast("AsyncMigrationCommands[Any]", self._impl).downgrade, raise_sync_error=False)(revision=revision)
        else:
            cast("SyncMigrationCommands[Any]", self._impl).downgrade(revision=revision)

    def stamp(self, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        if self._is_async:
            await_(cast("AsyncMigrationCommands[Any]", self._impl).stamp, raise_sync_error=False)(revision)
        else:
            cast("SyncMigrationCommands[Any]", self._impl).stamp(revision)

    def revision(self, message: str, file_type: str = "sql") -> None:
        """Create a new migration file.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
        """
        if self._is_async:
            await_(cast("AsyncMigrationCommands[Any]", self._impl).revision, raise_sync_error=False)(message, file_type)
        else:
            cast("SyncMigrationCommands[Any]", self._impl).revision(message, file_type)
