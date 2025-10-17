"""Migration command implementations for SQLSpec.

This module provides the main command interface for database migrations.
"""

from typing import TYPE_CHECKING, Any, cast

from rich.console import Console
from rich.table import Table

from sqlspec.builder import sql
from sqlspec.migrations.base import BaseMigrationCommands
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.fix import MigrationFixer
from sqlspec.migrations.runner import AsyncMigrationRunner, SyncMigrationRunner
from sqlspec.migrations.utils import create_migration_file
from sqlspec.migrations.validation import validate_migration_order
from sqlspec.utils.logging import get_logger
from sqlspec.utils.version import generate_conversion_map, generate_timestamp_version

if TYPE_CHECKING:
    from sqlspec.config import AsyncConfigT, SyncConfigT

__all__ = ("AsyncMigrationCommands", "SyncMigrationCommands", "create_migration_commands")

logger = get_logger("migrations.commands")
console = Console()


class SyncMigrationCommands(BaseMigrationCommands["SyncConfigT", Any]):
    """Synchronous migration commands."""

    def __init__(self, config: "SyncConfigT") -> None:
        """Initialize migration commands.

        Args:
            config: The SQLSpec configuration.
        """
        super().__init__(config)
        self.tracker = config.migration_tracker_type(self.version_table)

        # Create context with extension configurations
        context = MigrationContext.from_config(config)
        context.extension_config = self.extension_configs

        self.runner = SyncMigrationRunner(
            self.migrations_path, self._discover_extension_migrations(), context, self.extension_configs
        )

    def init(self, directory: str, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in.
            package: Whether to create __init__.py file.
        """
        self.init_directory(directory, package)

    def current(self, verbose: bool = False) -> "str | None":
        """Show current migration version.

        Args:
            verbose: Whether to show detailed migration history.

        Returns:
            The current migration version or None if no migrations applied.
        """
        with self.config.provide_session() as driver:
            self.tracker.ensure_tracking_table(driver)

            current = self.tracker.get_current_version(driver)
            if not current:
                console.print("[yellow]No migrations applied yet[/]")
                return None

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

            return cast("str | None", current)

    def _synchronize_version_records(self, driver: Any) -> int:
        """Synchronize database version records with migration files.

        Auto-updates DB tracking when migrations have been renamed by fix command.
        This allows developers to just run upgrade after pulling changes without
        manually running fix.

        Validates checksums match before updating to prevent incorrect matches.

        Args:
            driver: Database driver instance.

        Returns:
            Number of version records updated.
        """
        all_migrations = self.runner.get_migration_files()
        applied_migrations = self.tracker.get_applied_migrations(driver)
        applied_map = {m["version_num"]: m for m in applied_migrations}

        conversion_map = generate_conversion_map(all_migrations)

        updated_count = 0
        for old_version, new_version in conversion_map.items():
            if old_version in applied_map and new_version not in applied_map:
                applied_checksum = applied_map[old_version]["checksum"]

                file_path = next((path for v, path in all_migrations if v == new_version), None)
                if file_path:
                    migration = self.runner.load_migration(file_path, new_version)
                    if migration["checksum"] == applied_checksum:
                        self.tracker.update_version_record(driver, old_version, new_version)
                        console.print(f"  [dim]Reconciled version:[/] {old_version} → {new_version}")
                        updated_count += 1
                    else:
                        console.print(
                            f"  [yellow]Warning: Checksum mismatch for {old_version} → {new_version}, skipping auto-sync[/]"
                        )

        if updated_count > 0:
            console.print(f"[cyan]Reconciled {updated_count} version record(s)[/]")

        return updated_count

    def upgrade(
        self, revision: str = "head", allow_missing: bool = False, auto_sync: bool = True, dry_run: bool = False
    ) -> None:
        """Upgrade to a target revision.

        Validates migration order and warns if out-of-order migrations are detected.
        Out-of-order migrations can occur when branches merge in different orders
        across environments.

        Args:
            revision: Target revision or "head" for latest.
            allow_missing: If True, allow out-of-order migrations even in strict mode.
                Defaults to False.
            auto_sync: If True, automatically reconcile renamed migrations in database.
                Defaults to True. Can be disabled via --no-auto-sync flag.
            dry_run: If True, show what would be done without making changes.
        """
        if dry_run:
            console.print("[bold yellow]DRY RUN MODE:[/] No database changes will be applied\n")

        with self.config.provide_session() as driver:
            self.tracker.ensure_tracking_table(driver)

            if auto_sync:
                migration_config = getattr(self.config, "migration_config", {}) or {}
                config_auto_sync = migration_config.get("auto_sync", True)
                if config_auto_sync:
                    self._synchronize_version_records(driver)

            applied_migrations = self.tracker.get_applied_migrations(driver)
            applied_versions = [m["version_num"] for m in applied_migrations]
            applied_set = set(applied_versions)

            all_migrations = self.runner.get_migration_files()
            pending = []
            for version, file_path in all_migrations:
                if version not in applied_set and (revision == "head" or version <= revision):
                    pending.append((version, file_path))

            if not pending:
                if not all_migrations:
                    console.print(
                        "[yellow]No migrations found. Create your first migration with 'sqlspec create-migration'.[/]"
                    )
                else:
                    console.print("[green]Already at latest version[/]")
                return
            pending_versions = [v for v, _ in pending]

            migration_config = getattr(self.config, "migration_config", {}) or {}
            strict_ordering = migration_config.get("strict_ordering", False) and not allow_missing

            validate_migration_order(pending_versions, applied_versions, strict_ordering)

            console.print(f"[yellow]Found {len(pending)} pending migrations[/]")

            for version, file_path in pending:
                migration = self.runner.load_migration(file_path, version)

                action_verb = "Would apply" if dry_run else "Applying"
                console.print(f"\n[cyan]{action_verb} {version}:[/] {migration['description']}")

                if dry_run:
                    console.print(f"[dim]Migration file: {file_path}[/]")
                    continue

                try:
                    _, execution_time = self.runner.execute_upgrade(driver, migration)
                    self.tracker.record_migration(
                        driver, migration["version"], migration["description"], execution_time, migration["checksum"]
                    )
                    console.print(f"[green]✓ Applied in {execution_time}ms[/]")

                except Exception as e:
                    console.print(f"[red]✗ Failed: {e}[/]")
                    return

            if dry_run:
                console.print("\n[bold yellow]Dry run complete.[/] No changes were made to the database.")

    def downgrade(self, revision: str = "-1", *, dry_run: bool = False) -> None:
        """Downgrade to a target revision.

        Args:
            revision: Target revision or "-1" for one step back.
            dry_run: If True, show what would be done without making changes.
        """
        if dry_run:
            console.print("[bold yellow]DRY RUN MODE:[/] No database changes will be applied\n")

        with self.config.provide_session() as driver:
            self.tracker.ensure_tracking_table(driver)
            applied = self.tracker.get_applied_migrations(driver)
            if not applied:
                console.print("[yellow]No migrations to downgrade[/]")
                return
            to_revert = []
            if revision == "-1":
                to_revert = [applied[-1]]
            elif revision == "base":
                to_revert = list(reversed(applied))
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
                migration = self.runner.load_migration(all_files[version], version)

                action_verb = "Would revert" if dry_run else "Reverting"
                console.print(f"\n[cyan]{action_verb} {version}:[/] {migration['description']}")

                if dry_run:
                    console.print(f"[dim]Migration file: {all_files[version]}[/]")
                    continue

                try:
                    _, execution_time = self.runner.execute_downgrade(driver, migration)
                    self.tracker.remove_migration(driver, version)
                    console.print(f"[green]✓ Reverted in {execution_time}ms[/]")
                except Exception as e:
                    console.print(f"[red]✗ Failed: {e}[/]")
                    return

            if dry_run:
                console.print("\n[bold yellow]Dry run complete.[/] No changes were made to the database.")

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
        """Create a new migration file with timestamp-based versioning.

        Generates a unique timestamp version (YYYYMMDDHHmmss format) to avoid
        conflicts when multiple developers create migrations concurrently.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
        """
        version = generate_timestamp_version()
        file_path = create_migration_file(self.migrations_path, version, message, file_type)
        console.print(f"[green]Created migration:[/] {file_path}")

    def fix(self, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format.

        Implements hybrid versioning workflow where development uses timestamps
        and production uses sequential numbers. Creates backup before changes
        and provides rollback on errors.

        Args:
            dry_run: Preview changes without applying.
            update_database: Update migration records in database.
            yes: Skip confirmation prompt.

        Examples:
            >>> commands.fix(dry_run=True)  # Preview only
            >>> commands.fix(yes=True)  # Auto-approve
            >>> commands.fix(update_database=False)  # Files only
        """
        all_migrations = self.runner.get_migration_files()

        conversion_map = generate_conversion_map(all_migrations)

        if not conversion_map:
            console.print("[yellow]No timestamp migrations found - nothing to convert[/]")
            return

        fixer = MigrationFixer(self.migrations_path)
        renames = fixer.plan_renames(conversion_map)

        table = Table(title="Migration Conversions")
        table.add_column("Current Version", style="cyan")
        table.add_column("New Version", style="green")
        table.add_column("File")

        for rename in renames:
            table.add_row(rename.old_version, rename.new_version, rename.old_path.name)

        console.print(table)
        console.print(f"\n[yellow]{len(renames)} migrations will be converted[/]")

        if dry_run:
            console.print("[yellow][Preview Mode - No changes made][/]")
            return

        if not yes:
            response = input("\nProceed with conversion? [y/N]: ")
            if response.lower() != "y":
                console.print("[yellow]Conversion cancelled[/]")
                return

        try:
            backup_path = fixer.create_backup()
            console.print(f"[green]✓ Created backup in {backup_path.name}[/]")

            fixer.apply_renames(renames)
            for rename in renames:
                console.print(f"[green]✓ Renamed {rename.old_path.name} → {rename.new_path.name}[/]")

            if update_database:
                with self.config.provide_session() as driver:
                    self.tracker.ensure_tracking_table(driver)
                    applied_migrations = self.tracker.get_applied_migrations(driver)
                    applied_versions = {m["version_num"] for m in applied_migrations}

                    updated_count = 0
                    for old_version, new_version in conversion_map.items():
                        if old_version in applied_versions:
                            self.tracker.update_version_record(driver, old_version, new_version)
                            updated_count += 1

                    if updated_count > 0:
                        console.print(f"[green]✓ Updated {updated_count} database records[/]")
                    else:
                        console.print("[yellow]No database records to update[/]")

            fixer.cleanup()
            console.print("[green]✓ Conversion complete![/]")

        except Exception as e:
            logger.exception("Fix command failed")
            console.print(f"[red]✗ Error: {e}[/]")
            fixer.rollback()
            console.print("[yellow]Restored files from backup[/]")
            raise


class AsyncMigrationCommands(BaseMigrationCommands["AsyncConfigT", Any]):
    """Asynchronous migration commands."""

    def __init__(self, config: "AsyncConfigT") -> None:
        """Initialize migration commands.

        Args:
            config: The SQLSpec configuration.
        """
        super().__init__(config)
        self.tracker = config.migration_tracker_type(self.version_table)

        # Create context with extension configurations
        context = MigrationContext.from_config(config)
        context.extension_config = self.extension_configs

        self.runner = AsyncMigrationRunner(
            self.migrations_path, self._discover_extension_migrations(), context, self.extension_configs
        )

    async def init(self, directory: str, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory path for migrations.
            package: Whether to create __init__.py in the directory.
        """
        self.init_directory(directory, package)

    async def current(self, verbose: bool = False) -> "str | None":
        """Show current migration version.

        Args:
            verbose: Whether to show detailed migration history.

        Returns:
            The current migration version or None if no migrations applied.
        """
        async with self.config.provide_session() as driver:
            await self.tracker.ensure_tracking_table(driver)

            current = await self.tracker.get_current_version(driver)
            if not current:
                console.print("[yellow]No migrations applied yet[/]")
                return None

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

            return cast("str | None", current)

    async def _synchronize_version_records(self, driver: Any) -> int:
        """Synchronize database version records with migration files.

        Auto-updates DB tracking when migrations have been renamed by fix command.
        This allows developers to just run upgrade after pulling changes without
        manually running fix.

        Validates checksums match before updating to prevent incorrect matches.

        Args:
            driver: Database driver instance.

        Returns:
            Number of version records updated.
        """
        all_migrations = await self.runner.get_migration_files()
        applied_migrations = await self.tracker.get_applied_migrations(driver)
        applied_map = {m["version_num"]: m for m in applied_migrations}

        conversion_map = generate_conversion_map(all_migrations)

        updated_count = 0
        for old_version, new_version in conversion_map.items():
            if old_version in applied_map and new_version not in applied_map:
                applied_checksum = applied_map[old_version]["checksum"]

                file_path = next((path for v, path in all_migrations if v == new_version), None)
                if file_path:
                    migration = await self.runner.load_migration(file_path, new_version)
                    if migration["checksum"] == applied_checksum:
                        await self.tracker.update_version_record(driver, old_version, new_version)
                        console.print(f"  [dim]Reconciled version:[/] {old_version} → {new_version}")
                        updated_count += 1
                    else:
                        console.print(
                            f"  [yellow]Warning: Checksum mismatch for {old_version} → {new_version}, skipping auto-sync[/]"
                        )

        if updated_count > 0:
            console.print(f"[cyan]Reconciled {updated_count} version record(s)[/]")

        return updated_count

    async def upgrade(
        self, revision: str = "head", allow_missing: bool = False, auto_sync: bool = True, dry_run: bool = False
    ) -> None:
        """Upgrade to a target revision.

        Validates migration order and warns if out-of-order migrations are detected.
        Out-of-order migrations can occur when branches merge in different orders
        across environments.

        Args:
            revision: Target revision or "head" for latest.
            allow_missing: If True, allow out-of-order migrations even in strict mode.
                Defaults to False.
            auto_sync: If True, automatically reconcile renamed migrations in database.
                Defaults to True. Can be disabled via --no-auto-sync flag.
            dry_run: If True, show what would be done without making changes.
        """
        if dry_run:
            console.print("[bold yellow]DRY RUN MODE:[/] No database changes will be applied\n")

        async with self.config.provide_session() as driver:
            await self.tracker.ensure_tracking_table(driver)

            if auto_sync:
                migration_config = getattr(self.config, "migration_config", {}) or {}
                config_auto_sync = migration_config.get("auto_sync", True)
                if config_auto_sync:
                    await self._synchronize_version_records(driver)

            applied_migrations = await self.tracker.get_applied_migrations(driver)
            applied_versions = [m["version_num"] for m in applied_migrations]
            applied_set = set(applied_versions)

            all_migrations = await self.runner.get_migration_files()
            pending = []
            for version, file_path in all_migrations:
                if version not in applied_set and (revision == "head" or version <= revision):
                    pending.append((version, file_path))
            if not pending:
                if not all_migrations:
                    console.print(
                        "[yellow]No migrations found. Create your first migration with 'sqlspec create-migration'.[/]"
                    )
                else:
                    console.print("[green]Already at latest version[/]")
                return
            pending_versions = [v for v, _ in pending]

            migration_config = getattr(self.config, "migration_config", {}) or {}
            strict_ordering = migration_config.get("strict_ordering", False) and not allow_missing

            validate_migration_order(pending_versions, applied_versions, strict_ordering)

            console.print(f"[yellow]Found {len(pending)} pending migrations[/]")
            for version, file_path in pending:
                migration = await self.runner.load_migration(file_path, version)

                action_verb = "Would apply" if dry_run else "Applying"
                console.print(f"\n[cyan]{action_verb} {version}:[/] {migration['description']}")

                if dry_run:
                    console.print(f"[dim]Migration file: {file_path}[/]")
                    continue

                try:
                    _, execution_time = await self.runner.execute_upgrade(driver, migration)
                    await self.tracker.record_migration(
                        driver, migration["version"], migration["description"], execution_time, migration["checksum"]
                    )
                    console.print(f"[green]✓ Applied in {execution_time}ms[/]")
                except Exception as e:
                    console.print(f"[red]✗ Failed: {e}[/]")
                    return

            if dry_run:
                console.print("\n[bold yellow]Dry run complete.[/] No changes were made to the database.")

    async def downgrade(self, revision: str = "-1", *, dry_run: bool = False) -> None:
        """Downgrade to a target revision.

        Args:
            revision: Target revision or "-1" for one step back.
            dry_run: If True, show what would be done without making changes.
        """
        if dry_run:
            console.print("[bold yellow]DRY RUN MODE:[/] No database changes will be applied\n")

        async with self.config.provide_session() as driver:
            await self.tracker.ensure_tracking_table(driver)

            applied = await self.tracker.get_applied_migrations(driver)
            if not applied:
                console.print("[yellow]No migrations to downgrade[/]")
                return
            to_revert = []
            if revision == "-1":
                to_revert = [applied[-1]]
            elif revision == "base":
                to_revert = list(reversed(applied))
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

                migration = await self.runner.load_migration(all_files[version], version)

                action_verb = "Would revert" if dry_run else "Reverting"
                console.print(f"\n[cyan]{action_verb} {version}:[/] {migration['description']}")

                if dry_run:
                    console.print(f"[dim]Migration file: {all_files[version]}[/]")
                    continue

                try:
                    _, execution_time = await self.runner.execute_downgrade(driver, migration)
                    await self.tracker.remove_migration(driver, version)
                    console.print(f"[green]✓ Reverted in {execution_time}ms[/]")
                except Exception as e:
                    console.print(f"[red]✗ Failed: {e}[/]")
                    return

            if dry_run:
                console.print("\n[bold yellow]Dry run complete.[/] No changes were made to the database.")

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
        """Create a new migration file with timestamp-based versioning.

        Generates a unique timestamp version (YYYYMMDDHHmmss format) to avoid
        conflicts when multiple developers create migrations concurrently.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
        """
        version = generate_timestamp_version()
        file_path = create_migration_file(self.migrations_path, version, message, file_type)
        console.print(f"[green]Created migration:[/] {file_path}")

    async def fix(self, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format.

        Implements hybrid versioning workflow where development uses timestamps
        and production uses sequential numbers. Creates backup before changes
        and provides rollback on errors.

        Args:
            dry_run: Preview changes without applying.
            update_database: Update migration records in database.
            yes: Skip confirmation prompt.

        Examples:
            >>> await commands.fix(dry_run=True)  # Preview only
            >>> await commands.fix(yes=True)  # Auto-approve
            >>> await commands.fix(update_database=False)  # Files only
        """
        all_migrations = await self.runner.get_migration_files()

        conversion_map = generate_conversion_map(all_migrations)

        if not conversion_map:
            console.print("[yellow]No timestamp migrations found - nothing to convert[/]")
            return

        fixer = MigrationFixer(self.migrations_path)
        renames = fixer.plan_renames(conversion_map)

        table = Table(title="Migration Conversions")
        table.add_column("Current Version", style="cyan")
        table.add_column("New Version", style="green")
        table.add_column("File")

        for rename in renames:
            table.add_row(rename.old_version, rename.new_version, rename.old_path.name)

        console.print(table)
        console.print(f"\n[yellow]{len(renames)} migrations will be converted[/]")

        if dry_run:
            console.print("[yellow][Preview Mode - No changes made][/]")
            return

        if not yes:
            response = input("\nProceed with conversion? [y/N]: ")
            if response.lower() != "y":
                console.print("[yellow]Conversion cancelled[/]")
                return

        try:
            backup_path = fixer.create_backup()
            console.print(f"[green]✓ Created backup in {backup_path.name}[/]")

            fixer.apply_renames(renames)
            for rename in renames:
                console.print(f"[green]✓ Renamed {rename.old_path.name} → {rename.new_path.name}[/]")

            if update_database:
                async with self.config.provide_session() as driver:
                    await self.tracker.ensure_tracking_table(driver)
                    applied_migrations = await self.tracker.get_applied_migrations(driver)
                    applied_versions = {m["version_num"] for m in applied_migrations}

                    updated_count = 0
                    for old_version, new_version in conversion_map.items():
                        if old_version in applied_versions:
                            await self.tracker.update_version_record(driver, old_version, new_version)
                            updated_count += 1

                    if updated_count > 0:
                        console.print(f"[green]✓ Updated {updated_count} database records[/]")
                    else:
                        console.print("[yellow]No database records to update[/]")

            fixer.cleanup()
            console.print("[green]✓ Conversion complete![/]")

        except Exception as e:
            logger.exception("Fix command failed")
            console.print(f"[red]✗ Error: {e}[/]")
            fixer.rollback()
            console.print("[yellow]Restored files from backup[/]")
            raise


def create_migration_commands(
    config: "SyncConfigT | AsyncConfigT",
) -> "SyncMigrationCommands[SyncConfigT] | AsyncMigrationCommands[AsyncConfigT]":
    """Factory function to create the appropriate migration commands.

    Args:
        config: The SQLSpec configuration.

    Returns:
        Appropriate migration commands instance.
    """
    if config.is_async:
        return cast("AsyncMigrationCommands[AsyncConfigT]", AsyncMigrationCommands(cast("AsyncConfigT", config)))
    return cast("SyncMigrationCommands[SyncConfigT]", SyncMigrationCommands(cast("SyncConfigT", config)))
