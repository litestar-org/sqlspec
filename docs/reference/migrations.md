# Migrations

```python
from sqlspec.migrations import (
    AsyncMigrationCommands,
    AsyncMigrationRunner,
    AsyncMigrationTracker,
    MigrationSquasher,
    PythonFileLoader,
    SchemaEnsureResult,
    SchemaTarget,
    SQLFileLoader,
    SquashPlan,
    SyncMigrationCommands,
    SyncMigrationRunner,
    SyncMigrationTracker,
    create_migration_commands,
    create_migration_runner,
    ensure_schema_async,
    ensure_schema_sync,
)
```

Programmatic schema migration commands, SQL and Python migration loaders, version table trackers, declarative schema targets, and canonical DDL or concatenation squashing. See the [Migrations Guide](../learn/migrations.md) for end-to-end workflows.

## Migration Commands

::: sqlspec.migrations.commands.SyncMigrationCommands
    options:
      heading_level: 3
      members:
        - init
        - current
        - revision
        - upgrade
        - downgrade
        - verify
        - dump_schema
        - stamp
        - squash
        - fix

::: sqlspec.migrations.commands.AsyncMigrationCommands
    options:
      heading_level: 3
      members: false

::: sqlspec.migrations.commands.create_migration_commands
    options:
      heading_level: 3

## Migration Runners & Trackers

::: sqlspec.migrations.runner.SyncMigrationRunner
    options:
      heading_level: 4
      members: false

::: sqlspec.migrations.runner.AsyncMigrationRunner
    options:
      heading_level: 4
      members: false

::: sqlspec.migrations.tracker.SyncMigrationTracker
    options:
      heading_level: 4
      members: false

::: sqlspec.migrations.tracker.AsyncMigrationTracker
    options:
      heading_level: 4
      members: false

## Migration Loaders

::: sqlspec.migrations.loaders.BaseMigrationLoader
    options:
      heading_level: 4
      members: false

::: sqlspec.migrations.loaders.SQLFileLoader
    options:
      heading_level: 4
      members: false

::: sqlspec.migrations.loaders.PythonFileLoader
    options:
      heading_level: 4
      members: false

## Declarative Schema & Squashing

::: sqlspec.migrations.schema.SchemaTarget
    options:
      heading_level: 4
      members: false

::: sqlspec.migrations.schema.SchemaEnsureResult
    options:
      heading_level: 4
      members: false

::: sqlspec.migrations.schema.ensure_schema_sync
    options:
      heading_level: 4

::: sqlspec.migrations.schema.ensure_schema_async
    options:
      heading_level: 4

::: sqlspec.migrations.squash.MigrationSquasher
    options:
      heading_level: 4
      members: false

::: sqlspec.migrations.squash.SquashPlan
    options:
      heading_level: 4
      members: false

::: sqlspec.migrations.squash.fold_migration_ddl
    options:
      heading_level: 4
