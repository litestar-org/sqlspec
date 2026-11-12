# Migrations

```python
from sqlspec.config import (
    ExtensionMigrationMode,
    resolve_extension_migration_mode,
)
from sqlspec.migrations import (
    AsyncMigrationCommands,
    AsyncMigrationRunner,
    AsyncMigrationTracker,
    BaseMigrationLoader,
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
    fold_migration_ddl,
)
from sqlspec.migrations.context import MigrationContext
```

Programmatic schema migration commands, SQL and Python migration loaders, version table trackers, unversioned extension reconciler contexts, declarative schema targets, and canonical DDL or concatenation squashing. See the [Migrations Guide](../learn/migrations.md) for end-to-end workflows.

## Migration Commands

::: sqlspec.migrations.commands.SyncMigrationCommands
    options:
      heading_level: 3
      separate_signature: false
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
      separate_signature: false
      show_docstring_parameters: false

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

::: sqlspec.migrations.runner.create_migration_runner
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

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

## Extension Reconciliation & Context

Built-in reconcilers (`litestar`, `events`, and `adk`) execute `<migrations_path>/reconcile.py::reconcile(context)` across all 21 unconditional built-in adapters (`sqlite`, `aiosqlite`, `asyncpg`, `psycopg`, `psqlpy`, `cockroach_asyncpg`, `cockroach_psycopg`, `aiomysql`, `asyncmy`, `pymysql`, `mysqlconnector`, `duckdb`, `mssql_python`, `pymssql`, `oracledb`, `spanner`, `bigquery`, `db2`, `sqlanywhere`, `sybase`, `teradata`), `arrow_odbc` on `mssql`/`tsql`/`db2`, and `adbc` on backends with a resolved dialect (ADBC over FlightSQL requires an explicit `statement_config` dialect or `gizmosql_backend`). Inside `reconcile(context)`, pass `manage_schema=True, commit=False, strict=True` to `ensure_schema_sync` or `ensure_schema_async` so the migration runner owns transaction boundaries and unsafe schema drift fails closed with `MigrationError`.

::: sqlspec.config.ExtensionMigrationMode
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.migrations.context.MigrationContext
    options:
      heading_level: 4
      members: false

::: sqlspec.migrations.utils.resolve_extension_migration_mode
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true
      show_docstring_parameters: false

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
      separate_signature: false
      show_signature_annotations: true
      show_docstring_parameters: false

::: sqlspec.migrations.schema.ensure_schema_async
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true
      show_docstring_parameters: false

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
      separate_signature: false
      show_signature_annotations: true
      show_docstring_parameters: false
