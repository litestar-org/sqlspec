# Migrations

SQLSpec includes a built-in schema migration runner supporting sequential `.sql` and `.py` revisions across every sync and async adapter—without requiring SQLAlchemy or Alembic when your project is pure SQL.

## Configure `migration_config` and `pyproject.toml`

Enable migrations on any adapter configuration by supplying `migration_config`:

```{ .python title="docs/examples/migration_quickstart_config.py" }
--8<-- "docs/examples/migration_quickstart_config.py:example"
```

Point `[tool.sqlspec]` in `pyproject.toml` at your module attribute or factory callable so the `sqlspec` CLI discovers configurations automatically:

```toml
[tool.sqlspec]
config = "app.db:database_config"
```

Common `MigrationConfig` keys:

- `script_location`: Directory containing migration files (`"migrations"` by default).
- `version_table_name`: Tracking table name (`"ddl_migrations"` by default).
- `default_schema`: Default target schema for unqualified DDL objects.
- `version_table_schema`: Dedicated admin schema holding the version-tracking table.
- `include_extensions`: Optional list of extension schemas to bundle (such as `["litestar", "adk", "events"]`).

## Authoring SQL and Python Revisions

Revisions execute in lexical prefix order (`0001_...`, `0002_...` or timestamp prefixes after `sqlspec fix`). You can mix `.sql` and `.py` files in the same `migrations/` directory:

=== "SQL Revision (`0001_create_users.sql`)"

    ```sql
    -- name: migrate-0001-up
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- name: migrate-0001-down
    DROP TABLE IF EXISTS users;
    ```

=== "Python Revision (`0001_create_users.py`)"

    ```python
    def up() -> list[str]:
        return ["CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)"]


    def down() -> list[str]:
        return ["DROP TABLE IF EXISTS users"]
    ```

## Multi-Schema Migration Tracking

Enterprise deployments frequently store application tables in `app_schema` while isolating migration tracking metadata in `admin_schema`. Configure `default_schema` and `version_table_schema` together—or invoke `SyncMigrationCommands` / `AsyncMigrationCommands` programmatically:

```{ .python title="docs/examples/patterns/migrations_with_schema.py" }
--8<-- "docs/examples/patterns/migrations_with_schema.py:example"
```

When PostgreSQL adapters (`AsyncpgConfig`, `PsycopgSyncConfig`, `PsycopgAsyncConfig`, `PsqlpyConfig`) run behind transaction-mode PgBouncer (`pooler_mode="transaction"`) and define `direct_dsn`, `MigrationCommands` automatically routes DDL execution through `config.derive_direct_config()` so session-level locks and DDL statements bypass the transaction pooler.

## Upgrading, Stamping, and Squashing

| Action | CLI Command | Behavior |
| --- | --- | --- |
| Apply all pending revisions | `uv run sqlspec upgrade` | Runs every unapplied migration in lexical order and records each version in `version_table_name`. |
| Target a specific revision | `uv run sqlspec upgrade 0003` | Applies pending revisions up to and including `0003`. |
| Roll back revisions | `uv run sqlspec downgrade -1` | Executes the `down` section of the most recently applied revision (`downgrade base` reverts all). |
| Baseline an existing database | `uv run sqlspec stamp head` | Writes the target revision into `version_table_name` without executing DDL statements. |
| Consolidate history | `uv run sqlspec squash --start 0001 --end 0010` | Merges sequential revisions into one baseline file (pass `--update-database` to update tracker rows). |

## Recap

- Declare `migration_config` on your adapter config and `config = "module:attr"` under `[tool.sqlspec]` in `pyproject.toml`.
- Prefer `.sql` migration files for transparent, DBA-auditable DDL and `.py` revisions when statements depend on runtime environment inspection.
- Use `version_table_schema` and `direct_dsn` in pooled multi-schema production environments.
