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
- `schema_dump_path`: Default output path written by `dump_schema()` (`<script_location>/schema.sql` when unset).
- `auto_dump_schema`: Set to `True` (or a file path) to refresh `schema.sql` automatically after every `upgrade` that applies revisions.

## Authoring SQL and Python Revisions

Revisions execute in lexical prefix order (`0001_...`, `0002_...` or timestamp prefixes normalized via `sqlspec fix`). You can mix `.sql` and `.py` files in the same `migrations/` directory:

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
        return [
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL UNIQUE
            )
            """
        ]


    def down() -> list[str]:
        return ["DROP TABLE IF EXISTS users"]
    ```

## Multi-Schema Migration Tracking

Enterprise deployments frequently store application tables in `app_schema` while isolating migration tracking metadata in `admin_schema`. Configure `default_schema` and `version_table_schema` together—or invoke `SyncMigrationCommands` / `AsyncMigrationCommands` programmatically:

```{ .python title="docs/examples/patterns/migrations_with_schema.py" }
--8<-- "docs/examples/patterns/migrations_with_schema.py:example"
```

When PostgreSQL adapters (`AsyncpgConfig`, `PsycopgSyncConfig`, `PsycopgAsyncConfig`, `PsqlpyConfig`) run behind transaction-mode PgBouncer (`pooler_mode="transaction"`) and define `direct_dsn`, `MigrationCommands` automatically routes DDL execution through `config.derive_direct_config()` so session-level locks and DDL statements bypass the transaction pooler.

## Lifecycle, Squash, and Schema Commands

| Action | CLI Command | Behavior |
| --- | --- | --- |
| Apply pending revisions | `uv run sqlspec upgrade` | Runs unapplied migrations in lexical order and records each version in `version_table_name`. |
| Target a specific revision | `uv run sqlspec upgrade 0003` | Applies pending revisions up to and including `0003`. |
| Roll back revisions | `uv run sqlspec downgrade -1` | Executes the `down` section of the most recently applied revision (`downgrade base` reverts all). |
| Verify checksums & drift | `uv run sqlspec verify --strict` | Read-only audit of applied checksums, orphaned tracker rows, pending files, and partial squashes. |
| Export live schema DDL | `uv run sqlspec dump-schema -o migrations/schema.sql` | Writes topologically ordered schema DDL to `--output` or directly to stdout. |
| Bootstrap from live schema | `uv run sqlspec create-migration -m "baseline" --from-schema` | Populates a new `.sql` or `.py` revision with `CREATE` DDL in `up` and reverse `DROP` DDL in `down`. |
| Bootstrap from data | `uv run sqlspec create-migration -m "events" --from-data events.jsonl` | Infers a typed `CREATE TABLE` revision from a JSON/JSONL/Parquet/Arrow file or staging table. |
| Baseline an existing DB | `uv run sqlspec stamp head` | Records the target revision in `version_table_name` without executing DDL statements. |
| Consolidate history | `uv run sqlspec squash 1:10 -m "baseline" --strategy ddl --yes` | Collapses a revision range (or all revisions when omitted) into one file with `-- Squashed from:` metadata. |

## Squashing Strategies and Reconciliation

`sqlspec squash` (and `commands.squash(...)`) consolidates sequential or timestamp revisions into one baseline file while recording superseded versions in `-- Squashed from:` (SQL) or `__replaces__` / `Squashed from:` (Python) file headers:

```{ .python title="docs/examples/patterns/migration_squash_and_schema.py" }
--8<-- "docs/examples/patterns/migration_squash_and_schema.py:example"
```

Choose the squash strategy that matches your migration history:

- `concat` (**default**): Preserves every `up` statement verbatim in chronological order and reverses `down` statements. Safe for histories containing seed `INSERT`/`UPDATE` DML, views, triggers, functions, or procedural blocks.
- `ddl`: Parses chronological DDL with SQLGlot and folds `CREATE` / `ALTER` / `DROP` / `RENAME` operations into net `CREATE TABLE`, `CREATE SEQUENCE`, and `CREATE INDEX` statements. Fails closed with `SquashValidationError` whenever a statement (such as data DML, views/triggers, dynamic PL/SQL, or destructive column rewrites) cannot be folded faithfully.
- `schema`: Emits topological DDL reflected directly from the live database catalog via `driver.data_dictionary.get_schema_ddl(...)`.

When `upgrade()` runs against downstream environments after a squash has been committed:

1. **All replaced versions applied**: SQLSpec replaces the superseded tracker rows with the squashed revision (`replaces` metadata recorded) without re-executing DDL. Under `--dry-run`, reconciliation is reported without mutating `version_table_name`.
2. **No replaced versions applied**: Fresh databases execute the squashed migration once.
3. **Partial overlap**: If only a subset of replaced versions was applied, `upgrade()` and `verify --strict` raise an error rather than guessing which statements remain unapplied.

Once every environment has reconciled onto the squashed revision, remove the `-- Squashed from:` / `__replaces__` header line to finalize the baseline.

## Schema Snapshots, Verification, and Data Bootstrap

- **Read-Only `verify`**: Checks applied tracker rows against migration files on disk without creating `version_table_name` on empty databases. Combine `--strict` with `--fail-on-pending` as a pre-deploy CI gate.
- **Deterministic `dump-schema`**: Exports live schema DDL excluding `version_table_name` by default (`--include-migrations-table` includes it; `--target-dialect` transpiles across SQLGlot dialects). Omitting `--output` writes pure SQL to stdout so `sqlspec dump-schema > schema.sql` produces a clean SQL script.

### How `--from-data` Classifies Sources

`create-migration --from-data <source>` classifies `<source>` deterministically from its spelling rather than checking local filesystem state first:

- Values containing a path separator (`seeds/users.json`) or ending in a data suffix (`.json`, `.json.gz`, `.jsonl`, `.jsonl.gz`, `.ndjson`, `.ndjson.gz`, `.parquet`, `.arrow`, `.ipc`, `.feather`) are treated as **files**. Missing files, directories, or unsupported suffixes (such as `.csv`) fail immediately without writing a migration file.
- All other values are treated as **table names** (`raw_staging_users` or `staging.raw_users`) and profiled via `driver.data_dictionary.infer_table_ddl_from_data(...)`. Pass `--schema public --from-data json` when profiling a table whose name matches a file suffix.

## Recap

- Declare `migration_config` on your adapter config and `config = "module:attr"` under `[tool.sqlspec]` in `pyproject.toml`.
- Keep `--strategy concat` when squashing histories that mix DDL and data migrations, and use `--strategy ddl` or `--strategy schema` to produce compact `CREATE`-only baselines.
- Pair `sqlspec verify --strict --fail-on-pending` and `auto_dump_schema: True` to catch drift early and keep `schema.sql` reviewable in pull requests.
