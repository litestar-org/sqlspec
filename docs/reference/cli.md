# CLI Reference

```python
from sqlspec.cli import add_migration_commands, cli, get_sqlspec_group
```

Command-line interface for inspecting configurations, managing schema migrations, and maintaining extension stores. See the [Migrations Guide](../learn/migrations.md) and [CLI Guide](../learn/cli.md) for workflow examples.

## Global Options

```bash
sqlspec [OPTIONS] COMMAND [ARGS]...
```

SQLSpec CLI commands.

Configuration resolution prefers CLI flag, SQLSPEC_CONFIG env var, and finally the [tool.sqlspec] section. Comma-separated paths are split, deduplicated by bind key, and loaded from the current working directory so local modules can be imported. When --validate-config is used we report each config's async capability.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--config` | `text` | - | Dotted path to SQLSpec config(s) or callable function (env: `SQLSPEC_CONFIG`) |
| `--validate-config` | `flag` | `False` | Validate configuration before executing migrations |

## Inspection & Setup

#### `sqlspec show-config`

Show all configurations with migrations enabled.

```bash
sqlspec show-config [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |

#### `sqlspec show-current-revision`

Shows the current revision for the database.

```bash
sqlspec show-current-revision [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |
| `--verbose` | `flag` | `False` | Enable verbose output. |
| `--include` | `text` | - | Include only specific configurations (can be used multiple times) |
| `--exclude` | `text` | - | Exclude specific configurations (can be used multiple times) |

#### `sqlspec verify`

Verify database migration tracking records against migration files on disk.

```bash
sqlspec verify [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--strict` | `flag` | `False` | Exit non-zero on checksum mismatch, orphaned applied migration, or partially applied squash. |
| `--fail-on-pending` | `flag` | `False` | Exit non-zero when migrations are pending. |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |
| `--include` | `text` | - | Include only specific configurations (can be used multiple times) |
| `--exclude` | `text` | - | Exclude specific configurations (can be used multiple times) |
| `--use-logger` | `flag` | `False` | Emit migration output via structured logger instead of console output |
| `--no-echo` | `flag` | `False` | Disable console output for migration commands |

#### `sqlspec init`

Initialize migrations for the project.

```bash
sqlspec init [OPTIONS] [DIRECTORY]
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `directory` | `text` | No | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |
| `--package` | `flag` | `True` | Create `__init__.py` for created folder |
| `--no-prompt` | `flag` | `False` | Do not prompt for confirmation before executing the command. |

## Schema Migrations

#### `sqlspec create-migration`

Create a new migration revision.

```bash
sqlspec create-migration [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |
| `-m`, `--message` | `text` | - | Revision message |
| `--format`, `--file-type` | `choice(sql | py)` | - | File format for the generated migration (defaults to template profile) |
| `--from-schema` | `flag` | `False` | Bootstrap migration with current database schema DDL. |
| `--from-data` | `text` | - | Bootstrap migration DDL from a data file or existing staging table. Values containing a path separator or ending in a data suffix are files; other values are table names. |
| `--schema` | `text` | - | Database schema name for --from-schema or --from-data. |
| `--target-table` | `text` | - | Target table name when bootstrapping from data. |
| `--no-prompt` | `flag` | `False` | Do not prompt for confirmation before executing the command. |

#### `sqlspec upgrade`

Upgrade database to a specific revision.

```bash
sqlspec upgrade [OPTIONS] [REVISION]
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `revision` | `text` | No | `"head"` |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |
| `--no-prompt` | `flag` | `False` | Do not prompt for confirmation before executing the command. |
| `--include` | `text` | - | Include only specific configurations (can be used multiple times) |
| `--exclude` | `text` | - | Exclude specific configurations (can be used multiple times) |
| `--dry-run` | `flag` | `False` | Show what would be executed without making changes |
| `--use-logger` | `flag` | `False` | Emit migration output via structured logger instead of console output |
| `--no-echo` | `flag` | `False` | Disable console output for migration commands |
| `--summary` | `flag` | `False` | Emit a single summary log entry when logger output is enabled |
| `--no-auto-sync` | `flag` | `False` | Disable automatic version reconciliation when migrations have been renamed |
| `--allow-missing` | `flag` | `False` | Allow out-of-order migrations even when strict ordering is enabled. |

#### `sqlspec downgrade`

Downgrade database to a specific revision.

```bash
sqlspec downgrade [OPTIONS] [REVISION]
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `revision` | `text` | No | `"-1"` |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |
| `--no-prompt` | `flag` | `False` | Do not prompt for confirmation before executing the command. |
| `--include` | `text` | - | Include only specific configurations (can be used multiple times) |
| `--exclude` | `text` | - | Exclude specific configurations (can be used multiple times) |
| `--dry-run` | `flag` | `False` | Show what would be executed without making changes |
| `--use-logger` | `flag` | `False` | Emit migration output via structured logger instead of console output |
| `--no-echo` | `flag` | `False` | Disable console output for migration commands |
| `--summary` | `flag` | `False` | Emit a single summary log entry when logger output is enabled |

#### `sqlspec stamp`

Stamp the revision table with the given revision

```bash
sqlspec stamp [OPTIONS] <REVISION>
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `revision` | `text` | Yes | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |

#### `sqlspec squash`

Squash multiple sequential migrations into a single file.

VERSION_RANGE accepts START:END, START..END, or START-END, for example 1:7.

```bash
sqlspec squash [OPTIONS] [VERSION_RANGE]
```

| Argument | Type | Required | Default |
| --- | --- | --- | --- |
| `version_range` | `text` | No | - |

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |
| `-m`, `--message` | `text` | - | Description for the squashed migration. |
| `--dry-run` | `flag` | `False` | Show what would be executed without making changes |
| `-y`, `--yes` | `flag` | `False` | Skip confirmation prompt |
| `--no-prompt` | `flag` | `False` | Do not prompt for confirmation before executing the command. |
| `--include` | `text` | - | Include only specific configurations (can be used multiple times) |
| `--exclude` | `text` | - | Exclude specific configurations (can be used multiple times) |
| `--use-logger` | `flag` | `False` | Emit migration output via structured logger instead of console output |
| `--no-echo` | `flag` | `False` | Disable console output for migration commands |
| `--no-database` | `flag` | `False` | Skip database record updates |
| `--allow-gaps` | `flag` | `False` | Allow gaps in version sequence |
| `--output-format` | `choice(sql | py)` | `"sql"` | Output format for squashed migration (default: sql) |
| `--strategy` | `choice(ddl | schema | concat)` | `"concat"` | Squash strategy: keep every statement in order (concat), fold DDL into its final shape and refuse ranges that cannot be folded faithfully (ddl), or reflect the live database schema (schema). |
| `--single-file`, `--split` | `flag` | `False` | Force a single squashed file (--single-file) or split when file types or execution directives change (--split). |

#### `sqlspec fix`

Convert timestamp migrations to sequential format.

```bash
sqlspec fix [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |
| `--dry-run` | `flag` | `False` | Show what would be executed without making changes |
| `-y`, `--yes` | `flag` | `False` | Skip confirmation prompt |
| `--no-prompt` | `flag` | `False` | Do not prompt for confirmation before executing the command. |
| `--include` | `text` | - | Include only specific configurations (can be used multiple times) |
| `--exclude` | `text` | - | Exclude specific configurations (can be used multiple times) |
| `--use-logger` | `flag` | `False` | Emit migration output via structured logger instead of console output |
| `--no-echo` | `flag` | `False` | Disable console output for migration commands |
| `--no-database` | `flag` | `False` | Skip database record updates |

#### `sqlspec dump-schema`

Export the current database schema DDL.

```bash
sqlspec dump-schema [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `-o`, `--output` | `path` | - | Output file path for schema DDL. |
| `--schema` | `text` | - | Database schema name to inspect. |
| `--include-migrations-table` | `flag` | `False` | Include the migration tracking table in the exported DDL. |
| `--prefer-native`, `--synthesize` | `flag` | `True` | Prefer engine-native catalog DDL when available or synthesize via SQLSpec builders. |
| `--target-dialect` | `text` | - | Target SQLGlot dialect for cross-dialect DDL transpilation. |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |
| `--include` | `text` | - | Include only specific configurations (can be used multiple times) |
| `--exclude` | `text` | - | Exclude specific configurations (can be used multiple times) |
| `--no-echo` | `flag` | `False` | Disable console output for migration commands |

## ADK Extension Commands

#### `sqlspec adk memory cleanup`

Delete memory entries older than N days

```bash
sqlspec adk memory cleanup [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |
| `--days` | `integer` | - | **Required.** Delete entries older than this many days |

#### `sqlspec adk memory verify`

Verify memory table exists and is reachable

```bash
sqlspec adk memory verify [OPTIONS]
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--bind-key` | `text` | - | Specify which SQLSpec config to use by bind key |

## Programmatic CLI Factory

::: sqlspec.cli.get_sqlspec_group
    options:
      heading_level: 3

::: sqlspec.cli.add_migration_commands
    options:
      heading_level: 3
