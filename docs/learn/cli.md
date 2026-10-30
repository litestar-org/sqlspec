# CLI

SQLSpec installs a `sqlspec` command-line interface (powered by `rich-click`) for inspecting adapter configurations, managing schema migrations, verifying checksums, exporting schema DDL, and maintaining Google ADK memory tables.

See [CLI Reference](/reference/cli/) for the complete generated option and argument tables across every command.

## Configuration Discovery

The `sqlspec` CLI resolves database configurations in priority order:

1. Explicit `--config` flag: `uv run sqlspec --config app.db:get_configs show-config`
2. Environment variable: `SQLSPEC_CONFIG="app.db:get_configs"`
3. `pyproject.toml` `[tool.sqlspec]` `config` entry:

```toml
[tool.sqlspec]
config = "app.db:database_config"
```

Comma-separated dotted paths (`--config app.oltp:config,app.analytics:config`) are loaded from the current working directory and deduplicated by `bind_key`. Pass `--validate-config` to verify config loading and print whether each target runs synchronously or asynchronously.

### Global CLI Flags

| Flag | Environment Variable | Purpose |
| --- | --- | --- |
| `--config TEXT` | `SQLSPEC_CONFIG` | Dotted import path to a config attribute, sequence of configs, or zero-argument config factory callable. |
| `--validate-config` | — | Load and validate resolved configurations before executing the requested subcommand. |
| `--bind-key TEXT` | — | Restrict command execution to the single configuration matching `bind_key`. |
| `--include TEXT` | — | Repeatable allowlist of `bind_key` targets when managing multiple databases. |
| `--exclude TEXT` | — | Repeatable denylist of `bind_key` targets to skip during multi-database runs. |
| `--dry-run` | — | Preview pending SQL, version renames, or squash reconciliations without mutating database or file state. |
| `--no-prompt` | — | Skip interactive confirmation prompts in CI/CD pipelines and automated scripts. |
| `--use-logger` | — | Emit structured Python `logging` records instead of Rich interactive console output. |

## Core Migration Workflow

The example below exercises the end-to-end CLI lifecycle (`show-config`, `init`, `create-migration`, `upgrade`, `show-current-revision`) programmatically with Click's `CliRunner`:

```{ .python title="docs/examples/quickstart_migrations.py" }
--8<-- "docs/examples/quickstart_migrations.py:example"
```

From your terminal, run the equivalent commands directly—including live-schema and data-driven revision bootstrap flags:

```bash
uv run sqlspec show-config
uv run sqlspec init --no-prompt
uv run sqlspec create-migration -m "create users table" --no-prompt
uv run sqlspec create-migration -m "initial_baseline" --from-schema
uv run sqlspec create-migration -m "seed_events" \
    --from-data seeds/events.jsonl --target-table events
uv run sqlspec upgrade --no-prompt
uv run sqlspec show-current-revision
```

## Multi-Database Targeting and Dry Runs

When `[tool.sqlspec]` returns multiple configs, filter execution by `bind_key` using `--bind-key`, `--include`, or `--exclude`, and preview pending operations safely with `--dry-run`:

```bash
uv run sqlspec upgrade --bind-key primary --dry-run
uv run sqlspec upgrade --include primary --include analytics --no-prompt
uv run sqlspec downgrade -1 --bind-key analytics --use-logger
```

Pass `--use-logger` in CI/CD pipelines to emit structured log records instead of interactive Rich terminal tables.

## Verification, Schema Export, and Squashing

| Command | Typical Use Case |
| --- | --- |
| `sqlspec verify --strict` | Audit applied tracker checksums, orphaned versions, and partial squashes without creating the tracking table (`--fail-on-pending` fails when unapplied files exist). |
| `sqlspec dump-schema -o schema.sql` | Export current schema DDL in topological order to `--output` or byte-for-byte to stdout (`--target-dialect` transpiles DDL across engines). |
| `sqlspec stamp <revision>` | Mark an already-provisioned database at `<revision>` without executing SQL scripts. |
| `sqlspec fix` | Convert timestamp-prefixed migration filenames to sequential `0001_...` ordering and reconcile tracker records. |
| `sqlspec squash [VERSION_RANGE]` | Consolidate all migrations or an explicit `START:END` range using `--strategy concat` (default), `--strategy ddl`, or `--strategy schema`. |

## Google ADK Memory Commands

When using `sqlspec[adk]` (`extension_config={"adk": ...}`), the `sqlspec adk memory` command group provides operational checks and retention cleanup:

```bash
uv run sqlspec adk memory verify --bind-key agent_db
uv run sqlspec adk memory cleanup --days 30 --bind-key agent_db
```

- `adk memory verify`: Confirms the configured ADK memory table exists and accepts queries.
- `adk memory cleanup --days N`: Deletes persisted conversation memory entries older than `N` days using the adapter's sync or async ADK memory store.

## Recap

- Configure `[tool.sqlspec]` in `pyproject.toml` once so developers and CI jobs run `uv run sqlspec ...` without repeating module paths.
- Use `sqlspec verify --strict --fail-on-pending` in deployment checks and `sqlspec dump-schema` to maintain reviewable schema snapshots.
- Consult [CLI Reference](/reference/cli/) for every flag, environment variable, and subcommand signature.
