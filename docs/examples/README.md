# SQLSpec Example Catalog

This directory now mirrors the way developers explore SQLSpec:

- `shared/` contains reusable helpers for configs and demo datasets.
- `frameworks/` groups runnable apps (Litestar for now) that rely on lightweight backends (aiosqlite, duckdb).
- `adapters/` holds connection-focused snippets for production drivers such as asyncpg, psycopg, and oracledb.
- `patterns/` demonstrates SQL builder usage, migrations, and multi-tenant routing.
- `loaders/` shows how to hydrate SQL from files for quick demos.
- `extensions/` keeps integration-specific samples (Adapter Development Kit in this pass).

All scripts keep to a single entry point and stay under 75 lines so they are easy to read and embed directly into docs. Inline comments are avoided per the project standards; docstrings explain the scenario instead.

## Ruff configuration

`pyproject.toml` now scopes two Ruff ignore codes to `docs/examples/**/*.py`:

- `T201` – Allow `print()` calls in CLI-oriented examples.
- `INP001` – Ignore namespace-package warnings because these files intentionally live in a flat docs tree.

This keeps the rest of the rule set active, so examples remain formatted and type-safe without sprinkling `# noqa` markers.

## Running the smoke suite

```
make examples-smoke
```

The smoke target imports SQLite/AioSQLite/DuckDB demos (the adapters that do not need external services) via `docs/examples/run_smoke.py`. Adapter-specific files that expect a database simply print instructions rather than failing the run.
