# SQLSpec Documentation Examples

This directory contains standalone, runnable Python modules referenced throughout the Zensical documentation via `pymdownx.snippets`.

## Standalone Contract

Every module in `docs/examples/` follows a uniform layout so readers can copy, paste, and execute any example directly without pytest fixtures or hidden setup:

1. **Module Docstring**: A concise summary at the top of the file explaining the scenario.
2. **Top-Level Imports & `__all__`**: All imports live at module top level alongside explicit `__all__ = ("main",)` (plus any exported schema or configuration symbols).
3. **`main()` Entrypoint**: Each runnable module defines `def main() -> ...:` or `async def main() -> ...:` using `":memory:"` for in-memory SQLite demos or `tempfile.TemporaryDirectory()` when filesystem `.sql` or `.db` files are needed, followed by an `if __name__ == "__main__":` runner block.
4. **Zero Inline Comments**: Documentation snippets contain zero inline `#` comments; explanations belong in module/function docstrings or surrounding Markdown prose.

## Including Snippets in Markdown

Embed any example module in Markdown documentation pages using `pymdownx.snippets` relative to the repository root (`check_paths = true` in `zensical.toml` verifies every path at build time):

````markdown
```python
--8<-- "docs/examples/quickstart/first_query.py:example"
```
````

## Structure Overview

- `quickstart/`: First-time setup, sync/async sessions, configuration, and typed row mapping.
- `querying/`: Execution helpers, batch operations, streaming, and `StatementStack`.
- `sql_files/`: `SQLFileLoader`, named queries, declared parameters, and dynamic `WHERE` filters.
- `builder/`: Fluent SQL query builder (`SELECT`, `INSERT`, `UPDATE`, `MERGE`/upsert, joins, raw expressions).
- `drivers/`: Adapter connection pools, parameter binding, transactions, and service wrappers.
- `configuration/`: Multi-database registries and migration warm-up lifecycles.
- `frameworks/`: Litestar, FastAPI, Flask, Starlette, and Sanic integration examples.
- `extensions/`: Litestar plugin/dependency keys and Google ADK session and memory stores.
- `patterns/`: Offset/cursor pagination, filter dependencies, ETL pipelines, schema migrations, integration testing, and observability (`patterns/observability/`).
- `reference/`: Concise API-level snippets for reference pages.
- `contributing/`: Minimal driver adapter skeleton.

## Running and Verifying Examples

Run an individual example directly with `uv`:

```bash
uv run python docs/examples/quickstart/first_query.py
```

Run the automated AST, zero-comment, compilation, and execution suite across all examples:

```bash
uv run pytest tests/unit/docs/test_examples.py
```
