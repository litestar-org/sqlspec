# SQLSpec Examples

This directory contains the runnable, pytest-friendly example catalog used by the docs.
Each file is scoped to a single concept and uses `# start-example` / `# end-example`
markers for Sphinx `literalinclude` directives.

Structure overview:

- `quickstart/`: First-time setup and configuration.
- `frameworks/`: Litestar, FastAPI, Flask, and Starlette integration examples.
- `drivers/`: Adapter configuration and execution patterns.
- `querying/`: Core SQL execution helpers.
- `sql_files/`: SQL file loader and named query examples.
- `builder/`: Fluent SQL builder examples.
- `extensions/`: Litestar extension settings and ADK integration.
- `patterns/observability/`: Correlation, sampling, and cloud logging patterns.
- `reference/`: API-level snippets for reference docs.
- `contributing/`: Adapter skeletons.

Run the full example suite:

```bash
uv run pytest docs/examples/ -q
```
