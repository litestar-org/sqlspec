# SQLSpec Examples

This directory contains runnable examples and code snippets referenced throughout the documentation.
Each file illustrates a single concept or workflow, often using `# start-example` and `# end-example`
markers for Sphinx `literalinclude` directives.

## Examples vs. Library Tests

The scripts and snippets in this directory serve as reader-facing demonstrations and documentation
inclusions. They are designed to be read, adapted, or executed directly as stand-alone demonstrations
(e.g., via `uv run python docs/examples/...`).

Formal integration and regression testing for all SQLSpec behavior—including CLI configuration discovery,
migration execution, and framework integrations—belongs exclusively in the library test suite under
`tests/` (such as `tests/integration/cli/test_migration_quickstart.py`). Documentation examples do not
serve as test suites and should not be collected as pytest test paths.

## Structure Overview

- `quickstart/`: First-time setup and configuration.
- `frameworks/`: Litestar, FastAPI, Flask, Sanic, and Starlette integration examples.
- `drivers/`: Adapter configuration and execution patterns.
- `querying/`: Core SQL execution helpers.
- `sql_files/`: SQL file loader and named query examples.
- `builder/`: Fluent SQL builder examples.
- `extensions/`: Litestar extension settings and ADK integration.
- `patterns/observability/`: Correlation, sampling, and cloud logging patterns.
- `reference/`: API-level snippets for reference docs.
- `contributing/`: Adapter skeletons.

## Running Examples

Individual examples can be executed directly with Python:

```bash
uv run python docs/examples/quickstart_migrations.py
```
