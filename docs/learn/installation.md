# Installation

SQLSpec requires Python 3.11 or newer. The base package includes the synchronous
SQLite adapter, which uses Python's standard library `sqlite3` module.

=== "uv"

    ```bash
    uv add sqlspec
    ```

=== "pip"

    ```bash
    pip install sqlspec
    ```

=== "pdm"

    ```bash
    pdm add sqlspec
    ```

=== "poetry"

    ```bash
    poetry add sqlspec
    ```

## Verify the Install

Run this command in the environment where you installed SQLSpec:

```bash
python -c "import sqlspec; print(sqlspec.__version__)"
```

If you use a uv-managed project, prefix the command with `uv run`.
Continue to the [Quickstart](quickstart.md) to run your first query.

## Add Features When You Need Them

Each guide lists its required packages and any platform prerequisites:

- [Databases](../databases/index.md): driver choices, cloud connectors, and native clients.
- [Integrations](../integrations/index.md): framework plugins and Google ADK.
- [Schema Mapping](schema-mapping.md): optional model libraries for typed rows.
- [Arrow & Storage](arrow-and-storage.md): Arrow, DataFrames, and storage backends.
- [Observability](observability.md): OpenTelemetry and Prometheus.
- [Performance](performance.md): compiled wheels and runtime helpers.
- [Utilities](../reference/utils.md#schema-serialization-identifiers): JSON serializers and identifier generation.

Extras can be combined in one requirement, using `sqlspec[extra1,extra2]` with
the extra names from the relevant guides. Install only what your application needs.
