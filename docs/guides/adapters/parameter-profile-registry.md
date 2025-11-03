# Driver Parameter Profile Registry

The table below summarizes the canonical `DriverParameterProfile` entries defined in `sqlspec/core/parameters/_registry.py`. Use it as a quick reference when updating adapters or adding new ones.

| Adapter | Registry Key | JSON Strategy | Extras | Default Dialect | Notes |
| --- | --- | --- | --- | --- | --- |
| ADBC | `"adbc"` | `helper` | `type_coercion_overrides` (list/tuple array coercion) | dynamic (per detected dialect) | Shares AST transformer metadata with BigQuery dialect helpers. |
| AioSQLite | `"aiosqlite"` | `helper` | None | `sqlite` | Mirrors SQLite defaults; bools coerced to ints for driver parity. |
| AsyncMy | `"asyncmy"` | `helper` | None | `mysql` | Native list expansion currently disabled until connector parity confirmed. |
| AsyncPG | `"asyncpg"` | `driver` | None | `postgres` | Relies on asyncpg codecs; JSON serializers referenced for later registration. |
| BigQuery | `"bigquery"` | `helper` | `json_tuple_strategy="tuple"`, `type_coercion_overrides` | `bigquery` | Enforces named parameters; tuple JSON payloads preserved as tuples. |
| DuckDB | `"duckdb"` | `helper` | None | `duckdb` | Mixed-style binding disabled; aligns bool/datetime coercion with SQLite. |
| OracleDB | `"oracledb"` | `helper` | None | `oracle` | List expansion disabled; LOB handling delegated to adapter converters. |
| PSQLPy | `"psqlpy"` | `helper` | None | `postgres` | Decimal values currently downcast to float for driver compatibility. |
| Psycopg | `"psycopg"` | `helper` | None | `postgres` | Array coercion delegated to psycopg adapters; JSON handled by shared converters. |
| SQLite | `"sqlite"` | `helper` | None | `sqlite` | Shares bool/datetime handling with DuckDB and CLI defaults. |

## Adding or Updating Profiles

1. Define the profile in `_registry.py` using lowercase key naming.
2. Pick the JSON strategy that matches driver capabilities (`helper`, `driver`, or `none`).
3. Declare extras as an immutable mapping; document each addition in this file and the relevant adapter guide.
4. Add or update regression coverage (see `specs/archive/driver-quality-review/research/testing_deliverables.md`).
5. If behaviour changes, update changelog entries and adapter guides accordingly.

Refer to [AGENTS.md](../../AGENTS.md) for the full checklist when touching the registry.

## Example Usage

```python
from sqlspec.core.parameters import get_driver_profile, build_statement_config_from_profile

profile = get_driver_profile("duckdb")
config = build_statement_config_from_profile(profile)

print(config.parameter_config.default_parameter_style)
```

The snippet above retrieves the DuckDB profile, builds a `StatementConfig`, and prints the default parameter style (`?`). Use the same pattern for new adapters after defining their profiles.
