# Example from docs/usage/drivers_and_querying.rst - code-block 10
from sqlspec.adapters.duckdb import DuckDBConfig

# In-memory
config_inmemory = DuckDBConfig()

# Persistent
config_persistent = DuckDBConfig(
    pool_config={"database": "analytics.duckdb"}
)

# with spec.provide_session(config) as session:
#    session.execute(...) etc.

