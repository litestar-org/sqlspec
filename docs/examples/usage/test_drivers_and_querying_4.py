# Example from docs/usage/drivers_and_querying.rst - code-block 4
from sqlspec.adapters.psycopg import PsycopgConfig

# Sync usage example (placeholder)
config = PsycopgConfig(
    pool_config={
        "conninfo": "postgresql://localhost/db",
        "min_size": 5,
        "max_size": 10,
    }
)

# In real usage you'd create SQLSpec and call provide_session

