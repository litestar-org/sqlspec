# Example from docs/usage/drivers_and_querying.rst - code-block 6
from sqlspec.adapters.sqlite import SqliteConfig

# Typical sqlite sync usage - placeholder
config = SqliteConfig(
    pool_config={
        "database": "myapp.db",
        "timeout": 5.0,
        "check_same_thread": False,
    }
)

# with spec.provide_session(config) as session: ...

