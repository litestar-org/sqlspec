# Example from docs/usage/drivers_and_querying.rst - code-block 3
from sqlspec.adapters.psycopg import PsycopgConfig

async def example_psycopg_async():
    config = PsycopgConfig(
        pool_config={
            "conninfo": "postgresql://localhost/db",
            "min_size": 5,
            "max_size": 10,
        }
    )

    # usage would require SQLSpec etc; shown as example only

if __name__ == "__main__":
    pass

