# Example from docs/usage/drivers_and_querying.rst - code-block 5
from sqlspec.adapters.psqlpy import PsqlpyConfig

async def example_psqlpy():
    config = PsqlpyConfig(
        pool_config={
            "dsn": "postgresql://localhost/db",
            "max_pool_size": 20,
        }
    )

    # Async usage placeholder

if __name__ == "__main__":
    pass

