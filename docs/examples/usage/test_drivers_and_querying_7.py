# Example from docs/usage/drivers_and_querying.rst - code-block 7
from sqlspec.adapters.sqlite import SqliteConfig

def example_sync_sqlite():
    # This is a placeholder showing API usage
    config = SqliteConfig(
        pool_config={
            "database": "myapp.db",
            "timeout": 5.0,
            "check_same_thread": False,
        }
    )

    # with spec.provide_session(config) as session:
    #     session.execute(...) etc.

if __name__ == "__main__":
    example_sync_sqlite()

