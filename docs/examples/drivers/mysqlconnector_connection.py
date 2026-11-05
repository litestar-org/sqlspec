"""Configure MySQL Connector/Python synchronous and asynchronous adapters."""

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig

spec = SQLSpec()
sync_config = spec.add_config(
    MysqlConnectorSyncConfig(
        connection_config={
            "host": "localhost",
            "port": 3306,
            "user": "app",
            "password": "secret",
            "database": "app_db",
            "pool_size": 10,
        }
    )
)
async_config = spec.add_config(
    MysqlConnectorAsyncConfig(
        connection_config={"host": "localhost", "port": 3306, "user": "app", "password": "secret", "database": "app_db"}
    )
)
# --8<-- [end:example]

__all__ = ("main",)


def main() -> MysqlConnectorSyncConfig:
    """Construct MysqlConnectorSyncConfig and MysqlConnectorAsyncConfig instances."""
    assert sync_config.connection_config["database"] == "app_db"
    assert async_config.connection_config["database"] == "app_db"
    return sync_config


if __name__ == "__main__":
    print(main())
