"""Configure a MySQL Connector/Python synchronous adapter."""

from sqlspec.adapters.mysqlconnector import MysqlConnectorSyncConfig

__all__ = ("main",)


def main() -> MysqlConnectorSyncConfig:
    """Construct a MysqlConnectorSyncConfig for a MySQL database."""
    config = MysqlConnectorSyncConfig(
        connection_config={"host": "localhost", "user": "app", "password": "secret", "database": "app_db"}
    )
    assert config.connection_config["database"] == "app_db"
    return config


if __name__ == "__main__":
    print(main())
