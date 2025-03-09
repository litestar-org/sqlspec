from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.base import ConfigManager

dbs = ConfigManager()

config = DuckDBConfig(database="test.duckdb", extensions=[{"name": "vss"}])
etl_db = dbs.add_config(config)

connection = dbs.get_connection(etl_db)
