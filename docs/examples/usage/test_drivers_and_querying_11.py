# Example from docs/usage/drivers_and_querying.rst - code-block 11
from sqlspec.adapters.oracledb import OracleDBConfig

config = OracleDBConfig(
    pool_config={
        "user": "myuser",
        "password": "mypassword",
        "dsn": "localhost:1521/ORCLPDB",
    }
)

# with spec.provide_session(config) as session:
#     result = session.execute("SELECT * FROM employees WHERE employee_id = :id", id=100)

