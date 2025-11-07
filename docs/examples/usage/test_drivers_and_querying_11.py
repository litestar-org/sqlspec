# Test module converted from docs example - code-block 11
"""Minimal smoke test for drivers_and_querying example 11."""

from pytest_databases.docker.oracle import OracleService


def test_example_11_oracledb_config(oracle_service: OracleService) -> None:
    from sqlspec import SQLSpec
    from sqlspec.adapters.oracledb import OracleSyncConfig

    spec = SQLSpec()
    config = OracleSyncConfig(
        pool_config={
            "user": oracle_service.user,
            "password": oracle_service.password,
            "host": oracle_service.host,
            "port": oracle_service.port,
            "service_name": oracle_service.service_name,
        }
    )

    with spec.provide_session(config) as session:
        create_table_sql = """CREATE TABLE if not exists employees (
           employee_id NUMBER PRIMARY KEY,
           first_name VARCHAR2(50),
           last_name VARCHAR2(50)
       )"""
        session.execute(create_table_sql)
        session.execute("""
           INSERT INTO employees (employee_id, first_name, last_name) VALUES (100, 'John', 'Doe')
       """)

        session.execute("SELECT * FROM employees WHERE employee_id = :id", id=100)
