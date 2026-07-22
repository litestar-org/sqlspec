"""Verify the oracle_aq_privileges fixture unlocks DBMS_AQADM for the app user."""

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import OracleSyncConfig

pytestmark = pytest.mark.xdist_group("oracle")


def test_app_user_can_run_dbms_aqadm(oracle_aq_privileges: None, oracle_23ai_service: OracleService) -> None:
    """The app user can create and drop an AQ queue table once privileges are granted.

    Without the grant this raises ORA-01031 (insufficient privileges) or PLS-00201
    (dbms_aqadm not declared), so a passing run proves the fixture did its job.
    """

    config = OracleSyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        }
    )
    queue_table = "SQLSPEC_AQ_PRIV_PROBE"
    try:
        with config.provide_session() as driver:
            driver.execute_script(
                f"BEGIN dbms_aqadm.create_queue_table(queue_table => '{queue_table}', "
                "queue_payload_type => 'RAW'); END;"
            )
            visible = driver.select_value(
                "SELECT COUNT(*) FROM user_queue_tables WHERE queue_table = :name", {"name": queue_table}
            )
            driver.execute_script(f"BEGIN dbms_aqadm.drop_queue_table(queue_table => '{queue_table}'); END;")

            assert visible == 1
    finally:
        config.close_pool()
