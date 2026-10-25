"""Shared MySQL-family integration fixtures."""

import contextlib
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import msgspec
import pytest

from sqlspec.utils.module_loader import import_optional_attr
from tests.integration.fixtures.cleanup import mysql_cleanup_statements

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from pytest_databases.docker.mysql import MySQLService

    from sqlspec.adapters.aiomysql import AiomysqlConfig, AiomysqlDriver
    from sqlspec.adapters.aiomysql import default_statement_config as aiomysql_statement_config
    from sqlspec.adapters.aiomysql.adk import AiomysqlADKStore
    from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
    from sqlspec.adapters.asyncmy import default_statement_config as asyncmy_statement_config
    from sqlspec.adapters.asyncmy.adk import AsyncmyADKStore
    from sqlspec.adapters.mysqlconnector import (
        MysqlConnectorAsyncConfig,
        MysqlConnectorAsyncDriver,
        MysqlConnectorSyncConfig,
        MysqlConnectorSyncDriver,
    )
    from sqlspec.adapters.mysqlconnector import default_statement_config as mysqlconnector_statement_config
    from sqlspec.adapters.mysqlconnector.adk import MysqlConnectorAsyncADKStore, MysqlConnectorSyncADKStore
    from sqlspec.adapters.pymysql import PyMysqlConfig, PyMysqlDriver
    from sqlspec.adapters.pymysql import default_statement_config as pymysql_statement_config
    from sqlspec.adapters.pymysql.adk import PyMysqlADKStore
    from sqlspec.extensions.adk._types import StoredEvent, StoredSession
else:
    AiomysqlConfig = import_optional_attr("sqlspec.adapters.aiomysql", "AiomysqlConfig")
    AiomysqlDriver = import_optional_attr("sqlspec.adapters.aiomysql", "AiomysqlDriver")
    aiomysql_statement_config = import_optional_attr("sqlspec.adapters.aiomysql", "default_statement_config")
    AiomysqlADKStore = import_optional_attr("sqlspec.adapters.aiomysql.adk", "AiomysqlADKStore")
    AsyncmyConfig = import_optional_attr("sqlspec.adapters.asyncmy", "AsyncmyConfig")
    AsyncmyDriver = import_optional_attr("sqlspec.adapters.asyncmy", "AsyncmyDriver")
    asyncmy_statement_config = import_optional_attr("sqlspec.adapters.asyncmy", "default_statement_config")
    AsyncmyADKStore = import_optional_attr("sqlspec.adapters.asyncmy.adk", "AsyncmyADKStore")
    MysqlConnectorAsyncConfig = import_optional_attr("sqlspec.adapters.mysqlconnector", "MysqlConnectorAsyncConfig")
    MysqlConnectorAsyncDriver = import_optional_attr("sqlspec.adapters.mysqlconnector", "MysqlConnectorAsyncDriver")
    MysqlConnectorSyncConfig = import_optional_attr("sqlspec.adapters.mysqlconnector", "MysqlConnectorSyncConfig")
    MysqlConnectorSyncDriver = import_optional_attr("sqlspec.adapters.mysqlconnector", "MysqlConnectorSyncDriver")
    mysqlconnector_statement_config = import_optional_attr(
        "sqlspec.adapters.mysqlconnector", "default_statement_config"
    )
    MysqlConnectorAsyncADKStore = import_optional_attr(
        "sqlspec.adapters.mysqlconnector.adk", "MysqlConnectorAsyncADKStore"
    )
    MysqlConnectorSyncADKStore = import_optional_attr(
        "sqlspec.adapters.mysqlconnector.adk", "MysqlConnectorSyncADKStore"
    )
    PyMysqlConfig = import_optional_attr("sqlspec.adapters.pymysql", "PyMysqlConfig")
    PyMysqlDriver = import_optional_attr("sqlspec.adapters.pymysql", "PyMysqlDriver")
    pymysql_statement_config = import_optional_attr("sqlspec.adapters.pymysql", "default_statement_config")
    PyMysqlADKStore = import_optional_attr("sqlspec.adapters.pymysql.adk", "PyMysqlADKStore")

__all__ = (
    "MysqlAdkResidualContext",
    "aiomysql_clean_driver",
    "aiomysql_config",
    "aiomysql_driver",
    "asyncmy_clean_driver",
    "asyncmy_config",
    "asyncmy_driver",
    "mysql_adk_residual_context",
    "mysqlconnector_async_config",
    "mysqlconnector_async_driver",
    "mysqlconnector_clean_async_driver",
    "mysqlconnector_clean_sync_driver",
    "mysqlconnector_sync_config",
    "mysqlconnector_sync_driver",
    "mysqlconnector_sync_transaction_config",
    "pymysql_clean_driver",
    "pymysql_config",
    "pymysql_driver",
    "pymysql_transaction_config",
)

_MYSQL_ADK_DRIVER_PACKAGES: dict[str, str] = {
    "aiomysql": "aiomysql",
    "asyncmy": "asyncmy",
    "mysqlconnector-async": "mysql.connector",
    "mysqlconnector-sync": "mysql.connector",
    "pymysql": "pymysql",
}


class MysqlAdkResidualContext(msgspec.Struct, frozen=True, gc=False):
    """Unified test harness for MySQL ADK residual schema, precision, and FK checks."""

    driver_mode: str
    is_async: bool
    store: Any
    fk_store: Any
    config: Any
    fk_config: Any
    session_table: str
    events_table: str
    fk_session_table: str
    fk_events_table: str
    tenants_table: str

    async def create_session(
        self,
        store: Any,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        *,
        owner_id: int | None = None,
    ) -> "StoredSession":
        """Create an ADK session across sync or async MySQL stores."""
        if self.is_async:
            return cast(
                "StoredSession", await store.create_session(session_id, app_name, user_id, state, owner_id=owner_id)
            )
        return cast("StoredSession", store.create_session(session_id, app_name, user_id, state, owner_id=owner_id))

    async def get_session(self, store: Any, app_name: str, user_id: str, session_id: str) -> "StoredSession | None":
        """Fetch an ADK session across sync or async MySQL stores."""
        if self.is_async:
            return cast("StoredSession | None", await store.get_session(app_name, user_id, session_id))
        return cast("StoredSession | None", store.get_session(app_name, user_id, session_id))

    async def append_event(self, store: Any, event: "StoredEvent") -> None:
        """Append an ADK event across sync or async MySQL stores."""
        if self.is_async:
            await store.append_event(event)
            return
        store.append_event(event)

    async def get_events(self, store: Any, app_name: str, user_id: str, session_id: str) -> "list[StoredEvent]":
        """Read ADK events across sync or async MySQL stores."""
        if self.is_async:
            return cast("list[StoredEvent]", await store.get_events(app_name, user_id, session_id))
        return cast("list[StoredEvent]", store.get_events(app_name, user_id, session_id))

    async def select_rows(self, config: Any, sql: str, **params: Any) -> list[dict[str, Any]]:
        """Execute a SELECT statement on the given MySQL config and return dict rows."""
        if self.is_async:
            async with config.provide_session() as driver:
                return cast("list[dict[str, Any]]", await driver.select(sql, **params))
        with config.provide_session() as driver:
            return cast("list[dict[str, Any]]", driver.select(sql, **params))

    async def execute_sql(self, config: Any, sql: str, **params: Any) -> None:
        """Execute a DML/DDL statement on the given MySQL config and commit."""
        await _execute_mysql_sql(config, sql, is_async=self.is_async, **params)


@pytest.fixture(scope="session")
async def asyncmy_config(mysql_service: "MySQLService") -> "AsyncGenerator[AsyncmyConfig, None]":
    """Provide a session-scoped AsyncmyConfig with a shared pool."""
    pytest.importorskip("asyncmy")
    connection_config = _mysql_connection_config(mysql_service)
    connection_config.update({"minsize": 1, "maxsize": 5})
    config = AsyncmyConfig(connection_config=connection_config, statement_config=asyncmy_statement_config)
    try:
        yield config
    finally:
        await config.close_pool()


@pytest.fixture
async def asyncmy_driver(asyncmy_config: "AsyncmyConfig") -> "AsyncGenerator[AsyncmyDriver, None]":
    """Create an AsyncMy driver instance for testing."""
    async with asyncmy_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def asyncmy_clean_driver(asyncmy_config: "AsyncmyConfig") -> "AsyncGenerator[AsyncmyDriver, None]":
    """Create an AsyncMy driver with clean database state."""
    async with asyncmy_config.provide_session() as driver:
        await _run_async_cleanup(driver, "asyncmy")
        yield driver
        await _run_async_cleanup(driver, "asyncmy")


@pytest.fixture(scope="session")
async def aiomysql_config(mysql_service: "MySQLService") -> "AsyncGenerator[AiomysqlConfig, None]":
    """Provide a session-scoped AiomysqlConfig with a shared pool."""
    pytest.importorskip("aiomysql")
    connection_config = _mysql_connection_config(mysql_service, database_key="db")
    connection_config.update({"minsize": 1, "maxsize": 5})
    config = AiomysqlConfig(connection_config=connection_config, statement_config=aiomysql_statement_config)
    try:
        yield config
    finally:
        await config.close_pool()


@pytest.fixture
async def aiomysql_driver(aiomysql_config: "AiomysqlConfig") -> "AsyncGenerator[AiomysqlDriver, None]":
    """Create an aiomysql driver instance for testing."""
    async with aiomysql_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def aiomysql_clean_driver(aiomysql_config: "AiomysqlConfig") -> "AsyncGenerator[AiomysqlDriver, None]":
    """Create an aiomysql driver with clean database state."""
    async with aiomysql_config.provide_session() as driver:
        await _run_async_cleanup(driver, "aiomysql")
        yield driver
        await _run_async_cleanup(driver, "aiomysql")


@pytest.fixture(scope="session")
async def mysqlconnector_async_config(
    mysql_service: "MySQLService",
) -> "AsyncGenerator[MysqlConnectorAsyncConfig, None]":
    """Provide a session-scoped MysqlConnector async configuration."""
    pytest.importorskip("mysql.connector")
    connection_config = _mysql_connection_config(mysql_service)
    connection_config["use_pure"] = True
    config = MysqlConnectorAsyncConfig(
        connection_config=connection_config, statement_config=mysqlconnector_statement_config
    )
    try:
        yield config
    finally:
        await config.close_pool()


@pytest.fixture(scope="session")
def mysqlconnector_sync_config(mysql_service: "MySQLService") -> "Generator[MysqlConnectorSyncConfig, None, None]":
    """Create a MysqlConnector sync configuration for testing."""
    pytest.importorskip("mysql.connector")
    connection_config = _mysql_connection_config(mysql_service)
    connection_config.update({"use_pure": True, "pool_size": 5})
    config = MysqlConnectorSyncConfig(
        connection_config=connection_config, statement_config=mysqlconnector_statement_config
    )
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def mysqlconnector_sync_transaction_config(
    mysql_service: "MySQLService",
) -> "Generator[MysqlConnectorSyncConfig, None, None]":
    """Create a MysqlConnector sync configuration for transaction testing."""
    pytest.importorskip("mysql.connector")
    connection_config = _mysql_connection_config(mysql_service)
    connection_config.update({"autocommit": False, "use_pure": True})
    config = MysqlConnectorSyncConfig(
        connection_config=connection_config, statement_config=mysqlconnector_statement_config
    )
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
async def mysqlconnector_async_driver(
    mysqlconnector_async_config: "MysqlConnectorAsyncConfig",
) -> "AsyncGenerator[MysqlConnectorAsyncDriver, None]":
    """Create a MysqlConnector async driver instance for testing."""
    async with mysqlconnector_async_config.provide_session() as driver:
        yield driver


@pytest.fixture
def mysqlconnector_sync_driver(
    mysqlconnector_sync_config: "MysqlConnectorSyncConfig",
) -> "Generator[MysqlConnectorSyncDriver, None, None]":
    """Create a MysqlConnector sync driver instance for testing."""
    with mysqlconnector_sync_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def mysqlconnector_clean_async_driver(
    mysqlconnector_async_config: "MysqlConnectorAsyncConfig",
) -> "AsyncGenerator[MysqlConnectorAsyncDriver, None]":
    """Create a MysqlConnector async driver with clean database state."""
    async with mysqlconnector_async_config.provide_session() as driver:
        await _run_async_cleanup(driver, "mysqlconnector_async", procedure_suffix="mysqlconnector_async")
        yield driver
        await _run_async_cleanup(driver, "mysqlconnector_async", procedure_suffix="mysqlconnector_async")


@pytest.fixture
def mysqlconnector_clean_sync_driver(
    mysqlconnector_sync_config: "MysqlConnectorSyncConfig",
) -> "Generator[MysqlConnectorSyncDriver, None, None]":
    """Create a MysqlConnector sync driver with clean database state."""
    with mysqlconnector_sync_config.provide_session() as driver:
        _run_sync_cleanup(driver, "mysqlconnector_sync", procedure_suffix="mysqlconnector_sync")
        yield driver
        _run_sync_cleanup(driver, "mysqlconnector_sync", procedure_suffix="mysqlconnector_sync")


@pytest.fixture(scope="session")
def pymysql_config(mysql_service: "MySQLService") -> "Generator[PyMysqlConfig, None, None]":
    """Create a PyMySQL config for testing."""
    pytest.importorskip("pymysql")
    config = PyMysqlConfig(
        connection_config=_mysql_connection_config(mysql_service), statement_config=pymysql_statement_config
    )
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def pymysql_transaction_config(mysql_service: "MySQLService") -> "Generator[PyMysqlConfig, None, None]":
    """Create a PyMySQL config for transaction testing."""
    pytest.importorskip("pymysql")
    connection_config = _mysql_connection_config(mysql_service)
    connection_config["autocommit"] = False
    config = PyMysqlConfig(connection_config=connection_config, statement_config=pymysql_statement_config)
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def pymysql_driver(pymysql_config: "PyMysqlConfig") -> "Generator[PyMysqlDriver, None, None]":
    """Create a PyMySQL driver instance for testing."""
    with pymysql_config.provide_session() as driver:
        yield driver


@pytest.fixture
def pymysql_clean_driver(pymysql_config: "PyMysqlConfig") -> "Generator[PyMysqlDriver, None, None]":
    """Create a PyMySQL driver with clean database state."""
    with pymysql_config.provide_session() as driver:
        _run_sync_cleanup(driver, "pymysql")
        yield driver
        _run_sync_cleanup(driver, "pymysql")


@pytest.fixture(params=("aiomysql", "asyncmy", "mysqlconnector-async", "mysqlconnector-sync", "pymysql"))
async def mysql_adk_residual_context(
    request: pytest.FixtureRequest, mysql_service: "MySQLService"
) -> "AsyncGenerator[MysqlAdkResidualContext, None]":
    """Provide isolated ADK stores across all MySQL driver modes with deterministic pool teardown."""
    pytest.importorskip("google.adk")
    driver_mode = str(request.param)
    pytest.importorskip(_MYSQL_ADK_DRIVER_PACKAGES[driver_mode])

    suffix = uuid4().hex[:8]
    session_table = f"adk_rs_{suffix}"
    events_table = f"adk_re_{suffix}"
    app_state_table = f"adk_ras_{suffix}"
    user_state_table = f"adk_rus_{suffix}"
    metadata_table = f"adk_rm_{suffix}"

    fk_session_table = f"adk_fks_{suffix}"
    fk_events_table = f"adk_fke_{suffix}"
    fk_app_state_table = f"adk_fkas_{suffix}"
    fk_user_state_table = f"adk_fkus_{suffix}"
    fk_metadata_table = f"adk_fkm_{suffix}"
    tenants_table = f"adk_ten_{suffix}"

    base_adk_config: dict[str, Any] = {
        "session_table": session_table,
        "events_table": events_table,
        "app_state_table": app_state_table,
        "user_state_table": user_state_table,
        "metadata_table": metadata_table,
    }
    fk_adk_config: dict[str, Any] = {
        "session_table": fk_session_table,
        "events_table": fk_events_table,
        "app_state_table": fk_app_state_table,
        "user_state_table": fk_user_state_table,
        "metadata_table": fk_metadata_table,
        "owner_id_column": f"tenant_id BIGINT NOT NULL REFERENCES {tenants_table}(id) ON DELETE CASCADE",
    }

    is_async, config, store = _build_mysql_adk_config_and_store(mysql_service, driver_mode, base_adk_config)
    _, fk_config, fk_store = _build_mysql_adk_config_and_store(mysql_service, driver_mode, fk_adk_config)

    tables_to_drop = (
        events_table,
        session_table,
        app_state_table,
        user_state_table,
        metadata_table,
        fk_events_table,
        fk_session_table,
        fk_app_state_table,
        fk_user_state_table,
        fk_metadata_table,
        tenants_table,
    )

    try:
        await _execute_mysql_sql(
            fk_config,
            f"""
            CREATE TABLE IF NOT EXISTS {tenants_table} (
                id BIGINT PRIMARY KEY,
                name VARCHAR(128) NOT NULL UNIQUE
            ) ENGINE=InnoDB
            """,
            is_async=is_async,
        )
        await _execute_mysql_sql(
            fk_config,
            f"INSERT INTO {tenants_table} (id, name) VALUES (1, 'tenant1'), (2, 'tenant2')",
            is_async=is_async,
        )
        if is_async:
            await store.create_tables()
            await fk_store.create_tables()
        else:
            store.create_tables()
            fk_store.create_tables()

        yield MysqlAdkResidualContext(
            driver_mode=driver_mode,
            is_async=is_async,
            store=store,
            fk_store=fk_store,
            config=config,
            fk_config=fk_config,
            session_table=session_table,
            events_table=events_table,
            fk_session_table=fk_session_table,
            fk_events_table=fk_events_table,
            tenants_table=tenants_table,
        )
    finally:
        await _close_mysql_config(fk_config, is_async=is_async)
        for table_name in tables_to_drop:
            with contextlib.suppress(Exception):
                await _execute_mysql_sql(config, f"DROP TABLE IF EXISTS {table_name}", is_async=is_async)
        await _close_mysql_config(config, is_async=is_async)


def _mysql_connection_config(mysql_service: "MySQLService", *, database_key: str = "database") -> "dict[str, Any]":
    return {
        "host": mysql_service.host,
        "port": mysql_service.port,
        "user": mysql_service.user,
        "password": mysql_service.password,
        database_key: mysql_service.db,
        "autocommit": True,
    }


async def _run_async_cleanup(driver: Any, adapter_suffix: str, *, procedure_suffix: str | None = None) -> None:
    for statement in mysql_cleanup_statements(adapter_suffix, procedure_suffix=procedure_suffix):
        await driver.execute_script(statement)


def _run_sync_cleanup(driver: Any, adapter_suffix: str, *, procedure_suffix: str | None = None) -> None:
    for statement in mysql_cleanup_statements(adapter_suffix, procedure_suffix=procedure_suffix):
        driver.execute_script(statement)


def _build_mysql_adk_config_and_store(
    mysql_service: "MySQLService", driver_mode: str, adk_config: dict[str, Any]
) -> tuple[bool, Any, Any]:
    if driver_mode == "aiomysql":
        conn_cfg = _mysql_connection_config(mysql_service, database_key="db")
        conn_cfg.update({"minsize": 1, "maxsize": 3})
        aiomysql_cfg = AiomysqlConfig(connection_config=conn_cfg, extension_config={"adk": adk_config})
        return True, aiomysql_cfg, AiomysqlADKStore(aiomysql_cfg)
    if driver_mode == "asyncmy":
        conn_cfg = _mysql_connection_config(mysql_service)
        conn_cfg.update({"minsize": 1, "maxsize": 3})
        asyncmy_cfg = AsyncmyConfig(connection_config=conn_cfg, extension_config={"adk": adk_config})
        return True, asyncmy_cfg, AsyncmyADKStore(asyncmy_cfg)
    if driver_mode == "mysqlconnector-async":
        conn_cfg = _mysql_connection_config(mysql_service)
        conn_cfg["use_pure"] = True
        mc_async_cfg = MysqlConnectorAsyncConfig(connection_config=conn_cfg, extension_config={"adk": adk_config})
        return True, mc_async_cfg, MysqlConnectorAsyncADKStore(mc_async_cfg)
    if driver_mode == "mysqlconnector-sync":
        conn_cfg = _mysql_connection_config(mysql_service)
        conn_cfg.update({"use_pure": True, "pool_size": 3})
        mc_sync_cfg = MysqlConnectorSyncConfig(connection_config=conn_cfg, extension_config={"adk": adk_config})
        return False, mc_sync_cfg, MysqlConnectorSyncADKStore(mc_sync_cfg)
    if driver_mode == "pymysql":
        conn_cfg = _mysql_connection_config(mysql_service)
        pymysql_cfg = PyMysqlConfig(connection_config=conn_cfg, extension_config={"adk": adk_config})
        return False, pymysql_cfg, PyMysqlADKStore(pymysql_cfg)
    msg = f"Unsupported MySQL ADK driver mode: {driver_mode}"
    raise ValueError(msg)


async def _execute_mysql_sql(config: Any, sql: str, *, is_async: bool, **params: Any) -> None:
    if is_async:
        async with config.provide_session() as driver:
            await driver.execute(sql, **params)
            await driver.commit()
        return
    with config.provide_session() as driver:
        driver.execute(sql, **params)
        driver.commit()


async def _close_mysql_config(config: Any, *, is_async: bool) -> None:
    if is_async:
        await config.close_pool()
        return
    config.close_pool()
