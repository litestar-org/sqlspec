"""Test fixtures and configuration for ADBC integration tests."""

import contextlib
import functools
import os
from collections.abc import Callable, Generator
from importlib.util import find_spec
from typing import TYPE_CHECKING, Any, TypeVar, cast

import pytest
from pytest_databases.docker.snowflake import SNOWFLAKE_HTTP_PORT, SnowflakeService, check_snowflake_health
from pytest_databases.helpers import get_xdist_worker_num

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.core import StatementConfig
from tests.fixtures.snowflake import _EmbucketSnowflakeProxyHandler as _EmbucketSnowflakeProxyHandler
from tests.fixtures.snowflake import _provide_embucket_snowflake_proxy

if TYPE_CHECKING:
    from pytest_databases import ContainerService
    from pytest_databases.docker.db2 import Db2Service
    from pytest_databases.docker.gizmosql import GizmoSQLService
    from pytest_databases.docker.oracle import OracleService
    from pytest_databases.types import ServiceContainer, XdistIsolationLevel

__all__ = (
    "adbc_db2_config",
    "adbc_db2_session",
    "adbc_duckdb_config",
    "adbc_duckdb_driver",
    "adbc_duckdb_session",
    "adbc_gizmosql_config",
    "adbc_gizmosql_connection_config",
    "adbc_gizmosql_session",
    "adbc_gizmosql_sqlite_config",
    "adbc_gizmosql_sqlite_connection_config",
    "adbc_gizmosql_sqlite_session",
    "adbc_oracle_config",
    "adbc_oracle_session",
    "adbc_postgresql_config",
    "adbc_postgresql_session",
    "adbc_snowflake_config",
    "adbc_snowflake_session",
    "adbc_sqlite_config",
    "adbc_sqlite_session",
    "adbc_teradata_config",
    "adbc_teradata_flightsql_config",
    "adbc_teradata_flightsql_session",
    "adbc_teradata_session",
    "snowflake_service",
    "xfail_if_driver_missing",
)

F = TypeVar("F", bound=Callable[..., Any])


def xfail_if_driver_missing(func: F) -> F:
    """Decorator to xfail a test if the ADBC driver shared object is missing."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if (
                "cannot open shared object file" in str(e)
                or "No module named" in str(e)
                or "Failed to import connect function" in str(e)
                or "Could not configure connection" in str(e)
            ):
                pytest.xfail(f"ADBC driver not available: {e}")
            raise

    return cast("F", wrapper)


@pytest.fixture(scope="session")
def adbc_gizmosql_connection_config(gizmosql_service: "GizmoSQLService") -> dict[str, Any]:
    """Shared connection configuration for DuckDB-backed GizmoSQL tests."""
    return _gizmosql_connection_config(gizmosql_service, backend="duckdb")


@pytest.fixture(scope="session")
def adbc_gizmosql_config(gizmosql_service: "GizmoSQLService") -> Generator[AdbcConfig, None, None]:
    """Provide an ADBC config targeting DuckDB-backed GizmoSQL."""
    config = _build_gizmosql_config(gizmosql_service, backend="duckdb")
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_gizmosql_session(adbc_gizmosql_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Create a DuckDB-backed GizmoSQL ADBC session."""
    with adbc_gizmosql_config.provide_session() as session:
        _prepare_gizmosql_test_table(session)
        yield session


@pytest.fixture(scope="session")
def adbc_gizmosql_sqlite_connection_config(gizmosql_sqlite_service: "GizmoSQLService") -> dict[str, Any]:
    """Shared connection configuration for SQLite-backed GizmoSQL tests."""
    return _gizmosql_connection_config(gizmosql_sqlite_service, backend="sqlite")


@pytest.fixture(scope="session")
def adbc_gizmosql_sqlite_config(gizmosql_sqlite_service: "GizmoSQLService") -> Generator[AdbcConfig, None, None]:
    """Provide an ADBC config targeting SQLite-backed GizmoSQL."""
    config = _build_gizmosql_config(gizmosql_sqlite_service, backend="sqlite")
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_gizmosql_sqlite_session(adbc_gizmosql_sqlite_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Create a SQLite-backed GizmoSQL ADBC session."""
    with adbc_gizmosql_sqlite_config.provide_session() as session:
        _prepare_gizmosql_test_table(session)
        yield session


@pytest.fixture(scope="session")
def adbc_postgresql_config(adbc_postgres_connection_config: dict[str, str]) -> Generator[AdbcConfig, None, None]:
    """ADBC config using the PostgreSQL driver implementation."""
    connection_config = dict(adbc_postgres_connection_config)
    connection_config["driver_name"] = "adbc_driver_postgresql"
    config = AdbcConfig(connection_config=connection_config)
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_postgresql_session(adbc_postgresql_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session with test table handling."""
    with adbc_postgresql_config.provide_session() as session:
        session.execute_script(
            """
                CREATE TABLE IF NOT EXISTS test_table_adbc (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        )
        session.execute("DELETE FROM test_table_adbc")
        yield session
        try:
            session.execute_script("DROP TABLE IF EXISTS test_table_adbc")
        except Exception:
            try:
                session.execute("ROLLBACK")
                session.execute_script("DROP TABLE IF EXISTS test_table_adbc")
            except Exception:
                pass


@pytest.fixture(scope="function")
def adbc_sqlite_config() -> Generator[AdbcConfig, None, None]:
    """ADBC configuration for SQLite tests."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_sqlite_session(adbc_sqlite_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Yield a SQLite-backed ADBC session."""
    with adbc_sqlite_config.provide_session() as session:
        session.execute_script(
            """
                CREATE TABLE IF NOT EXISTS test_table_adbc (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
        )
        session.execute("DELETE FROM test_table_adbc")
        yield session


@pytest.fixture(scope="function")
def adbc_duckdb_config() -> Generator[AdbcConfig, None, None]:
    """ADBC configuration for DuckDB tests."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_duckdb_session(adbc_duckdb_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Yield a DuckDB-backed ADBC session if the driver is available."""
    try:
        with adbc_duckdb_config.provide_session() as session:
            session.execute_script(
                """
                    CREATE TABLE IF NOT EXISTS test_table_adbc (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        value INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
            )
            session.execute("DELETE FROM test_table_adbc")
            yield session
    except Exception as exc:
        if (
            "cannot open shared object file" in str(exc)
            or "No module named" in str(exc)
            or "Failed to import connect function" in str(exc)
            or "Could not configure connection" in str(exc)
        ):
            pytest.skip("DuckDB ADBC driver unavailable")
        raise


@pytest.fixture(scope="function")
def adbc_duckdb_driver(adbc_duckdb_session: AdbcDriver) -> Generator[AdbcDriver, None, None]:
    """Alias fixture to emphasize driver usage in tests."""
    yield adbc_duckdb_session


@pytest.fixture(scope="session")
def adbc_oracle_config(oracle_23ai_service: "OracleService") -> Generator[AdbcConfig, None, None]:
    """Provide an ADBC configuration connected to Oracle 23ai via pure-Go adbc-driver-oracle."""
    pytest.importorskip("adbc_driver_oracle")
    config = AdbcConfig(
        connection_config={
            "driver_name": "oracle",
            "uri": f"{oracle_23ai_service.host}:{oracle_23ai_service.port}/{oracle_23ai_service.service_name}",
            "username": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        }
    )
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_oracle_session(adbc_oracle_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Yield an Oracle ADBC session with a clean scratch table."""
    with adbc_oracle_config.provide_session() as session:
        _drop_table_quietly(session, "DROP TABLE test_table_adbc_oracle PURGE")
        session.execute(
            """
            CREATE TABLE test_table_adbc_oracle (
                id NUMBER(10) PRIMARY KEY,
                name VARCHAR2(100) NOT NULL,
                amount NUMBER(10) NOT NULL
            )
            """
        )
        session.commit()
        try:
            yield session
        finally:
            _drop_table_quietly(session, "DROP TABLE test_table_adbc_oracle PURGE")


@pytest.fixture(scope="session")
def adbc_db2_config(db2_service: "Db2Service") -> Generator[AdbcConfig, None, None]:
    """Provide an ADBC configuration connected to IBM Db2 via pure-Go adbc-driver-db2."""
    pytest.importorskip("adbc_driver_db2")
    config = AdbcConfig(connection_config={"driver_name": "db2", "uri": db2_service.dsn})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_db2_session(adbc_db2_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Yield an IBM Db2 ADBC session with a clean scratch table."""
    with adbc_db2_config.provide_session() as session:
        _drop_table_quietly(session, "DROP TABLE test_table_adbc_db2")
        session.execute(
            """
            CREATE TABLE test_table_adbc_db2 (
                id INTEGER NOT NULL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                amount INTEGER NOT NULL
            )
            """
        )
        session.commit()
        try:
            yield session
        finally:
            _drop_table_quietly(session, "DROP TABLE test_table_adbc_db2")


@pytest.fixture(scope="session")
def snowflake_service(
    container_service: "ContainerService",
    snowflake_image: str,
    snowflake_user: str,
    snowflake_password: str,
    snowflake_account: str,
    snowflake_database: str,
    snowflake_schema: str,
    snowflake_warehouse: str,
    snowflake_role: str,
    xdist_snowflake_isolation_level: "XdistIsolationLevel",
) -> Generator[SnowflakeService, None, None]:
    """Start Embucket with a Snowflake REST wire-compatibility proxy for gosnowflake ADBC."""
    worker_num = get_xdist_worker_num()
    container_name = (
        f"snowflake_{worker_num}"
        if worker_num is not None and xdist_snowflake_isolation_level == "server"
        else "snowflake"
    )
    transient = worker_num is not None and xdist_snowflake_isolation_level == "server"
    with (
        container_service.run(
            image=snowflake_image,
            name=container_name,
            container_port=SNOWFLAKE_HTTP_PORT,
            env={"EMBUCKET_PORT": str(SNOWFLAKE_HTTP_PORT), "NO_COLOR": "true"},
            check=_is_embucket_ready,
            timeout=90,
            pause=0.5,
            transient=transient,
        ) as service,
        _provide_embucket_snowflake_proxy(service.host, service.port) as proxy_port,
    ):
        yield SnowflakeService(
            container=service.container,
            host="127.0.0.1",
            port=proxy_port,
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            database=snowflake_database,
            schema=snowflake_schema,
            warehouse=snowflake_warehouse,
            role=snowflake_role,
        )


@pytest.fixture(scope="session")
def adbc_snowflake_config(snowflake_service: SnowflakeService) -> Generator[AdbcConfig, None, None]:
    """Provide an ADBC configuration connected to Embucket Snowflake emulator via adbc-driver-snowflake."""
    pytest.importorskip("adbc_driver_snowflake")
    config = AdbcConfig(
        connection_config={
            "driver": "snowflake",
            "username": snowflake_service.user,
            "password": snowflake_service.password,
            "account": snowflake_service.account,
            "database": snowflake_service.database,
            "schema": snowflake_service.schema,
            "warehouse": snowflake_service.warehouse,
            "role": snowflake_service.role,
            "protocol": "http",
            "host": snowflake_service.host,
            "port": snowflake_service.port,
            "query_tag": "sqlspec-embucket",
        }
    )
    try:
        with config.provide_session() as bootstrap_session:
            bootstrap_session.execute(
                "CREATE EXTERNAL VOLUME IF NOT EXISTS embucket_mem_vol "
                "STORAGE_LOCATIONS = ((NAME = 'mem' STORAGE_PROVIDER = 'MEMORY'))"
            )
            bootstrap_session.execute(
                f"CREATE DATABASE IF NOT EXISTS {snowflake_service.database} EXTERNAL_VOLUME = 'embucket_mem_vol'"
            )
            bootstrap_session.execute(
                f"CREATE SCHEMA IF NOT EXISTS {snowflake_service.database}.{snowflake_service.schema}"
            )
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_snowflake_session(adbc_snowflake_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Yield a Snowflake ADBC session with a clean scratch table."""
    with adbc_snowflake_config.provide_session() as session:
        _drop_table_quietly(session, "DROP TABLE IF EXISTS test_table_adbc_snowflake")
        session.execute(
            """
            CREATE TABLE test_table_adbc_snowflake (
                id INTEGER,
                name VARCHAR,
                amount INTEGER
            )
            """
        )
        try:
            yield session
        finally:
            _drop_table_quietly(session, "DROP TABLE IF EXISTS test_table_adbc_snowflake")


def _is_embucket_ready(service: "ServiceContainer") -> bool:
    """Return True when the Embucket HTTP health endpoint responds with HTTP 200."""
    return check_snowflake_health(service.host, service.port)


def _drop_table_quietly(session: AdbcDriver, drop_sql: str) -> None:
    """Best-effort table teardown with transaction recovery."""
    with contextlib.suppress(Exception):
        session.rollback()
    try:
        session.execute(drop_sql)
        with contextlib.suppress(Exception):
            session.commit()
    except Exception:
        with contextlib.suppress(Exception):
            session.rollback()


def _require_gizmosql_driver() -> None:
    """Skip when neither GizmoSQL nor FlightSQL ADBC driver package is installed."""
    if find_spec("adbc_driver_gizmosql") is None and find_spec("adbc_driver_flightsql") is None:
        pytest.skip("adbc_driver_gizmosql or adbc_driver_flightsql required")


def _gizmosql_connection_config(service: "GizmoSQLService", *, backend: str) -> dict[str, Any]:
    """Build SQLSpec GizmoSQL ADBC connection parameters from a GizmoSQLService."""
    _require_gizmosql_driver()
    return {
        "driver_name": "gizmosql",
        "uri": service.uri,
        "username": service.username,
        "password": service.password,
        "tls_skip_verify": True,
        "gizmosql_backend": backend,
    }


def _build_gizmosql_config(service: "GizmoSQLService", *, backend: str = "duckdb") -> AdbcConfig:
    """Create an AdbcConfig bound to an upstream GizmoSQLService."""
    return AdbcConfig(connection_config=_gizmosql_connection_config(service, backend=backend))


def _prepare_gizmosql_test_table(session: AdbcDriver) -> None:
    """Reset the shared GizmoSQL ADBC scratch table before a test runs."""
    try:
        session.execute("DROP TABLE IF EXISTS test_table_adbc")
    except Exception:
        pass
    session.execute(
        """
            CREATE TABLE test_table_adbc (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                value INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    )
    session.execute("DELETE FROM test_table_adbc")
    session.commit()


@pytest.fixture
def adbc_teradata_config() -> Generator[AdbcConfig, None, None]:
    """Yield a native Columnar Teradata configuration when explicitly configured."""
    uri = os.getenv("SQLSPEC_TERADATA_ADBC_URI", "").strip()
    if not uri:
        pytest.skip("SQLSPEC_TERADATA_ADBC_URI is not set")
    pytest.importorskip("adbc_driver_manager")
    config = AdbcConfig(connection_config={"driver": os.getenv("SQLSPEC_TERADATA_ADBC_DRIVER", "teradata"), "uri": uri})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def adbc_teradata_session(adbc_teradata_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Yield a native Teradata ADBC session."""
    with adbc_teradata_config.provide_session() as session:
        yield session


@pytest.fixture
def adbc_teradata_flightsql_config() -> Generator[AdbcConfig, None, None]:
    """Yield a Teradata Flight SQL configuration when explicitly configured."""
    uri = os.getenv("SQLSPEC_TERADATA_FLIGHTSQL_URI", "").strip()
    if not uri:
        pytest.skip("SQLSPEC_TERADATA_FLIGHTSQL_URI is not set")
    pytest.importorskip("adbc_driver_flightsql")
    options: dict[str, Any] = {"driver": "flightsql", "uri": uri}
    for key in ("username", "password"):
        value = os.getenv(f"SQLSPEC_TERADATA_FLIGHTSQL_{key.upper()}")
        if value:
            options[key] = value
    if os.getenv("SQLSPEC_TERADATA_FLIGHTSQL_TLS_SKIP_VERIFY", "").lower() in {"1", "true"}:
        options["db_kwargs"] = {"adbc.flight.sql.client_option.tls_skip_verify": "true"}
    config = AdbcConfig(connection_config=options, statement_config=StatementConfig(dialect="teradata"))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def adbc_teradata_flightsql_session(adbc_teradata_flightsql_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Yield a Teradata Flight SQL session."""
    with adbc_teradata_flightsql_config.provide_session() as session:
        yield session
