"""Test fixtures and configuration for ADBC integration tests."""

import functools
from collections.abc import Callable, Generator
from typing import TYPE_CHECKING, Any, TypeVar, cast

import pytest
from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql import dbapi as flightsql
from pytest_databases.docker.gizmosql import GizmoSQLService
from pytest_databases.docker.postgres import PostgresService
from pytest_databases.helpers import get_xdist_worker_num

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver

if TYPE_CHECKING:
    from pytest_databases._service import DockerService
    from pytest_databases.types import XdistIsolationLevel

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
            raise e

    return cast("F", wrapper)


@pytest.fixture(scope="session")
def xdist_gizmosql_isolation_level() -> "XdistIsolationLevel":
    """Use one GizmoSQL server per xdist worker."""

    return "server"


def _gizmosql_db_kwargs(username: str, password: str) -> "dict[str, str]":
    return {"username": username, "password": password, DatabaseOptions.TLS_SKIP_VERIFY.value: "true"}


def _gizmosql_connection_config(service: "GizmoSQLService", *, backend: str) -> "dict[str, Any]":
    return {
        "driver_name": "gizmosql",
        "uri": service.uri,
        "username": service.username,
        "password": service.password,
        "tls_skip_verify": True,
        "gizmosql_backend": backend,
    }


def _prepare_gizmosql_test_table(session: "AdbcDriver") -> None:
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


@pytest.fixture(scope="session")
def gizmosql_service(
    docker_service: "DockerService", gizmosql_image: str, gizmosql_username: str, gizmosql_password: str
) -> "Generator[GizmoSQLService, None, None]":
    """Run the default GizmoSQL container using the DuckDB backend."""

    def check(service: Any) -> bool:
        try:
            uri = f"grpc+tls://{service.host}:{service.port}"
            db_kwargs = _gizmosql_db_kwargs(gizmosql_username, gizmosql_password)
            with flightsql.connect(uri=uri, db_kwargs=db_kwargs, autocommit=True) as conn:
                vendor_version = conn.adbc_get_info().get("vendor_version", "").lower()
                return "duckdb" in vendor_version
        except Exception:
            return False

    worker_num = get_xdist_worker_num()
    name = "gizmosql"
    if worker_num is not None:
        name += f"_{worker_num}"

    env = {
        "DATABASE_BACKEND": "duckdb",
        "GIZMOSQL_PASSWORD": gizmosql_password,
        "GIZMOSQL_USERNAME": gizmosql_username,
        "TLS_ENABLED": "1",
    }

    with docker_service.run(
        image=gizmosql_image,
        check=check,
        container_port=31337,
        name=name,
        env=env,
        timeout=90,
        pause=1.0,
        transient=True,
    ) as service:
        yield GizmoSQLService(
            host=service.host,
            port=service.port,
            container=service.container,
            username=gizmosql_username,
            password=gizmosql_password,
        )


@pytest.fixture(scope="session")
def gizmosql_sqlite_service(
    docker_service: "DockerService", gizmosql_image: str, gizmosql_username: str, gizmosql_password: str
) -> "Generator[GizmoSQLService, None, None]":
    """Run a second GizmoSQL container using the SQLite backend."""

    def check(service: Any) -> bool:
        try:
            uri = f"grpc+tls://{service.host}:{service.port}"
            db_kwargs = _gizmosql_db_kwargs(gizmosql_username, gizmosql_password)
            with flightsql.connect(uri=uri, db_kwargs=db_kwargs, autocommit=True) as conn:
                vendor_version = conn.adbc_get_info().get("vendor_version", "").lower()
                return "sqlite" in vendor_version
        except Exception:
            return False

    worker_num = get_xdist_worker_num()
    name = "gizmosql_sqlite"
    if worker_num is not None:
        name += f"_{worker_num}"

    env = {
        "DATABASE_BACKEND": "sqlite",
        "GIZMOSQL_PASSWORD": gizmosql_password,
        "GIZMOSQL_USERNAME": gizmosql_username,
        "TLS_ENABLED": "1",
    }

    with docker_service.run(
        image=gizmosql_image,
        check=check,
        container_port=31337,
        name=name,
        env=env,
        timeout=90,
        pause=1.0,
        transient=True,
    ) as service:
        yield GizmoSQLService(
            host=service.host,
            port=service.port,
            container=service.container,
            username=gizmosql_username,
            password=gizmosql_password,
        )


@pytest.fixture(scope="session")
def adbc_gizmosql_connection_config(gizmosql_service: "GizmoSQLService") -> "dict[str, Any]":
    """Shared connection configuration for DuckDB-backed GizmoSQL tests."""

    return _gizmosql_connection_config(gizmosql_service, backend="duckdb")


@pytest.fixture(scope="session")
def adbc_gizmosql_config(adbc_gizmosql_connection_config: "dict[str, Any]") -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config targeting DuckDB-backed GizmoSQL."""

    config = AdbcConfig(connection_config=dict(adbc_gizmosql_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_gizmosql_session(adbc_gizmosql_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
    """Create a DuckDB-backed GizmoSQL ADBC session."""

    with adbc_gizmosql_config.provide_session() as session:
        _prepare_gizmosql_test_table(session)
        yield session


@pytest.fixture(scope="session")
def adbc_gizmosql_sqlite_connection_config(gizmosql_sqlite_service: "GizmoSQLService") -> "dict[str, Any]":
    """Shared connection configuration for SQLite-backed GizmoSQL tests."""

    return _gizmosql_connection_config(gizmosql_sqlite_service, backend="sqlite")


@pytest.fixture(scope="session")
def adbc_gizmosql_sqlite_config(
    adbc_gizmosql_sqlite_connection_config: "dict[str, Any]",
) -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config targeting SQLite-backed GizmoSQL."""

    config = AdbcConfig(connection_config=dict(adbc_gizmosql_sqlite_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_gizmosql_sqlite_session(adbc_gizmosql_sqlite_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
    """Create a SQLite-backed GizmoSQL ADBC session."""

    with adbc_gizmosql_sqlite_config.provide_session() as session:
        _prepare_gizmosql_test_table(session)
        yield session


@pytest.fixture(scope="session")
def adbc_postgres_connection_config(postgres_service: "PostgresService") -> "dict[str, str]":
    """Shared PostgreSQL connection configuration for ADBC tests."""

    return {
        "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    }


@pytest.fixture(scope="session")
def adbc_postgres_config(adbc_postgres_connection_config: "dict[str, str]") -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config targeting PostgreSQL."""

    config = AdbcConfig(connection_config=dict(adbc_postgres_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_sync_driver(adbc_postgres_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
    """Create an ADBC driver for data dictionary testing."""

    with adbc_postgres_config.provide_session() as session:
        yield session


@pytest.fixture(scope="session")
def adbc_postgresql_config(adbc_postgres_connection_config: "dict[str, str]") -> "Generator[AdbcConfig, None, None]":
    """ADBC config using the PostgreSQL driver implementation."""

    connection_config = dict(adbc_postgres_connection_config)
    connection_config["driver_name"] = "adbc_driver_postgresql"
    config = AdbcConfig(connection_config=connection_config)
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_postgresql_session(adbc_postgresql_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
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
        except Exception:  # pragma: no cover - defensive cleanup
            try:
                session.execute("ROLLBACK")
                session.execute_script("DROP TABLE IF EXISTS test_table_adbc")
            except Exception:
                pass


@pytest.fixture(scope="function")
def adbc_sqlite_config() -> "Generator[AdbcConfig, None, None]":
    """ADBC configuration for SQLite tests."""

    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_sqlite_session(adbc_sqlite_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
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
def adbc_duckdb_config() -> "Generator[AdbcConfig, None, None]":
    """ADBC configuration for DuckDB tests."""

    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_duckdb_session(adbc_duckdb_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
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
def adbc_duckdb_driver(adbc_duckdb_session: "AdbcDriver") -> "Generator[AdbcDriver, None, None]":
    """Alias fixture to emphasize driver usage in tests."""

    yield adbc_duckdb_session
