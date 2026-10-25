"""Test fixtures and configuration for ADBC integration tests."""

import functools
from collections.abc import Callable, Generator
from importlib.util import find_spec
from typing import TYPE_CHECKING, Any, TypeVar, cast

import pytest

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver

if TYPE_CHECKING:
    from pytest_databases.docker.gizmosql import GizmoSQLService

__all__ = (
    "adbc_duckdb_config",
    "adbc_duckdb_driver",
    "adbc_duckdb_session",
    "adbc_gizmosql_config",
    "adbc_gizmosql_connection_config",
    "adbc_gizmosql_session",
    "adbc_gizmosql_sqlite_config",
    "adbc_gizmosql_sqlite_connection_config",
    "adbc_gizmosql_sqlite_session",
    "adbc_postgresql_config",
    "adbc_postgresql_session",
    "adbc_sqlite_config",
    "adbc_sqlite_session",
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
