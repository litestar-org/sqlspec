"""Shared PostgreSQL-family integration fixtures."""

import contextlib
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import msgspec
import pytest
from pytest_databases.docker.postgres import PostgresService, create_worker_database, exec_psql
from pytest_databases.helpers import get_xdist_worker_num

from sqlspec.utils.module_loader import import_optional_attr

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from pytest_databases import DockerService
    from pytest_databases.types import ServiceContainer, XdistIsolationLevel

    from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
    from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
    from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver, PsqlpyPoolParams
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgPoolParams, PsycopgSyncConfig
else:
    AdbcConfig = import_optional_attr("sqlspec.adapters.adbc", "AdbcConfig")
    AdbcDriver = import_optional_attr("sqlspec.adapters.adbc", "AdbcDriver")
    AsyncpgConfig = import_optional_attr("sqlspec.adapters.asyncpg", "AsyncpgConfig")
    AsyncpgDriver = import_optional_attr("sqlspec.adapters.asyncpg", "AsyncpgDriver")
    PsqlpyConfig = import_optional_attr("sqlspec.adapters.psqlpy", "PsqlpyConfig")
    PsqlpyDriver = import_optional_attr("sqlspec.adapters.psqlpy", "PsqlpyDriver")
    PsqlpyPoolParams = import_optional_attr("sqlspec.adapters.psqlpy", "PsqlpyPoolParams")
    PsycopgAsyncConfig = import_optional_attr("sqlspec.adapters.psycopg", "PsycopgAsyncConfig")
    PsycopgPoolParams = import_optional_attr("sqlspec.adapters.psycopg", "PsycopgPoolParams")
    PsycopgSyncConfig = import_optional_attr("sqlspec.adapters.psycopg", "PsycopgSyncConfig")

__all__ = (
    "AlloydbOmniHarness",
    "adbc_postgres_config",
    "adbc_postgres_connection_config",
    "adbc_sync_driver",
    "alloydb_omni_18_service",
    "alloydb_omni_harness",
    "asyncpg_async_driver",
    "asyncpg_config",
    "asyncpg_connection_config",
    "paradedb_config_adbc",
    "paradedb_config_asyncpg",
    "paradedb_config_psqlpy",
    "paradedb_config_psycopg",
    "pgvector_config_adbc",
    "pgvector_config_asyncpg",
    "pgvector_config_psqlpy",
    "pgvector_config_psycopg",
    "psqlpy_config",
    "psqlpy_driver",
    "psqlpy_session",
    "psycopg_async_config",
    "psycopg_sync_config",
)


class AlloydbOmniHarness(msgspec.Struct, frozen=True, gc=False):
    """Cross-driver test harness for AlloyDB Omni 18 columnar and vector execution."""

    driver_mode: str
    is_async: bool
    service: PostgresService
    session_config: Any
    transaction_config: Any
    table_name: str

    async def show_session_columnar_settings(self) -> tuple[str, str]:
        """Return (google_columnar_engine.enabled, google_columnar_engine.enable_columnar_scan) in session mode."""
        if self.is_async:
            async with self.session_config.provide_session() as driver:
                enabled = await driver.select_value("SHOW google_columnar_engine.enabled")
                scan_enabled = await driver.select_value("SHOW google_columnar_engine.enable_columnar_scan")
                return str(enabled), str(scan_enabled)
        with self.session_config.provide_session() as driver:
            enabled = driver.select_value("SHOW google_columnar_engine.enabled")
            scan_enabled = driver.select_value("SHOW google_columnar_engine.enable_columnar_scan")
            return str(enabled), str(scan_enabled)

    async def verify_session_ddl_and_dml(self) -> list[dict[str, Any]]:
        """Create a scratch table, insert parameterized rows, and read them back."""
        create_sql = (
            f"CREATE TABLE {self.table_name} ("
            "id INTEGER PRIMARY KEY, "
            "label VARCHAR(64) NOT NULL, "
            "metric INTEGER NOT NULL"
            ")"
        )
        insert_sql = f"INSERT INTO {self.table_name} (id, label, metric) VALUES ($1, $2, $3)"
        select_sql = f"SELECT id, label, metric FROM {self.table_name} ORDER BY id"
        if self.is_async:
            async with self.session_config.provide_session() as driver:
                await driver.execute(create_sql)
                await driver.execute(insert_sql, (1, "alpha", 10))
                await driver.execute(insert_sql, (2, "beta", 25))
                await driver.commit()
                return cast("list[dict[str, Any]]", await driver.select(select_sql))
        with self.session_config.provide_session() as driver:
            driver.execute(create_sql)
            driver.execute(insert_sql, (1, "alpha", 10))
            driver.execute(insert_sql, (2, "beta", 25))
            driver.commit()
            return cast("list[dict[str, Any]]", driver.select(select_sql))

    async def show_transaction_columnar_transition(self) -> tuple[str, str]:
        """Return google_columnar_engine.enable_columnar_scan inside begin() and after commit()."""
        guc_sql = "SHOW google_columnar_engine.enable_columnar_scan"
        if self.is_async:
            async with self.transaction_config.provide_session() as driver:
                await driver.begin()
                try:
                    in_tx = await driver.select_value(guc_sql)
                finally:
                    await driver.commit()
                after_commit = await driver.select_value(guc_sql)
                return str(in_tx), str(after_commit)
        with self.transaction_config.provide_session() as driver:
            driver.begin()
            try:
                in_tx = driver.select_value(guc_sql)
            finally:
                driver.commit()
            after_commit = driver.select_value(guc_sql)
            return str(in_tx), str(after_commit)

    async def verify_vector_extension(self) -> tuple[str, float]:
        """Enable the AlloyDB Omni vector extension and compute L2 distance."""
        _ensure_postgres_extension(self.service, "vector")
        ext_sql = "SELECT extname FROM pg_extension WHERE extname = $1"
        dist_sql = "SELECT ('[1,2,3]'::vector <-> '[1,2,3]'::vector)::float8"
        if self.is_async:
            async with self.session_config.provide_session() as driver:
                ext_name = await driver.select_value(ext_sql, ("vector",))
                distance = await driver.select_value(dist_sql)
                return str(ext_name), float(distance)
        with self.session_config.provide_session() as driver:
            ext_name = driver.select_value(ext_sql, ("vector",))
            distance = driver.select_value(dist_sql)
            return str(ext_name), float(distance)


@pytest.fixture(scope="session")
def asyncpg_connection_config(postgres_service: PostgresService) -> dict[str, Any]:
    """Base pool configuration for AsyncPG tests."""
    return _postgres_connection_config(postgres_service)


@pytest.fixture(scope="session")
async def asyncpg_config(asyncpg_connection_config: dict[str, Any]) -> "AsyncGenerator[AsyncpgConfig, None]":
    """Provide a session-scoped AsyncpgConfig with a shared pool."""
    pytest.importorskip("asyncpg")
    config = AsyncpgConfig(connection_config=dict(asyncpg_connection_config))
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            await config.close_pool()
            config.connection_instance = None


@pytest.fixture
async def asyncpg_async_driver(asyncpg_config: "AsyncpgConfig") -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create an AsyncPG driver for integration tests."""
    pytest.importorskip("asyncpg")
    async with asyncpg_config.provide_session() as session:
        yield session


@pytest.fixture(scope="session")
def psycopg_sync_config(postgres_service: PostgresService) -> "Generator[PsycopgSyncConfig, None, None]":
    """Create a psycopg sync configuration."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    config = PsycopgSyncConfig(connection_config={"conninfo": _postgres_conninfo(postgres_service)})
    try:
        yield config
    finally:
        if config.connection_instance:
            config.close_pool()


@pytest.fixture(scope="session")
async def psycopg_async_config(postgres_service: PostgresService) -> "AsyncGenerator[PsycopgAsyncConfig, None]":
    """Create a psycopg async configuration."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    config = PsycopgAsyncConfig(
        connection_config={"conninfo": _postgres_conninfo(postgres_service), "min_size": 1, "max_size": 4}
    )
    try:
        yield config
    finally:
        try:
            if config.connection_instance:
                await config.close_pool()
            config.connection_instance = None
        except RuntimeError:
            pass


@pytest.fixture(scope="session")
async def psqlpy_config(postgres_service: PostgresService) -> "AsyncGenerator[PsqlpyConfig, None]":
    """Provide a session-scoped psqlpy configuration."""
    pytest.importorskip("psqlpy")
    config = PsqlpyConfig(connection_config={"dsn": _psqlpy_dsn(postgres_service), "max_db_pool_size": 5})
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            config.connection_instance.close()
            config.connection_instance = None


@pytest.fixture
async def psqlpy_driver(psqlpy_config: "PsqlpyConfig") -> "AsyncGenerator[PsqlpyDriver, None]":
    """Yield a raw psqlpy driver session."""
    pytest.importorskip("psqlpy")
    async with psqlpy_config.provide_session() as session:
        yield session


@pytest.fixture
async def psqlpy_session(psqlpy_config: "PsqlpyConfig") -> "AsyncGenerator[PsqlpyDriver, None]":
    """Create a psqlpy session with test table setup and cleanup."""
    pytest.importorskip("psqlpy")
    async with psqlpy_config.provide_session() as session:
        await session.execute_script(
            """
                CREATE TABLE IF NOT EXISTS test_table_psqlpy (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50)
                );
            """
        )
        try:
            yield session
        finally:
            try:
                await session.execute_script("DROP TABLE IF EXISTS test_table_psqlpy;")
            except Exception:
                pass


@pytest.fixture(scope="session")
def adbc_postgres_connection_config(postgres_service: PostgresService) -> dict[str, str]:
    """Shared PostgreSQL connection configuration for ADBC tests."""
    return {"uri": _adbc_postgres_uri(postgres_service)}


@pytest.fixture(scope="session")
def adbc_postgres_config(adbc_postgres_connection_config: dict[str, str]) -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config targeting PostgreSQL."""
    pytest.importorskip("adbc_driver_manager")
    pytest.importorskip("pyarrow")
    config = AdbcConfig(connection_config=dict(adbc_postgres_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def adbc_sync_driver(adbc_postgres_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
    """Create an ADBC driver for data dictionary testing."""
    pytest.importorskip("adbc_driver_manager")
    pytest.importorskip("pyarrow")
    with adbc_postgres_config.provide_session() as session:
        yield session


@pytest.fixture
def pgvector_config_adbc(pgvector_service: PostgresService) -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config connected to a pgvector-enabled PostgreSQL service."""
    pytest.importorskip("adbc_driver_manager")
    pytest.importorskip("pyarrow")
    _ensure_postgres_extension(pgvector_service, "vector")
    config = AdbcConfig(connection_config={"uri": _adbc_postgres_uri(pgvector_service)})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def pgvector_config_asyncpg(pgvector_service: PostgresService) -> "AsyncpgConfig":
    """Provide an asyncpg config connected to a pgvector-enabled PostgreSQL service."""
    pytest.importorskip("asyncpg")
    _ensure_postgres_extension(pgvector_service, "vector")
    return AsyncpgConfig(connection_config=_asyncpg_pool_config(pgvector_service))


@pytest.fixture
def pgvector_config_psqlpy(pgvector_service: PostgresService) -> "PsqlpyConfig":
    """Provide a psqlpy config connected to a pgvector-enabled PostgreSQL service."""
    pytest.importorskip("psqlpy")
    _ensure_postgres_extension(pgvector_service, "vector")
    return PsqlpyConfig(connection_config=PsqlpyPoolParams(dsn=_psqlpy_dsn(pgvector_service)))


@pytest.fixture
def pgvector_config_psycopg(pgvector_service: PostgresService) -> "Generator[PsycopgSyncConfig, None, None]":
    """Provide a psycopg config connected to a pgvector-enabled PostgreSQL service."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    _ensure_postgres_extension(pgvector_service, "vector")
    config = PsycopgSyncConfig(
        connection_config=PsycopgPoolParams(conninfo=_postgres_conninfo(pgvector_service), min_size=1, max_size=4)
    )
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            config.close_pool()
            config.connection_instance = None


@pytest.fixture
def paradedb_config_adbc(paradedb_service: PostgresService) -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config connected to a ParadeDB service."""
    pytest.importorskip("adbc_driver_manager")
    pytest.importorskip("pyarrow")
    config = AdbcConfig(connection_config={"uri": _adbc_postgres_uri(paradedb_service)})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def paradedb_config_asyncpg(paradedb_service: PostgresService) -> "AsyncpgConfig":
    """Provide an asyncpg config connected to a ParadeDB service."""
    pytest.importorskip("asyncpg")
    return AsyncpgConfig(connection_config=_asyncpg_pool_config(paradedb_service))


@pytest.fixture
def paradedb_config_psqlpy(paradedb_service: PostgresService) -> "PsqlpyConfig":
    """Provide a psqlpy config connected to a ParadeDB service."""
    pytest.importorskip("psqlpy")
    return PsqlpyConfig(connection_config=PsqlpyPoolParams(dsn=_psqlpy_dsn(paradedb_service)))


@pytest.fixture
def paradedb_config_psycopg(paradedb_service: PostgresService) -> "Generator[PsycopgSyncConfig, None, None]":
    """Provide a psycopg config connected to a ParadeDB service."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    config = PsycopgSyncConfig(
        connection_config=PsycopgPoolParams(conninfo=_postgres_conninfo(paradedb_service), min_size=1, max_size=4)
    )
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            config.close_pool()
            config.connection_instance = None


@pytest.fixture(scope="session")
def alloydb_omni_18_service(
    docker_service: "DockerService",
    xdist_postgres_isolation_level: "XdistIsolationLevel",
    postgres_host: str,
    postgres_user: str,
    postgres_password: str,
    alloydb_omni_18_port: int | None,
) -> "Generator[PostgresService, None, None]":
    """Provide an AlloyDB Omni 18 service with an extended initdb startup timeout.

    AlloyDB Omni preloads ``g_columnar_engine``, ``g_stats``, ``google_job_scheduler``,
    ``google_ml_integration``, and ``google_storage`` during cold ``initdb``, which
    takes ~15-20s and exceeds ``provide_postgres_service``'s 10s default timeout.
    """

    def check(service_container: "ServiceContainer") -> bool:
        exit_code, output = exec_psql(
            service_container.container,
            user=postgres_user,
            password=postgres_password,
            database="postgres",
            sql="SELECT 1",
            tuples_only=True,
        )
        return exit_code == 0 and output.strip() == b"1"

    name = "alloydb-omni-18"
    db_name = "pytest_databases"
    worker_num = get_xdist_worker_num()
    if worker_num is not None:
        suffix = f"_{worker_num}"
        if xdist_postgres_isolation_level == "server":
            name += suffix
        else:
            db_name += suffix

    with docker_service.run(
        image="gcr.io/alloydb-omni/alloydbomni:18",
        command=[
            "postgres",
            "-c",
            "google_columnar_engine.enabled=on",
            "-c",
            "google_columnar_engine.enable_columnar_scan=off",
        ],
        check=check,
        container_host=postgres_host,
        container_port=5432,
        name=name,
        env={"POSTGRES_PASSWORD": postgres_password},
        transient=xdist_postgres_isolation_level == "server",
        host_port=alloydb_omni_18_port,
        timeout=90,
        pause=0.5,
    ) as service:
        create_worker_database(service.container, user=postgres_user, password=postgres_password, db_name=db_name)
        yield PostgresService(
            host=service.host,
            port=service.port,
            container=service.container,
            database=db_name,
            user=postgres_user,
            password=postgres_password,
        )


@pytest.fixture(params=("asyncpg", "psycopg-sync", "psycopg-async", "psqlpy"))
async def alloydb_omni_harness(
    request: pytest.FixtureRequest, alloydb_omni_18_service: PostgresService
) -> "AsyncGenerator[AlloydbOmniHarness, None]":
    """Provide session-mode and transaction-pooler-mode configs targeting AlloyDB Omni 18."""
    driver_mode = str(request.param)
    _require_alloydb_driver(driver_mode)

    is_async, session_config = _build_alloydb_omni_config(
        alloydb_omni_18_service, driver_mode, driver_features={"enable_alloydb_columnar": True}
    )
    _, transaction_config = _build_alloydb_omni_config(
        alloydb_omni_18_service,
        driver_mode,
        driver_features={"enable_alloydb_columnar": True, "pooler_mode": "transaction"},
    )
    table_name = f"alloydb_col_{uuid4().hex[:8]}"

    try:
        yield AlloydbOmniHarness(
            driver_mode=driver_mode,
            is_async=is_async,
            service=alloydb_omni_18_service,
            session_config=session_config,
            transaction_config=transaction_config,
            table_name=table_name,
        )
    finally:
        await _close_postgres_config(transaction_config, driver_mode=driver_mode)
        with contextlib.suppress(Exception):
            await _drop_postgres_table(session_config, table_name, is_async=is_async)
        await _close_postgres_config(session_config, driver_mode=driver_mode)


def _require_alloydb_driver(driver_mode: str) -> None:
    if driver_mode == "asyncpg":
        pytest.importorskip("asyncpg")
        return
    if driver_mode in {"psycopg-sync", "psycopg-async"}:
        pytest.importorskip("psycopg")
        pytest.importorskip("psycopg_pool")
        return
    if driver_mode == "psqlpy":
        pytest.importorskip("psqlpy")
        return
    msg = f"Unsupported AlloyDB Omni driver mode: {driver_mode}"
    raise ValueError(msg)


def _build_alloydb_omni_config(
    service: PostgresService, driver_mode: str, *, driver_features: dict[str, Any]
) -> tuple[bool, Any]:
    if driver_mode == "asyncpg":
        conn_cfg = _postgres_connection_config(service)
        conn_cfg.update({"min_size": 1, "max_size": 3})
        return True, AsyncpgConfig(connection_config=conn_cfg, driver_features=dict(driver_features))
    if driver_mode == "psycopg-sync":
        return False, PsycopgSyncConfig(
            connection_config={"conninfo": _postgres_conninfo(service), "min_size": 1, "max_size": 3},
            driver_features=dict(driver_features),
        )
    if driver_mode == "psycopg-async":
        return True, PsycopgAsyncConfig(
            connection_config={"conninfo": _postgres_conninfo(service), "min_size": 1, "max_size": 3},
            driver_features=dict(driver_features),
        )
    if driver_mode == "psqlpy":
        return True, PsqlpyConfig(
            connection_config={"dsn": _psqlpy_dsn(service), "max_db_pool_size": 3},
            driver_features=dict(driver_features),
        )
    msg = f"Unsupported AlloyDB Omni driver mode: {driver_mode}"
    raise ValueError(msg)


async def _drop_postgres_table(config: Any, table_name: str, *, is_async: bool) -> None:
    drop_sql = f"DROP TABLE IF EXISTS {table_name}"
    if is_async:
        async with config.provide_session() as driver:
            await driver.execute(drop_sql)
            await driver.commit()
        return
    with config.provide_session() as driver:
        driver.execute(drop_sql)
        driver.commit()


async def _close_postgres_config(config: Any, *, driver_mode: str) -> None:
    if config.connection_instance is None:
        return
    if driver_mode == "psqlpy":
        config.connection_instance.close()
        config.connection_instance = None
        return
    if driver_mode == "psycopg-sync":
        config.close_pool()
        config.connection_instance = None
        return
    with contextlib.suppress(RuntimeError):
        await config.close_pool()
    config.connection_instance = None


def _postgres_connection_config(postgres_service: PostgresService) -> dict[str, Any]:
    return {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "database": postgres_service.database,
    }


def _postgres_conninfo(postgres_service: PostgresService) -> str:
    return (
        f"postgresql://{postgres_service.user}:{postgres_service.password}"
        f"@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )


def _psqlpy_dsn(postgres_service: PostgresService) -> str:
    return (
        f"postgres://{postgres_service.user}:{postgres_service.password}"
        f"@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )


def _adbc_postgres_uri(postgres_service: PostgresService) -> str:
    return _postgres_conninfo(postgres_service)


def _asyncpg_pool_config(postgres_service: PostgresService) -> dict[str, Any]:
    return _postgres_connection_config(postgres_service)


def _ensure_postgres_extension(service: PostgresService, extension: str) -> None:
    """Ensure a PostgreSQL extension is enabled inside the service database."""
    exit_code, output = exec_psql(
        service.container,
        user=service.user,
        password=service.password,
        database=service.database,
        sql=f'CREATE EXTENSION IF NOT EXISTS "{extension}"',
    )
    if exit_code != 0:
        decoded_output = output.decode("utf-8", errors="replace")
        msg = f"Failed to enable PostgreSQL extension {extension!r}: {decoded_output}"
        raise RuntimeError(msg)
