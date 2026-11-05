"""Pytest configuration and fixtures for integration tests."""

import pkgutil
from typing import TYPE_CHECKING
from urllib.error import HTTPError, URLError

import pytest
from pytest_databases.docker.rustfs import RustfsService
from pytest_databases.helpers import get_xdist_worker_num

import tests.integration.fixtures as fixtures_pkg
from tests.fixtures.modules import module_is_available
from tests.fixtures.rustfs import ensure_rustfs_bucket, ensure_s3_bucket

if TYPE_CHECKING:
    from collections.abc import Generator

    from pytest_databases import DockerService
    from pytest_databases.types import ServiceContainer, XdistIsolationLevel

_DATABASE_PLUGINS: tuple[str, ...] = (
    "pytest_databases.docker.azure_blob",
    "pytest_databases.docker.bigquery",
    "pytest_databases.docker.cockroachdb",
    "pytest_databases.docker.db2",
    "pytest_databases.docker.floci",
    "pytest_databases.docker.gizmosql",
    "pytest_databases.docker.mariadb",
    "pytest_databases.docker.mssql",
    "pytest_databases.docker.mysql",
    "pytest_databases.docker.oracle",
    "pytest_databases.docker.postgres",
    "pytest_databases.docker.rustfs",
    "pytest_databases.docker.snowflake",
    "pytest_databases.docker.spanner",
    "pytest_databases.docker.sqlanywhere",
)

_NON_PLUGIN_FIXTURE_MODULES: frozenset[str] = frozenset({"cleanup"})

_FIXTURE_DRIVER_REQUIREMENTS: dict[str, tuple[tuple[str, ...], ...]] = {
    "adbc": (("adbc_driver_manager", "pyarrow"),),
    "adapter_cases": ((),),
    "bigquery": (("google.cloud.bigquery",),),
    "cloud_storage": ((),),
    "cockroach": (("asyncpg",), ("psycopg", "psycopg_pool")),
    "db2": (("ibm_db",),),
    "duckdb": (("duckdb",),),
    "mssql": (("arrow_odbc", "pyarrow"), ("mssql_python",), ("pymssql",)),
    "mysql": (("aiomysql",), ("asyncmy",), ("mysql.connector",), ("pymysql",)),
    "oracle": (("oracledb",),),
    "oracle_events": (("oracledb",),),
    "postgres": (("adbc_driver_manager", "pyarrow"), ("asyncpg",), ("psqlpy",), ("psycopg", "psycopg_pool")),
    "spanner": (("google.cloud.spanner",),),
    "spanner_adk": (("google.cloud.spanner",),),
    "spanner_events": (("google.cloud.spanner",),),
    "spanner_litestar": (("google.cloud.spanner", "litestar"),),
    "sqlanywhere": (("sqlanydb",),),
    "sqlite": ((),),
}


def pytest_configure(config: pytest.Config) -> None:
    """Import integration fixture plugins whose backing driver packages are installed."""
    for plugin in _DATABASE_PLUGINS:
        if not config.pluginmanager.has_plugin(plugin):
            config.pluginmanager.import_plugin(plugin)
    ordered_modules = sorted(
        pkgutil.iter_modules(fixtures_pkg.__path__), key=lambda item: (item.name == "adapter_cases", item.name)
    )
    for module_info in ordered_modules:
        if module_info.name in _NON_PLUGIN_FIXTURE_MODULES:
            continue
        requirements = _FIXTURE_DRIVER_REQUIREMENTS.get(module_info.name)
        if requirements is None or not _requirements_satisfied(requirements):
            continue
        plugin_name = f"{fixtures_pkg.__name__}.{module_info.name}"
        if not config.pluginmanager.has_plugin(plugin_name):
            config.pluginmanager.import_plugin(plugin_name)


@pytest.fixture(scope="session")
def rustfs_service(
    docker_service: "DockerService",
    rustfs_access_key: str,
    rustfs_secret_key: str,
    rustfs_secure: bool,
    rustfs_default_bucket_name: str,
    xdist_rustfs_isolation_level: "XdistIsolationLevel",
    rustfs_image: str,
) -> "Generator[RustfsService, None, None]":
    """Start RustFS after signed bucket creation succeeds.

    Yields:
        RustfsService: Ready RustFS service with the default bucket provisioned.
    """

    def check(service: "ServiceContainer") -> bool:
        scheme = "https" if rustfs_secure else "http"
        try:
            ensure_s3_bucket(
                f"{scheme}://{service.host}:{service.port}",
                rustfs_default_bucket_name,
                access_key=rustfs_access_key,
                secret_key=rustfs_secret_key,
            )
        except HTTPError as exc:
            if exc.code == 503:
                return False
            raise
        except (URLError, ConnectionError, TimeoutError):
            return False
        return True

    worker_num = get_xdist_worker_num()
    name = "rustfs"
    transient = False
    if worker_num is not None and xdist_rustfs_isolation_level == "server":
        name = f"{name}_{worker_num}"
        transient = True

    with docker_service.run(
        image=rustfs_image,
        name=name,
        container_port=9000,
        timeout=30,
        pause=0.5,
        env={"RUSTFS_ROOT_USER": rustfs_access_key, "RUSTFS_ROOT_PASSWORD": rustfs_secret_key},
        check=check,
        transient=transient,
    ) as service:
        yield RustfsService(
            host=service.host,
            port=service.port,
            container=service.container,
            endpoint=f"{service.host}:{service.port}",
            access_key=rustfs_access_key,
            secret_key=rustfs_secret_key,
            secure=rustfs_secure,
        )


@pytest.fixture(scope="session")
def rustfs_bucket_name(rustfs_service: "RustfsService", rustfs_default_bucket_name: str) -> str:
    """Return the verified RustFS bucket used by S3-compatible integration tests."""
    return ensure_rustfs_bucket(rustfs_service, rustfs_default_bucket_name)


def _requirements_satisfied(requirements: tuple[tuple[str, ...], ...]) -> bool:
    """Return True when at least one required driver package group is importable."""
    return any(all(module_is_available(package) for package in group) for group in requirements)
