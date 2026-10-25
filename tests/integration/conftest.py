"""Pytest configuration and fixtures for integration tests."""

import pkgutil
from importlib.util import find_spec
from typing import TYPE_CHECKING

import pytest

import tests.integration.fixtures as fixtures_pkg

if TYPE_CHECKING:
    from pytest_databases.docker.rustfs import RustfsService

pytest_plugins = [
    "pytest_databases.docker.bigquery",
    "pytest_databases.docker.cockroachdb",
    "pytest_databases.docker.db2",
    "pytest_databases.docker.gizmosql",
    "pytest_databases.docker.mssql",
    "pytest_databases.docker.mysql",
    "pytest_databases.docker.oracle",
    "pytest_databases.docker.postgres",
    "pytest_databases.docker.rustfs",
    "pytest_databases.docker.spanner",
]

_NON_PLUGIN_FIXTURE_MODULES: frozenset[str] = frozenset({"cleanup"})

_FIXTURE_DRIVER_REQUIREMENTS: dict[str, tuple[tuple[str, ...], ...]] = {
    "adbc": (("adbc_driver_manager", "pyarrow"),),
    "adapter_cases": ((),),
    "bigquery": (("google.cloud.bigquery",),),
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
    "sqlite": ((),),
}


def pytest_configure(config: pytest.Config) -> None:
    """Import integration fixture plugins whose backing driver packages are installed."""
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
def rustfs_bucket_name(rustfs_service: "RustfsService", rustfs_default_bucket_name: str) -> str:
    """Return the verified RustFS bucket used by S3-compatible integration tests."""
    _ = rustfs_service
    return rustfs_default_bucket_name


def _package_available(package: str) -> bool:
    """Return True when a top-level or dotted package specification can be resolved."""
    try:
        return find_spec(package) is not None
    except ModuleNotFoundError:
        return False


def _requirements_satisfied(requirements: tuple[tuple[str, ...], ...]) -> bool:
    """Return True when at least one required driver package group is importable."""
    return any(all(_package_available(package) for package in group) for group in requirements)
