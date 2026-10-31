"""Root pytest configuration and shared test hooks."""

import importlib.machinery
import logging
import os
import warnings
from collections.abc import Generator
from pathlib import Path

import pytest

from sqlspec.core.cache import clear_all_caches
from tests.fixtures.modules import module_is_available as _module_is_available

warnings.filterwarnings(
    "ignore", message="You are using a Python version.*which Google will stop supporting", category=FutureWarning
)

for _logger_name, _level in {
    "httpx": logging.WARNING,
    "httpcore": logging.WARNING,
    "mysql.connector": logging.WARNING,
    "asyncmy": logging.ERROR,
    "sqlspec.migrations.tracker": logging.WARNING,
    "psycopg.pool": logging.ERROR,
}.items():
    logging.getLogger(_logger_name).setLevel(_level)

pytestmark = pytest.mark.anyio
here = Path(__file__).parent

COMPILED_EXTENSION_SUFFIXES: tuple[str, ...] = tuple(
    dict.fromkeys((*importlib.machinery.EXTENSION_SUFFIXES, ".so", ".pyd"))
)
_TRUTHY_ENV_VALUES: frozenset[str] = frozenset({"1", "true", "yes", "on"})


def is_compiled() -> bool:
    """Detect if sqlspec driver modules are mypyc-compiled.

    Returns:
        True when the driver modules have been compiled with mypyc.
    """
    try:
        from sqlspec.driver import _sync

        module_file = getattr(_sync, "__file__", None) or ""
        return bool(module_file and module_file.endswith(COMPILED_EXTENSION_SUFFIXES))
    except ImportError:
        return False


def pytest_report_header(config: pytest.Config) -> list[str]:
    """Report driver module origin and compilation status in pytest header."""
    _ = config
    try:
        from sqlspec.driver import _sync

        module_file = getattr(_sync, "__file__", None) or "unknown"
        status = "compiled" if is_compiled() else "interpreted"
        return [f"sqlspec driver: {status} ({module_file})"]
    except ImportError as err:
        return [f"sqlspec driver: unavailable ({err})"]


requires_interpreted = pytest.mark.skipif(
    is_compiled(), reason="Test uses interpreted subclass of compiled base (mypyc GC conflict)"
)
requires_patchable_internals = pytest.mark.skipif(
    is_compiled(), reason="Test patches module internals that mypyc binds at compile time"
)


def should_run_optional_cloud_suite(cli_enabled: bool, env_var: str) -> bool:
    """Return True when an optional cloud emulator suite is enabled via CI, CLI flag, or env var."""
    return os.getenv("CI") == "true" or bool(cli_enabled) or _env_flag_enabled(env_var)


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    """Skip optional adapter trees before importing unavailable dependencies."""
    _ = config
    path = str(collection_path)
    if "tests/unit/adapters/test_mssql_python" in path and not _module_is_available("mssql_python"):
        return True
    if "tests/unit/adapters/test_spanner" in path and not _module_is_available("google.cloud.spanner_v1"):
        return True
    if "tests/integration/adapters/oracle/oracledb" in path and not _module_is_available("oracledb"):
        return True
    return None


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add custom pytest command line options."""
    parser.addoption(
        "--run-bigquery-tests",
        action="store_true",
        default=False,
        help="Run BigQuery integration tests locally (enabled by default in CI; local runs accept --run-bigquery-tests or SQLSPEC_ENABLE_BIGQUERY_TESTS=1)",
    )
    parser.addoption(
        "--run-spanner-tests",
        action="store_true",
        default=False,
        help="Run Spanner integration tests locally (enabled by default in CI; local runs accept --run-spanner-tests or SQLSPEC_ENABLE_SPANNER_TESTS=1)",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Apply environment-sensitive collection skips."""
    bigquery_enabled = should_run_optional_cloud_suite(
        bool(config.getoption("--run-bigquery-tests", default=False)), "SQLSPEC_ENABLE_BIGQUERY_TESTS"
    )
    spanner_enabled = should_run_optional_cloud_suite(
        bool(config.getoption("--run-spanner-tests", default=False)), "SQLSPEC_ENABLE_SPANNER_TESTS"
    )
    skip_bigquery = pytest.mark.skip(
        reason="BigQuery integration tests run by default in CI; local runs accept --run-bigquery-tests or SQLSPEC_ENABLE_BIGQUERY_TESTS=1"
    )
    skip_spanner = pytest.mark.skip(
        reason="Spanner integration tests run by default in CI; local runs accept --run-spanner-tests or SQLSPEC_ENABLE_SPANNER_TESTS=1"
    )
    for item in items:
        item_path = str(getattr(item, "path", getattr(item, "fspath", "")))
        is_bigquery = (
            "tests/integration/adapters/bigquery" in item_path or item.get_closest_marker("bigquery") is not None
        )
        is_spanner = "tests/integration/adapters/spanner" in item_path or item.get_closest_marker("spanner") is not None
        if is_bigquery and not bigquery_enabled:
            item.add_marker(skip_bigquery)
        if is_spanner and not spanner_enabled:
            item.add_marker(skip_spanner)

    if not is_compiled():
        return

    skip_adbc = pytest.mark.skip(reason="Skip ADBC tests when running against mypyc-compiled modules.")
    for item in items:
        if item.get_closest_marker("adbc") is not None:
            item.add_marker(skip_adbc)


@pytest.fixture(scope="session", autouse=True)
def disable_spanner_builtin_metrics() -> Generator[None, None, None]:
    """Disable Spanner built-in metrics export during tests."""
    if os.getenv("SPANNER_DISABLE_BUILTIN_METRICS") is None:
        os.environ["SPANNER_DISABLE_BUILTIN_METRICS"] = "true"
        yield
        os.environ.pop("SPANNER_DISABLE_BUILTIN_METRICS", None)
    else:
        yield


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Session-scoped asyncio-only anyio backend.

    Trio is not tested. Session scope is required so session-scoped
    async fixtures (pool configs, etc.) share a single event loop.
    """
    return "asyncio"


@pytest.fixture(autouse=True)
def disable_sync_to_thread_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    """Suppress Litestar implicit sync-to-thread warnings during tests."""
    monkeypatch.setenv("LITESTAR_WARN_IMPLICIT_SYNC_TO_THREAD", "0")


@pytest.fixture(autouse=True)
def clear_sql_caches() -> Generator[None, None, None]:
    """Clear SQL caches before each test to ensure isolation."""
    clear_all_caches()
    yield
    clear_all_caches()


def _env_flag_enabled(name: str) -> bool:
    """Return True when an environment variable holds a truthy opt-in value."""
    return os.getenv(name, "").strip().lower() in _TRUTHY_ENV_VALUES
