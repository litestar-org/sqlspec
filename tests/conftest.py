import logging
import os
import warnings

warnings.filterwarnings(
    "ignore", message="You are using a Python version.*which Google will stop supporting", category=FutureWarning
)

from collections.abc import Generator  # noqa: E402
from pathlib import Path  # noqa: E402

import pytest  # noqa: E402


def is_compiled() -> bool:
    """Detect if sqlspec driver modules are mypyc-compiled.

    Returns:
        True when the driver modules have been compiled with mypyc.
    """
    try:
        from sqlspec.driver import _sync

        return hasattr(_sync, "__file__") and (_sync.__file__ or "").endswith(".so")
    except ImportError:
        return False


# Marker for tests incompatible with mypyc-compiled base classes.
# These tests create interpreted subclasses of compiled bases, which
# can trigger GC conflicts during pytest error reporting.
requires_interpreted = pytest.mark.skipif(
    is_compiled(), reason="Test uses interpreted subclass of compiled base (mypyc GC conflict)"
)


pytest_plugins = [
    "pytest_databases.docker.postgres",
    "pytest_databases.docker.oracle",
    "pytest_databases.docker.mysql",
    "pytest_databases.docker.bigquery",
    "pytest_databases.docker.spanner",
    "pytest_databases.docker.cockroachdb",
]

pytestmark = pytest.mark.anyio
here = Path(__file__).parent


@pytest.fixture(scope="session", autouse=True)
def disable_spanner_builtin_metrics() -> "Generator[None, None, None]":
    """Disable Spanner built-in metrics export during tests."""
    if os.getenv("SPANNER_DISABLE_BUILTIN_METRICS") is None:
        os.environ["SPANNER_DISABLE_BUILTIN_METRICS"] = "true"
        yield
        os.environ.pop("SPANNER_DISABLE_BUILTIN_METRICS", None)
    else:
        yield


@pytest.fixture(scope="session", autouse=True)
def suppress_noisy_test_loggers() -> "Generator[None, None, None]":
    """Lower especially noisy library loggers during test runs."""
    overrides = {
        "httpx": logging.WARNING,
        "httpcore": logging.WARNING,
        "mysql.connector": logging.WARNING,
        "asyncmy": logging.ERROR,
        "sqlspec.migrations.tracker": logging.WARNING,
    }
    original_levels = {name: logging.getLogger(name).level for name in overrides}
    for name, level in overrides.items():
        logging.getLogger(name).setLevel(level)
    try:
        yield
    finally:
        for name, level in original_levels.items():
            logging.getLogger(name).setLevel(level)


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add custom pytest command line options."""
    parser.addoption(
        "--run-bigquery-tests",
        action="store_true",
        default=False,
        help="Run BigQuery integration tests locally (enabled by default in CI; otherwise requires SQLSPEC_ENABLE_BIGQUERY_TESTS=1)",
    )
    parser.addoption(
        "--run-spanner-tests",
        action="store_true",
        default=False,
        help="Run Spanner integration tests locally (enabled by default in CI; otherwise requires SQLSPEC_ENABLE_SPANNER_TESTS=1)",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Apply environment-sensitive collection skips."""
    bigquery_enabled = os.getenv("CI") == "true" or bool(
        config.getoption("--run-bigquery-tests", default=False) and os.getenv("SQLSPEC_ENABLE_BIGQUERY_TESTS") == "1"
    )
    spanner_enabled = os.getenv("CI") == "true" or bool(
        config.getoption("--run-spanner-tests", default=False) and os.getenv("SQLSPEC_ENABLE_SPANNER_TESTS") == "1"
    )
    skip_bigquery = pytest.mark.skip(
        reason="BigQuery integration tests run by default in CI; local runs require SQLSPEC_ENABLE_BIGQUERY_TESTS=1 and --run-bigquery-tests"
    )
    skip_spanner = pytest.mark.skip(
        reason="Spanner integration tests run by default in CI; local runs require SQLSPEC_ENABLE_SPANNER_TESTS=1 and --run-spanner-tests"
    )
    for item in items:
        item_path = str(getattr(item, "path", getattr(item, "fspath", "")))
        if "tests/integration/adapters/bigquery" in item_path and not bigquery_enabled:
            item.add_marker(skip_bigquery)
        if "tests/integration/adapters/spanner" in item_path and not spanner_enabled:
            item.add_marker(skip_spanner)

    if not is_compiled():
        return

    skip_adbc = pytest.mark.skip(reason="Skip ADBC tests when running against mypyc-compiled modules.")
    skip_compiled = pytest.mark.skip(
        reason="Skip tests that rely on interpreted subclasses or mocks of compiled driver bases."
    )
    for item in items:
        item_path = str(getattr(item, "path", getattr(item, "fspath", "")))
        if item.get_closest_marker("adbc") is not None or "tests/integration/adapters/adbc" in item_path:
            item.add_marker(skip_adbc)
            continue
        if (
            "tests/unit/adapters/" in item_path
            or "tests/unit/driver/" in item_path
            or item_path.endswith("tests/unit/config/test_storage_capabilities.py")
            or "tests/unit/observability/" in item_path
        ):
            item.add_marker(skip_compiled)
            continue


@pytest.fixture
def anyio_backend() -> str:
    """Configure AnyIO to use asyncio backend only.

    Disables trio backend to prevent duplicate test runs and compatibility issues
    with pytest-xdist parallel execution.
    """
    return "asyncio"


@pytest.fixture(autouse=True)
def disable_sync_to_thread_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LITESTAR_WARN_IMPLICIT_SYNC_TO_THREAD", "0")


@pytest.fixture(autouse=True)
def clear_sql_caches() -> Generator[None, None, None]:
    """Clear SQL caches before each test to ensure isolation."""
    from sqlspec.core.cache import clear_all_caches

    clear_all_caches()
    yield
    clear_all_caches()
