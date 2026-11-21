"""Unit tests for PyMySQL Cloud SQL connector integration."""

import math
import sys
from collections.abc import Callable, Generator
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pymysql
import pytest

from sqlspec.adapters.pymysql.config import PyMysqlConfig
from sqlspec.adapters.pymysql.core import calculate_connector_backoff_seconds, is_transient_connector_error
from sqlspec.adapters.pymysql.pool import PyMysqlConnectionPool
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.utils.module_loader import reset_dependency_cache

# pyright: reportPrivateUsage=false


@pytest.fixture(autouse=True)
def disable_cloud_sql_by_default() -> Generator[None, None, None]:
    """Disable Cloud SQL by default for clean test state."""
    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", False, create=True):
        yield


@pytest.fixture
def mock_cloud_sql_module() -> Generator[MagicMock, None, None]:
    """Create and register mock google.cloud.sql module."""
    mock_connector_class = MagicMock()
    mock_module = MagicMock()
    mock_module.connector.Connector = mock_connector_class

    sys.modules["google.cloud.sql"] = mock_module
    sys.modules["google.cloud.sql.connector"] = mock_module.connector
    reset_dependency_cache("google.cloud.sql.connector")

    yield mock_connector_class

    sys.modules.pop("google.cloud.sql", None)
    sys.modules.pop("google.cloud.sql.connector", None)
    reset_dependency_cache("google.cloud.sql.connector")


def test_cloud_sql_defaults_to_false() -> None:
    """Cloud SQL connector should require explicit opt-in."""
    config = PyMysqlConfig(connection_config={})

    assert config.driver_features["enable_cloud_sql"] is False


def test_cloud_sql_explicit_disable_uses_direct_pymysql_path() -> None:
    """Disabling Cloud SQL should leave the pool on the normal PyMySQL path."""
    config = PyMysqlConfig(connection_config={}, driver_features={"enable_cloud_sql": False})
    pool = config._create_pool()

    assert config.driver_features["enable_cloud_sql"] is False
    assert pool._connection_factory is None
    assert pool._connection_parameters["host"] == "localhost"
    assert pool._connection_parameters["port"] == 3306


def test_cloud_sql_missing_package_raises_error() -> None:
    """Enabling Cloud SQL without the connector package should raise."""
    with pytest.raises(MissingDependencyError, match="cloud-sql-python-connector"):
        PyMysqlConfig(
            connection_config={},
            driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance"},
        )


def test_cloud_sql_missing_instance_raises_error() -> None:
    """Cloud SQL requires an instance connection name."""
    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", True, create=True):
        with pytest.raises(
            ImproperConfigurationError, match="cloud_sql_instance required when enable_cloud_sql is True"
        ):
            PyMysqlConfig(connection_config={}, driver_features={"enable_cloud_sql": True})


@pytest.mark.parametrize("instance", ["invalid-format", "project:region:instance:extra"])
def test_cloud_sql_invalid_instance_format_raises_error(instance: str) -> None:
    """Cloud SQL instance names must be project:region:instance."""
    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", True, create=True):
        with pytest.raises(ImproperConfigurationError, match="Invalid Cloud SQL instance format"):
            PyMysqlConfig(
                connection_config={}, driver_features={"enable_cloud_sql": True, "cloud_sql_instance": instance}
            )


def test_cloud_sql_setup_strips_direct_connection_parameters(mock_cloud_sql_module: MagicMock) -> None:
    """Cloud SQL setup should remove direct network/auth params from pool kwargs."""
    mock_connector = MagicMock()
    mock_cloud_sql_module.return_value = mock_connector

    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", True, create=True):
        config = PyMysqlConfig(
            connection_config={
                "host": "127.0.0.1",
                "port": 3307,
                "unix_socket": "/cloudsql/project:region:instance",
                "bind_address": "127.0.0.1",
                "user": "testuser",
                "password": "testpass",
                "database": "testdb",
                "autocommit": True,
                "charset": "utf8mb4",
            },
            driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance"},
        )
        pool = config._create_pool()

    mock_cloud_sql_module.assert_called_once()
    assert config._get_cloud_sql_connector() is mock_connector
    assert pool._connection_factory is not None
    assert "host" not in pool._connection_parameters
    assert "port" not in pool._connection_parameters
    assert "unix_socket" not in pool._connection_parameters
    assert "bind_address" not in pool._connection_parameters
    assert "user" not in pool._connection_parameters
    assert "password" not in pool._connection_parameters
    assert "database" not in pool._connection_parameters
    assert pool._connection_parameters["autocommit"] is True
    assert pool._connection_parameters["charset"] == "utf8mb4"


def test_cloud_sql_connection_factory_calls_connector(mock_cloud_sql_module: MagicMock) -> None:
    """The pool factory should connect through google.cloud.sql.connector."""
    cloud_connection = MagicMock()
    mock_connector = MagicMock()
    mock_connector.connect.return_value = cloud_connection
    mock_cloud_sql_module.return_value = mock_connector

    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", True, create=True):
        config = PyMysqlConfig(
            connection_config={"user": "testuser", "password": "testpass", "database": "testdb", "autocommit": True},
            driver_features={
                "enable_cloud_sql": True,
                "cloud_sql_instance": "project:region:instance",
                "cloud_sql_enable_iam_auth": True,
                "cloud_sql_ip_type": "PUBLIC",
            },
        )
        pool = config._create_pool()

    with patch("sqlspec.adapters.pymysql.pool._pymysql_connect") as mock_pymysql_connect:
        connection = pool._create_connection()

    assert connection is cloud_connection
    mock_pymysql_connect.assert_not_called()
    mock_connector.connect.assert_called_once_with(
        instance_connection_string="project:region:instance",
        driver="pymysql",
        enable_iam_auth=True,
        ip_type="PUBLIC",
        autocommit=True,
        local_infile=False,
        charset="utf8mb4",
        user="testuser",
        password="testpass",
        database="testdb",
    )


def test_pool_runs_connection_create_callback_after_direct_or_factory_paths() -> None:
    """Connection creation callbacks should run after direct and factory creation."""
    direct_connection = MagicMock()
    cloud_connection = MagicMock()
    seen_connections: list[Any] = []

    def on_connection_create(connection: Any) -> None:
        seen_connections.append(connection)

    direct_pool = PyMysqlConnectionPool({"host": "localhost"}, on_connection_create=on_connection_create)
    with patch("sqlspec.adapters.pymysql.pool._pymysql_connect", return_value=direct_connection):
        assert direct_pool._create_connection() is direct_connection

    factory_pool = PyMysqlConnectionPool(
        {}, connection_factory=lambda: cloud_connection, on_connection_create=on_connection_create
    )
    assert factory_pool._create_connection() is cloud_connection

    assert seen_connections == [direct_connection, cloud_connection]


def test_cloud_sql_connector_cleanup(mock_cloud_sql_module: MagicMock) -> None:
    """Cloud SQL connector should be closed when the config closes."""
    mock_connector = MagicMock()
    mock_cloud_sql_module.return_value = mock_connector

    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", True, create=True):
        config = PyMysqlConfig(
            connection_config={},
            driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance"},
        )
        config.connection_instance = config._create_pool()
        config._close_pool()

    mock_connector.close.assert_called_once()
    assert config._get_cloud_sql_connector() is None


def _cloud_sql_pool(mock_cloud_sql_module: MagicMock, mock_connector: MagicMock, **features: Any) -> Any:
    mock_cloud_sql_module.return_value = mock_connector
    with patch("sqlspec.adapters.pymysql.config.CLOUD_SQL_CONNECTOR_INSTALLED", True, create=True):
        config = PyMysqlConfig(
            connection_config={"user": "testuser", "password": "testpass", "database": "testdb"},
            driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance", **features},
        )
        return config._create_pool()


def test_connector_retry_defaults() -> None:
    """Retry defaults are populated on PyMysqlConfig.driver_features."""
    config = PyMysqlConfig(connection_config={})

    assert config.driver_features["connector_retry_count"] == 3
    assert config.driver_features["connector_retry_delay"] == 0.5
    assert config.driver_features["connector_retry_max_delay"] == 5.0


@pytest.mark.parametrize(
    ("features", "message"),
    [
        ({"connector_retry_count": -1}, "connector_retry_count"),
        ({"connector_retry_count": True}, "connector_retry_count"),
        ({"connector_retry_count": "3"}, "connector_retry_count"),
        ({"connector_retry_delay": -0.1}, "connector_retry_delay"),
        ({"connector_retry_delay": True}, "connector_retry_delay"),
        ({"connector_retry_delay": math.nan}, "connector_retry_delay"),
        ({"connector_retry_max_delay": -1.0}, "connector_retry_max_delay"),
        ({"connector_retry_max_delay": math.inf}, "connector_retry_max_delay"),
        ({"connector_retry_delay": 4.0, "connector_retry_max_delay": 2.0}, "greater than or equal"),
    ],
)
def test_connector_retry_settings_are_validated_when_cloud_sql_is_disabled(
    features: dict[str, Any], message: str
) -> None:
    """Invalid retry settings fail at config construction even without the connector."""
    with pytest.raises(ImproperConfigurationError, match=message):
        PyMysqlConfig(connection_config={}, driver_features=features)


@pytest.mark.parametrize(
    ("make_error", "expected"),
    [
        (lambda: TimeoutError("timed out"), True),
        (lambda: ConnectionResetError("reset by peer"), True),
        (lambda: pymysql.err.OperationalError(1045, "Access denied"), False),
        (lambda: pymysql.err.InterfaceError("bad interface"), False),
        (lambda: ImproperConfigurationError("bad config"), False),
        (lambda: ValueError("bad value"), False),
        (lambda: RuntimeError("unknown"), False),
    ],
    ids=[
        "timeout",
        "conn-reset",
        "mysql-access-denied",
        "mysql-interface",
        "improper-config",
        "value-error",
        "unknown",
    ],
)
def test_is_transient_connector_error(make_error: Callable[[], BaseException], expected: bool) -> None:
    """Transport timeouts retry while MySQL, configuration, and unknown errors fail fast."""
    assert is_transient_connector_error(make_error()) is expected


def test_is_transient_connector_error_reads_google_api_errors() -> None:
    """Control-plane errors are classified by type and through wrapped causes."""
    api_exc = pytest.importorskip("google.api_core.exceptions")

    assert is_transient_connector_error(api_exc.ServiceUnavailable("503")) is True
    assert is_transient_connector_error(api_exc.PermissionDenied("denied")) is False
    wrapped = RuntimeError("refresh failed")
    wrapped.__cause__ = api_exc.DeadlineExceeded("504")
    assert is_transient_connector_error(wrapped) is True
    assert is_transient_connector_error(api_exc.RetryError("retry", cause=api_exc.PermissionDenied("denied"))) is False


def test_calculate_connector_backoff_seconds_bounds() -> None:
    """Delays stay inside the jittered, capped interval."""
    assert calculate_connector_backoff_seconds(0, 0.0, 5.0) == 0.0
    assert 0.25 <= calculate_connector_backoff_seconds(0, 0.5, 5.0) <= 0.5
    assert 2.5 <= calculate_connector_backoff_seconds(10, 0.5, 5.0) <= 5.0


def test_cloud_sql_connection_factory_retries_transient_errors(mock_cloud_sql_module: MagicMock) -> None:
    """A transient connector error is retried with a delay and the connection is returned."""
    cloud_connection = MagicMock()
    mock_connector = MagicMock()
    mock_connector.connect.side_effect = [TimeoutError("timed out"), ConnectionResetError("reset"), cloud_connection]
    pool = _cloud_sql_pool(
        mock_cloud_sql_module, mock_connector, connector_retry_delay=0.2, connector_retry_max_delay=1.0
    )

    with patch("sqlspec.adapters.pymysql.config.time.sleep") as mock_sleep:
        connection = pool._create_connection()

    assert connection is cloud_connection
    assert mock_connector.connect.call_count == 3
    delays = [call.args[0] for call in mock_sleep.call_args_list]
    assert len(delays) == 2
    assert 0.1 <= delays[0] <= 0.2
    assert 0.2 <= delays[1] <= 0.4


@pytest.mark.parametrize("retry_count", [0, 2])
def test_cloud_sql_connection_factory_stops_after_retry_count(
    mock_cloud_sql_module: MagicMock, retry_count: int
) -> None:
    """A persistent transient error is raised after the configured number of retries."""
    mock_connector = MagicMock()
    mock_connector.connect.side_effect = TimeoutError("still timing out")
    pool = _cloud_sql_pool(
        mock_cloud_sql_module,
        mock_connector,
        connector_retry_count=retry_count,
        connector_retry_delay=0.0,
        connector_retry_max_delay=0.0,
    )

    with (
        patch("sqlspec.adapters.pymysql.config.time.sleep") as mock_sleep,
        pytest.raises(Exception, match="timing out"),
    ):
        pool._create_connection()

    assert mock_connector.connect.call_count == retry_count + 1
    mock_sleep.assert_not_called()


def test_cloud_sql_connection_factory_does_not_retry_mysql_errors(mock_cloud_sql_module: MagicMock) -> None:
    """A MySQL authentication error is raised on the first attempt."""
    mock_connector = MagicMock()
    mock_connector.connect.side_effect = pymysql.err.OperationalError(1045, "Access denied")
    pool = _cloud_sql_pool(mock_cloud_sql_module, mock_connector)

    with patch("sqlspec.adapters.pymysql.config.time.sleep") as mock_sleep, pytest.raises(Exception, match="1045"):
        pool._create_connection()

    assert mock_connector.connect.call_count == 1
    mock_sleep.assert_not_called()


class _StatusError(Exception):
    """Error exposing a gRPC-style status through ``grpc_status_code`` or ``code()``."""

    def __init__(self, status: Any = None, *, attribute: Any = None, code_requires_argument: bool = False) -> None:
        super().__init__("status")
        self._status = status
        self._code_requires_argument = code_requires_argument
        if attribute is not None:
            self.grpc_status_code = attribute

    def code(self, *args: Any) -> Any:
        if self._code_requires_argument and not args:
            msg = "code() requires an argument"
            raise TypeError(msg)
        return self._status


@pytest.mark.parametrize(
    ("make_error", "expected"),
    [
        (lambda: _StatusError(attribute="UNAVAILABLE"), True),
        (lambda: _StatusError("StatusCode.PERMISSION_DENIED"), False),
        (lambda: _StatusError("NOT_A_KNOWN_STATUS"), False),
        (lambda: _StatusError(14), False),
        (lambda: _StatusError(code_requires_argument=True), False),
    ],
    ids=["attribute-string", "dotted-permanent", "unknown-name", "numeric-status", "code-needs-argument"],
)
def test_is_transient_connector_error_reads_status_shapes(
    make_error: Callable[[], BaseException], expected: bool
) -> None:
    """Status names are read from an attribute or ``code()`` and unknown shapes are not retried."""
    assert is_transient_connector_error(make_error()) is expected


def test_is_transient_connector_error_follows_implicit_context() -> None:
    """An implicit ``__context__`` is followed unless the raise suppressed it."""
    implicit = RuntimeError("wrapper")
    implicit.__context__ = TimeoutError("inner timeout")
    assert is_transient_connector_error(implicit) is True

    suppressed = RuntimeError("wrapper")
    suppressed.__context__ = TimeoutError("inner timeout")
    suppressed.__suppress_context__ = True
    assert is_transient_connector_error(suppressed) is False


def test_is_transient_connector_error_without_optional_modules() -> None:
    """Built-in and status-based checks still work when no connector libraries are loaded."""
    absent = dict.fromkeys((
        "google.cloud.sql.connector.exceptions",
        "google.api_core.exceptions",
        "google.auth.exceptions",
        "aiohttp",
    ))
    with patch.dict(sys.modules, absent):
        assert is_transient_connector_error(TimeoutError("timed out")) is True
        assert is_transient_connector_error(_StatusError("UNAVAILABLE")) is True
        assert is_transient_connector_error(RuntimeError("unknown")) is False


def test_is_transient_connector_error_reads_connector_library_errors() -> None:
    """Connector, auth, and HTTP errors are split into retryable and permanent failures."""
    cloud_sql_exc = pytest.importorskip("google.cloud.sql.connector.exceptions")
    auth_exc = pytest.importorskip("google.auth.exceptions")
    aiohttp = pytest.importorskip("aiohttp")

    assert is_transient_connector_error(cloud_sql_exc.ClosedConnectorError("closed")) is False
    assert is_transient_connector_error(cloud_sql_exc.ResourceExhaustedError("busy", "p:r:i", None)) is True
    exhausted_permanent = cloud_sql_exc.ResourceExhaustedError("busy", "p:r:i", ValueError("bad value"))
    assert is_transient_connector_error(exhausted_permanent) is False

    assert is_transient_connector_error(auth_exc.TransportError("metadata timeout")) is True
    assert is_transient_connector_error(auth_exc.DefaultCredentialsError("missing creds")) is False
    assert is_transient_connector_error(auth_exc.RefreshError("transient", retryable=True)) is True
    assert is_transient_connector_error(auth_exc.RefreshError("invalid_grant")) is False

    def response_error(status: int) -> BaseException:
        return cast(
            "BaseException",
            aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=status, message="http"),
        )

    assert is_transient_connector_error(response_error(503)) is True
    assert is_transient_connector_error(response_error(403)) is False
    assert is_transient_connector_error(aiohttp.ServerDisconnectedError("disconnected")) is True
    assert is_transient_connector_error(aiohttp.ServerFingerprintMismatch(b"a", b"b", "host", 443)) is False


def test_cloud_sql_connection_factory_requires_initialized_connector(mock_cloud_sql_module: MagicMock) -> None:
    """Calling the factory after the connector is closed raises a configuration error."""
    pool = _cloud_sql_pool(mock_cloud_sql_module, MagicMock())
    factory = pool._connection_factory
    assert factory is not None
    factory._config._cloud_sql_connector = None

    with pytest.raises(ImproperConfigurationError, match="connector is not initialized"):
        factory()
