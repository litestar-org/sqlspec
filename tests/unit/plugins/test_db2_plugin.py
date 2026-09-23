"""Unit tests for the custom pytest-databases Db2 Docker service plugin."""

from unittest.mock import MagicMock, patch

from pytest_databases.types import ServiceContainer

from tests.plugins.db2 import Db2Service, _provide_db2_service, db2_connection_config, db2_responsive


def test_db2_service_dataclass() -> None:
    """Verify Db2Service inherits from ServiceContainer and stores connection attributes."""
    service = Db2Service(
        host="127.0.0.1",
        port=50000,
        container=MagicMock(),
        user="db2inst1",
        password="secretpassword",
        database="testdb",
    )
    assert isinstance(service, ServiceContainer)
    assert service.host == "127.0.0.1"
    assert service.port == 50000
    assert service.user == "db2inst1"
    assert service.password == "secretpassword"
    assert service.database == "testdb"


def test_db2_connection_config_helper() -> None:
    """Verify db2_connection_config transforms Db2Service into normalized config dict."""
    service = Db2Service(
        host="localhost",
        port=50001,
        container=MagicMock(),
        user="customuser",
        password="custompassword",
        database="proddb",
    )
    unwrapped = getattr(db2_connection_config, "__wrapped__", db2_connection_config)
    config_dict = unwrapped(service)
    assert config_dict == {
        "database": "proddb",
        "hostname": "localhost",
        "port": 50001,
        "username": "customuser",
        "password": "custompassword",
    }


def test_db2_default_fixtures(db2_image: str, db2_database: str, db2_user: str, db2_password: str) -> None:
    """Verify default fixture values for Db2 Docker service."""
    assert db2_image == "icr.io/db2_community/db2:11.5.9.0"
    assert db2_database == "testdb"
    assert db2_user == "db2inst1"
    assert db2_password == "password"


def test_db2_responsive_socket_failure() -> None:
    """Verify db2_responsive returns False when socket connection fails."""
    with patch("socket.create_connection", side_effect=OSError("Connection refused")):
        assert db2_responsive("127.0.0.1", 50000, "testdb", "user", "pass") is False


def test_db2_responsive_socket_success_without_driver() -> None:
    """Verify db2_responsive returns True when socket succeeds and ibm_db_dbi is absent."""
    with patch("socket.create_connection", return_value=MagicMock()):
        with patch.dict("sys.modules", {"ibm_db_dbi": None}):
            assert db2_responsive("127.0.0.1", 50000, "testdb", "user", "pass") is True


def test_provide_db2_service_runs_container() -> None:
    """Verify _provide_db2_service configures DockerService.run with expected parameters."""
    mock_container_service = MagicMock(spec=ServiceContainer)
    mock_container_service.host = "10.0.0.1"
    mock_container_service.port = 50000
    mock_container_service.container = MagicMock()

    mock_docker_service = MagicMock()
    mock_docker_service.run.return_value.__enter__.return_value = mock_container_service

    with _provide_db2_service(
        docker_service=mock_docker_service,
        image="custom/db2:11.5",
        name="test_db2_instance",
        database="app_db",
        user="test_user",
        password="test_password",
    ) as service:
        assert isinstance(service, Db2Service)
        assert service.host == "10.0.0.1"
        assert service.port == 50000
        assert service.database == "app_db"
        assert service.user == "test_user"
        assert service.password == "test_password"

    mock_docker_service.run.assert_called_once()
    kwargs = mock_docker_service.run.call_args.kwargs
    assert kwargs["image"] == "custom/db2:11.5"
    assert kwargs["container_port"] == 50000
    assert kwargs["timeout"] == 180
    assert kwargs["env"]["DB2INSTANCE"] == "test_user"
    assert kwargs["env"]["DB2INST1_PASSWORD"] == "test_password"
    assert kwargs["env"]["DBNAME"] == "app_db"
