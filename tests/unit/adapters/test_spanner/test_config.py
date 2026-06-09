from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest
from google.cloud.spanner_v1.pool import AbstractSessionPool, BurstyPool, FixedSizePool

from sqlspec.adapters.spanner import config as config_module
from sqlspec.adapters.spanner.config import SpannerConnectionParams, SpannerPoolParams, SpannerSyncConfig
from sqlspec.adapters.spanner.core import default_statement_config
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import ImproperConfigurationError
from tests.conftest import requires_interpreted

if TYPE_CHECKING:
    from google.api_core.client_info import ClientInfo
    from google.auth.credentials import Credentials
    from google.cloud.spanner_v1 import DirectedReadOptions, ExecuteSqlRequest
    from google.cloud.spanner_v1.transaction import DefaultTransactionOptions

pytestmark = requires_interpreted


class _DummyDriver(SyncDriverAdapterBase):
    dialect = "spanner"

    def __init__(self, connection: object, **_: object) -> None:
        super().__init__(connection=connection, statement_config=default_statement_config, driver_features={})

    def handle_database_exceptions(self):
        raise NotImplementedError

    def with_cursor(self, connection):
        return connection


def test_config_initialization() -> None:
    """Test basic configuration initialization."""
    config = SpannerSyncConfig(
        connection_config={"project": "my-project", "instance_id": "my-instance", "database_id": "my-database"}
    )
    assert config.connection_config is not None
    assert config.connection_config["project"] == "my-project"
    assert config.connection_config["instance_id"] == "my-instance"
    assert config.connection_config["database_id"] == "my-database"


def test_config_defaults() -> None:
    """Test default values."""
    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    assert config.connection_config is not None
    assert "min_sessions" not in config.connection_config
    assert config.connection_config["size"] == 10


def test_min_sessions_is_rejected() -> None:
    """Spanner's current session pool classes do not support min_sessions."""
    with pytest.raises(ImproperConfigurationError, match="min_sessions"):
        SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d", "min_sessions": 1})


def test_improper_configuration() -> None:
    """Test validation of required fields."""
    config = SpannerSyncConfig()
    with pytest.raises(ImproperConfigurationError):
        config.provide_pool()


def test_driver_features_defaults() -> None:
    """Test driver features defaults."""
    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    assert config.driver_features["enable_uuid_conversion"] is True
    assert config.driver_features["json_serializer"] is not None


def test_driver_feature_session_labels_are_routed_to_pool() -> None:
    """Legacy driver feature session labels should configure pool session labels."""
    labels = {"workload": "analytics"}
    config = SpannerSyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
        driver_features={"session_labels": labels},
    )

    pool = config.provide_pool()

    assert pool.labels == labels
    assert "session_labels" not in config.driver_features


def test_fixed_size_pool_routes_current_session_controls() -> None:
    """FixedSizePool should receive the declared Spanner session pool settings."""
    labels = {"service": "api"}
    config = SpannerSyncConfig(
        connection_config={
            "project": "p",
            "instance_id": "i",
            "database_id": "d",
            "pool_type": FixedSizePool,
            "size": 4,
            "default_timeout": 6,
            "session_labels": labels,
            "database_role": "reader",
            "max_age_minutes": 23,
        }
    )

    pool = config.provide_pool()

    assert isinstance(pool, FixedSizePool)
    assert pool.size == 4
    assert pool.default_timeout == 6
    assert pool.labels == labels
    assert pool.database_role == "reader"
    assert pool._max_age == timedelta(minutes=23)


def test_bursty_pool_uses_target_size() -> None:
    """BurstyPool should receive target_size instead of the FixedSize size key."""
    config = SpannerSyncConfig(
        connection_config={
            "project": "p",
            "instance_id": "i",
            "database_id": "d",
            "pool_type": BurstyPool,
            "target_size": 7,
        }
    )

    pool = config.provide_pool()

    assert isinstance(pool, BurstyPool)
    assert pool.target_size == 7


def test_get_database_routes_client_instance_and_database_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Current Google client, instance, and database settings should be forwarded."""

    class _FakeDatabase:
        def __init__(self, database_id: str, kwargs: dict[str, Any]) -> None:
            self.database_id = database_id
            self.kwargs = kwargs

    class _FakeInstance:
        def __init__(self, instance_id: str, kwargs: dict[str, Any]) -> None:
            self.instance_id = instance_id
            self.kwargs = kwargs
            self.database_calls: list[tuple[str, dict[str, Any]]] = []

        def database(self, database_id: str, **kwargs: Any) -> "_FakeDatabase":
            self.database_calls.append((database_id, kwargs))
            return _FakeDatabase(database_id, kwargs)

    class _FakeClient:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.instances: list[_FakeInstance] = []
            created_clients.append(self)

        def instance(self, instance_id: str, **kwargs: Any) -> _FakeInstance:
            instance = _FakeInstance(instance_id, kwargs)
            self.instances.append(instance)
            return instance

    created_clients: list[_FakeClient] = []
    monkeypatch.setattr(config_module, "Client", _FakeClient)

    client_info = object()
    query_options = object()
    directed_read_options = object()
    observability_options = object()
    default_transaction_options = object()
    client_context = object()
    client_options = {"api_endpoint": "spanner.example.test"}
    credentials = object()
    pool = cast(AbstractSessionPool, object())
    logger = object()
    encryption_config = {"kms_key_name": "projects/p/locations/l/keyRings/r/cryptoKeys/k"}

    config = SpannerSyncConfig(
        connection_config={
            "project": "p",
            "credentials": credentials,
            "client_options": client_options,
            "client_info": client_info,
            "query_options": query_options,
            "route_to_leader_enabled": False,
            "directed_read_options": directed_read_options,
            "observability_options": observability_options,
            "default_transaction_options": default_transaction_options,
            "disable_builtin_metrics": True,
            "client_context": client_context,
            "use_plain_text": True,
            "ca_certificate": "ca",
            "client_certificate": "cert",
            "client_key": "key",
            "instance_type": "cloud",
            "instance_id": "instance",
            "configuration_name": "regional-us-central1",
            "display_name": "Instance",
            "node_count": 1,
            "instance_labels": {"env": "test"},
            "database_id": "database",
            "logger": logger,
            "encryption_config": encryption_config,
            "database_role": "reader",
            "enable_drop_protection": True,
            "enable_interceptors_in_tests": True,
            "proto_descriptors": b"proto",
        },
        connection_instance=pool,
    )

    database = cast(_FakeDatabase, config.get_database())

    client = created_clients[0]
    assert client.kwargs == {
        "project": "p",
        "credentials": credentials,
        "client_options": client_options,
        "client_info": client_info,
        "query_options": query_options,
        "route_to_leader_enabled": False,
        "directed_read_options": directed_read_options,
        "observability_options": observability_options,
        "default_transaction_options": default_transaction_options,
        "disable_builtin_metrics": True,
        "client_context": client_context,
        "use_plain_text": True,
        "ca_certificate": "ca",
        "client_certificate": "cert",
        "client_key": "key",
        "instance_type": "cloud",
    }
    instance = client.instances[0]
    assert instance.instance_id == "instance"
    assert instance.kwargs == {
        "configuration_name": "regional-us-central1",
        "display_name": "Instance",
        "node_count": 1,
        "labels": {"env": "test"},
    }
    assert database.database_id == "database"
    assert database.kwargs == {
        "pool": pool,
        "logger": logger,
        "encryption_config": encryption_config,
        "database_role": "reader",
        "enable_drop_protection": True,
        "enable_interceptors_in_tests": True,
        "proto_descriptors": b"proto",
    }


def test_spanner_params_type_current_client_database_and_pool_settings() -> None:
    """Static type check coverage for modern client, database, and pool options."""
    connection_config: SpannerConnectionParams = {
        "project": "p",
        "credentials": cast("Credentials", object()),
        "client_options": {"api_endpoint": "spanner.example.test"},
        "client_info": cast("ClientInfo", object()),
        "query_options": cast("ExecuteSqlRequest.QueryOptions", object()),
        "route_to_leader_enabled": False,
        "directed_read_options": cast("DirectedReadOptions", object()),
        "observability_options": object(),
        "default_transaction_options": cast("DefaultTransactionOptions", object()),
        "disable_builtin_metrics": True,
        "client_context": {"trace": "enabled"},
        "use_plain_text": True,
        "ca_certificate": "ca",
        "client_certificate": "cert",
        "client_key": "key",
        "instance_type": "cloud",
        "instance_id": "i",
        "configuration_name": "regional-us-central1",
        "display_name": "Instance",
        "node_count": 1,
        "instance_labels": {"env": "test"},
        "database_id": "d",
        "database_role": "reader",
        "enable_drop_protection": True,
        "enable_interceptors_in_tests": True,
        "proto_descriptors": b"proto",
    }
    pool_config: SpannerPoolParams = {
        "project": "p",
        "credentials": cast("Credentials", object()),
        "client_options": {"api_endpoint": "spanner.example.test"},
        "client_info": cast("ClientInfo", object()),
        "query_options": cast("ExecuteSqlRequest.QueryOptions", object()),
        "route_to_leader_enabled": False,
        "directed_read_options": cast("DirectedReadOptions", object()),
        "observability_options": object(),
        "default_transaction_options": cast("DefaultTransactionOptions", object()),
        "disable_builtin_metrics": True,
        "client_context": {"trace": "enabled"},
        "use_plain_text": True,
        "ca_certificate": "ca",
        "client_certificate": "cert",
        "client_key": "key",
        "instance_type": "cloud",
        "instance_id": "i",
        "configuration_name": "regional-us-central1",
        "display_name": "Instance",
        "node_count": 1,
        "instance_labels": {"env": "test"},
        "database_id": "d",
        "database_role": "reader",
        "enable_drop_protection": True,
        "enable_interceptors_in_tests": True,
        "proto_descriptors": b"proto",
        "pool_type": FixedSizePool,
        "size": 4,
        "target_size": 4,
        "default_timeout": 6,
        "session_labels": {"service": "api"},
        "max_age_minutes": 23,
        "ping_interval": 300,
    }

    assert connection_config["project"] == "p"
    assert pool_config["size"] == 4


def test_provide_connection_batch_and_snapshot() -> None:
    """Ensure provide_connection selects snapshot vs transaction correctly."""
    snap_obj = object()

    class _Ctx:
        def __init__(self, val: object):
            self.val = val

        def __enter__(self):
            return self.val

        def __exit__(self, *_):
            return False

    class _Txn:
        _transaction_id = "test-txn-id"

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def commit(self):
            pass

        def rollback(self):
            pass

    class _Session:
        def create(self):
            pass

        def delete(self):
            pass

        def transaction(self):
            return _Txn()

    class _DB:
        def session(self):
            return _Session()

        def snapshot(self, multi_use: bool = False):
            return _Ctx(snap_obj)

    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    config.get_database = lambda: _DB()  # type: ignore[assignment]

    with config.provide_connection(transaction=True) as conn:
        assert isinstance(conn, _Txn)

    with config.provide_connection(transaction=False) as conn:
        assert conn is snap_obj


def test_provide_session_uses_batch_when_transaction_requested() -> None:
    """Driver should receive transaction connection when transaction=True."""

    class _Txn:
        _transaction_id = "test-txn-id"

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def commit(self):
            pass

        def rollback(self):
            pass

    class _Session:
        def create(self):
            pass

        def delete(self):
            pass

        def transaction(self):
            return _Txn()

    class _Ctx:
        def __enter__(self):
            return object()

        def __exit__(self, *_):
            return False

    class _DB:
        def session(self):
            return _Session()

        def snapshot(self, multi_use: bool = False):
            return _Ctx()

    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    config.get_database = lambda: _DB()  # type: ignore[assignment]

    with config.provide_session(transaction=True) as driver:
        assert isinstance(driver.connection, _Txn)


def test_provide_write_session_alias() -> None:
    """provide_write_session should always give a transaction-backed driver."""

    class _Txn:
        _transaction_id = "test-txn-id"

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def commit(self):
            pass

        def rollback(self):
            pass

    class _Session:
        def create(self):
            pass

        def delete(self):
            pass

        def transaction(self):
            return _Txn()

    class _Ctx:
        def __enter__(self):
            return object()

        def __exit__(self, *_):
            return False

    class _DB:
        def session(self):
            return _Session()

        def snapshot(self, multi_use: bool = False):
            return _Ctx()

    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    config.get_database = lambda: _DB()  # type: ignore[assignment]
    config.driver_type = _DummyDriver  # type: ignore[assignment,misc]

    with config.provide_write_session() as driver:
        assert isinstance(driver.connection, _Txn)


def test_create_connection_delegates_to_get_database() -> None:
    """create_connection should use get_database rather than rebuilding Database."""
    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    sentinel = object()
    get_database_call_count = 0

    class _DB:
        def snapshot(self):
            return sentinel

    def _get_database() -> _DB:
        nonlocal get_database_call_count
        get_database_call_count += 1
        return _DB()

    def _unexpected_get_client() -> object:
        raise AssertionError("create_connection should delegate to get_database")

    config.get_database = _get_database  # type: ignore[assignment]
    config._get_client = _unexpected_get_client  # type: ignore[method-assign]
    config.connection_instance = object()  # type: ignore[assignment]

    assert config.create_connection() is sentinel
    assert get_database_call_count == 1

    assert config.create_connection() is sentinel
    assert get_database_call_count == 2
