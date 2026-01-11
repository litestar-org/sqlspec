import pytest

from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.spanner.core import default_statement_config
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import ImproperConfigurationError
from tests.conftest import requires_interpreted

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
    assert config.connection_config["min_sessions"] == 1
    assert config.connection_config["max_sessions"] == 10


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
