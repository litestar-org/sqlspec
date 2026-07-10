# pyright: reportPrivateUsage=false
"""Unit tests for adapter-specific event backend factories."""

import pytest


def test_asyncpg_factory_notify_backend() -> None:
    """Asyncpg factory creates the transient notification backend."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import AsyncpgEventsBackend, create_event_backend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "notify", {})

    assert isinstance(backend, AsyncpgEventsBackend)
    assert backend.backend_name == "notify"
    assert backend.supports_sync is False
    assert backend.supports_async is True


def test_asyncpg_factory_notify_queue_backend() -> None:
    """Asyncpg factory creates hybrid durable backend."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import AsyncpgHybridEventsBackend, create_event_backend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "notify_queue", {})

    assert isinstance(backend, AsyncpgHybridEventsBackend)
    assert backend.backend_name == "notify_queue"
    assert backend._ensure_hub()._backend_name == "notify_queue"


def test_asyncpg_factory_unknown_backend_returns_none() -> None:
    """Asyncpg factory returns None for unknown backend names."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import create_event_backend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "unknown_backend", {})

    assert backend is None


def test_asyncpg_factory_rejects_single_connection_listener_pool() -> None:
    """Asyncpg native listeners reserve capacity for a publisher session."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import create_event_backend
    from sqlspec.exceptions import ImproperConfigurationError

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test", "max_size": 1})

    with pytest.raises(ImproperConfigurationError, match="max_size >= 2"):
        create_event_backend(config, "notify", {})


def test_asyncpg_factory_passes_extension_settings() -> None:
    """Asyncpg factory passes extension settings to hybrid backend."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import AsyncpgHybridEventsBackend, create_event_backend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "notify_queue", {"queue_table": "custom_queue", "lease_seconds": 60})
    assert isinstance(backend, AsyncpgHybridEventsBackend)
    assert backend._queue._table_name == "custom_queue"
    assert backend._queue._lease_seconds == 60


def test_psycopg_factory_notify_async() -> None:
    """Psycopg factory creates the notification backend for async config."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import PsycopgAsyncEventsBackend, create_event_backend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = create_event_backend(config, "notify", {})

    assert isinstance(backend, PsycopgAsyncEventsBackend)
    assert backend.supports_sync is False
    assert backend.supports_async is True


def test_psycopg_factory_notify_sync() -> None:
    """Psycopg factory creates the notification backend for sync config."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgSyncConfig
    from sqlspec.adapters.psycopg.events.backend import PsycopgSyncEventsBackend, create_event_backend

    config = PsycopgSyncConfig(connection_config={"dbname": "test"})
    backend = create_event_backend(config, "notify", {})

    assert isinstance(backend, PsycopgSyncEventsBackend)


def test_psycopg_factory_hybrid_backend() -> None:
    """Psycopg factory creates hybrid durable backend."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import PsycopgAsyncHybridEventsBackend, create_event_backend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = create_event_backend(config, "notify_queue", {})

    assert isinstance(backend, PsycopgAsyncHybridEventsBackend)
    assert backend.backend_name == "notify_queue"
    assert backend._ensure_hub()._backend_name == "notify_queue"


def test_psycopg_factory_unknown_returns_none() -> None:
    """Psycopg factory returns None for unknown backend names."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import create_event_backend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = create_event_backend(config, "unknown_backend", {})

    assert backend is None


def test_psycopg_factory_rejects_single_connection_listener_pool() -> None:
    """Psycopg native listeners reserve capacity for a publisher session."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import create_event_backend
    from sqlspec.exceptions import ImproperConfigurationError

    config = PsycopgAsyncConfig(connection_config={"dbname": "test", "max_size": 1})

    with pytest.raises(ImproperConfigurationError, match="max_size >= 2"):
        create_event_backend(config, "notify", {})


def test_psqlpy_factory_notify_backend() -> None:
    """Psqlpy factory creates the transient notification backend."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyEventsBackend, create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "notify", {})

    assert isinstance(backend, PsqlpyEventsBackend)
    assert backend.backend_name == "notify"
    assert backend.supports_sync is False
    assert backend.supports_async is True


def test_psqlpy_factory_hybrid_backend() -> None:
    """Psqlpy factory creates hybrid durable backend."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyHybridEventsBackend, create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "notify_queue", {})

    assert isinstance(backend, PsqlpyHybridEventsBackend)
    assert backend.backend_name == "notify_queue"
    assert backend._ensure_hub()._backend_name == "notify_queue"


def test_psqlpy_factory_hybrid_passes_settings() -> None:
    """Psqlpy hybrid backend passes extension settings to queue."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyHybridEventsBackend, create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "notify_queue", {"queue_table": "custom_events", "lease_seconds": 45})
    assert isinstance(backend, PsqlpyHybridEventsBackend)
    assert backend._queue._table_name == "custom_events"
    assert backend._queue._lease_seconds == 45


def test_psqlpy_factory_unknown_returns_none() -> None:
    """Psqlpy factory returns None for unknown backend names."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "unknown_backend", {})

    assert backend is None


def test_psqlpy_factory_rejects_single_connection_listener_pool() -> None:
    """Psqlpy native listeners reserve capacity for a publisher session."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import create_event_backend
    from sqlspec.exceptions import ImproperConfigurationError

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test", "max_db_pool_size": 1})

    with pytest.raises(ImproperConfigurationError, match="max_db_pool_size >= 2"):
        create_event_backend(config, "notify", {})


def test_oracle_factory_aq_backend() -> None:
    """Oracle factory creates aq backend."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.backend import OracleSyncAQEventBackend, create_event_backend

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    backend = create_event_backend(config, "aq", {})

    assert isinstance(backend, OracleSyncAQEventBackend)
    assert backend.backend_name == "aq"
    assert backend.supports_sync is True
    assert backend.supports_async is False


def test_oracle_factory_custom_queue_name() -> None:
    """Oracle factory accepts custom queue name."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.backend import create_event_backend

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    backend = create_event_backend(config, "aq", {"aq_queue": "MY_CUSTOM_QUEUE"})
    assert backend is not None
    assert backend._queue_name == "MY_CUSTOM_QUEUE"


def test_oracle_factory_async_config() -> None:
    """Oracle AQ backend supports async configurations."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig
    from sqlspec.adapters.oracledb.events.backend import OracleAsyncAQEventBackend, create_event_backend

    config = OracleAsyncConfig(connection_config={"dsn": "localhost/xe"})
    backend = create_event_backend(config, "aq", {})

    assert isinstance(backend, OracleAsyncAQEventBackend)
    assert backend.backend_name == "aq"
    assert backend.supports_sync is False
    assert backend.supports_async is True


def test_oracle_factory_unknown_returns_none() -> None:
    """Oracle factory returns None for unknown backend names."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.backend import create_event_backend

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    backend = create_event_backend(config, "notify", {})

    assert backend is None


def test_oracle_factory_txeventq_backend() -> None:
    """Oracle factory creates a txeventq backend (sync)."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.backend import OracleSyncTxEventQEventBackend, create_event_backend

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    backend = create_event_backend(config, "txeventq", {})

    assert isinstance(backend, OracleSyncTxEventQEventBackend)
    assert backend.backend_name == "txeventq"
    assert backend.supports_sync is True
    assert backend.supports_async is False


def test_oracle_factory_txeventq_async() -> None:
    """Oracle factory creates a txeventq backend (async)."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig
    from sqlspec.adapters.oracledb.events.backend import OracleAsyncTxEventQEventBackend, create_event_backend

    config = OracleAsyncConfig(connection_config={"dsn": "localhost/xe"})
    backend = create_event_backend(config, "txeventq", {})

    assert isinstance(backend, OracleAsyncTxEventQEventBackend)
    assert backend.backend_name == "txeventq"
    assert backend.supports_sync is False
    assert backend.supports_async is True


def test_oracle_factory_txeventq_custom_queue_name() -> None:
    """TxEventQ factory honors a custom queue name."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.backend import create_event_backend

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    backend = create_event_backend(config, "txeventq", {"aq_queue": "MY_TXEVENTQ"})

    assert backend is not None
    assert backend._queue_name == "MY_TXEVENTQ"


def test_oracle_txeventq_backend_labels_hub_with_backend_name() -> None:
    """TxEventQ backend propagates its backend_name to the shared AQ hub for logging."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.backend import OracleSyncTxEventQEventBackend, create_event_backend

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    backend = create_event_backend(config, "txeventq", {})

    assert isinstance(backend, OracleSyncTxEventQEventBackend)
    hub = backend._ensure_hub()
    assert hub._backend_name == "txeventq"


def test_oracle_txeventq_sync_backend_rejects_async_config() -> None:
    """The sync TxEventQ backend rejects an async config with a class-aware message."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig
    from sqlspec.adapters.oracledb.events.backend import OracleSyncTxEventQEventBackend
    from sqlspec.exceptions import ImproperConfigurationError

    config = OracleAsyncConfig(connection_config={"dsn": "localhost/xe"})
    with pytest.raises(ImproperConfigurationError, match="OracleSyncTxEventQEventBackend requires a sync adapter"):
        OracleSyncTxEventQEventBackend(config, {})  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]


def test_oracle_txeventq_async_backend_rejects_sync_config() -> None:
    """The async TxEventQ backend rejects a sync config with a class-aware message."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.backend import OracleAsyncTxEventQEventBackend
    from sqlspec.exceptions import ImproperConfigurationError

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    with pytest.raises(ImproperConfigurationError, match="OracleAsyncTxEventQEventBackend requires an async adapter"):
        OracleAsyncTxEventQEventBackend(config, {})  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]


def test_oracle_backend_envelope_round_trip() -> None:
    """An Oracle event envelope parses back into an equivalent EventMessage."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.events.backend import _build_envelope, _parse_message

    envelope = _build_envelope("alerts", "evt_round_trip", {"level": "high"}, {"source": "api"})
    message = _parse_message("alerts", envelope)

    assert message.event_id == "evt_round_trip"
    assert message.channel == "alerts"
    assert message.payload == {"level": "high"}
    assert message.metadata == {"source": "api"}


def test_shared_payload_max_notify_bytes_constant() -> None:
    """Shared payload module has MAX_NOTIFY_BYTES constant."""
    from sqlspec.extensions.events import _payload

    assert hasattr(_payload, "MAX_NOTIFY_BYTES")
    assert _payload.MAX_NOTIFY_BYTES == 8000


def test_shared_encode_notify_payload() -> None:
    """Shared encode_notify_payload encodes payloads correctly."""
    import json

    from sqlspec.extensions.events import encode_notify_payload

    encoded = encode_notify_payload("evt123", {"action": "test"}, {"user": "admin"})
    decoded = json.loads(encoded)

    assert decoded["event_id"] == "evt123"
    assert decoded["payload"] == {"action": "test"}
    assert decoded["metadata"] == {"user": "admin"}
    assert "published_at" in decoded


def test_shared_decode_notify_payload() -> None:
    """Shared decode_notify_payload decodes payloads correctly."""
    import json

    from sqlspec.extensions.events import decode_notify_payload

    payload = json.dumps({
        "event_id": "evt456",
        "payload": {"data": "value"},
        "metadata": {"source": "test"},
        "published_at": "2024-01-15T10:00:00+00:00",
    })

    message = decode_notify_payload("test_channel", payload)

    assert message.event_id == "evt456"
    assert message.channel == "test_channel"
    assert message.payload == {"data": "value"}
    assert message.metadata == {"source": "test"}


def test_shared_decode_notify_payload_non_dict() -> None:
    """Shared decode_notify_payload wraps non-dict payloads."""
    import json

    from sqlspec.extensions.events import decode_notify_payload

    payload = json.dumps("simple_string")
    message = decode_notify_payload("channel", payload)

    assert message.payload == {"value": "simple_string"}


def test_shared_parse_event_timestamp() -> None:
    """Shared parse_event_timestamp decodes timestamps correctly."""
    import json

    from sqlspec.extensions.events import decode_notify_payload

    payload = json.dumps({"event_id": "evt_decode", "payload": {"action": "refresh"}, "metadata": None})

    message = decode_notify_payload("alerts", payload)

    assert message.event_id == "evt_decode"
    assert message.channel == "alerts"
    assert message.payload == {"action": "refresh"}


def test_oracle_backend_build_envelope() -> None:
    """Oracle AQ backend builds correct message envelope."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.events.backend import _build_envelope

    envelope = _build_envelope("alerts", "evt_oracle", {"level": "high"}, {"source": "api"})

    assert envelope["channel"] == "alerts"
    assert envelope["event_id"] == "evt_oracle"
    assert envelope["payload"] == {"level": "high"}
    assert envelope["metadata"] == {"source": "api"}
    assert "published_at" in envelope


def test_shared_parse_timestamp_iso_string() -> None:
    """Shared parse_event_timestamp parses ISO timestamp strings."""
    from datetime import datetime

    from sqlspec.extensions.events import parse_event_timestamp

    result = parse_event_timestamp("2024-01-15T10:30:00+00:00")

    assert isinstance(result, datetime)
    assert result.tzinfo is not None


def test_shared_parse_timestamp_datetime() -> None:
    """Shared parse_event_timestamp passes through datetime objects."""
    from datetime import datetime, timezone

    from sqlspec.extensions.events import parse_event_timestamp

    now = datetime.now(timezone.utc)
    result = parse_event_timestamp(now)

    assert result is now


def test_shared_parse_timestamp_invalid() -> None:
    """Shared parse_event_timestamp returns current time for invalid timestamps."""
    from datetime import datetime

    from sqlspec.extensions.events import parse_event_timestamp

    result = parse_event_timestamp("not a timestamp")

    assert isinstance(result, datetime)


def test_shared_parse_timestamp_naive_datetime() -> None:
    """Shared parse_event_timestamp adds UTC timezone to naive datetimes."""
    from datetime import datetime, timezone

    from sqlspec.extensions.events import parse_event_timestamp

    result = parse_event_timestamp("2024-06-15T12:00:00")

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc


# Backend shutdown tests


def test_asyncpg_backend_has_shutdown() -> None:
    """Asyncpg listen_notify backend has shutdown method."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import AsyncpgEventsBackend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = AsyncpgEventsBackend(config)

    assert hasattr(backend, "shutdown")
    assert callable(backend.shutdown)


def test_asyncpg_hybrid_backend_has_shutdown() -> None:
    """Asyncpg hybrid backend has shutdown method."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import create_event_backend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "notify_queue", {})
    assert backend is not None
    assert hasattr(backend, "shutdown")
    assert callable(backend.shutdown)


@pytest.mark.anyio
async def test_asyncpg_backend_shutdown_idempotent() -> None:
    """Asyncpg backend shutdown is idempotent when no listener exists."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import AsyncpgEventsBackend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = AsyncpgEventsBackend(config)

    await backend.shutdown()
    await backend.shutdown()


@pytest.mark.anyio
async def test_asyncpg_hybrid_backend_shutdown_idempotent() -> None:
    """Asyncpg hybrid backend shutdown is idempotent when no listener exists."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import create_event_backend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "notify_queue", {})
    assert backend is not None
    await backend.shutdown()
    await backend.shutdown()


def test_psycopg_backend_has_shutdown() -> None:
    """Psycopg listen_notify backend has shutdown method."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import PsycopgAsyncEventsBackend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = PsycopgAsyncEventsBackend(config)

    assert hasattr(backend, "shutdown")
    assert callable(backend.shutdown)


def test_psycopg_hybrid_backend_has_shutdown() -> None:
    """Psycopg hybrid backend has shutdown method."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import create_event_backend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = create_event_backend(config, "notify_queue", {})
    assert backend is not None
    assert hasattr(backend, "shutdown")
    assert callable(backend.shutdown)


@pytest.mark.anyio
async def test_psycopg_backend_shutdown_idempotent() -> None:
    """Psycopg backend shutdown is idempotent when no listener exists."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import PsycopgAsyncEventsBackend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = PsycopgAsyncEventsBackend(config)

    await backend.shutdown()
    await backend.shutdown()


@pytest.mark.anyio
async def test_psycopg_hybrid_backend_shutdown_idempotent() -> None:
    """Psycopg hybrid backend shutdown is idempotent when no listener exists."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import PsycopgAsyncHybridEventsBackend, create_event_backend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = create_event_backend(config, "notify_queue", {})
    assert isinstance(backend, PsycopgAsyncHybridEventsBackend)
    await backend.shutdown()
    await backend.shutdown()


def test_psqlpy_backend_has_shutdown() -> None:
    """Psqlpy listen_notify backend has shutdown method."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyEventsBackend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = PsqlpyEventsBackend(config)

    assert hasattr(backend, "shutdown")
    assert callable(backend.shutdown)


def test_psqlpy_hybrid_backend_has_shutdown() -> None:
    """Psqlpy hybrid backend has shutdown method."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "notify_queue", {})
    assert backend is not None
    assert hasattr(backend, "shutdown")
    assert callable(backend.shutdown)


@pytest.mark.anyio
async def test_psqlpy_backend_shutdown_idempotent() -> None:
    """Psqlpy backend shutdown is idempotent when no listener exists."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyEventsBackend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = PsqlpyEventsBackend(config)

    await backend.shutdown()
    await backend.shutdown()


@pytest.mark.anyio
async def test_psqlpy_hybrid_backend_shutdown_idempotent() -> None:
    """Psqlpy hybrid backend shutdown is idempotent when no listener exists."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "notify_queue", {})
    assert backend is not None
    await backend.shutdown()
    await backend.shutdown()


def test_all_postgres_backends_have_shutdown() -> None:
    """All PostgreSQL backends have consistent shutdown method."""
    asyncpg = pytest.importorskip("asyncpg")
    psycopg = pytest.importorskip("psycopg")
    psqlpy = pytest.importorskip("psqlpy")

    from sqlspec.adapters.asyncpg.events.backend import AsyncpgEventsBackend, AsyncpgHybridEventsBackend
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyEventsBackend, PsqlpyHybridEventsBackend
    from sqlspec.adapters.psycopg.events.backend import PsycopgAsyncEventsBackend, PsycopgAsyncHybridEventsBackend

    _ = asyncpg, psycopg, psqlpy

    backend_classes = [
        AsyncpgEventsBackend,
        AsyncpgHybridEventsBackend,
        PsycopgAsyncEventsBackend,
        PsycopgAsyncHybridEventsBackend,
        PsqlpyEventsBackend,
        PsqlpyHybridEventsBackend,
    ]

    for backend_class in backend_classes:
        assert hasattr(backend_class, "shutdown"), f"{backend_class.__name__} missing shutdown"
