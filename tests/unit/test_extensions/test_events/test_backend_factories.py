"""Unit tests for adapter-specific event backend factories."""

import pytest


def test_asyncpg_factory_listen_notify_backend() -> None:
    """Asyncpg factory creates listen_notify backend."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import AsyncpgEventsBackend, create_event_backend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "listen_notify", {})

    assert isinstance(backend, AsyncpgEventsBackend)
    assert backend.backend_name == "listen_notify"
    assert backend.supports_sync is False
    assert backend.supports_async is True


def test_asyncpg_factory_listen_notify_durable_backend() -> None:
    """Asyncpg factory creates hybrid durable backend."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import AsyncpgHybridEventsBackend, create_event_backend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "listen_notify_durable", {})

    assert isinstance(backend, AsyncpgHybridEventsBackend)
    assert backend.backend_name == "listen_notify_durable"


def test_asyncpg_factory_unknown_backend_returns_none() -> None:
    """Asyncpg factory returns None for unknown backend names."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import create_event_backend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "unknown_backend", {})

    assert backend is None


def test_asyncpg_factory_passes_extension_settings() -> None:
    """Asyncpg factory passes extension settings to hybrid backend."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import create_event_backend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(
        config, "listen_notify_durable", {"queue_table": "custom_queue", "lease_seconds": 60}
    )

    queue = backend._queue._queue
    assert queue._table_name == "custom_queue"
    assert queue._lease_seconds == 60


def test_psycopg_factory_listen_notify_async() -> None:
    """Psycopg factory creates listen_notify backend for async config."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import PsycopgEventsBackend, create_event_backend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = create_event_backend(config, "listen_notify", {})

    assert isinstance(backend, PsycopgEventsBackend)
    assert backend.supports_sync is True
    assert backend.supports_async is True


def test_psycopg_factory_listen_notify_sync() -> None:
    """Psycopg factory creates listen_notify backend for sync config."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgSyncConfig
    from sqlspec.adapters.psycopg.events.backend import PsycopgEventsBackend, create_event_backend

    config = PsycopgSyncConfig(connection_config={"dbname": "test"})
    backend = create_event_backend(config, "listen_notify", {})

    assert isinstance(backend, PsycopgEventsBackend)


def test_psycopg_factory_hybrid_backend() -> None:
    """Psycopg factory creates hybrid durable backend."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import PsycopgHybridEventsBackend, create_event_backend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = create_event_backend(config, "listen_notify_durable", {})

    assert isinstance(backend, PsycopgHybridEventsBackend)
    assert backend.backend_name == "listen_notify_durable"


def test_psycopg_factory_unknown_returns_none() -> None:
    """Psycopg factory returns None for unknown backend names."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import create_event_backend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = create_event_backend(config, "unknown_backend", {})

    assert backend is None


def test_psqlpy_factory_listen_notify_backend() -> None:
    """Psqlpy factory creates listen_notify backend."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyEventsBackend, create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "listen_notify", {})

    assert isinstance(backend, PsqlpyEventsBackend)
    assert backend.backend_name == "listen_notify"
    assert backend.supports_sync is False
    assert backend.supports_async is True


def test_psqlpy_factory_hybrid_backend() -> None:
    """Psqlpy factory creates hybrid durable backend."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyHybridEventsBackend, create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "listen_notify_durable", {})

    assert isinstance(backend, PsqlpyHybridEventsBackend)
    assert backend.backend_name == "listen_notify_durable"


def test_psqlpy_factory_json_passthrough_not_set_by_default() -> None:
    """Psqlpy hybrid backend does not enable json_passthrough by default.

    This ensures consistency with other PostgreSQL adapters (asyncpg, psycopg)
    which also don't enable json_passthrough by default.
    """
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "listen_notify_durable", {})

    queue = backend._queue._queue
    assert queue._json_passthrough is False


def test_psqlpy_factory_json_passthrough_explicit() -> None:
    """Psqlpy hybrid backend accepts explicit json_passthrough setting."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "listen_notify_durable", {"json_passthrough": True})

    queue = backend._queue._queue
    assert queue._json_passthrough is True


def test_psqlpy_factory_unknown_returns_none() -> None:
    """Psqlpy factory returns None for unknown backend names."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "unknown_backend", {})

    assert backend is None


def test_oracle_factory_advanced_queue_backend() -> None:
    """Oracle factory creates advanced_queue backend."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.backend import OracleAQEventBackend, create_event_backend

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    backend = create_event_backend(config, "advanced_queue", {})

    assert isinstance(backend, OracleAQEventBackend)
    assert backend.backend_name == "advanced_queue"
    assert backend.supports_sync is True
    assert backend.supports_async is False


def test_oracle_factory_custom_queue_name() -> None:
    """Oracle factory accepts custom queue name."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.backend import create_event_backend

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    backend = create_event_backend(config, "advanced_queue", {"aq_queue": "MY_CUSTOM_QUEUE"})

    assert backend._queue_name == "MY_CUSTOM_QUEUE"


def test_oracle_factory_rejects_async_config() -> None:
    """Oracle AQ backend rejects async configurations."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig
    from sqlspec.adapters.oracledb.events.backend import create_event_backend

    config = OracleAsyncConfig(connection_config={"dsn": "localhost/xe"})
    backend = create_event_backend(config, "advanced_queue", {})

    assert backend is None


def test_oracle_factory_unknown_returns_none() -> None:
    """Oracle factory returns None for unknown backend names."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.config import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.backend import create_event_backend

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    backend = create_event_backend(config, "listen_notify", {})

    assert backend is None


def test_asyncpg_backend_max_notify_bytes_constant() -> None:
    """Asyncpg backend has MAX_NOTIFY_BYTES constant."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.events import backend

    assert hasattr(backend, "MAX_NOTIFY_BYTES")
    assert backend.MAX_NOTIFY_BYTES == 8000


def test_psycopg_backend_encode_payload() -> None:
    """Psycopg backend encodes payloads correctly."""
    pytest.importorskip("psycopg")
    import json

    from sqlspec.adapters.psycopg.events.backend import PsycopgEventsBackend

    encoded = PsycopgEventsBackend._encode_payload("evt123", {"action": "test"}, {"user": "admin"})
    decoded = json.loads(encoded)

    assert decoded["event_id"] == "evt123"
    assert decoded["payload"] == {"action": "test"}
    assert decoded["metadata"] == {"user": "admin"}
    assert "published_at" in decoded


def test_psycopg_backend_decode_payload() -> None:
    """Psycopg backend decodes payloads correctly."""
    pytest.importorskip("psycopg")
    import json

    from sqlspec.adapters.psycopg.events.backend import PsycopgEventsBackend

    payload = json.dumps({
        "event_id": "evt456",
        "payload": {"data": "value"},
        "metadata": {"source": "test"},
        "published_at": "2024-01-15T10:00:00+00:00",
    })

    message = PsycopgEventsBackend._decode_payload("test_channel", payload)

    assert message.event_id == "evt456"
    assert message.channel == "test_channel"
    assert message.payload == {"data": "value"}
    assert message.metadata == {"source": "test"}


def test_psycopg_backend_decode_non_dict_payload() -> None:
    """Psycopg backend wraps non-dict payloads."""
    pytest.importorskip("psycopg")
    import json

    from sqlspec.adapters.psycopg.events.backend import PsycopgEventsBackend

    payload = json.dumps("simple_string")
    message = PsycopgEventsBackend._decode_payload("channel", payload)

    assert message.payload == {"value": "simple_string"}


def test_psqlpy_backend_encode_payload() -> None:
    """Psqlpy backend encodes payloads correctly."""
    pytest.importorskip("psqlpy")
    import json

    from sqlspec.adapters.psqlpy.events.backend import PsqlpyEventsBackend

    encoded = PsqlpyEventsBackend._encode_payload("evt789", {"type": "notification"}, None)
    decoded = json.loads(encoded)

    assert decoded["event_id"] == "evt789"
    assert decoded["payload"] == {"type": "notification"}
    assert decoded["metadata"] is None


def test_psqlpy_backend_decode_payload() -> None:
    """Psqlpy backend decodes payloads correctly."""
    pytest.importorskip("psqlpy")
    import json

    from sqlspec.adapters.psqlpy.events.backend import PsqlpyEventsBackend

    payload = json.dumps({"event_id": "evt_decode", "payload": {"action": "refresh"}, "metadata": None})

    message = PsqlpyEventsBackend._decode_payload("alerts", payload)

    assert message.event_id == "evt_decode"
    assert message.channel == "alerts"
    assert message.payload == {"action": "refresh"}


def test_oracle_backend_build_envelope() -> None:
    """Oracle AQ backend builds correct message envelope."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb.events.backend import OracleAQEventBackend

    envelope = OracleAQEventBackend._build_envelope("alerts", "evt_oracle", {"level": "high"}, {"source": "api"})

    assert envelope["channel"] == "alerts"
    assert envelope["event_id"] == "evt_oracle"
    assert envelope["payload"] == {"level": "high"}
    assert envelope["metadata"] == {"source": "api"}
    assert "published_at" in envelope


def test_oracle_backend_parse_timestamp_iso_string() -> None:
    """Oracle backend parses ISO timestamp strings."""
    pytest.importorskip("oracledb")
    from datetime import datetime

    from sqlspec.adapters.oracledb.events.backend import OracleAQEventBackend

    result = OracleAQEventBackend._parse_timestamp("2024-01-15T10:30:00+00:00")

    assert isinstance(result, datetime)
    assert result.tzinfo is not None


def test_oracle_backend_parse_timestamp_datetime() -> None:
    """Oracle backend passes through datetime objects."""
    pytest.importorskip("oracledb")
    from datetime import datetime, timezone

    from sqlspec.adapters.oracledb.events.backend import OracleAQEventBackend

    now = datetime.now(timezone.utc)
    result = OracleAQEventBackend._parse_timestamp(now)

    assert result is now


def test_oracle_backend_parse_timestamp_invalid() -> None:
    """Oracle backend returns current time for invalid timestamps."""
    pytest.importorskip("oracledb")
    from datetime import datetime

    from sqlspec.adapters.oracledb.events.backend import OracleAQEventBackend

    result = OracleAQEventBackend._parse_timestamp("not a timestamp")

    assert isinstance(result, datetime)


def test_asyncpg_backend_parse_timestamp() -> None:
    """Asyncpg backend parses timestamps correctly."""
    pytest.importorskip("asyncpg")
    from datetime import datetime, timezone

    from sqlspec.adapters.asyncpg.events.backend import AsyncpgEventsBackend

    result = AsyncpgEventsBackend._parse_timestamp("2024-06-15T12:00:00")

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc


# Backend shutdown tests


def test_asyncpg_backend_has_shutdown_async() -> None:
    """Asyncpg listen_notify backend has shutdown_async method."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import AsyncpgEventsBackend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = AsyncpgEventsBackend(config)

    assert hasattr(backend, "shutdown_async")
    assert callable(backend.shutdown_async)


def test_asyncpg_hybrid_backend_has_shutdown_async() -> None:
    """Asyncpg hybrid backend has shutdown_async method."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import create_event_backend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "listen_notify_durable", {})

    assert hasattr(backend, "shutdown_async")
    assert callable(backend.shutdown_async)


@pytest.mark.anyio
async def test_asyncpg_backend_shutdown_idempotent() -> None:
    """Asyncpg backend shutdown is idempotent when no listener exists."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import AsyncpgEventsBackend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = AsyncpgEventsBackend(config)

    await backend.shutdown_async()
    await backend.shutdown_async()


@pytest.mark.anyio
async def test_asyncpg_hybrid_backend_shutdown_idempotent() -> None:
    """Asyncpg hybrid backend shutdown is idempotent when no listener exists."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.backend import create_event_backend

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "listen_notify_durable", {})

    await backend.shutdown_async()
    await backend.shutdown_async()


def test_psycopg_backend_has_shutdown_async() -> None:
    """Psycopg listen_notify backend has shutdown_async method."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import PsycopgEventsBackend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = PsycopgEventsBackend(config)

    assert hasattr(backend, "shutdown_async")
    assert callable(backend.shutdown_async)


def test_psycopg_hybrid_backend_has_shutdown_async() -> None:
    """Psycopg hybrid backend has shutdown_async method."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import create_event_backend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = create_event_backend(config, "listen_notify_durable", {})

    assert hasattr(backend, "shutdown_async")
    assert callable(backend.shutdown_async)


@pytest.mark.anyio
async def test_psycopg_backend_shutdown_idempotent() -> None:
    """Psycopg backend shutdown is idempotent when no listener exists."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import PsycopgEventsBackend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = PsycopgEventsBackend(config)

    await backend.shutdown_async()
    await backend.shutdown_async()


@pytest.mark.anyio
async def test_psycopg_hybrid_backend_shutdown_idempotent() -> None:
    """Psycopg hybrid backend shutdown is idempotent when no listener exists."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.backend import create_event_backend

    config = PsycopgAsyncConfig(connection_config={"dbname": "test"})
    backend = create_event_backend(config, "listen_notify_durable", {})

    await backend.shutdown_async()
    await backend.shutdown_async()


def test_psqlpy_backend_has_shutdown_async() -> None:
    """Psqlpy listen_notify backend has shutdown_async method."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyEventsBackend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = PsqlpyEventsBackend(config)

    assert hasattr(backend, "shutdown_async")
    assert callable(backend.shutdown_async)


def test_psqlpy_hybrid_backend_has_shutdown_async() -> None:
    """Psqlpy hybrid backend has shutdown_async method."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "listen_notify_durable", {})

    assert hasattr(backend, "shutdown_async")
    assert callable(backend.shutdown_async)


@pytest.mark.anyio
async def test_psqlpy_backend_shutdown_idempotent() -> None:
    """Psqlpy backend shutdown is idempotent when no listener exists."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyEventsBackend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = PsqlpyEventsBackend(config)

    await backend.shutdown_async()
    await backend.shutdown_async()


@pytest.mark.anyio
async def test_psqlpy_hybrid_backend_shutdown_idempotent() -> None:
    """Psqlpy hybrid backend shutdown is idempotent when no listener exists."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.backend import create_event_backend

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    backend = create_event_backend(config, "listen_notify_durable", {})

    await backend.shutdown_async()
    await backend.shutdown_async()


def test_all_postgres_backends_have_shutdown_async() -> None:
    """All PostgreSQL backends have consistent shutdown_async method."""
    asyncpg = pytest.importorskip("asyncpg")
    psycopg = pytest.importorskip("psycopg")
    psqlpy = pytest.importorskip("psqlpy")

    from sqlspec.adapters.asyncpg.events.backend import AsyncpgEventsBackend, AsyncpgHybridEventsBackend
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyEventsBackend, PsqlpyHybridEventsBackend
    from sqlspec.adapters.psycopg.events.backend import PsycopgEventsBackend, PsycopgHybridEventsBackend

    _ = asyncpg, psycopg, psqlpy

    backend_classes = [
        AsyncpgEventsBackend,
        AsyncpgHybridEventsBackend,
        PsycopgEventsBackend,
        PsycopgHybridEventsBackend,
        PsqlpyEventsBackend,
        PsqlpyHybridEventsBackend,
    ]

    for backend_class in backend_classes:
        assert hasattr(backend_class, "shutdown_async"), f"{backend_class.__name__} missing shutdown_async"
