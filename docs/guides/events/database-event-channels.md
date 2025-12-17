# Database Event Channels

SQLSpec now ships a portable event channel API that wraps native LISTEN/NOTIFY
(or tapers down to a durable queue table when a driver lacks native support).
This guide documents the queue-backed fallback delivered in
`sqlspec.extensions.events.EventChannel`, the native PostgreSQL backend, and how to enable
each option across adapters.

## Quick start

```python
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

spec = SQLSpec()
config = SqliteConfig(
    connection_config={"database": ":memory:"},
    migration_config={
        "script_location": "migrations",
        "include_extensions": ["events"],
    },
    extension_config={
        "events": {
            "queue_table": "app_events",
        }
    },
)
spec.add_config(config)

channel = spec.event_channel(config)
channel.publish("notifications", {"type": "cache_invalidate", "key": "user:1"})
for message in channel.iter_events("notifications"):
    print(message.payload)
    channel.ack(message.event_id)
    break
```

## PostgreSQL native notifications

All async PostgreSQL adapters (AsyncPG, Psycopg async, and Psqlpy) support
native `LISTEN/NOTIFY` via `events_backend="listen_notify"`. When enabled,
events flow directly through PostgreSQL's notification system with no
migrations required.

```python
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

spec = SQLSpec()
config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/db"})
spec.add_config(config)
channel = spec.event_channel(config)

event_id = await channel.publish_async("notifications", {"type": "native"})
async for message in channel.iter_events_async("notifications"):
    assert message.event_id == event_id
    break
```

Native events are fire-and-forget: there is no durable queue row, so
`ack_async()` becomes a no-op used solely for API parity. If you need durable
storage (e.g., for retries or multi-consumer fan-out) keep the queue backend
enabled as described below.


## Oracle Advanced Queuing (sync adapters)

Set ``config.driver_features["events_backend"] = "advanced_queue"`` to opt into native AQ.
When enabled, ``EventChannel`` publishes JSON payloads via ``connection.queue`` and
skips the durable table migrations. The backend currently targets synchronous drivers
(async configs fall back to the queue extension automatically).

Optional ``extension_config["events"]`` keys:

- ``aq_queue``: AQ queue name (default ``SQLSPEC_EVENTS_QUEUE``)
- ``aq_wait_seconds``: dequeue wait timeout (default 5 seconds)
- ``aq_visibility``: visibility constant (e.g., ``AQMSG_VISIBLE``)

If AQ is not configured or the Python driver lacks the feature, SQLSpec logs a warning
and transparently falls back to the table-backed queue backend.

## Enabling the events extension

1. **Include the extension migrations**

   ```python
   migration_config={
       "script_location": "migrations",
       "include_extensions": ["events"],
   }
   ```

   When `extension_config["events"]` is present SQLSpec automatically
   appends `"events"` to `include_extensions`, but setting it explicitly makes
   the intent clear and mirrors other extension guides. Running
   `sqlspec upgrade` (or `config.migrate_up()`) applies
   `ext_events_0001`, which creates the queue table and composite index.

2. **Provide extension settings (optional)**

   ```python
   extension_config={
       "events": {
           "queue_table": "app_events",  # defaults to sqlspec_event_queue
           "lease_seconds": 60,           # defaults to 30 seconds
           "retention_seconds": 86400,    # defaults to 24h cleanup window
           "in_memory": True,             # Oracle-specific option
       }
   }
   ```

3. **Use `SQLSpec.event_channel(config)`** to obtain a ready-to-use channel.

### Oracle `INMEMORY` support

When `in_memory=True` and the adapter dialect is Oracle, the migration adds the
`INMEMORY PRIORITY HIGH` clause so queue rows live in the column store. Other
adapters ignore this flag.

## Publishing events

Both sync and async adapters share the same API surface. Call the method that
matches your driver type:

```python
channel.publish("notifications", {"type": "user_update", "user_id": 42})
await channel.publish_async("notifications", {"type": "refresh", "user_id": 42})
```

Payloads must be JSON-serialisable. Optional metadata maps can be stored via the
`metadata` keyword argument when you need additional context for listeners.

## Consuming events

Import ``EventMessage`` from ``sqlspec.extensions.events`` when typing your handlers:

```python
from sqlspec.extensions.events import EventMessage
```

### Async listeners

```python
async def handle(message: EventMessage) -> None:
    # do work, then ack when auto_ack=False
    print(message.channel, message.payload)

listener = channel.listen_async(
    "notifications",
    handle,
    poll_interval=0.5,
    auto_ack=True,
)

# later
await channel.stop_listener_async(listener.id)
```

For manual iteration instead of background tasks:

```python
async for message in channel.iter_events_async("notifications", poll_interval=1):
    await process(message)
    await channel.ack_async(message.event_id)
```

### Sync listeners

```python
def handle_sync(message: EventMessage) -> None:
    print(message.payload)

listener = channel.listen(
    "notifications",
    handle_sync,
    poll_interval=1.0,
    auto_ack=False,
)

# stop when shutting down
channel.stop_listener(listener.id)
```

Manual iteration is also available via `channel.iter_events(...)` which yields
`EventMessage` objects until you break the loop.

#### Using sync APIs with async adapters

When you call `SQLSpec.event_channel()` with an async adapter (AsyncPG,
AioSQLite, etc.) the extension automatically enables a *portal bridge* so the
sync APIs (`publish`, `iter_events`, `listen`, `ack`) remain usable. Under the
hood SQLSpec runs the async backend inside a background event loop via
`sqlspec.utils.portal`. Disable this by setting
`extension_config["events"]["portal_bridge"] = False` if you prefer to guard
against accidental sync usage.

## Configuration reference

| Option             | Default                 | Description |
| ------------------ | ----------------------- | ----------- |
| `queue_table`      | `sqlspec_event_queue`   | Table name used by migrations and runtime. |
| `lease_seconds`    | `30`                    | How long a consumer owns a message before it can be retried. |
| `retention_seconds`| `86400`                 | How long acknowledged rows remain before automatic cleanup. |
| `poll_interval`    | adapter-specific        | Default sleep window between dequeue attempts; see table below. |
| `in_memory`        | `False`                 | Oracle-only flag that adds `INMEMORY PRIORITY HIGH` to the queue table. |
| `aq_queue`         | `SQLSPEC_EVENTS_QUEUE`  | Native AQ queue name when `events_backend="advanced_queue"`. |
| `aq_wait_seconds`  | `5`                     | Wait timeout (seconds) for AQ dequeue operations. |
| `aq_visibility`    | *unset*                 | Optional visibility constant (e.g., `AQMSG_VISIBLE`). |

### Adapter defaults

`EventChannel` ships with tuned defaults per adapter so you rarely have to tweak the queue knobs. Override any value via `extension_config["events"]` when your workload differs from the defaults.

| Adapter | Backend | Default poll interval | Lease window | Locking hints |
| --- | --- | --- | --- | --- |
| AsyncPG / Psycopg / Psqlpy | Native LISTEN/NOTIFY | `N/A` (native notifications) | `N/A` | Dedicated listener connections reuse the driver's native APIs. |
| Oracle | `advanced_queue` (sync adapters) | `aq_wait_seconds` (default `5s`) | `N/A` – AQ removes messages when dequeued | Exposes AQ dequeue options via `extension_config`. |
| Asyncmy (MySQL) | Queue fallback | `0.25s` | `5s` | Adds `FOR UPDATE SKIP LOCKED` to reduce contention. |
| DuckDB | Queue fallback | `0.15s` | `15s` | Favor short leases/poll windows so embedded engines do not spin. |
| BigQuery / ADBC | Queue fallback | `2.0s` | `60s` | Coarser cadence avoids hammering remote warehouses; still safe to override. |
| Spanner | Queue fallback | `1.0s` | `30s` | Uses Spanner-native JSON and TIMESTAMP types; requires separate DDL execution. |
| SQLite / AioSQLite | Queue fallback | `1.0s` | `30s` | General-purpose defaults that suit most local deployments. |

## Telemetry & observability

`EventChannel` reuses the adapter's observability runtime. Publishing and acking
increment `events.publish` / `events.ack` counters (native backends emit
`events.publish.native`), consumers add `events.deliver`, and listener lifecycle
is tracked via `events.listener.start/stop`. Each publish/dequeue/ack operation
also opens a span (`sqlspec.events.publish`, `sqlspec.events.dequeue`,
`sqlspec.events.ack`) so enabling `extension_config["otel"]` automatically
exports structured traces. The counters and spans flow through
`SQLSpec.telemetry_snapshot()` plus the Prometheus helper when
`extension_config["prometheus"]` is enabled.

## Architecture

The events extension consists of two layers that work together:

### Event Backends

Backends handle the actual pub/sub communication mechanism. SQLSpec supports
three backend types:

| Backend | Description | When to use |
| --- | --- | --- |
| `listen_notify` | Native PostgreSQL LISTEN/NOTIFY | Real-time, fire-and-forget events |
| `listen_notify_durable` | Hybrid: queue table + NOTIFY wakeups | Real-time with durability and retries |
| `advanced_queue` | Oracle Advanced Queuing | Enterprise Oracle deployments |
| `table_queue` | Polling-based queue table | Universal fallback for any database |

Configure the backend via `driver_features["events_backend"]`:

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

# Native LISTEN/NOTIFY (default for PostgreSQL adapters)
config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://localhost/db"},
    driver_features={"events_backend": "listen_notify"},
)

# Hybrid: durable queue with NOTIFY wakeups
config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://localhost/db"},
    driver_features={"events_backend": "listen_notify_durable"},
)
```

### Event Queue Stores

Stores generate adapter-specific DDL for the queue table. Each adapter has
a store class in `sqlspec/adapters/{adapter}/events/store.py` that handles:

- Column type mapping (JSON, JSONB, CLOB, etc.)
- Timestamp types (TIMESTAMPTZ, DATETIME, TIMESTAMP)
- Index creation strategies
- Database-specific DDL wrapping (IF NOT EXISTS, PL/SQL blocks, etc.)

Example store implementations:

| Adapter | Payload Type | Timestamp Type | Special Handling |
| --- | --- | --- | --- |
| AsyncPG / Psycopg / Psqlpy | JSONB | TIMESTAMPTZ | Standard PostgreSQL |
| Oracle | CLOB | TIMESTAMP | PL/SQL exception blocks for idempotent DDL |
| MySQL / Asyncmy | JSON | DATETIME(6) | FOR UPDATE SKIP LOCKED |
| DuckDB | JSON | TIMESTAMP | Short poll intervals |
| BigQuery | JSON | TIMESTAMP | CLUSTER BY for partitioning |
| Spanner | JSON | TIMESTAMP | Separate DDL execution (no IF NOT EXISTS) |
| ADBC | Dialect-detected | Dialect-detected | Multi-dialect support based on connection URI |
| SQLite / AioSQLite | TEXT | TIMESTAMP | General-purpose defaults |

### ADBC Multi-Dialect Support

The ADBC adapter auto-detects the underlying database from the connection URI
and generates appropriate DDL:

```python
from sqlspec.adapters.adbc import AdbcConfig

# Connects to PostgreSQL via ADBC - uses JSONB columns
config = AdbcConfig(
    connection_config={"uri": "postgresql://localhost/db"},
    extension_config={"events": {}},
)

# Connects to BigQuery via ADBC - uses JSON with CLUSTER BY
config = AdbcConfig(
    connection_config={"driver_name": "bigquery"},
    extension_config={"events": {}},
)
```

## Litestar/ADK integration

- The Litestar and ADK plugins already call `SQLSpec.event_channel()` for you.
  Enable the extension in your adapter config, then rely on dependency
  injection to pull channels into handlers:

```python
from litestar import Litestar, Router
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.litestar import SQLSpecPlugin

sql = SQLSpec()
config = sql.add_config(
    AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/db"},
        extension_config={"events": {"queue_table": "app_events"}},
    )
)

plugin = SQLSpecPlugin(sql)

async def broadcast_refresh(channel=sql.event_channel(config)) -> None:
    await channel.publish_async("notifications", {"action": "refresh"})

app = Litestar(route_handlers=[Router(after_request=[broadcast_refresh])], plugins=[plugin])
```

- Litestar automatically populates `CorrelationContext`, so listener spans and
  metrics inherit the same correlation IDs as the rest of the request.
