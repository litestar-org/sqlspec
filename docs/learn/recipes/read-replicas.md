# Read Replicas and Session Routing

Send reads to a reader pool while keeping writes on the primary. Start with an explicit read session. Add request scopes or a routing session when one unit of work needs both reads and writes.

## Start with One Reader

A reader endpoint is enough for a PostgreSQL config:

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://app:secret@writer/app"},
    replicas="postgresql://app:secret@reader/app",
)
```

Use `async with config.provide_read_session() as session:` for reads and `config.provide_session()` for writes. The read provider yields the same concrete driver type as the ordinary provider. `SQLSpec.provide_read_session(config)` provides the same choice through a registry. Without a reader group, the generic provider uses the primary.

## Try a Local WAL Reader

SQLite can open a read-only pool over the same WAL database:

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlspec.adapters.sqlite import SqliteConfig

with TemporaryDirectory() as directory:
    config = SqliteConfig(
        connection_config={"database": str(Path(directory) / "orders.db")},
        replicas=True,
    )
    try:
        with config.provide_session() as writer:
            writer.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
            writer.execute("INSERT INTO orders (id) VALUES (:id)", {"id": 1})
        with config.provide_read_session() as reader:
            assert reader.select_value("SELECT count(*) FROM orders") == 1
    finally:
        config.close_pool()
```

The reader uses `mode=ro` and `query_only=1`. This gives the OS and database a chance to reject writes. A reader on another server also needs a suitable role and server policy. A read provider is a routing choice; generic primary fallback is not a client-side write sandbox.

## Choose a Topology

| Topology | Adapters | Read path |
| --- | --- | --- |
| Separate endpoints or native read intent | asyncpg, psycopg, psqlpy, adbc, oracledb, mssql-python, pymssql, Db2, asyncmy, aiomysql, PyMySQL, mysql-connector, SQL Anywhere, Sybase | Derived pools with reader endpoints or vendor read intent |
| Shared consensus pool | Spanner, cockroach-asyncpg, cockroach-psycopg | A read snapshot or follower-read session with bounds |
| Local WAL readers | sqlite, aiosqlite | A read-only pool over a file-backed WAL database |
| Compute or analytical isolation | Snowflake, Teradata, BigQuery, Arrow ODBC, DuckDB | A reader workload, separate compute, or a native read-only config |

Adapter guides describe accepted `replicas=` values. `True` has vendor-specific meaning and is rejected where an explicit endpoint is required. It does not provision a replica or copy data.

## Groups, Weights, and Failover

Use a sequence for several readers, or a mapping for named groups. Weights belong to a `ReplicaSpec`:

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.core.routing import ReplicaSpec

config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://app:secret@writer/app"},
    replicas={
        "default": [
            ReplicaSpec({"host": "reader-a"}, weight=3),
            ReplicaSpec({"host": "reader-b"}, weight=1),
        ],
        "reporting": [{"host": "reporting-reader"}],
    },
)
```

Request a group with `bind_group="reporting"`. `ReadReplicaGroup` accepts explicit configs and exposes `strategy="round_robin"` or `"random"`. The first uses smooth weighted round robin; the second uses weighted random selection. A group can set failure/recovery limits, a causal validator, and `fallback_to_primary=False`. The default permits primary fallback after eligible readers fail checkout. Failover never retries SQL after a session has been yielded.

Owned replica pools share the primary's startup, shutdown, and observability setup. Registry telemetry gives each reader a distinct key, such as `orders:replica:0` or `orders:replica:reporting:0`. A separately registered config is counted once.

## Choose the Read Interface

| Interface | Use it for |
| --- | --- |
| `provide_read_session()` | A query whose read intent is explicit at the call site |
| `SQLSpecSyncService` or `SQLSpecAsyncService` | Shared read helpers, lists, pagination, and primary write blocks |
| Framework read dependency/accessor | A route that only needs a reader |
| `provide_routing_session()` | An opt-in session that chooses a physical driver lazily for each eligible operation |

Service helpers such as `get_one`, `exists`, and pagination use the reader unless an explicit or already-bound session owns the work. `read_config=` can select a separate reader config. `begin_transaction()` keeps all statements on one primary driver.

A routing session sends only parsed, read-only statements to readers. Writes, locking reads, scripts, and transactions pin it to the primary. Parsing failures also choose the primary. The facade owns its physical checkouts; it is not a replacement for every adapter-specific driver method. Request a concrete driver for vendor APIs.

## Scope a Consistency Choice

```python
from sqlspec.utils.routing import RoutingContext

with RoutingContext.scope():
    with RoutingContext.replica(bind_group="reporting"):
        pass
    with RoutingContext.primary():
        pass
```

An explicit `RoutingContext.replica()` choice requires a reader. It never falls back to the primary; missing replica groups or exhausted readers raise `ReplicaUnavailableError`. Automatic read providers retain the group's configured primary fallback. A historical write pin can keep work on the primary even inside an explicit replica scope outside an active transaction.

Each scope works with `with` or `async with` and restores its caller's state. `primary`, `replica`, and `auto` can target a `bind_key`. A write pins only the active logical bind in the active request scope. An explicit conflicting replica choice during a primary transaction fails rather than splitting that transaction across sockets.

Frameworks expose `read_session`, `get_read_session`, or FastAPI's `provide_read_session` dependency factory. The [integration guides](../../integrations/index.md) show each form. For lag, failures, tenant policies, and request headers, continue with [replica operations](read-replicas-operations.md).
