# Operating Read Replicas

Reader lag and failed checkouts affect what a caller sees. Set a freshness policy for each query path and test it on the database topology you deploy.

## Read After a Write

A routing session pins itself to primary after a write, locking read, or transaction. A request scope can carry the same choice to other sessions through `RoutingContext.mark_write(bind_key)`. The outermost successful service write block marks that bind after commit. Neither path keeps a process-global tenant or user stickiness cache.

Use `RoutingContext.scope()` in workers. `mark_write()` outside a scope returns `False` and creates no lasting state. Child tasks copy context at task creation; give detached jobs their own scope. Do not share one active driver across concurrent tasks.

## Carry a Choice Between Requests

`SQLSpecRoutingMiddleware` binds a routing scope for an ASGI request and restores it in `finally`. The default `x-sqlspec-routing` header accepts `auto`, `primary`, `replica`, or `replica:reporting`. A configured write-pin cookie can carry a primary pin or causal token. A custom extractor can return a `RoutingEntry` or `RoutingContextPayload`.

```python
from sqlspec.utils.routing import SQLSpecRoutingMiddleware, sign_write_pin

application = SQLSpecRoutingMiddleware(
    app,
    routing_header=None,
    write_pin_cookie="recent_write",
    routing_secret=app_secret,
)
cookie_value = sign_write_pin(app_secret, ttl_seconds=5.0, causal_token="0/16B3748")
```

Here `app` is the existing ASGI application. In the built-in Litestar, Starlette, FastAPI, Flask, and Sanic extensions, `enable_routing_middleware` defaults to `True` with `routing_header=None` and `write_pin_cookie=None`, isolating each request's `RoutingContext` without trusting external client headers or cookies by default. For browser or public-client read-your-own-writes, configure `write_pin_cookie` alongside `routing_secret` and issue short-lived `HttpOnly` cookies via `sign_write_pin()`, which binds the expiration timestamp, `bind_key`, and optional `causal_token` under HMAC-SHA256. Enable `routing_header` only behind an ingress proxy or gateway that strips that header from untrusted external requests. Treat client hints as read intent, never as permission to access a tenant. Use `strict_headers=True` if invalid header values should fail rather than be ignored.

## Native Freshness Barriers

| Database | Helper or bound | What to check |
| --- | --- | --- |
| PostgreSQL | `current_wal_lsn`, `has_replayed_wal_lsn`, `wait_for_wal_lsn` | The reader has replayed the writer's WAL position |
| MySQL/MariaDB | `wait_for_gtid_set` | The requested GTID set is applied before the timeout |
| Oracle ADG | `standby_max_data_delay`, `sync_with_primary`, `retry_on_standby_lag` | The standby meets the bound; fallback occurs only before yield |
| Spanner | `exact_staleness`, `max_staleness`, `read_timestamp`, `min_read_timestamp` | One snapshot bound fits the read's freshness needs |
| CockroachDB | `follower_read_timestamp()` or a bounded staleness interval | The query runs at the chosen historical read timestamp |

Use a positive timeout for bounded MySQL GTID waits. MySQL treats `0` as unlimited; MariaDB treats `0` as a nonblocking check.

`ReadReplicaGroup(causal_validator=...)` can validate an application token after checkout and before yielding. A failed check cleans up that candidate and tries the next eligible reader or the configured primary fallback. Causal tokens are opaque to the generic router; it cannot infer replica lag from a host name or weight.

## Failures and Recovery

Groups track repeated checkout failures through `ReplicaHealthBreaker`. `failure_threshold` opens a failed candidate's circuit. After `recovery_seconds`, one attempt probes it; success closes the circuit. Caller query errors after yield do not trigger checkout retry or replay.

For automatic read providers, the default `fallback_to_primary=True` permits primary checkout after reader exhaustion. Set it to `False` when primary load must remain bounded; exhaustion raises `ReplicaUnavailableError`. An explicit `RoutingContext.replica()` choice never uses this fallback: missing replica groups or exhausted readers raise `ReplicaUnavailableError`. Outside an active transaction, an explicit replica scope can override a historical write pin; normal automatic routing remains pinned. Named groups must exist. A cancelled call releases its half-open probe. It finishes the failed lease before another attempt uses it.

Close a routing stream before changing its replica policy. An open stream retains its selected lease; conflicting policy changes raise `RoutingContextError` until the stream closes.

`ReadOnlyTransactionError` covers SQLSTATE `25006` and qualified native read-only failures. For example, MySQL errno `1290` maps this way only when the message names read-only mode; another option restriction keeps its existing error category. A generic primary fallback may accept writes, so retain reader roles and server policy where write refusal is required.

## Compose Tenant Isolation

Derived pools keep their tenant strategy, hooks, and lease cleanup. PostgreSQL RLS, Oracle VPD or client context, and Snowflake row policies still need database-side enforcement. A routing pin is not an access check and never selects a tenant on its own.

The request's captured tenant payload is rebound around each physical checkout, including replicas. Flask copies tenant and routing context across its portal thread. Cleanup uses the captured lease identity even if ambient context changes. A failed reset discards the socket before another tenant can use it.

## Keep Administrative Work on Primary

Migrations reject replica and tenant application configs. Events and queue polling also use primary sessions: locks, lease renewal, acknowledgements, and writes must observe one authoritative state. Use a separate admin config and pool.

ADK can offload lag-tolerant reads with an explicit `read_config` or enabled replica reads. Writes, session lease renewal, and mutation-internal reads remain primary. See [Google ADK](../../integrations/adk.md#read-replica-offload).

## Validate the Deployment

Test a reader outage, delayed replication, and primary fallback under real load. Check read-after-write behavior across separate requests and a size-one tenant pool. Include cancellation and failed cleanup. Local SQLite or container tests prove those paths only; cloud warehouses, Oracle standby/True Cache, and production tenant policies need their own endpoints and credentials.
