# Health Checks

Container orchestrators (Kubernetes, Cloud Run, ECS) distinguish between **liveness** probes (is the Python process responsive) and **readiness** probes (can the service acquire a pooled connection and execute a round-trip query within a strict deadline).

## Separating Liveness from Database Readiness

| Probe Endpoint | Checks Database? | Action on Failure | Recommended Timeout |
| --- | --- | --- | --- |
| `/health/live` | **No** | Orchestrator restarts container | `< 200 ms` |
| `/health/ready` | **Yes** (`SELECT 1`) | Load balancer stops routing new traffic without restarting healthy pods | `1–2 s` |

Never wire database ping queries to `/health/live`: transient database failover or brief pool saturation would cause Kubernetes to restart every application pod simultaneously, amplifying connection storms during recovery.

## Synchronous and Asynchronous Readiness Probes

Execute a lightweight scalar query inside a bounded timeout and return structured status metadata per configured database:

=== "Async Readiness Check"

    ```python
    import asyncio
    import time
    import msgspec

    from sqlspec.adapters.aiosqlite import AiosqliteConfig


    class DatabaseHealth(msgspec.Struct, frozen=True, gc=False):
        bind_key: str
        healthy: bool
        latency_ms: float
        error: str | None = None


    async def check_async_database(
        config: AiosqliteConfig, *, timeout_s: float = 1.5
    ) -> DatabaseHealth:
        name = config.bind_key or "default"
        started = time.perf_counter()
        try:
            async with (
                asyncio.timeout(timeout_s),
                config.provide_session() as session,
            ):
                probe = await session.select_value("SELECT 1", value_type=int)
                healthy = probe == 1
        except Exception as exc:
            elapsed = round((time.perf_counter() - started) * 1000, 2)
            return DatabaseHealth(
                bind_key=name,
                healthy=False,
                latency_ms=elapsed,
                error=type(exc).__name__,
            )

        elapsed = round((time.perf_counter() - started) * 1000, 2)
        return DatabaseHealth(
            bind_key=name, healthy=healthy, latency_ms=elapsed
        )
    ```

=== "Sync Readiness Check"

    ```python
    import time
    import msgspec

    from sqlspec.adapters.sqlite import SqliteConfig


    class DatabaseHealth(msgspec.Struct, frozen=True, gc=False):
        bind_key: str
        healthy: bool
        latency_ms: float
        error: str | None = None


    def check_sync_database(config: SqliteConfig) -> DatabaseHealth:
        name = config.bind_key or "default"
        started = time.perf_counter()
        try:
            with config.provide_session() as session:
                probe = session.select_value("SELECT 1", value_type=int)
                healthy = probe == 1
        except Exception as exc:
            elapsed = round((time.perf_counter() - started) * 1000, 2)
            return DatabaseHealth(
                bind_key=name,
                healthy=False,
                latency_ms=elapsed,
                error=type(exc).__name__,
            )

        elapsed = round((time.perf_counter() - started) * 1000, 2)
        return DatabaseHealth(
            bind_key=name, healthy=healthy, latency_ms=elapsed
        )
    ```

## Dialect-Specific Ping Queries

Most engines accept `SELECT 1`, while two enterprise dialects require table-clause syntax on older releases:

| Engine Family | Recommended Probe SQL |
| --- | --- |
| PostgreSQL, CockroachDB, SQLite, MySQL, MariaDB, SQL Server, DuckDB, Spanner, BigQuery | `SELECT 1` |
| Oracle Database (`oracledb` — portable across 19c and 23ai/26ai) | `SELECT 1 FROM DUAL` |
| IBM Db2 (`db2`) | `VALUES (1)` or `SELECT 1 FROM SYSIBM.SYSDUMMY1` |

Note that on Cloud Spanner (`SpannerSyncConfig` / `SpannerAsyncConfig`), passing `config.provide_session(transaction=False)` runs the readiness probe on a lock-free read-only snapshot without issuing an unnecessary `Commit` RPC.

## Preventing Probe Noise in Public Responses and Traces

- **Sanitize HTTP error payloads**: Return generic failure classifications (such as `type(exc).__name__` or HTTP `503`) on public readiness endpoints rather than raw DSNs, hostnames, or driver stack traces.
- **Avoid pool exhaustion**: Keep readiness probe intervals (`periodSeconds: 10`) and timeouts (`1.5 s`) conservative so health checks never contend with request traffic for pool slots.

## Recap

- Keep `/health/live` database-free and verify pooled connectivity exclusively on `/health/ready`.
- Wrap async readiness checks in `asyncio.timeout(...)` so hung sockets fail fast.
- Use `SELECT 1 FROM DUAL` on Oracle, `VALUES (1)` on Db2, and `provide_session(transaction=False)` on Cloud Spanner.
