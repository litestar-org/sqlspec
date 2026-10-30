# Extensions Reference

API reference for SQLSpec's framework plugins, Google ADK stores, real-time event channels, OpenTelemetry tracing, and Prometheus metrics. For setup examples and architectural walkthroughs, see the [Integrations Guide](../../integrations/index.md).

## Extension Modules

| Public Import Path | Reference Page | Summary |
| --- | --- | --- |
| `sqlspec.extensions.litestar` | [Litestar Extension](litestar.md) | `SQLSpecPlugin`, `CommitMode`, `CorrelationMiddleware`, `BaseSQLSpecStore`, and `SQLSpecChannelsBackend`. |
| `sqlspec.extensions.adk` | [Google ADK Extension](adk.md) | `SQLSpecSessionService`, `SQLSpecMemoryService`, `SQLSpecArtifactService`, base stores, and maintenance helpers. |
| `sqlspec.extensions.events` | [Events & Channels Extension](events.md) | `SyncEventChannel`, `AsyncEventChannel`, `EventMessage`, `SyncTableEventQueue`, `AsyncTableEventQueue`, and `BaseEventQueueStore`. |
| `sqlspec.extensions.fastapi` | [FastAPI Extension](fastapi.md) | `SQLSpecPlugin`, `provide_filters`, `FilterConfig`, and `DependencyDefaults`. |
| `sqlspec.extensions.starlette` | [Starlette Extension](starlette.md) | `SQLSpecPlugin`, `SQLSpecAutocommitMiddleware`, `SQLSpecManualMiddleware`, and request session helpers. |
| `sqlspec.extensions.flask` | [Flask Extension](flask.md) | `SQLSpecPlugin` and `FlaskConfigState` for WSGI request-scoped sessions and async portal bridging. |
| `sqlspec.extensions.sanic` | [Sanic Extension](sanic.md) | `SQLSpecPlugin`, `SanicConfigState`, and request context session helpers. |
| `sqlspec.extensions.otel` | [OpenTelemetry Extension](opentelemetry.md) | `enable_tracing` helper for configuring `ObservabilityConfig` with OpenTelemetry spans. |
| `sqlspec.extensions.prometheus` | [Prometheus Extension](prometheus.md) | `PrometheusStatementObserver` and `enable_metrics` for statement counter and histogram export. |
