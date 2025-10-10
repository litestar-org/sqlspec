# Litestar LLM Playbook

Primer for agents integrating SQLSpec with Litestar. Focuses on fast setup, dependency wiring, and consistent project patterns.

## When to Use Litestar

- Expose SQLSpec-backed services over HTTP for demos or acceptance tests.
- Validate adapter behaviors via Litestar route handlers that exercise sessions and transactions.
- Plug SQLSpec stores into Litestar dependency injection for background tasks or CLI commands.
- Scaffold reference apps in docs or examples that need async API coverage.

## Speed-First Habits

- Instantiate `Litestar` with pre-built controllers or handlers; avoid dynamic route registration at runtime.
- Prefer `Provide` dependencies for sessions using `sqlspec.extensions.litestar.providers` so lifetimes stay managed.
- Cache configuration objects (configs, registries) at module import—Litestar app creation happens once per worker.
- Use `async with sql.provide_session(...)` inside handlers; wrap with `sync_to_thread` only when absolutely necessary.
- Reuse the project CLI via `litestar --app sqlspec.extensions.litestar.cli:app` for local runs instead of custom scripts.

## Core API Patterns

- `Litestar(route_handlers=[...], dependencies={...})` to wire standalone handlers or controllers.
- `@get`, `@post`, `@patch`, `@delete` decorators for CRUD endpoints; declare typed return models for OpenAPI.
- `Provide(lambda)`, `Dependency` objects for injecting SQLSpec sessions or stores into handlers.
- `PluginRegistry([SQLSpecPlugin(...)])` when registering SQLSpec integrations alongside other framework plugins.
- `app.run()` or CLI `litestar run --app module:app` for launching dev servers; rely on uvicorn under the hood.

## Project Integration Hooks

- Use `sqlspec.extensions.litestar.plugin.SQLSpecPlugin` to auto-register configs and session providers.
- `sqlspec.extensions.litestar.store` exposes `BaseStore` classes wrapping SQLSpec drivers; reuse rather than rolling custom stores.
- CLI entry points live in `sqlspec/extensions/litestar/cli.py`; extend via command groups instead of duplicating wiring.
- For migrations, combine Litestar CLI (management commands) with `sqlspec.migrations` to keep DB changes discoverable.
- When testing, mount Litestar apps with `litestar.testing.TestClient` and inject in-memory SQLSpec configs for isolation.

## Common Pitfalls

- Spawning new configs per request. Define configs once at module scope to avoid connection churn.
- Forgetting to await async dependencies; the plugin returns awaitables for sessions.
- Mixing sync handlers with async SQLSpec APIs. Mark handlers `async` or use background tasks to bridge.
- Building custom middleware for session cleanup; the plugin already handles lifecycle via dependency injection.
- Omitting OpenAPI metadata for SQLSpec-driven responses. Use typed DTOs so downstream tooling understands payloads.

## Retrieval Targets

- Framework docs home: <https://docs.litestar.dev/latest/>
- Dependency injection: <https://docs.litestar.dev/latest/usage/dependency-injection.html>
- Controllers & routing: <https://docs.litestar.dev/latest/usage/controllers.html>
- CLI reference: <https://docs.litestar.dev/latest/reference/cli/>
- Async testing utilities: <https://docs.litestar.dev/latest/testing/>
- SQLAlchemy tutorial (pattern mirror): <https://docs.litestar.dev/latest/tutorials/sqlalchemy/>
- Repository for examples: <https://github.com/litestar-org/litestar>

## Ship Checklist

- Litestar app defined at module import, not inside handlers.
- SQLSpec configs registered once and injected via plugin/provider utilities.
- Handlers express return types for schema generation and typed responses.
- Example code references official docs links above for deeper dives.
- Documented workflows land in `docs/guides/` so retrieval surfaces guidance alongside code.
