# API Reference

Curated module-by-module and per-adapter API reference for SQLSpec. Every page opens with the canonical public import path, links to the corresponding conceptual guide, and documents public classes, protocols, `TypedDict` schemas, and functions without duplicating inherited driver methods across 20 adapters.

## Core & Root Modules

| Public Import Path | Reference Page | Summary |
| --- | --- | --- |
| `sqlspec` / `sqlspec.base` | [SQLSpec Registry](sqlspec.md) | Central `SQLSpec` registry for configurations, sessions, pools, named SQL files, and cache controls. |
| `sqlspec.config` | [Configuration](config.md) | `SyncDatabaseConfig`, `AsyncDatabaseConfig`, `NoPoolSyncConfig`, `NoPoolAsyncConfig`, `MigrationConfig`, and extension config `TypedDict` schemas. |
| `sqlspec.driver` | [Driver Overview](driver/index.md) | [`SyncDriverAdapterBase`](driver/sync.md), [`AsyncDriverAdapterBase`](driver/async.md), and [shared driver types](driver/common.md). |
| `sqlspec.core` | [Core Pipeline](core/index.md) | [`SQL` & `StatementConfig`](core/statement.md), [results](core/results.md), [parameters](core/parameters.md), [filters](core/filters.md), [cache](core/cache.md), and [compiler](core/compiler.md). |
| `sqlspec.builder` | [Query Builders](builder/index.md) | [`sql` factory](builder/factory.md), [`Select`](builder/select.md), [DML builders](builder/dml.md), [DDL builders](builder/ddl.md), and [expression wrappers](builder/expressions.md). |
| `sqlspec.loader` | [SQL File Loader](loader.md) | `SQLFileLoader`, `SQLFile`, `NamedStatement`, and `SlotDeclaration`. |

## Subsystems & Tooling

| Public Import Path | Reference Page | Summary |
| --- | --- | --- |
| `sqlspec.migrations` | [Migrations](migrations.md) | `SyncMigrationCommands`, `AsyncMigrationCommands`, runners, trackers, loaders, and squash helpers. |
| `sqlspec.storage` | [Storage & Pipelines](storage.md) | `StorageRegistry`, `SyncStoragePipeline`, `AsyncStoragePipeline`, and Arrow storage bridge jobs. |
| `sqlspec.service` | [Service Layer](service.md) | `SQLSpecSyncService` and `SQLSpecAsyncService` base classes for framework-agnostic data services. |
| `sqlspec.data_dictionary` | [Data Dictionary](data-dictionary.md) | Cross-database schema introspection metadata (`TableMetadata`, `ColumnMetadata`, `IndexMetadata`, `VersionInfo`) and topological dependency sorting. |
| `sqlspec.dialects` | [SQLGlot Dialects](dialects.md) | Custom SQLGlot dialects (`Spanner`, `Spangres`, `DB2`, `PGVector`, `ParadeDB`, `PGTextSearch`). |
| `sqlspec.observability` | [Observability](observability.md) | `ObservabilityConfig`, `TelemetryConfig`, `StatementEvent`, `RedactionConfig`, and cloud log formatters. |
| `sqlspec.cli` | [CLI Reference](cli.md) | `sqlspec` command-line interface options, migration subcommands, and ADK maintenance commands. |

## Types, Protocols, Exceptions & Extensions

| Public Import Path | Reference Page | Summary |
| --- | --- | --- |
| `sqlspec.exceptions` | [Exceptions](exceptions.md) | `SQLSpecError` hierarchy (`IntegrityError`, `UniqueViolationError`, `DeadlockError`, `SerializationConflictError`) and `map_sqlstate_to_exception`. |
| `sqlspec.protocols` | [Protocols](protocols.md) | Runtime-checkable structural protocols for drivers, cursors, builders, and Arrow results. |
| `sqlspec.typing` | [Typing & Schema Aliases](typing.md) | `SupportedSchemaModel`, `StatementParameters`, `ArrowReturnFormat`, and optional dependency sentinels. |
| `sqlspec.utils` | [Utilities](utils.md) | Type guards, serializers, schema converters, UUID/NanoID generators, fixture loaders, and sync/async bridges. |
| `sqlspec.extensions.*` | [Extensions](extensions/index.md) | [Litestar](extensions/litestar.md), [Google ADK](extensions/adk.md), [Events](extensions/events.md), [FastAPI](extensions/fastapi.md), [Starlette](extensions/starlette.md), [Flask](extensions/flask.md), [Sanic](extensions/sanic.md), [OpenTelemetry](extensions/opentelemetry.md), and [Prometheus](extensions/prometheus.md). |
| `sqlspec.adapters.*` | [Database Adapters](../databases/index.md) | Adapter-specific configurations, connection/pool `TypedDict` parameters, drivers, and extension stores across all 20 adapters. |
