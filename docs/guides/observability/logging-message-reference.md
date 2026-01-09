# Logging Message Reference

This reference lists standardized SQLSpec log events and their key fields.
The `message` field is always the static event name.

## Core Query Logging

| Event | Module | Key Fields |
| --- | --- | --- |
| `db.query` | `sqlspec.observability` | `db.system`, `db.operation`, `db.statement`, `duration_ms`, `rows_affected`, `correlation_id` |
| `stack.execute.start` | `sqlspec.driver` | `db.system`, `stack_size`, `native_pipeline` |
| `stack.execute.complete` | `sqlspec.driver` | `db.system`, `duration_ms`, `success_count`, `error_count` |
| `stack.execute.failed` | `sqlspec.driver` | `db.system`, `duration_ms`, `operation_index` |

## Migrations

| Event | Module | Key Fields |
| --- | --- | --- |
| `migration.apply` | `sqlspec.migrations.runner` | `db_system`, `version`, `duration_ms` |
| `migration.rollback` | `sqlspec.migrations.runner` | `db_system`, `version`, `duration_ms` |
| `migration.list` | `sqlspec.migrations.commands` | `db_system`, `current_version`, `status`, `applied_count` |
| `migration.create` | `sqlspec.migrations.commands` | `path`, `version`, `description` |
| `migration.track` | `sqlspec.migrations.tracker` | `db_system`, `version`, `status`, `execution_time_ms` |
| `migration.history` | `sqlspec.migrations.tracker` | `db_system`, `count`, `status` |

## Loader

| Event | Module | Key Fields |
| --- | --- | --- |
| `sql.load` | `sqlspec.loader` | `file_path`, `query_name`, `count` |
| `sql.parse` | `sqlspec.loader` | `file_path`, `query_name`, `duration_ms` |

## Storage

| Event | Module | Key Fields |
| --- | --- | --- |
| `storage.read` | `sqlspec.storage.backends` | `backend_type`, `protocol`, `path`, `duration_ms`, `size_bytes` |
| `storage.write` | `sqlspec.storage.backends` | `backend_type`, `protocol`, `path`, `duration_ms`, `size_bytes` |
| `storage.list` | `sqlspec.storage.backends` | `backend_type`, `protocol`, `path`, `duration_ms`, `count` |
| `storage.object.missing` | `sqlspec.storage.errors` | `backend_type`, `operation`, `path`, `exception_type`, `retryable` |
| `storage.operation.failed` | `sqlspec.storage.errors` | `backend_type`, `operation`, `path`, `exception_type`, `retryable` |

## Extensions

| Event | Module | Key Fields |
| --- | --- | --- |
| `extension.init` | `sqlspec.extensions.*` | `framework`, `stage`, `config_count` |
| `session.create` | `sqlspec.extensions.*` | `framework`, `connection_key`, `session_key` |
| `session.close` | `sqlspec.extensions.*` | `framework`, `connection_key`, `session_key` |

## Events Extension

| Event | Module | Key Fields |
| --- | --- | --- |
| `event.publish` | `sqlspec.extensions.events` | `channel`, `event_type`, `delivery_mode` |
| `event.receive` | `sqlspec.extensions.events` | `channel`, `event_type`, `duration_ms` |
| `event.listen` | `sqlspec.extensions.events` | `channel`, `status` |

## ADK Extension

| Event | Module | Key Fields |
| --- | --- | --- |
| `adk.session.create` | `sqlspec.extensions.adk` | `session_id`, `app_name` |
| `adk.session.get` | `sqlspec.extensions.adk` | `session_id`, `app_name` |

## Compiler + Cache

| Event | Module | Key Fields |
| --- | --- | --- |
| `sql.compile` | `sqlspec.core.compiler` | `db_system`, `duration_ms`, `sql_length` |
| `sql.validate` | `sqlspec.core.compiler` | `db_system`, `duration_ms`, `valid` |
| `cache.hit` | `sqlspec.core.cache` | `cache_key`, `cache_size` |
| `cache.miss` | `sqlspec.core.cache` | `cache_key`, `cache_size` |
| `cache.evict` | `sqlspec.core.cache` | `cache_key`, `cache_size` |

## Data Dictionary

| Event | Module | Key Fields |
| --- | --- | --- |
| `schema.introspect` | `sqlspec.adapters.*.data_dictionary` | `db.system`, `duration_ms` |
| `table.describe` | `sqlspec.adapters.*.data_dictionary` | `db.system`, `table_name`, `duration_ms` |
