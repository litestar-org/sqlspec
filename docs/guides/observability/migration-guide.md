# Logging Migration Guide

This guide helps update dashboards and log parsers to the standardized SQLSpec
logging format introduced in the clean-break release.

## What Changed

- Dynamic log messages were replaced with static event names.
- Structured fields now carry context (`db.system`, `duration_ms`, etc.).
- Statement logging uses OpenTelemetry-style keys.

## Event Name Mapping

| Old Pattern | New Event |
| --- | --- |
| `Executing query ...` | `db.query` |
| `Applying migration ...` | `migration.apply` |
| `Rolling back migration ...` | `migration.rollback` |
| `Listing migrations ...` | `migration.list` |
| `Creating migration ...` | `migration.create` |

## Field Mapping

| Old Field | New Field |
| --- | --- |
| `duration` or `duration_ms` (mixed) | `duration_ms` |
| `rows` / `rows_affected` (mixed) | `rows_affected` |
| `statement` | `db.statement` |
| `sql_hash` (ad-hoc) | `db.statement.hash` |
| `adapter` / `driver` | `db.system` + `sqlspec.driver` |

## Regex Updates

Old grep pattern:

```
Executing query .* duration=(\d+)
```

New pattern:

```
message=db.query .* duration_ms=(\d+)
```

Old migration pattern:

```
Applying migration (\d+)
```

New pattern:

```
message=migration.apply .* version=(\d+)
```

## Dashboard Example

### Slow Queries

- Filter: `message="db.query"`
- Group by: `db.system`, `db.operation`
- Threshold: `duration_ms > 500`

### Migration Duration

- Filter: `message="migration.apply"`
- Aggregate: `avg(duration_ms)` by `db_system`
