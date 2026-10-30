# Exceptions

```python
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    IntegrityError,
    MigrationError,
    MissingDependencyError,
    MultipleResultsFoundError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLSpecError,
    StackExecutionError,
    TransactionError,
    UniqueViolationError,
    map_sqlstate_to_exception,
)
```

Normalized exception hierarchy raised across database drivers, query builders, SQL file loaders, migrations, and storage pipelines.

## Base & Configuration Exceptions

::: sqlspec.exceptions.SQLSpecError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.ImproperConfigurationError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.ConfigResolverError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.MissingDependencyError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.DialectNotSupportedError
    options:
      heading_level: 4
      members: false

## Integrity & Constraint Violations

::: sqlspec.exceptions.IntegrityError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.UniqueViolationError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.ForeignKeyViolationError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.NotNullViolationError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.CheckViolationError
    options:
      heading_level: 4
      members: false

## Execution, Concurrency & Operational Errors

::: sqlspec.exceptions.OperationalError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.DatabaseConnectionError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.ConnectionTimeoutError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.QueryTimeoutError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.OperationCancelledError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.PermissionDeniedError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.TransactionError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.SerializationConflictError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.DeadlockError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.DataError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.InvalidCursorError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.NotFoundError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.MultipleResultsFoundError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.RepositoryError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.StackExecutionError
    options:
      heading_level: 4
      members: false

## SQL Parsing, File Loader, Migration & Storage Errors

::: sqlspec.exceptions.SQLParsingError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.SQLConversionError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.SQLBuilderError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.SQLFileNotFoundError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.SQLFileParseError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.SQLStatementNotFoundError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.SQLFragmentNotFoundError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.SQLSlotError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.MigrationError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.OutOfOrderMigrationError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.SquashValidationError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.StorageOperationFailedError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.FileNotFoundInStorageError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.StorageCapabilityError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.StoragePathTraversalError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.EventChannelError
    options:
      heading_level: 4
      members: false

## SQLSTATE Mapping Helper

`SQLSTATE_EXCEPTION_MAP` maps standard 5-character ANSI/PostgreSQL SQLSTATE codes (`"23505"`, `"40001"`, `"40P01"`, `"57014"`, etc.) to their corresponding `SQLSpecError` subclass.

::: sqlspec.exceptions.map_sqlstate_to_exception
    options:
      heading_level: 3
