# Protocols

```python
from sqlspec.protocols import (
    AsyncDataDictionaryProtocol,
    CursorMetadataProtocol,
    ObjectStoreProtocol,
    PipelineCapableProtocol,
    QueryResultProtocol,
    SQLBuilderProtocol,
    StatementProtocol,
    SupportsArrowResults,
    SyncDataDictionaryProtocol,
)
```

Runtime-checkable structural `Protocol` definitions used across SQLSpec for static type checking and `isinstance()` narrowing.

## Statement & Builder Protocols

::: sqlspec.protocols.StatementProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.SQLBuilderProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasSQLGlotExpressionProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasExpressionProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasExpressionAndSQLProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasExpressionAndParametersProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasParameterBuilderProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasWhereProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.WithMethodProtocol
    options:
      heading_level: 4
      members: false

## Driver, Cursor & Result Protocols

::: sqlspec.protocols.QueryResultProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.CursorMetadataProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.SupportsArrowResults
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.ArrowTableStatsProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.PipelineCapableProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.SupportsCloseProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasRowcountProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasLastRowIdProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasSqlStateProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasErrnoProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasSqliteErrorProtocol
    options:
      heading_level: 4
      members: false

## Data Dictionary & Storage Protocols

::: sqlspec.protocols.SyncDataDictionaryProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.AsyncDataDictionaryProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.ObjectStoreProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.ReadableProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.AsyncReadableProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.AsyncReadBytesProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.AsyncWriteBytesProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.AsyncDeleteProtocol
    options:
      heading_level: 4
      members: false

## Configuration, Event & Model Structural Protocols

::: sqlspec.protocols.HasConfigProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasConnectionConfigProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasMigrationConfigProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasExtensionConfigProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasStatementConfigProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasDatabaseUrlAndBindKeyProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.NotificationProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasNotifiesProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasAddListenerProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.DictProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.MappingLikeProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.protocols.HasAsDictProtocol
    options:
      heading_level: 4
      members: false
