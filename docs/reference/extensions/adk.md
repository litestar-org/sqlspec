# Google ADK Extension

```python
from sqlspec.extensions.adk import (
    ADKConfig,
    BaseAsyncADKArtifactStore,
    BaseAsyncADKMemoryStore,
    BaseAsyncADKStore,
    BaseSyncADKArtifactStore,
    BaseSyncADKMemoryStore,
    BaseSyncADKStore,
    PruneReport,
    SessionOrderBy,
    SQLSpecArtifactService,
    SQLSpecMemoryService,
    SQLSpecSessionService,
    SQLSpecSyncMemoryService,
    StoredArtifact,
    StoredEvent,
    StoredMemory,
    StoredSession,
    prune_artifacts,
    prune_events,
    prune_memory,
    prune_sessions,
    prune_user_state,
)
```

Session, event, long-term memory, and artifact metadata services and base stores for the Google Agent Development Kit (ADK). Adapter-specific ADK store subclasses are documented on each [Database Adapter](../adapters/index.md) page. See the [Google ADK Integration Guide](../../integrations/adk.md) for setup examples.

## ADK Services

::: sqlspec.extensions.adk.service.SQLSpecSessionService
    options:
      heading_level: 3
      members:
        - create_session
        - get_session
        - append_event

::: sqlspec.extensions.adk.memory.SQLSpecMemoryService
    options:
      heading_level: 3
      members: false

::: sqlspec.extensions.adk.memory.SQLSpecSyncMemoryService
    options:
      heading_level: 3
      members: false

::: sqlspec.extensions.adk.artifact.SQLSpecArtifactService
    options:
      heading_level: 3
      members: false

## Base ADK Stores

::: sqlspec.extensions.adk.store.BaseAsyncADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.adk.store.BaseSyncADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.adk.memory.BaseAsyncADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.adk.memory.BaseSyncADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.adk.artifact.BaseAsyncADKArtifactStore
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.adk.artifact.BaseSyncADKArtifactStore
    options:
      heading_level: 4
      members: false

## Configuration & Record Schemas

::: sqlspec.config.ADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.extensions.adk._types.StoredSession
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.adk._types.StoredEvent
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.adk.memory.StoredMemory
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.adk.artifact.StoredArtifact
    options:
      heading_level: 4
      members: false

## Maintenance & Pruning Helpers

::: sqlspec.extensions.adk.maintenance.PruneReport
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.adk.maintenance.prune_sessions
    options:
      heading_level: 4
