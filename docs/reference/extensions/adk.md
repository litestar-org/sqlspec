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

Session, event, long-term memory, and artifact metadata services and base stores for the Google Agent Development Kit (ADK). Adapter-specific ADK store and memory store subclasses are documented on each [Database Adapter](../adapters/index.md) page. See the [Google ADK Integration Guide](../../integrations/adk.md) for setup examples.

## ADK Services

::: sqlspec.extensions.adk.service.SQLSpecSessionService
    options:
      heading_level: 3
      members:
        - create_session
        - get_session
        - list_sessions
        - delete_session
        - append_event
        - get_user_state
        - close

::: sqlspec.extensions.adk.memory.SQLSpecMemoryService
    options:
      heading_level: 3
      members:
        - add_session_to_memory
        - add_events_to_memory
        - add_memory
        - search_memory

::: sqlspec.extensions.adk.memory.SQLSpecSyncMemoryService
    options:
      heading_level: 3
      members:
        - add_session_to_memory
        - add_events_to_memory
        - add_memory
        - search_memory

::: sqlspec.extensions.adk.artifact.SQLSpecArtifactService
    options:
      heading_level: 3
      members:
        - save_artifact
        - load_artifact
        - list_artifact_keys
        - list_versions
        - delete_artifact
        - delete_artifacts_older_than

## Base ADK Stores

::: sqlspec.extensions.adk.store.BaseAsyncADKStore
    options:
      heading_level: 4
      members:
        - create_tables
        - ensure_tables
        - reconcile_schema
        - create_session
        - get_session
        - list_sessions
        - delete_session
        - append_event
        - append_event_and_update_state
        - get_events

::: sqlspec.extensions.adk.store.BaseSyncADKStore
    options:
      heading_level: 4
      members:
        - create_tables
        - ensure_tables
        - reconcile_schema
        - create_session
        - get_session
        - list_sessions
        - delete_session
        - append_event
        - append_event_and_update_state
        - get_events

::: sqlspec.extensions.adk.memory.BaseAsyncADKMemoryStore
    options:
      heading_level: 4
      members:
        - create_tables
        - ensure_tables
        - reconcile_schema
        - insert_memory_entries
        - search_entries
        - delete_entries_by_session
        - delete_entries_older_than

::: sqlspec.extensions.adk.memory.BaseSyncADKMemoryStore
    options:
      heading_level: 4
      members:
        - create_tables
        - ensure_tables
        - reconcile_schema
        - insert_memory_entries
        - search_entries
        - delete_entries_by_session
        - delete_entries_older_than

::: sqlspec.extensions.adk.artifact.BaseAsyncADKArtifactStore
    options:
      heading_level: 4
      members:
        - create_table
        - ensure_table
        - insert_artifact
        - get_artifact
        - list_artifact_keys
        - list_artifact_versions
        - delete_artifact
        - delete_artifacts_older_than

::: sqlspec.extensions.adk.artifact.BaseSyncADKArtifactStore
    options:
      heading_level: 4
      members:
        - create_table
        - ensure_table
        - insert_artifact
        - get_artifact
        - list_artifact_keys
        - list_artifact_versions
        - delete_artifact
        - delete_artifacts_older_than

## Configuration & Record Schemas

::: sqlspec.config.ADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.extensions.adk._types.StoredSession
    options:
      heading_level: 4

::: sqlspec.extensions.adk._types.StoredEvent
    options:
      heading_level: 4

::: sqlspec.extensions.adk.memory.StoredMemory
    options:
      heading_level: 4

::: sqlspec.extensions.adk.artifact.StoredArtifact
    options:
      heading_level: 4

## Maintenance & Pruning Helpers

::: sqlspec.extensions.adk.maintenance.PruneReport
    options:
      heading_level: 4

::: sqlspec.extensions.adk.maintenance.prune_sessions
    options:
      heading_level: 4

::: sqlspec.extensions.adk.maintenance.prune_events
    options:
      heading_level: 4

::: sqlspec.extensions.adk.maintenance.prune_memory
    options:
      heading_level: 4

::: sqlspec.extensions.adk.maintenance.prune_artifacts
    options:
      heading_level: 4

::: sqlspec.extensions.adk.maintenance.prune_user_state
    options:
      heading_level: 4
