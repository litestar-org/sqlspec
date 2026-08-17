===
API
===

.. currentmodule:: sqlspec.extensions.adk

Services
========

.. autoclass:: SQLSpecSessionService
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

.. autoclass:: sqlspec.extensions.adk.memory.SQLSpecMemoryService
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

.. autoclass:: sqlspec.extensions.adk.memory.SQLSpecSyncMemoryService
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

.. autoclass:: SQLSpecArtifactService
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

Session Stores
==============

.. autoclass:: BaseAsyncADKStore
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

.. autoclass:: BaseSyncADKStore
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

Memory Stores
=============

.. autoclass:: BaseAsyncADKMemoryStore
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

.. autoclass:: BaseSyncADKMemoryStore
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

Artifact Stores
===============

.. autoclass:: BaseAsyncADKArtifactStore
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

.. autoclass:: BaseSyncADKArtifactStore
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

Retention Helpers
=================

Standalone helpers that delete aged ADK rows, decoupled from any framework
worker so retention can run from a cron job, a CLI, or a task queue. Each helper
returns a :class:`PruneReport` and has a ``_sync`` twin
(``prune_sessions_sync``, ``prune_events_sync``, ``prune_memory_sync``,
``prune_user_state_sync``, ``prune_artifacts_sync``) that takes the same
arguments and produces the same report.

.. autoclass:: PruneReport
   :members:
   :show-inheritance:
   :no-index:

.. autofunction:: prune_sessions
   :no-index:

.. autofunction:: prune_events
   :no-index:

.. autofunction:: prune_memory
   :no-index:

.. autofunction:: prune_user_state
   :no-index:

.. autofunction:: prune_artifacts
   :no-index:

Pruning Artifacts
-----------------

Artifact retention is the one helper that spans two systems: version metadata
lives in a SQL table, while the content bytes live in object storage. Pass the
:class:`SQLSpecArtifactService` itself, since only the service holds both the
metadata store and the storage registry and URI needed to remove content:

.. code-block:: python

   from sqlspec.extensions.adk import prune_artifacts

   report = await prune_artifacts(artifact_service, older_than_days=30, app_name="my_agent")
   print(report["deleted_count"], report["table"], report["elapsed_ms"])

Use ``prune_artifacts_sync(artifact_service, older_than_days=30)`` from
synchronous callers.

A database config or a bare artifact metadata store is rejected with
``TypeError``: neither carries the storage registry and base URI, so neither can
clean up content objects.

``older_than_days`` must be a positive integer. Booleans, zero, negative values,
floats, strings, and ``None`` raise ``ValueError`` before the target is resolved
and before anything is deleted.

``app_name`` is a real filter, not an advisory hint. A prune scoped to one
application never deletes another application's artifacts.

Counts and cleanup guarantees
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``deleted_count`` is the number of artifact **version rows** deleted from the
metadata table, not the number of distinct filenames. Pruning three versions of
one file reports ``3``.

Metadata deletion is authoritative and happens first; if it fails, the error
propagates and nothing is reported as pruned. Content deletion is best-effort
and is **not** transactional with the metadata delete. Every deleted version
gets one content-deletion attempt, and a failed attempt is logged as a warning
carrying that version's canonical URI and version number before the remaining
versions are processed. The report still counts the metadata rows that were
deleted.

Because the metadata row is already gone, rerunning ``prune_artifacts`` cannot
rediscover or retry that content, and it does not try to. Monitor the
``adk.artifact.delete.content_cleanup_failed`` warnings and use their canonical
URIs to drive alerting or an independent object-store orphan sweep.

Record Types
============

.. autoclass:: StoredSession
   :members:
   :show-inheritance:
   :no-index:

.. autoclass:: StoredEvent
   :members:
   :show-inheritance:
   :no-index:

.. autoclass:: StoredMemory
   :members:
   :show-inheritance:
   :no-index:

.. autoclass:: StoredArtifact
   :members:
   :show-inheritance:
   :no-index:

Configuration
=============

.. autoclass:: ADKConfig
   :members:
   :show-inheritance:
   :no-index:

Converters
==========

.. automodule:: sqlspec.extensions.adk.converters
   :members:
   :undoc-members:
   :no-index:
