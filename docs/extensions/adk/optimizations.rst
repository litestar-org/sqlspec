=============
Optimizations
=============

The ADK clean break locks a catalog of latency-oriented variations on top of
the shared session/event/memory contract. Every variation is **opt-out** at the
``BaseSessionService``/``BaseMemoryService``/``BaseArtifactService`` boundary —
public service behavior never changes when a variation is enabled or disabled.

Variation Catalog
=================

V1 — NULL-encoded empty state
-----------------------------

Status: planned per `Chapter 16 <https://github.com/litestar-org/sqlspec/blob/main/.agents/specs/adk-clean-break/latency-optimized-data-model/spec.md>`__.

When ``state == {}``, store NULL instead of ``"{}"`` in the session ``state``
column. Cheaper writes, smaller TOAST/dictionary pages, and the round-trip
returns ``{}`` so the public ``Session.state`` API is unchanged.

V2 — Skip no-op session UPDATE
-------------------------------

Status: planned.

When ``event.actions.state_delta`` is empty (and no scoped-state delta is
provided), the store skips the session ``UPDATE`` and instead bumps
``update_time`` via a lightweight ``UPDATE ... SET update_time = CURRENT_TIMESTAMP``
or omits the bump entirely depending on the freshness contract.

V3 — Generated columns from JSON
---------------------------------

Status: planned per-driver.

Adapters whose dialect supports generated columns expose hot-path predicates
(``app_name``, ``user_id``, ``invocation_id``) as virtual or stored columns
derived from the JSON state/event blob, so indexes can be built on the JSON
contents without changing the wire shape.

V4 — Event partitioning
-----------------------

Status: per-driver.

Append-only event tables benefit from native partitioning where supported
(PostgreSQL declarative partitioning, CockroachDB hash-sharded indexes, Spanner
``INTERLEAVE IN PARENT``, BigQuery ``PARTITION BY DATE`` — already landed for
BigQuery).

V5 — Covering indexes
---------------------

Status: per-driver.

The six hot service paths (``get_session``, ``list_sessions``, ``get_events``,
``get_app_state``, ``get_user_state``, ``get_metadata``) get covering indexes
in dialects that support ``INCLUDE`` clauses or storing columns (PostgreSQL,
CockroachDB, Spanner).

V6 — DuckDB STRUCT-typed events
--------------------------------

Status: planned (DuckDB only).

For DuckDB, store events using a STRUCT column derived from the event JSON so
vectorized execution can scan event fields without JSON parsing.

V7 — Spanner commit-timestamp PK suffix
----------------------------------------

Status: planned (Spanner only).

Events table primary key gains a commit-timestamp suffix (``(app_name, user_id,
session_id, commit_timestamp, id)``) so ``ORDER BY timestamp`` reads use the
index directly.

V8 — AlloyDB columnar engine autopromote
-----------------------------------------

Status: planned (AlloyDB only).

When the AlloyDB data dictionary reports columnar engine availability, the ADK
events table is auto-promoted into the columnar engine for analytical scans.

Configuration
=============

All variations are controlled by
``extension_config["adk"]["optimizations"]`` (``ADKOptimizationConfig``):

.. code-block:: python

   config = AsyncpgConfig(
       extension_config={
           "adk": {
               "optimizations": {
                   "null_encoded_empty_state": True,
                   "skip_noop_session_update": True,
                   "generated_columns": "auto",      # "auto" | "enable" | "disable"
                   "event_partitioning": "auto",
                   "covering_indexes": "auto",
                   "vector_indexes": "auto",
               }
           }
       }
   )

``"auto"`` defers to the data dictionary's capability detection. ``"enable"``
forces the variation and fails fast if detection reports the feature as
unsupported. ``"disable"`` opts out unconditionally.

Memory Embedding Presets
=========================

The ADK memory store does **not** assume a single embedding dimension. Set
``extension_config["adk"]["memory"]["embedding_preset"]`` or
``embedding_dimension`` explicitly:

.. code-block:: python

   extension_config = {
       "adk": {
           "memory": {
               "embedding_preset": "gemini-embedding-002",   # 1536-dim
           }
       }
   }

Available presets (see :mod:`sqlspec.extensions.adk.memory.presets`):

.. list-table::
   :header-rows: 1
   :widths: 30 10 15 15 30

   * - Preset
     - Dim
     - Precision
     - Normalize
     - Source
   * - ``gemini-embedding-002``
     - 1536
     - float32
     - true
     - Google Vertex AI (current generation)
   * - ``gemini-embedding-001``
     - 768
     - float32
     - true
     - Google Vertex AI (legacy)
   * - ``embeddinggemma-300m``
     - 768
     - float32
     - true
     - Google open-weights EmbeddingGemma
   * - ``text-embedding-005``
     - 768
     - float32
     - true
     - Google Vertex AI
   * - ``text-embedding-004``
     - 768
     - float32
     - true
     - Google Vertex AI (legacy)
   * - ``text-embedding-3-large``
     - 3072
     - float32
     - true
     - OpenAI; supports MRL truncation
   * - ``text-embedding-3-small``
     - 1536
     - float32
     - true
     - OpenAI; supports MRL truncation
   * - ``text-embedding-ada-002``
     - 1536
     - float32
     - true
     - OpenAI (legacy)

Pass ``embedding_dimension`` explicitly to override the preset (for example, an
MRL-truncated dim-512 vector while keeping the ``text-embedding-3-large``
preset for documentation purposes). Register custom models at runtime via
:func:`~sqlspec.extensions.adk.memory.presets.register_embedding_preset`.

Resolution order — explicit ``embedding_dimension`` wins over
``embedding_preset``; if neither is set, the memory store raises a clear
:class:`~sqlspec.exceptions.ImproperConfigurationError` that lists every
available preset.
