=======
AsyncPG
=======

High-performance async PostgreSQL adapter using `asyncpg <https://github.com/MagicStack/asyncpg>`_.
Supports native pipelines, Arrow export, and Cloud SQL / AlloyDB connectors.

Configuration
=============

.. autoclass:: sqlspec.adapters.asyncpg.AsyncpgConfig
   :members:
   :show-inheritance:

Connection Parameters
=====================

.. autoclass:: sqlspec.adapters.asyncpg.AsyncpgConnectionConfig
   :members:
   :show-inheritance:

Pool Parameters
===============

.. autoclass:: sqlspec.adapters.asyncpg.AsyncpgPoolConfig
   :members:
   :show-inheritance:

Driver Features
===============

.. autoclass:: sqlspec.adapters.asyncpg.AsyncpgDriverFeatures
   :members:
   :show-inheritance:

JSON and JSONB Codecs
=====================

AsyncPG connections register binary ``json`` and ``jsonb`` codecs by default.
This lets regular statement execution and ``load_from_arrow()`` pass Python
``dict`` and ``list`` values into PostgreSQL ``JSON`` / ``JSONB`` columns while
preserving asyncpg's binary COPY protocol expectations for ``jsonb`` payloads.
Set ``driver_features={"enable_json_codecs": False}`` when an application needs
to manage asyncpg JSON codecs manually.

Driver
======

.. autoclass:: sqlspec.adapters.asyncpg.AsyncpgDriver
   :members:
   :show-inheritance:

Extension Dialects
==================

AsyncPG supports the :doc:`pgvector, pg_textsearch, and ParadeDB dialects <../dialects>` for vector
similarity search and full-text search operators. See the :doc:`Dialects <../dialects>`
reference for operator details.

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.asyncpg.data_dictionary.AsyncpgDataDictionary
   :members:
   :show-inheritance:

Extension Settings
==================

Use the configuration types below in their corresponding ``extension_config``
namespace: ``"litestar"``, ``"events"``, or ``"adk"`` as supported by this adapter.

.. autoclass:: sqlspec.adapters.asyncpg.litestar.AsyncpgLitestarConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.asyncpg.events.AsyncpgEventsConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.asyncpg.adk.AsyncpgADKConfig
   :members:
   :show-inheritance:

JSONB existence operator
------------------------

All PostgreSQL adapters support ``data ? 'key'``, ``data ? $1``,
``data ? :key``, ``?|``, and ``?&`` without treating the operator as a
placeholder. Write ``data ?? other_col`` for identifier or function right-hand operands. Write parameterized intervals as ``? * interval '1 day'``
(or use ``$1``).

Native execution options
------------------------

Set ``execution_args={"timeout": seconds}`` on ``StatementConfig`` to forward
an asyncpg timeout to queries, batches, script statements, stack operations,
and stream fetches. ``command_timeout`` is accepted as an alias; ``timeout``
takes precedence. Explicit ``0`` and ``None`` values are preserved.

``driver_features={"pgbouncer": True}`` disables asyncpg's statement cache and
SQLSpec's explicit prepared statements in stacks, while preserving transaction
cleanup. The option is also accepted in ``connection_config``. Use this mode
when the proxy configuration does not support prepared statements; PgBouncer
can support them when configured to track protocol-level prepared statements.

``driver_features["type_codecs"]`` accepts a list of native codec specifications.
Each entry requires ``typename``, ``encoder``, and ``decoder``; ``schema`` defaults
to ``public`` and ``format`` to ``text``. Codecs register after SQLSpec's built-in
JSON and vector setup and before ``on_connection_create``.
