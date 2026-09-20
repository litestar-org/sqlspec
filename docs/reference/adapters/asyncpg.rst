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
placeholder. Identifier or function right-hand operands require an explicit
operator escape. Write parameterized intervals as ``? * interval '1 day'``
(or use ``$1``).
