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

JSON and JSONB Codecs
=====================

AsyncPG connections register binary ``json`` and ``jsonb`` codecs by default.
This lets regular statement execution and ``load_from_arrow()`` pass Python
``dict`` and ``list`` values into PostgreSQL ``JSON`` / ``JSONB`` columns while
preserving asyncpg's binary COPY protocol expectations for ``jsonb`` payloads.
Set ``driver_features={"enable_json_codecs": False}`` when an application needs
to manage asyncpg JSON codecs manually.

.. autoclass:: sqlspec.adapters.asyncpg.config.AsyncpgPoolConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.asyncpg.config.AsyncpgConnectionConfig
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.asyncpg.AsyncpgDriver
   :members:
   :show-inheritance:

Extension Dialects
==================

AsyncPG supports the :doc:`pgvector and ParadeDB dialects <../dialects>` for vector
similarity search and full-text search operators. See the :doc:`Dialects <../dialects>`
reference for operator details.

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.asyncpg.data_dictionary.AsyncpgDataDictionary
   :members:
   :show-inheritance:
