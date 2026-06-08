======
PsqlPy
======

Async PostgreSQL adapter using `psqlpy <https://github.com/qaspen-python/psqlpy>`_,
a Rust-backed PostgreSQL driver with native connection pooling.

Configuration
=============

.. autoclass:: sqlspec.adapters.psqlpy.PsqlpyConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.psqlpy.config.PsqlpyConnectionParams
   :members:
   :show-inheritance:

Extension Dialects
==================

PsqlPy supports the :doc:`pgvector and ParadeDB dialects <../dialects>` for vector
similarity search and full-text search operators. See the :doc:`Dialects <../dialects>`
reference for operator details.

``driver_features={"enable_pgvector": True}`` enables extension detection and
promotes the runtime dialect to ``pgvector`` when the PostgreSQL ``vector``
extension is installed. It does not register automatic psqlpy vector type
handlers; pass ``psqlpy.extra_types.PgVector`` values or use explicit SQL casts
for vector parameters.

Driver
======

.. autoclass:: sqlspec.adapters.psqlpy.PsqlpyDriver
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.psqlpy.data_dictionary.PsqlpyDataDictionary
   :members:
   :show-inheritance:
