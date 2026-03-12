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

Driver
======

.. autoclass:: sqlspec.adapters.psqlpy.PsqlpyDriver
   :members:
   :show-inheritance:
