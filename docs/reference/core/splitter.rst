==================
Statement Splitter
==================

Dialect-aware SQL script splitting for multi-statement scripts, migration files,
and procedural blocks.

.. currentmodule:: sqlspec.core.splitter

Classes
=======

.. autoclass:: StatementSplitter
   :members:
   :show-inheritance:

.. autoclass:: DialectConfig
   :members:
   :show-inheritance:

.. autoclass:: PostgreSQLDialectConfig
   :members:
   :show-inheritance:

.. autoclass:: OracleDialectConfig
   :members:
   :show-inheritance:

.. autoclass:: TSQLDialectConfig
   :members:
   :show-inheritance:

Functions
=========

.. autofunction:: split_sql_script
