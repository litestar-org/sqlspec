=======
Factory
=======

The ``SQLFactory`` class is the main entry point for the query builder system.
It provides methods to create SELECT, INSERT, UPDATE, DELETE, MERGE, and DDL
builders, plus COPY statement generation.

.. currentmodule:: sqlspec.builder

SQLFactory
==========

.. autoclass:: SQLFactory
   :members:
   :show-inheritance:

Convenience Instance
====================

.. py:data:: sqlspec.builder.sql
   :type: SQLFactory

   Pre-configured ``SQLFactory`` instance for convenient query building.

   Callable as ``sql(statement, dialect=None)`` to create a builder from a SQL string.

COPY Statement Helpers
======================

.. autofunction:: build_copy_statement

.. autofunction:: build_copy_from_statement

.. autofunction:: build_copy_to_statement

Temporal Table Support
======================

.. autofunction:: create_temporal_table

.. autofunction:: register_version_generators
