==========
arrow-odbc
==========

Sync Arrow-over-ODBC adapter built on `arrow-odbc <https://pypi.org/project/arrow-odbc/>`_.
Streams ``pyarrow.RecordBatchReader`` results from any ODBC-compliant driver,
making it a good fit for read-heavy analytical transfer between SQL Server,
PostgreSQL, MySQL, or other ODBC sources and the Arrow ecosystem.

Configuration
=============

.. autoclass:: sqlspec.adapters.arrow_odbc.ArrowOdbcConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.arrow_odbc.ArrowOdbcConnectionParams
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.arrow_odbc.ArrowOdbcDriverFeatures
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.arrow_odbc.ArrowOdbcDriver
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.arrow_odbc.data_dictionary.ArrowOdbcDataDictionary
   :members:
   :show-inheritance:

Schema Discovery
================

``ArrowOdbcDataDictionary.get_columns`` first uses bundled dialect catalog
queries. When no query exists for the detected dialect (or it returns no
rows) and a table name is given, the driver issues a zero-row probe
(``SELECT * FROM "schema"."table" WHERE 1=0``) and derives column names,
ordering, nullability, and SQL type names from the Arrow reader schema.
Arrow-derived type names are approximations (for example ``VARCHAR`` for any
string column); ``mssql_python`` and other ODBC adapters without native
metadata APIs remain SQL-only.
