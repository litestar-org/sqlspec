=======
Results
=======

Result types for SQL operations. Every driver method returns one of these
result wrappers, providing helpers like ``all()``, ``one()``, ``scalar()``,
``to_pandas()``, and ``to_arrow()``.

.. currentmodule:: sqlspec.core.result

StatementResult
===============

.. autoclass:: StatementResult
   :members:
   :show-inheritance:

SQLResult
=========

.. autoclass:: SQLResult
   :members:
   :show-inheritance:

ArrowResult
===========

.. autoclass:: ArrowResult
   :members:
   :show-inheritance:

DMLResult
=========

.. autoclass:: DMLResult
   :members:
   :show-inheritance:

EmptyResult
===========

.. autoclass:: EmptyResult
   :members:
   :show-inheritance:

StackResult
===========

.. autoclass:: StackResult
   :members:
   :show-inheritance:

Factory Functions
=================

.. autofunction:: create_sql_result

.. autofunction:: create_arrow_result

.. autofunction:: build_arrow_result_from_table

.. autofunction:: build_arrow_result_from_reader
