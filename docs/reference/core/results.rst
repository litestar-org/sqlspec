=======
Results
=======

Result types for SQL operations. Every driver method returns one of these
result wrappers, providing helpers like ``all()``, ``one()``, ``scalar()``,
``to_pandas()``, and ``to_arrow()``.

.. currentmodule:: sqlspec.core.result

StatementResult
===============

.. autoclass:: sqlspec.core.result._base.StatementResult
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

.. autoclass:: sqlspec.core.result._base.DMLResult
   :members:
   :show-inheritance:

EmptyResult
===========

.. autoclass:: sqlspec.core.result._base.EmptyResult
   :members:
   :show-inheritance:

StackResult
===========

.. autoclass:: StackResult
   :members:
   :show-inheritance:

Factory Functions
=================

.. autofunction:: sqlspec.core.result.create_sql_result

.. autofunction:: sqlspec.core.result.create_arrow_result

.. autofunction:: sqlspec.core.result.build_arrow_result_from_table
