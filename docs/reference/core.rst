====
Core
====

The core module provides SQL statement handling, parameter conversion, result
helpers, and caching utilities used by every driver.

.. currentmodule:: sqlspec.core

Example
=======

.. literalinclude:: /examples/reference/core_api.py
   :language: python
   :caption: ``core SQL usage``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

SQL Statement
=============

.. currentmodule:: sqlspec.core.statement

.. autoclass:: SQL
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: StatementConfig
   :members:
   :undoc-members:
   :show-inheritance:

Parameter Handling
==================

.. currentmodule:: sqlspec.core.parameters

.. autoclass:: ParameterProcessor
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: ParameterConverter
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: ParameterValidator
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: ParameterStyleConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: ParameterStyle
   :members:
   :undoc-members:
   :show-inheritance:

Result Processing
=================

.. currentmodule:: sqlspec.core.result

.. autoclass:: SQLResult
   :members:
   :undoc-members:
   :show-inheritance:

SQLSpec results expose helper methods like ``all()``, ``one()``, ``one_or_none()``,
``scalar()``, ``to_pandas()``, and ``to_arrow()`` for structured access.
