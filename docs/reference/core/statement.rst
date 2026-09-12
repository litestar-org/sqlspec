====================
Statement & Compiler
====================

SQL statement representation, configuration, and compilation.

.. currentmodule:: sqlspec.core.statement

SQL
===

.. autoclass:: SQL
   :members:
   :show-inheritance:

.. data:: Statement

   Alias for :class:`SQL`.

StatementConfig
===============

.. autoclass:: StatementConfig
   :members:
   :show-inheritance:

ProcessedState
==============

.. autoclass:: ProcessedState
   :members:
   :show-inheritance:

Helper Functions
================

.. autofunction:: get_default_config

.. autofunction:: get_default_parameter_config

Compiler
========

.. currentmodule:: sqlspec.core.compiler

.. autoclass:: SQLProcessor
   :members:
   :show-inheritance:

.. autoclass:: CompiledSQL
   :members:
   :show-inheritance:

.. autoclass:: OperationProfile
   :members:
   :show-inheritance:

.. data:: OperationType

   Literal type for operation classifications (``SELECT``, ``INSERT``, ``UPDATE``, ``DELETE``, ``COPY``, ``COPY_FROM``, ``COPY_TO``, ``EXECUTE``, ``SCRIPT``, ``DDL``, ``PRAGMA``, ``MERGE``, ``COMMAND``).

.. autofunction:: is_copy_operation

.. autofunction:: is_copy_from_operation

.. autofunction:: is_copy_to_operation
