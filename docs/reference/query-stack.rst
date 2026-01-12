=============
Query Stack
=============

The Query Stack APIs let you compose multiple SQL operations into an immutable ``StatementStack`` and execute them in a single driver call. Each operation preserves the underlying ``SQLResult``/``ArrowResult`` so downstream helpers continue to work without copying data.

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
========

The stack system is composed of:

- ``StatementStack`` – immutable builder with push helpers for execute/execute_many/execute_script/execute_arrow
- ``StackOperation`` – the tuple-like value object stored inside the stack (method, statement, arguments, keyword arguments)
- ``StackResult`` – wraps the driver’s raw result while surfacing stack metadata (rows_affected, warning, error)
- ``AsyncDriverAdapterBase.execute_stack`` / ``SyncDriverAdapterBase.execute_stack`` – adapter hooks that select native pipelines or the sequential fallback

Execute a Stack
==============

.. literalinclude:: /examples/querying/statement_stack.py
   :language: python
   :caption: ``statement stack``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Native Pipelines and Round Trips
================================

``StatementStack`` always runs in order. When a driver supports native pipelines,
the stack can be sent as a single pipeline or batch, reducing round trips. Drivers
without native support fall back to sequential execution while preserving results
per operation.

Native stack execution is available in:

- AsyncPG
- Psycopg (pipeline mode)
- OracleDB

StatementStack
==============

.. currentmodule:: sqlspec.core.stack

.. autoclass:: StatementStack
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: StackOperation
   :members:
   :undoc-members:
   :show-inheritance:

StackResult
===========

.. currentmodule:: sqlspec.core.result

.. autoclass:: StackResult
   :members:
   :undoc-members:
   :show-inheritance:

Driver APIs
===========

.. currentmodule:: sqlspec.driver

.. automethod:: AsyncDriverAdapterBase.execute_stack
   :noindex:

.. currentmodule:: sqlspec.driver

.. automethod:: SyncDriverAdapterBase.execute_stack
   :noindex:

Exceptions
==========

.. currentmodule:: sqlspec.exceptions

.. autoclass:: StackExecutionError
   :members:
   :undoc-members:
   :show-inheritance:

Usage Highlights
================

- Build stacks once and reuse them across requests/tasks.
- Call ``session.execute_stack(stack, continue_on_error=False)`` to run fail-fast or set ``continue_on_error=True`` to record per-operation errors.
- Inspect ``StackResult.result`` to call helpers like ``all()``, ``one()``, ``to_pandas()``, or ``to_arrow()``.
- :doc:`/reference/adapters` lists per-adapter capabilities, including whether native pipelines or sequential fallback are used for stacks.
