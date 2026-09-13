====
Base
====

The ``sqlspec.base`` module defines the SQLSpec registry that owns configuration,
connection lifecycles, and session creation.

.. currentmodule:: sqlspec.base

Example
=======

.. literalinclude:: /examples/reference/base_api.py
   :language: python
   :caption: ``sqlspec registry``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Core Responsibilities
=====================

- Register and manage database configuration handles.
- Provide sync and async connection and session context managers.
- Manage connection pool startup, tracking, and shutdown.
- Track database configurations by instance identity for multi-database setups.
- Manage named SQL queries and parameter declarations through an integrated :class:`~sqlspec.loader.SQLFileLoader`.
- Publish and subscribe to events across sync and async event channels.

Connection and Session Management
=================================

- ``provide_session`` and ``get_session`` yield or return driver adapter instances bound to a specific config.
- ``provide_connection`` and ``get_connection`` provide raw database connections.
- Sync and async operations share consistent registry APIs.
- Pool lifecycle helpers (``get_pool``, ``close_pool``, ``close_all_pools``) handle resource cleanup.

API Reference
=============

.. autoclass:: SQLSpec
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

See Also
========

- :doc:`/usage/configuration` for configuration patterns.
- :doc:`/usage/drivers_and_querying` for execution patterns.
- :doc:`/reference/driver` for driver APIs.
