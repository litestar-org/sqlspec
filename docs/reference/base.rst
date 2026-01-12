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

- Register database configurations.
- Provide sync and async session context managers.
- Manage connection pool startup and shutdown.
- Track configs by bind key for multi-database setups.

Session Management
==================

- ``provide_session`` yields a session bound to a specific config.
- Sync and async sessions share the same registry API.
- Close pools explicitly with ``close_all_pools`` when needed.

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
