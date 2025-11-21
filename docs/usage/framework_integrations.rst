========================
Framework Integrations
========================

SQLSpec integrates seamlessly with popular Python web frameworks through plugins and dependency injection. This guide covers integration with Litestar, FastAPI, and other frameworks.

Overview
--------

SQLSpec provides framework-specific plugins that handle:

- Connection lifecycle management
- Dependency injection
- Transaction management
- Request-scoped sessions
- Automatic cleanup

Litestar Integration
--------------------

The Litestar plugin provides first-class integration with comprehensive features.

Basic Setup
^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_framework_integrations_1.py
   :language: python
   :dedent: 0
   :start-adter: # start-example
   :end-before: # end-example


Using Dependency Injection
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The plugin provides dependency injection for connections, pools, and sessions:

.. literalinclude:: ../examples/usage/usage_framework_integrations_2.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Commit Modes
^^^^^^^^^^^^

The plugin supports different transaction commit strategies configured via ``extension_config``:

**Manual Commit Mode (Default)**

You control transaction boundaries explicitly:

.. literalinclude:: ../examples/usage/usage_framework_integrations_3.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


**Autocommit Mode**

Automatically commits on successful requests (2xx responses):

.. literalinclude:: ../examples/usage/usage_framework_integrations_4.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


**Autocommit with Redirects**

Commits on both 2xx and 3xx responses:

.. literalinclude:: ../examples/usage/usage_framework_integrations_5.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Custom Dependency Keys
^^^^^^^^^^^^^^^^^^^^^^

Customize the dependency injection keys via ``extension_config``:

.. literalinclude:: ../examples/usage/usage_framework_integrations_6.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Multiple Database Configurations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The plugin supports multiple database configurations through a single SQLSpec instance:

.. literalinclude:: ../examples/usage/usage_framework_integrations_7.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Session Storage Backend
^^^^^^^^^^^^^^^^^^^^^^^

Use SQLSpec as a session backend for Litestar:

.. literalinclude:: ../examples/usage/usage_framework_integrations_8.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


CLI Integration
^^^^^^^^^^^^^^^

The plugin provides CLI commands for database management:

.. code-block:: bash

   # Generate migration
   litestar db migrations generate -m "Add users table"

   # Apply migrations (includes extension migrations)
   litestar db migrations upgrade

   # Rollback migration
   litestar db migrations downgrade

   # Show current migration version
   litestar db migrations current

   # Show migration history (verbose)
   litestar db migrations current --verbose

.. note::

   Extension migrations (like Litestar session tables) are included automatically when ``include_extensions`` contains ``"litestar"`` in your migration config.

Correlation Middleware
^^^^^^^^^^^^^^^^^^^^^^

Enable request correlation tracking via ``extension_config``:

.. literalinclude:: ../examples/usage/usage_framework_integrations_9.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


FastAPI Integration
-------------------

While SQLSpec doesn't have a dedicated FastAPI plugin, integration is straightforward using dependency injection.

Basic Setup
^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_framework_integrations_10.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Dependency Injection
^^^^^^^^^^^^^^^^^^^^

Create a dependency function for database sessions:

.. literalinclude:: ../examples/usage/usage_framework_integrations_11.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Transaction Management
^^^^^^^^^^^^^^^^^^^^^^

Implement transaction handling with FastAPI:

.. literalinclude:: ../examples/usage/usage_framework_integrations_12.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Multiple Databases
^^^^^^^^^^^^^^^^^^

Support multiple databases with different dependencies:

.. literalinclude:: ../examples/usage/usage_framework_integrations_13.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Sanic Integration
-----------------

Integrate SQLSpec with Sanic using listeners and app context.

Basic Setup
^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_framework_integrations_14.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Using in Route Handlers
^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_framework_integrations_15.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Middleware for Automatic Sessions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_framework_integrations_16.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Flask Integration
-----------------

Integrate SQLSpec with Flask using synchronous drivers.

Basic Setup
^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_framework_integrations_17.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Using Request Context
^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_framework_integrations_18.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Custom Integration Patterns
----------------------------

Context Manager Pattern
^^^^^^^^^^^^^^^^^^^^^^^

For frameworks without built-in dependency injection:

.. literalinclude:: ../examples/usage/usage_framework_integrations_19.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Request-Scoped Sessions
^^^^^^^^^^^^^^^^^^^^^^^

Implement request-scoped database sessions:

.. literalinclude:: ../examples/usage/usage_framework_integrations_20.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Singleton Pattern
^^^^^^^^^^^^^^^^^

For simple applications with a single database:

.. literalinclude:: ../examples/usage/usage_framework_integrations_21.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Best Practices
--------------

**1. Use Framework-Specific Plugins When Available**

.. literalinclude:: ../examples/usage/usage_framework_integrations_22.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


**2. Always Clean Up Pools**

.. literalinclude:: ../examples/usage/usage_framework_integrations_23.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


**3. Use Dependency Injection**

.. literalinclude:: ../examples/usage/usage_framework_integrations_24.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


**4. Handle Transactions Appropriately**

.. literalinclude:: ../examples/usage/usage_framework_integrations_25.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


**5. Separate Database Logic**

.. literalinclude:: ../examples/usage/usage_framework_integrations_26.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Testing
-------

Testing with Framework Integration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_framework_integrations_27.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example


Next Steps
----------

- :doc:`../examples/index` - Complete framework integration examples
- :doc:`configuration` - Configure databases for production
- :doc:`drivers_and_querying` - Execute queries in framework handlers

See Also
--------

- :doc:`../reference/extensions` - Extension API reference
- `Litestar Documentation <https://docs.litestar.dev>`_
- `FastAPI Documentation <https://fastapi.tiangolo.com>`_
