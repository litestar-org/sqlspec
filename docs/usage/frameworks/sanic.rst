=====
Sanic
=====

SQLSpec provides a Sanic extension that uses Sanic-native application and
request context. Connection pools are stored on ``app.ctx`` during the worker
lifecycle, and request-scoped connections and sessions are stored on
``request.ctx``.

Installation
============

Install SQLSpec with the Sanic extra:

.. tab-set::

   .. tab-item:: uv

      .. code-block:: bash

         uv add "sqlspec[sanic]"

   .. tab-item:: pip

      .. code-block:: bash

         pip install "sqlspec[sanic]"

   .. tab-item:: Poetry

      .. code-block:: bash

         poetry add "sqlspec[sanic]"

   .. tab-item:: PDM

      .. code-block:: bash

         pdm add "sqlspec[sanic]"

Basic Setup
===========

Create a SQLSpec instance, register your database config, and attach
``SQLSpecPlugin`` to your Sanic app. Configure Sanic-specific options under
``extension_config["sanic"]``.

.. literalinclude:: /examples/frameworks/sanic/basic_setup.py
   :language: python
   :caption: ``sanic basic setup``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Transaction Modes
=================

Sanic supports the same transaction modes as the other request-oriented
framework integrations:

``manual``
   SQLSpec manages connection scope only. Your handler commits or rolls back.

``autocommit``
   SQLSpec commits 2xx responses and rolls back other responses.

``autocommit_include_redirect``
   SQLSpec commits 2xx and 3xx responses and rolls back other responses.

Extra commit and rollback statuses are configured per database config.

.. literalinclude:: /examples/frameworks/sanic/commit_modes.py
   :language: python
   :caption: ``sanic commit modes``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Multiple Databases
==================

For multiple databases, give each config unique ``connection_key``,
``pool_key``, and ``session_key`` values. The plugin can then look up sessions
by session key.

.. literalinclude:: /examples/frameworks/sanic/multi_database.py
   :language: python
   :caption: ``sanic multi database``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Request Access
==============

Use ``plugin.get_session(request, key=None)`` for driver sessions and
``plugin.get_connection(request, key=None)`` for raw connection access. The
default key is the first registered config's ``session_key``; passing a key is
recommended when an app has multiple configs.

``get_connection_from_request(request, config_state)`` and
``get_or_create_session(request, config_state)`` are lower-level helpers for
custom integrations.

disable_di
==========

Set ``disable_di=True`` when another dependency injection system owns
request-scoped connection management. SQLSpec still creates and closes pools
on Sanic startup and shutdown, but it does not put connections or sessions on
``request.ctx``.

.. literalinclude:: /examples/frameworks/sanic/disable_di.py
   :language: python
   :caption: ``sanic disable di``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Correlation IDs and SQLCommenter
================================

Set ``enable_correlation_middleware=True`` to extract request correlation IDs
from headers and return ``X-Correlation-ID`` on the response. The active value
is also available from ``request.ctx.correlation_id`` and
``CorrelationContext`` during the request.

SQLCommenter is enabled when the driver ``StatementConfig`` has
``enable_sqlcommenter=True`` and the Sanic config keeps
``enable_sqlcommenter_middleware=True``. Request attributes include
``framework="sanic"``, route, and action.

.. literalinclude:: /examples/frameworks/sanic/observability.py
   :language: python
   :caption: ``sanic observability``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Related Guides
==============

- :doc:`/usage/configuration` for detailed config options.
- :doc:`/usage/observability` for correlation IDs and SQLCommenter.
- :doc:`/reference/adapters` for adapter-specific settings.
