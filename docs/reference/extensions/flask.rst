=====
Flask
=====

Flask extension providing request-scoped session management, automatic
transaction handling, and async adapter support via the portal pattern.

Configuration
=============

Use ``sqlspec.config.FlaskConfig`` in ``extension_config["flask"]``.
These settings apply across adapters; no adapter-specific subtype is needed.

.. autoclass:: sqlspec.config.FlaskConfig
   :members:
   :show-inheritance:
   :no-index:

Plugin
======

.. autoclass:: sqlspec.extensions.flask.SQLSpecPlugin
   :members:
   :show-inheritance:

State
=====

.. autoclass:: sqlspec.extensions.flask.FlaskConfigState
   :members:
   :show-inheritance:
