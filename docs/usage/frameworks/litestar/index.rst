====================
Litestar Integration
====================

SQLSpec ships a Litestar plugin that wires database configs into the app lifecycle,
dependency injection, and transactional hooks.

Choose a guide
==============

.. grid:: 1 1 2 3
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Installation
      :link: installation
      :link-type: doc

      Install the plugin and wire it into your Litestar app.

   .. grid-item-card:: Quickstart
      :link: quickstart
      :link-type: doc

      A fast path from configuration to your first request.

   .. grid-item-card:: Dependency Injection
      :link: dependency_injection
      :link-type: doc

      Bind sessions to handlers with explicit keys.

   .. grid-item-card:: Transactions
      :link: transactions
      :link-type: doc

      Use context-aware commit and rollback flows.

   .. grid-item-card:: Session Stores
      :link: session_stores
      :link-type: doc

      Persist Litestar session state with SQLSpec stores.

   .. grid-item-card:: API Reference
      :link: api
      :link-type: doc

      Detailed plugin configuration and hooks.

.. toctree::
   :hidden:

   installation
   quickstart
   dependency_injection
   transactions
   session_stores
   api
