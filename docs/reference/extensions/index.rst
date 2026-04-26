==========
Extensions
==========

SQLSpec extensions integrate the registry with frameworks, event systems,
and services such as Litestar, FastAPI, Flask, Sanic, Starlette, and Google ADK.

.. grid:: 2

   .. grid-item-card:: Litestar
      :link: litestar
      :link-type: doc

      Plugin-based integration with dependency injection, CLI, and lifecycle management.

   .. grid-item-card:: FastAPI
      :link: fastapi
      :link-type: doc

      Dependency injection helpers for FastAPI's Depends() system.

   .. grid-item-card:: Flask
      :link: flask
      :link-type: doc

      Request-scoped session management with portal pattern for async adapters.

   .. grid-item-card:: Starlette
      :link: starlette
      :link-type: doc

      Middleware-based session management and connection pooling lifecycle.

   .. grid-item-card:: Sanic
      :link: sanic
      :link-type: doc

      App/request context integration with lifecycle listeners and middleware.

   .. grid-item-card:: Google ADK
      :link: adk
      :link-type: doc

      Session, event, and memory storage for Google Agent Development Kit.

   .. grid-item-card:: Events
      :link: events
      :link-type: doc

      Pub/sub event channels with database-backed queues.

.. toctree::
   :hidden:

   litestar
   fastapi
   flask
   sanic
   starlette
   adk
   events
