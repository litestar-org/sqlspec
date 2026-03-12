=======
Recipes
=======

Production patterns and integration examples from real-world projects. These go beyond
sqlspec's core API to show how it fits into larger application architectures.

Unlike the :doc:`usage guides </usage/index>`, which explain sqlspec's own features,
recipes show how to combine sqlspec with external libraries and common application
patterns.

.. grid:: 1 1 2 3
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Dishka Dependency Injection
      :link: dishka
      :link-type: doc

      Request-scoped drivers and app-scoped configs with Dishka DI containers.

   .. grid-item-card:: Service Layer Pattern
      :link: service_layer
      :link-type: doc

      Base service classes with pagination, get-or-404, and transaction helpers.


.. toctree::
   :hidden:

   dishka
   service_layer
