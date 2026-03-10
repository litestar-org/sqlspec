====
Core
====

The core module provides the SQL processing pipeline: statement compilation,
parameter processing, result handling, caching, and filtering.

.. grid:: 2

   .. grid-item-card:: Statement & Compiler
      :link: statement
      :link-type: doc

      SQL object, StatementConfig, SQLProcessor, and CompiledSQL.

   .. grid-item-card:: Parameters
      :link: parameters
      :link-type: doc

      Parameter processing, style conversion, and validation.

   .. grid-item-card:: Results
      :link: results
      :link-type: doc

      SQLResult, ArrowResult, StackResult, and DMLResult.

   .. grid-item-card:: Filters
      :link: filters
      :link-type: doc

      Composable statement filters for pagination, search, and ordering.

   .. grid-item-card:: Cache
      :link: cache
      :link-type: doc

      LRU cache with TTL, namespaced caching, and cache configuration.

   .. grid-item-card:: Query Modifiers
      :link: query-modifiers
      :link-type: doc

      ConditionFactory, WHERE helpers, and expression functions.

   .. grid-item-card:: Type Converters
      :link: type-converters
      :link-type: doc

      Output and input type conversion for database values.

.. toctree::
   :hidden:

   statement
   parameters
   results
   filters
   cache
   query-modifiers
   type-converters
