=====
Cache
=====

Thread-safe LRU caching system with configurable TTL, size limits, and
namespace support. Used internally for statement caching, expression caching,
and builder result caching.

.. currentmodule:: sqlspec.core.cache

CacheConfig
===========

.. autoclass:: CacheConfig
   :members:
   :show-inheritance:

LRUCache
========

.. autoclass:: LRUCache
   :members:
   :show-inheritance:

NamespacedCache
===============

.. autoclass:: NamespacedCache
   :members:
   :show-inheritance:

CachedStatement
===============

.. autoclass:: CachedStatement
   :members:
   :show-inheritance:

Supporting Types
================

.. autoclass:: CacheKey
   :members:
   :show-inheritance:

.. autoclass:: CacheStats
   :members:
   :show-inheritance:

.. autoclass:: CacheNode
   :members:
   :show-inheritance:

.. autoclass:: FiltersView
   :members:
   :show-inheritance:

Module-Level Functions
======================

.. autofunction:: get_cache

.. autofunction:: get_default_cache

.. autofunction:: get_cache_config

.. autofunction:: update_cache_config

.. autofunction:: get_cache_statistics

.. autofunction:: get_cache_stats

.. autofunction:: log_cache_stats

.. autofunction:: reset_cache_stats

.. autofunction:: clear_all_caches

.. autofunction:: create_cache_key

.. autofunction:: canonicalize_filters
