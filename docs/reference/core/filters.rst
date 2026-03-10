=======
Filters
=======

Composable SQL statement filters for building WHERE clauses, pagination,
ordering, and search conditions. Filters can be applied to any SQL statement
via :func:`apply_filter`.

.. currentmodule:: sqlspec.core.filters

apply_filter
============

.. autofunction:: apply_filter

Base
====

.. autoclass:: StatementFilter
   :members:
   :show-inheritance:

Date Filters
============

.. autoclass:: BeforeAfterFilter
   :members:
   :show-inheritance:

.. autoclass:: OnBeforeAfterFilter
   :members:
   :show-inheritance:

Collection Filters
==================

.. autoclass:: InCollectionFilter
   :members:
   :show-inheritance:

.. autoclass:: NotInCollectionFilter
   :members:
   :show-inheritance:

.. autoclass:: AnyCollectionFilter
   :members:
   :show-inheritance:

.. autoclass:: NotAnyCollectionFilter
   :members:
   :show-inheritance:

Null Filters
============

.. autoclass:: NullFilter
   :members:
   :show-inheritance:

.. autoclass:: NotNullFilter
   :members:
   :show-inheritance:

Pagination
==========

.. autoclass:: LimitOffsetFilter
   :members:
   :show-inheritance:

.. autoclass:: OffsetPagination
   :members:
   :show-inheritance:

Ordering
========

.. autoclass:: OrderByFilter
   :members:
   :show-inheritance:

Search
======

.. autoclass:: SearchFilter
   :members:
   :show-inheritance:

.. autoclass:: NotInSearchFilter
   :members:
   :show-inheritance:

Type Aliases
============

.. data:: FilterTypes

   Union type of all filter classes.
