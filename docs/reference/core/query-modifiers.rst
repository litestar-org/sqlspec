===============
Query Modifiers
===============

Helpers for modifying SQL statements by appending WHERE, LIMIT, OFFSET clauses
and building condition expressions programmatically.

.. currentmodule:: sqlspec.core.query_modifiers

ConditionFactory
================

.. autoclass:: ConditionFactory
   :members:
   :show-inheritance:

Statement Modifiers
===================

.. autofunction:: apply_where

.. autofunction:: apply_or_where

.. autofunction:: apply_limit

.. autofunction:: apply_offset

.. autofunction:: apply_select_only

.. autofunction:: safe_modify_with_cte

Condition Builders
==================

.. autofunction:: create_condition

.. autofunction:: create_between_condition

.. autofunction:: create_in_condition

.. autofunction:: create_not_in_condition

.. autofunction:: create_exists_condition

.. autofunction:: create_not_exists_condition

Expression Helpers
==================

.. autofunction:: expr_eq

.. autofunction:: expr_neq

.. autofunction:: expr_gt

.. autofunction:: expr_gte

.. autofunction:: expr_lt

.. autofunction:: expr_lte

.. autofunction:: expr_like

.. autofunction:: expr_not_like

.. autofunction:: expr_ilike

.. autofunction:: expr_is_null

.. autofunction:: expr_is_not_null

Utilities
=========

.. autofunction:: extract_column_name

.. autofunction:: parse_column_for_condition
