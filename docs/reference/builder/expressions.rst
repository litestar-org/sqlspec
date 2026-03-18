===========
Expressions
===========

Column definitions, function columns, expression wrappers, and vector expressions
used in query building.

.. currentmodule:: sqlspec.builder

Columns
=======

.. autoclass:: Column
   :members:
   :show-inheritance:

.. autoclass:: ColumnExpression
   :members:
   :show-inheritance:

.. autoclass:: FunctionColumn
   :members:
   :show-inheritance:

Expression Wrappers
===================

.. autoclass:: sqlspec.builder._expression_wrappers.ExpressionWrapper
   :members:
   :show-inheritance:

.. autoclass:: AggregateExpression
   :members:
   :show-inheritance:

.. autoclass:: FunctionExpression
   :members:
   :show-inheritance:

.. autoclass:: MathExpression
   :members:
   :show-inheritance:

.. autoclass:: StringExpression
   :members:
   :show-inheritance:

.. autoclass:: ConversionExpression
   :members:
   :show-inheritance:

Vector Expressions
==================

.. autofunction:: VectorDistance

Base Classes
============

.. autoclass:: QueryBuilder
   :members:
   :show-inheritance:

.. autoclass:: ExpressionBuilder
   :members:
   :show-inheritance:

.. autoclass:: BuiltQuery
   :members:
   :show-inheritance:

Parsing Utilities
=================

.. autofunction:: sqlspec.builder.extract_expression

.. autofunction:: sqlspec.builder.parse_column_expression

.. autofunction:: sqlspec.builder.parse_condition_expression

.. autofunction:: sqlspec.builder.parse_order_expression

.. autofunction:: sqlspec.builder.parse_table_expression

.. autofunction:: sqlspec.builder.to_expression
