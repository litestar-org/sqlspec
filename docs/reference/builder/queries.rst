=======
Queries
=======

DML query builders for SELECT, INSERT, UPDATE, DELETE, and MERGE operations.

.. currentmodule:: sqlspec.builder

Select
======

.. autoclass:: Select
   :members:
   :show-inheritance:

Select Clause Mixins
--------------------

.. autoclass:: SelectClauseMixin
   :members:
   :show-inheritance:

.. autoclass:: WhereClauseMixin
   :members:
   :show-inheritance:

.. autoclass:: OrderByClauseMixin
   :members:
   :show-inheritance:

.. autoclass:: LimitOffsetClauseMixin
   :members:
   :show-inheritance:

.. autoclass:: HavingClauseMixin
   :members:
   :show-inheritance:

.. autoclass:: ReturningClauseMixin
   :members:
   :show-inheritance:

.. autoclass:: CommonTableExpressionMixin
   :members:
   :show-inheritance:

.. autoclass:: SetOperationMixin
   :members:
   :show-inheritance:

.. autoclass:: PivotClauseMixin
   :members:
   :show-inheritance:

.. autoclass:: UnpivotClauseMixin
   :members:
   :show-inheritance:

Window Functions
----------------

.. autoclass:: WindowFunctionBuilder
   :members:
   :show-inheritance:

Case Expressions
----------------

.. autoclass:: Case
   :members:
   :show-inheritance:

.. autoclass:: CaseBuilder
   :members:
   :show-inheritance:

Subqueries
----------

.. autoclass:: SubqueryBuilder
   :members:
   :show-inheritance:

Insert
======

.. autoclass:: Insert
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.builder._insert.ConflictBuilder
   :members:
   :show-inheritance:

Update
======

.. autoclass:: Update
   :members:
   :show-inheritance:

Delete
======

.. autoclass:: Delete
   :members:
   :show-inheritance:

Merge
=====

.. autoclass:: Merge
   :members:
   :show-inheritance:

Explain
=======

.. autoclass:: Explain
   :members:
   :show-inheritance:

Joins
=====

.. autoclass:: JoinBuilder
   :members:
   :show-inheritance:
