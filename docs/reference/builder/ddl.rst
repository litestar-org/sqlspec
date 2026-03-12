===
DDL
===

Data Definition Language builders for tables, indexes, views, and schemas.

.. currentmodule:: sqlspec.builder

Base
====

.. autoclass:: DDLBuilder
   :members:
   :show-inheritance:

Tables
======

.. autoclass:: CreateTable
   :members:
   :show-inheritance:

.. autoclass:: CreateTableAsSelect
   :members:
   :show-inheritance:

.. autoclass:: AlterTable
   :members:
   :show-inheritance:

.. autoclass:: DropTable
   :members:
   :show-inheritance:

.. autoclass:: RenameTable
   :members:
   :show-inheritance:

.. autoclass:: Truncate
   :members:
   :show-inheritance:

.. autoclass:: CommentOn
   :members:
   :show-inheritance:

Column and Constraint Definitions
=================================

.. autoclass:: sqlspec.builder._ddl.ColumnDefinition
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.builder._ddl.ConstraintDefinition
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.builder._ddl.AlterOperation
   :members:
   :show-inheritance:

Indexes
=======

.. autoclass:: CreateIndex
   :members:
   :show-inheritance:

.. autoclass:: DropIndex
   :members:
   :show-inheritance:

Views
=====

.. autoclass:: CreateView
   :members:
   :show-inheritance:

.. autoclass:: CreateMaterializedView
   :members:
   :show-inheritance:

.. autoclass:: DropView
   :members:
   :show-inheritance:

.. autoclass:: DropMaterializedView
   :members:
   :show-inheritance:

Schemas
=======

.. autoclass:: CreateSchema
   :members:
   :show-inheritance:

.. autoclass:: DropSchema
   :members:
   :show-inheritance:
