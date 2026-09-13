==============
SQL File Loader
==============

Load and cache SQL files with named statement support. SQL files can contain
multiple named statements separated by ``-- name:`` comments.

.. currentmodule:: sqlspec.loader

SQLFileLoader
=============

.. autoclass:: SQLFileLoader
   :members:
   :show-inheritance:

SQLFile
=======

.. autoclass:: SQLFile
   :members:
   :show-inheritance:

NamedStatement
==============

.. autoclass:: NamedStatement
   :members:
   :show-inheritance:

Fragments and Slots
===================

SQL files can declare reusable ``-- fragment:`` sections, splice them into queries
with ``/* include: name */``, and mark fill points with ``/* slot: name */``. See
:ref:`Fragments and Slots <sql-fragments-and-slots>` for the syntax and the
``get_sql(name, **slots)`` call.

.. autoclass:: SlotDeclaration
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.exceptions.SQLSlotError
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.exceptions.SQLFragmentNotFoundError
   :members:
   :show-inheritance:

SQLFileCacheEntry
=================

.. autoclass:: SQLFileCacheEntry
   :members:
   :show-inheritance:

Declared Parameters
===================

Parameters declared in SQL files via ``-- param:`` directives are exposed as
:class:`ParameterDeclaration` objects. See :ref:`Declared Parameters <declared-parameters>`
for the grammar and validation behavior.

.. autoclass:: sqlspec.ParameterDeclaration
   :members:
   :show-inheritance:
   :no-index:

.. autofunction:: sqlspec.register_param_type

.. autofunction:: sqlspec.resolve_param_type
