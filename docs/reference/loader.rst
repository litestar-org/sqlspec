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

Declared Parameters
===================

Parameters declared in SQL files via ``-- param:`` directives are exposed as
:class:`ParameterDeclaration` objects. See :ref:`Declared Parameters <declared-parameters>`
for the grammar and validation behavior.

.. autoclass:: sqlspec.ParameterDeclaration
   :members:
   :show-inheritance:

.. autofunction:: sqlspec.register_param_type

.. autofunction:: sqlspec.resolve_param_type
