==========
Migrations
==========

Native migration system for SQLSpec that leverages the SQL file loader and
driver system for database versioning. Supports SQL and Python migration files,
squashing, and validation.

Commands
========

.. autoclass:: sqlspec.migrations.SyncMigrationCommands
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.migrations.AsyncMigrationCommands
   :members:
   :show-inheritance:

.. autofunction:: sqlspec.migrations.commands.create_migration_commands

Runners
=======

.. autoclass:: sqlspec.migrations.SyncMigrationRunner
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.migrations.AsyncMigrationRunner
   :members:
   :show-inheritance:

.. autofunction:: sqlspec.migrations.runner.create_migration_runner

Trackers
========

.. autoclass:: sqlspec.migrations.SyncMigrationTracker
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.migrations.AsyncMigrationTracker
   :members:
   :show-inheritance:

Loaders
=======

.. autoclass:: sqlspec.migrations.BaseMigrationLoader
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.migrations.SQLFileLoader
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.migrations.PythonFileLoader
   :members:
   :show-inheritance:

.. autofunction:: sqlspec.migrations.loaders.get_migration_loader

Squashing
=========

.. autoclass:: sqlspec.migrations.MigrationSquasher
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.migrations.SquashPlan
   :members:
   :show-inheritance:

Version Management
==================

.. autoclass:: sqlspec.migrations.version.MigrationVersion
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.migrations.version.VersionType
   :members:
   :show-inheritance:

Context
=======

.. autoclass:: sqlspec.migrations.context.MigrationContext
   :members:
   :show-inheritance:

Fixer
=====

.. autoclass:: sqlspec.migrations.fix.MigrationFixer
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.migrations.fix.MigrationRename
   :members:
   :show-inheritance:

Templates
=========

.. autoclass:: sqlspec.migrations.templates.MigrationTemplateSettings
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.migrations.templates.MigrationTemplateProfile
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.migrations.templates.SQLTemplateDefinition
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.migrations.templates.PythonTemplateDefinition
   :members:
   :show-inheritance:

Utilities
=========

.. autofunction:: sqlspec.migrations.create_migration_file

.. autofunction:: sqlspec.migrations.drop_all

.. autofunction:: sqlspec.migrations.get_author
