=========
Changelog
=========

All commits to this project will be documented in this file.

SQLSpec Changelog
==================

Recent Updates
==============

Extension Migration Configuration
----------------------------------

Extension migrations now receive automatic version prefixes and configuration has been simplified:

1. **Version Prefixing** (Automatic)

   Extension migrations are automatically prefixed to prevent version collisions:

   .. code-block:: text

      # User migrations
      0001_initial.py       → version: 0001

      # Extension migrations (automatic prefix)
      0001_create_tables.py → version: ext_adk_0001
      0001_create_session.py → version: ext_litestar_0001

2. **Configuration Format** (Important)

   Extension settings must be in ``extension_config`` only:

   .. code-block:: python

      # Incorrect format
      migration_config={
          "include_extensions": [
              {"name": "adk", "session_table": "custom"}
          ]
      }

      # Correct format
      extension_config={
          "adk": {"session_table": "custom"}
      },
      migration_config={
          "include_extensions": ["adk"]  # Simple string list
      }

**Configuration Guide**: See :doc:`/migration_guides/extension_config`

Features
--------

- Extension migrations now automatically prefixed (``ext_adk_0001``, ``ext_litestar_0001``)
- Eliminated version collision between extension and user migrations
- Simplified extension configuration API
- Single source of truth for extension settings (``extension_config``)

Bug Fixes
---------

- Fixed version collision when extension and user migrations had the same version number
- Fixed duplicate key violation in ``ddl_migrations`` table when using extensions
- Improved migration tracking with clear extension identification

Technical Changes
-----------------

- ``_load_migration_metadata()`` now accepts optional ``version`` parameter
- ``_parse_extension_configs()`` rewritten to read from ``extension_config`` only
- Extension migration version prefixing handled in ``_get_migration_files_sync()``
- Removed dict format support from ``include_extensions``

**Previous Versions**
=====================
