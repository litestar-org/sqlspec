=========
Changelog
=========

All commits to this project will be documented in this file.

SQLSpec Changelog
==================

Recent Updates
==============

Hybrid Versioning with Fix Command
-----------------------------------

Added comprehensive hybrid versioning support for database migrations:

- **Fix Command** - Convert timestamp migrations to sequential format
- **Hybrid Workflow** - Use timestamps in development, sequential in production
- **Automatic Conversion** - CI integration for seamless workflow
- **Safety Features** - Automatic backup, rollback on errors, dry-run preview

Key Features:

- **Zero merge conflicts**: Developers use timestamps (``20251011120000``) during development
- **Deterministic ordering**: Production uses sequential format (``0001``, ``0002``, etc.)
- **Database synchronization**: Automatically updates version tracking table
- **File operations**: Renames files and updates SQL query names
- **CI-ready**: ``--yes`` flag for automated workflows

.. code-block:: bash

   # Preview changes
   sqlspec --config myapp.config fix --dry-run

   # Apply conversion
   sqlspec --config myapp.config fix

   # CI/CD mode
   sqlspec --config myapp.config fix --yes --no-database

Example conversion:

.. code-block:: text

   Before:                              After:
   migrations/                          migrations/
   ├── 0001_initial.sql                ├── 0001_initial.sql
   ├── 0002_add_users.sql              ├── 0002_add_users.sql
   ├── 20251011120000_products.sql →   ├── 0003_add_products.sql
   └── 20251012130000_orders.sql   →   └── 0004_add_orders.sql

**Documentation:**

- Complete CLI reference: :doc:`usage/cli`
- Workflow guide: :ref:`hybrid-versioning-guide`
- CI integration examples for GitHub Actions and GitLab CI

**Use Cases:**

- Teams with parallel development avoiding migration number conflicts
- Projects requiring deterministic migration ordering in production
- CI/CD pipelines that standardize migrations before deployment

Shell Completion Support
-------------------------

Added comprehensive shell completion support for the SQLSpec CLI:

- **Bash, Zsh, and Fish support** - Tab completion for commands and options
- **Easy setup** - One-time eval command in your shell rc file
- **Comprehensive documentation** - Setup instructions in :doc:`usage/cli`

.. code-block:: bash

   # Bash - add to ~/.bashrc
   eval "$(_SQLSPEC_COMPLETE=bash_source sqlspec)"

   # Zsh - add to ~/.zshrc
   eval "$(_SQLSPEC_COMPLETE=zsh_source sqlspec)"

   # Fish - add to ~/.config/fish/completions/sqlspec.fish
   eval (env _SQLSPEC_COMPLETE=fish_source sqlspec)

After setup, tab completion works for all commands and options:

.. code-block:: bash

   sqlspec <TAB>              # Shows: create-migration, downgrade, init, ...
   sqlspec create-migration --<TAB>  # Shows: --bind-key, --help, --message, ...

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
