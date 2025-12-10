==================
Command Line (CLI)
==================

SQLSpec provides a command-line interface for managing database migrations and other operations.
This guide covers CLI installation, shell completion setup, and available commands.

.. contents:: Table of Contents
   :local:
   :depth: 2

Installation
============

The CLI is included when you install SQLSpec with the ``cli`` extra:

.. code-block:: bash

   pip install sqlspec[cli]
   # or with uv
   uv add sqlspec[cli]

The CLI is also automatically available when you install SQLSpec with any database adapter,
as ``rich-click`` is a core dependency.

Shell Completion
================

SQLSpec supports tab completion for bash, zsh, and fish shells. This feature helps you discover
commands and options faster without having to remember exact syntax.

Overview
--------

Shell completion allows you to:

- Tab-complete command names (``sqlspec <TAB>`` shows all commands)
- Tab-complete option flags (``sqlspec create-migration --<TAB>`` shows all options)
- Discover available commands interactively
- Reduce typos and improve productivity

Supported Shells
----------------

- **bash** 4.4+
- **zsh** 5.0+
- **fish** 3.0+

Setup Instructions
------------------

Bash
^^^^

Add the following to your ``~/.bashrc`` or ``~/.bash_profile``:

.. code-block:: bash

   # SQLSpec shell completion
   eval "$(_SQLSPEC_COMPLETE=bash_source sqlspec)"

Then reload your shell:

.. code-block:: bash

   source ~/.bashrc

**Alternative: System-wide installation** (requires sudo)

.. code-block:: bash

   # Generate completion script
   _SQLSPEC_COMPLETE=bash_source sqlspec > /etc/bash_completion.d/sqlspec

   # Reload completions
   source /etc/bash_completion.d/sqlspec

Zsh
^^^

Add the following to your ``~/.zshrc``:

.. code-block:: zsh

   # SQLSpec shell completion
   eval "$(_SQLSPEC_COMPLETE=zsh_source sqlspec)"

Then reload your shell:

.. code-block:: zsh

   source ~/.zshrc

**Alternative: Using completion directory**

.. code-block:: zsh

   # Create completion directory if it doesn't exist
   mkdir -p ~/.zsh/completion

   # Generate completion script
   _SQLSPEC_COMPLETE=zsh_source sqlspec > ~/.zsh/completion/_sqlspec

   # Add to ~/.zshrc (before compinit)
   fpath=(~/.zsh/completion $fpath)
   autoload -Uz compinit && compinit

Fish
^^^^

Add the following to ``~/.config/fish/completions/sqlspec.fish``:

.. code-block:: fish

   # SQLSpec shell completion
   eval (env _SQLSPEC_COMPLETE=fish_source sqlspec)

Fish automatically loads completions from this directory, so no reload is needed.
Open a new terminal to activate.

**Alternative: One-liner**

.. code-block:: fish

   _SQLSPEC_COMPLETE=fish_source sqlspec > ~/.config/fish/completions/sqlspec.fish

Verification
------------

After setup, test completion by typing:

.. code-block:: bash

   sqlspec <TAB>

You should see available commands:

.. code-block:: text

   create-migration  downgrade  fix  init  show-config  show-current-revision  stamp  upgrade

Try option completion:

.. code-block:: bash

   sqlspec create-migration --<TAB>

Expected output:

.. code-block:: text

   --bind-key  --help  --message  --no-prompt

Troubleshooting
---------------

**Completion not working after setup**

1. Make sure you reloaded your shell configuration:

   .. code-block:: bash

      # Bash
      source ~/.bashrc

      # Zsh
      source ~/.zshrc

      # Fish - open new terminal

2. Verify ``sqlspec`` is in your PATH:

   .. code-block:: bash

      which sqlspec

3. Test completion generation manually:

   .. code-block:: bash

      _SQLSPEC_COMPLETE=bash_source sqlspec

   This should output the completion script without errors.

**"command not found: sqlspec"**

Install SQLSpec with the CLI extra:

.. code-block:: bash

   pip install sqlspec[cli]

**Completion works but shows no results**

This is usually because the ``--config`` option is required for most commands.
The completion will show available options, but actual command execution requires
a valid configuration path.

**Performance issues with completion**

If completion feels slow, consider using the alternative installation methods
that generate static completion files (system-wide bash, zsh completion directory).

Available Commands
==================

The SQLSpec CLI provides commands for managing database migrations. Configure the CLI
using ``--config`` flag, ``SQLSPEC_CONFIG`` environment variable, or ``[tool.sqlspec]``
section in pyproject.toml.

Configuration Loading
---------------------

SQLSpec CLI supports three ways to specify your database configuration, in order of precedence:

1. **CLI Flag** (``--config``) - Explicit override for one-off commands
2. **Environment Variable** (``SQLSPEC_CONFIG``) - Convenient for development workflows
3. **pyproject.toml** (``[tool.sqlspec]``) - Project-wide default configuration

The ``--config`` option and ``SQLSPEC_CONFIG`` environment variable accept:

- **A single config object**: ``myapp.config.db_config``
- **A config list**: ``myapp.config.configs``
- **A callable function**: ``myapp.config.get_configs()``
- **Multiple config paths (comma-separated)**: ``myapp.config.primary_config,myapp.config.analytics_config``

Each config path is resolved independently, and if a callable returns a list of configs, all configs are collected.

Example configuration file (``myapp/config.py``):

.. literalinclude:: /examples/usage/usage_cli_1.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example
   :caption: `configuration loading`

Config Discovery Methods
^^^^^^^^^^^^^^^^^^^^^^^^

**Method 1: CLI Flag (Highest Priority)**

.. code-block:: bash

   sqlspec --config myapp.config.get_configs upgrade head

Use for one-off commands or to override other config sources.

**Method 2: Environment Variable**

.. code-block:: bash

   export SQLSPEC_CONFIG=myapp.config.get_configs
   sqlspec upgrade head  # Uses environment variable

Convenient for development. Add to your shell profile:

.. code-block:: bash

   # ~/.bashrc or ~/.zshrc
   export SQLSPEC_CONFIG=myapp.config.get_configs

Multiple configs (comma-separated):

.. code-block:: bash

   export SQLSPEC_CONFIG="app.db.primary_config,app.db.analytics_config"

**Method 3: pyproject.toml (Project Default)**

.. code-block:: toml

   [tool.sqlspec]
   config = "myapp.config.get_configs"

Best for team projects - config is version controlled.

Multiple configs (array):

.. code-block:: toml

   [tool.sqlspec]
   config = [
       "myapp.config.primary_config",
       "myapp.config.analytics_config"
   ]

**Precedence:** CLI flag > Environment variable > pyproject.toml

If no config is found from any source, SQLSpec will show a helpful error message with examples.

Multi-Config Resolution Details
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using comma-separated config paths or list format in pyproject.toml, SQLSpec:

1. **Resolves each path independently**: Each dotted path is imported and resolved
2. **Flattens callable results**: If a callable returns a list, all configs are collected
3. **Deduplicates by bind_key**: Later configs override earlier ones with the same ``bind_key``
4. **Validates final list**: Empty config lists result in an error

**Deduplication Example:**

.. code-block:: bash

   # If primary_config has bind_key="db" and backup_config has bind_key="db"
   export SQLSPEC_CONFIG="app.primary_config,app.backup_config"

   # Result: Only backup_config is used (last wins)

**Combined callable and list:**

.. code-block:: python

   # myapp/config.py
   def get_all_configs():
       """Returns list of configs."""
       return [primary_config, analytics_config]

   single_config = AsyncpgConfig(bind_key="backup", ...)

.. code-block:: bash

   # This resolves to 3 configs: primary, analytics, and backup
   export SQLSPEC_CONFIG="myapp.config.get_all_configs,myapp.config.single_config"

**Deduplication with callables:**

.. code-block:: python

   # myapp/config.py
   primary = AsyncpgConfig(bind_key="db", ...)
   updated = AsyncpgConfig(bind_key="db", ...)  # Same bind_key

   def get_primary():
       return primary

   def get_updated():
       return updated

.. code-block:: bash

   # Only updated config is used (last wins for bind_key="db")
   sqlspec --config "myapp.config.get_primary,myapp.config.get_updated" upgrade

Global Options
--------------

``--config PATH``
   Dotted path to SQLSpec config(s) or callable function. Supports comma-separated
   multiple paths. Optional when using environment variable or pyproject.toml config discovery.

   Examples:

   - Single config: ``--config myapp.config.db_config``
   - Callable: ``--config myapp.config.get_configs``
   - Multiple paths: ``--config "myapp.config.primary_config,myapp.config.analytics_config"``

   Configs with duplicate ``bind_key`` values are deduplicated (last wins).

``--validate-config``
   Validate configuration before executing migrations. Shows loaded configs
   and their types.

   Example:

   .. code-block:: bash

      sqlspec --config myapp.config --validate-config init

   Output:

   .. code-block:: text

      ✓ Successfully loaded 1 config(s)
        • postgres: AsyncpgConfig (async-capable)

Commands Reference
------------------

init
^^^^

Initialize migration directory structure.

.. code-block:: bash

   sqlspec --config myapp.config init [DIRECTORY]

**Arguments:**

``DIRECTORY``
   Optional. Migration directory path. Defaults to ``migration_config["script_location"]``
   from your config (typically ``"migrations"``).

**Options:**

``--bind-key KEY``
   Specify which config to use (if you have multiple configs).

``--package / --no-package``
   Create ``__init__.py`` in migration folder. Default: ``--package``

``--no-prompt``
   Skip confirmation prompt.

**Example:**

.. code-block:: bash

   # Use default location from config
   sqlspec --config myapp.config init

   # Custom location
   sqlspec --config myapp.config init db/migrations

   # Skip confirmation
   sqlspec --config myapp.config init --no-prompt

**Creates:**

.. code-block:: text

   migrations/
   ├── __init__.py
   └── versions/
       └── __init__.py

create-migration
^^^^^^^^^^^^^^^^

Create a new migration file.

.. code-block:: bash

   sqlspec --config myapp.config create-migration [OPTIONS]

**Aliases:** ``make-migration``

**Options:**

``-m, --message TEXT``
   Migration description. If not provided, you'll be prompted.

``--bind-key KEY``
   Specify which config to use.

``--no-prompt``
   Use default message "new migration" if ``--message`` not provided.

**Example:**

.. code-block:: bash

   # With message
   sqlspec --config myapp.config create-migration -m "Add user table"

   # Interactive (prompts for message)
   sqlspec --config myapp.config create-migration

   # No prompt mode
   sqlspec --config myapp.config create-migration --no-prompt

**Creates:**

.. code-block:: text

   migrations/versions/0001_add_user_table.py

The generated file contains empty ``upgrade()`` and ``downgrade()`` functions
where you add your SQL:

.. code-block:: python

   """Add user table

   Revision ID: 0001_add_user_table
   Created at: 2025-10-10 15:30:45
   """

   def upgrade():
       """Apply migration."""
       return """
       CREATE TABLE users (
           id SERIAL PRIMARY KEY,
           email TEXT NOT NULL UNIQUE
       );
       """

   def downgrade():
       """Revert migration."""
       return """
       DROP TABLE users;
       """

upgrade
^^^^^^^

Apply pending migrations up to a specific revision.

.. code-block:: bash

   sqlspec --config myapp.config upgrade [REVISION]

**Arguments:**

``REVISION``
   Target revision. Default: ``"head"`` (latest).

   - ``head`` - Upgrade to latest migration
   - ``0001`` - Upgrade to specific revision
   - ``+1`` - Upgrade one revision forward
   - ``+2`` - Upgrade two revisions forward

**Options:**

``--bind-key KEY``
   Target specific config.

``--include NAME``
   Only upgrade specified configs (can use multiple times).

``--exclude NAME``
   Exclude specific configs (can use multiple times).

``--dry-run``
   Show what would be executed without making changes.

``--execution-mode {auto,sync,async}``
   Force execution mode. Default: ``auto`` (auto-detects).

``--no-prompt``
   Skip confirmation prompt.

``--no-auto-sync``
   Disable automatic version reconciliation. When enabled (default), SQLSpec automatically
   updates database tracking when migrations are renamed from timestamp to sequential format.
   Use this flag when you want explicit control over version reconciliation.

**Examples:**

.. code-block:: bash

   # Upgrade to latest (with auto-sync enabled by default)
   sqlspec --config myapp.config upgrade

   # Upgrade to specific revision
   sqlspec --config myapp.config upgrade 0005

   # Upgrade one step
   sqlspec --config myapp.config upgrade +1

   # Dry run (show what would happen)
   sqlspec --config myapp.config upgrade --dry-run

   # Multi-config: only upgrade specific configs
   sqlspec --config myapp.config upgrade --include postgres --include mysql

   # Multi-config: exclude specific config
   sqlspec --config myapp.config upgrade --exclude oracle

   # No confirmation
   sqlspec --config myapp.config upgrade --no-prompt

   # Disable auto-sync for manual control
   sqlspec --config myapp.config upgrade --no-auto-sync

downgrade
^^^^^^^^^

Rollback migrations to a specific revision.

.. code-block:: bash

   sqlspec --config myapp.config downgrade [REVISION]

**Arguments:**

``REVISION``
   Target revision. Default: ``"-1"`` (one step back).

   - ``-1`` - Downgrade one revision
   - ``-2`` - Downgrade two revisions
   - ``0003`` - Downgrade to specific revision
   - ``base`` - Rollback all migrations

**Options:**

``--bind-key KEY``
   Target specific config.

``--include NAME``
   Only downgrade specified configs.

``--exclude NAME``
   Exclude specific configs.

``--dry-run``
   Show what would be executed without making changes.

``--no-prompt``
   Skip confirmation prompt.

**Examples:**

.. code-block:: bash

   # Downgrade one step
   sqlspec --config myapp.config downgrade

   # Downgrade to specific revision
   sqlspec --config myapp.config downgrade 0003

   # Rollback all migrations
   sqlspec --config myapp.config downgrade base

   # Dry run
   sqlspec --config myapp.config downgrade --dry-run

   # No confirmation
   sqlspec --config myapp.config downgrade --no-prompt

.. warning::

   Downgrade operations can result in data loss. Always backup your database
   before running downgrade commands in production.

fix
^^^

Convert timestamp migrations to sequential format for hybrid versioning workflow.

.. code-block:: bash

   sqlspec --config myapp.config fix [OPTIONS]

**Purpose:**

The ``fix`` command implements a hybrid versioning workflow that combines the benefits
of both timestamp and sequential migration numbering:

- **Development**: Use timestamps to avoid merge conflicts
- **Production**: Use sequential numbers for deterministic ordering

This command converts timestamp-format migrations (YYYYMMDDHHmmss) to sequential
format (0001, 0002, etc.) while preserving migration history in the database.

**Options:**

``--bind-key KEY``
   Target specific config.

``--dry-run``
   Preview changes without applying them.

``--yes``
   Skip confirmation prompt (useful for CI/CD).

``--no-database``
   Only rename files, skip database record updates.

**Examples:**

.. code-block:: bash

   # Preview what would change
   sqlspec --config myapp.config fix --dry-run

   # Apply changes with confirmation
   sqlspec --config myapp.config fix

   # CI/CD mode (auto-approve)
   sqlspec --config myapp.config fix --yes

   # Only fix files, don't update database
   sqlspec --config myapp.config fix --no-database

**Before Fix:**

.. code-block:: text

   migrations/
   ├── 0001_initial.sql
   ├── 0002_add_users.sql
   ├── 20251011120000_add_products.sql    # Timestamp format
   ├── 20251012130000_add_orders.sql      # Timestamp format

**After Fix:**

.. code-block:: text

   migrations/
   ├── 0001_initial.sql
   ├── 0002_add_users.sql
   ├── 0003_add_products.sql              # Converted to sequential
   ├── 0004_add_orders.sql                # Converted to sequential

**What Gets Updated:**

1. **File Names**: ``20251011120000_add_products.sql`` → ``0003_add_products.sql``
2. **SQL Query Names**: ``-- name: migrate-20251011120000-up`` → ``-- name: migrate-0003-up``
3. **Database Records**: Version tracking table updated to reflect new version numbers

**Backup & Safety:**

The command automatically creates a timestamped backup before making changes:

.. code-block:: text

   migrations/
   ├── .backup_20251012_143022/    # Automatic backup
   │   ├── 20251011120000_add_products.sql
   │   └── 20251012130000_add_orders.sql
   ├── 0003_add_products.sql
   └── 0004_add_orders.sql

If conversion fails, files are automatically restored from backup.
Remove backup with ``rm -rf migrations/.backup_*`` after verifying success.

**Auto-Sync Integration:**

As of SQLSpec 0.18+, the ``upgrade`` command automatically reconciles renamed migrations
when you pull changes from teammates. This means developers typically don't need to run
``fix`` manually after pulling - just run ``upgrade`` and it handles reconciliation
automatically.

The ``fix`` command is still useful for:

- **Pre-merge CI**: Convert timestamps before merging to main branch
- **Initial conversion**: One-time conversion of existing timestamp migrations
- **Manual control**: When you've disabled auto-sync and want explicit control

See the :ref:`hybrid-versioning-guide` for complete workflows and examples.

**Use Cases:**

- **Pre-merge CI check**: Convert timestamps before merging to main branch
- **Production deployment**: Ensure deterministic migration ordering
- **Repository cleanup**: Standardize on sequential format after development

.. seealso::

   :ref:`hybrid-versioning-guide` for complete workflow documentation and best practices.

.. warning::

   Always commit migration files before running ``fix`` command. While automatic
   backups are created, version control provides the safest recovery option.

stamp
^^^^^

Mark the migration table with a specific revision without running migrations.

.. code-block:: bash

   sqlspec --config myapp.config stamp REVISION

**Arguments:**

``REVISION``
   **Required**. Revision to mark as current.

**Options:**

``--bind-key KEY``
   Target specific config.

**Example:**

.. code-block:: bash

   # Mark as head (latest)
   sqlspec --config myapp.config stamp head

   # Mark specific revision
   sqlspec --config myapp.config stamp 0005

**Use cases:**

- Initializing migration tracking on existing database
- Recovering from migration failures
- Syncing migration state after manual changes

show-current-revision
^^^^^^^^^^^^^^^^^^^^^

Display the current migration revision for your database.

.. code-block:: bash

   sqlspec --config myapp.config show-current-revision

**Options:**

``--bind-key KEY``
   Target specific config.

``--include NAME``
   Only show specified configs.

``--exclude NAME``
   Exclude specific configs.

``--verbose``
   Show detailed information.

**Example:**

.. code-block:: bash

   # Show current revision
   sqlspec --config myapp.config show-current-revision

   # Verbose output
   sqlspec --config myapp.config show-current-revision --verbose

   # Multi-config: show all
   sqlspec --config myapp.config show-current-revision

   # Multi-config: specific configs only
   sqlspec --config myapp.config show-current-revision --include postgres

**Output:**

.. code-block:: text

   Current Revision: 0005_add_user_preferences
   Database: postgres
   Applied at: 2025-10-10 14:30:00

show-config
^^^^^^^^^^^

List all configurations with migrations enabled.

.. code-block:: bash

   sqlspec --config myapp.config show-config

**Options:**

``--bind-key KEY``
   Show only specific config.

**Example:**

.. code-block:: bash

   # Show all configs
   sqlspec --config myapp.config show-config

   # Show specific config
   sqlspec --config myapp.config show-config --bind-key postgres

**Output:**

.. code-block:: text

   ┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┓
   ┃ Configuration Name   ┃ Migration Path       ┃ Status           ┃
   ┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━┩
   │ postgres            │ migrations/postgres  │ Migration Enabled│
   │ mysql               │ migrations/mysql     │ Migration Enabled│
   └─────────────────────┴──────────────────────┴──────────────────┘
   Found 2 configuration(s) with migrations enabled.

Multi-Config Operations
=======================

When you have multiple database configurations, SQLSpec provides options to manage
them collectively or selectively.

Quick Reference: Multi-Config Patterns
---------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Pattern
     - Example
     - Behavior
   * - Single config
     - ``--config "app.config.db"``
     - Load one config
   * - Config list
     - ``--config "app.config.configs"``
     - Load all configs in list
   * - Callable returning list
     - ``--config "app.config.get_configs"``
     - Call function, load returned configs
   * - Comma-separated paths
     - ``--config "app.config.db1,app.config.db2"``
     - Load multiple configs, deduplicate by bind_key
   * - Env var (comma-separated)
     - ``SQLSPEC_CONFIG="app.config.db1,app.config.db2"``
     - Same as comma-separated CLI flag
   * - pyproject.toml (list)
     - ``config = ["app.config.db1", "app.config.db2"]``
     - Load all paths in array
   * - Mixed callables and configs
     - ``--config "app.config.get_configs,app.config.backup"``
     - Flatten callable results + direct configs
   * - Duplicate bind_key
     - ``--config "app.config.old,app.config.new"``
     - Later config overrides (new wins)

Scenario: Multiple Databases
-----------------------------

.. literalinclude:: /examples/usage/usage_cli_2.py
   :language: python
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example
   :caption: `multiple databases`

Upgrade All Enabled Configs
----------------------------

.. code-block:: bash

   # Upgrades all configs with enabled=True
   sqlspec --config myapp.config upgrade

Selective Operations
--------------------

**Include specific configs:**

.. code-block:: bash

   # Only upgrade postgres and mysql
   sqlspec --config myapp.config upgrade --include postgres --include mysql

**Exclude specific configs:**

.. code-block:: bash

   # Upgrade all except analytics
   sqlspec --config myapp.config upgrade --exclude analytics

**Target single config:**

.. code-block:: bash

   # Only upgrade postgres
   sqlspec --config myapp.config upgrade --bind-key postgres

Dry Run Preview
---------------

See what would happen without executing:

.. code-block:: bash

   sqlspec --config myapp.config upgrade --dry-run

**Single Configuration Output:**

.. code-block:: text

   DRY RUN MODE: No database changes will be applied

   Found 2 pending migrations

   Would apply 0001_initial: Initial migration
   Migration file: migrations/versions/0001_initial.py

   Would apply 0002_add_users: Add users table
   Migration file: migrations/versions/0002_add_users.py

   Dry run complete. No changes were made to the database.

**Multi-Configuration Output:**

When using ``--include`` or ``--exclude`` with multiple configs:

.. code-block:: bash

   sqlspec --config myapp.config upgrade --dry-run --include postgres --include mysql

.. code-block:: text

   Dry run: Would upgrade 2 configuration(s)
     • postgres
     • mysql

The dry-run mode will then show the detailed migration list for each configuration.

Best Practices
==============

1. **Version Control**

   Always commit your migration files:

   .. code-block:: bash

      git add migrations/
      git commit -m "Add user table migration"

2. **Test Migrations**

   Test on a copy of production data before applying to production:

   .. code-block:: bash

      # Test downgrade as well
      sqlspec --config test.config upgrade
      sqlspec --config test.config downgrade

3. **Backup Before Downgrade**

   Always backup your database before running downgrade:

   .. code-block:: bash

      # Backup first
      pg_dump mydb > backup.sql

      # Then downgrade
      sqlspec --config myapp.config downgrade

4. **Use Descriptive Messages**

   Make migration messages clear and actionable:

   .. code-block:: bash

      # Good
      sqlspec --config myapp.config create-migration -m "Add email index to users table"

      # Bad
      sqlspec --config myapp.config create-migration -m "update"

5. **Review Migration Files**

   Always review generated migration files before applying:

   .. code-block:: bash

      # After creating migration
      cat migrations/versions/0001_add_user_table.py

6. **Use Dry Run**

   Preview changes before applying:

   .. code-block:: bash

      sqlspec --config myapp.config upgrade --dry-run

7. **Use Unique bind_key for Multi-Config**

   When managing multiple databases, always specify unique ``bind_key`` values:

   .. code-block:: python

      # Good - unique bind_keys
      configs = [
          AsyncpgConfig(bind_key="primary", ...),
          AsyncpgConfig(bind_key="analytics", ...),
      ]

      # Problematic - configs will overwrite each other
      configs = [
          AsyncpgConfig(bind_key="db", ...),  # Same key
          AsyncmyConfig(bind_key="db", ...),  # Will override above
      ]

8. **Prefer pyproject.toml for Team Projects**

   Store config paths in version control for consistency:

   .. code-block:: toml

      [tool.sqlspec]
      config = [
          "myapp.config.primary_db",
          "myapp.config.analytics_db"
      ]

   Team members automatically use the same config without manual setup.

Framework Integration
=====================

Litestar
--------

When using SQLSpec with Litestar, use the Litestar CLI instead:

.. code-block:: bash

   # Instead of: sqlspec --config myapp.config init
   litestar database init

   # Instead of: sqlspec --config myapp.config create-migration
   litestar database create-migration -m "Add user table"

   # Instead of: sqlspec --config myapp.config upgrade
   litestar database upgrade

The Litestar CLI automatically discovers your SQLSpec configuration from
the application instance.

See Also
========

- :doc:`configuration` - Learn about migration configuration options
- :doc:`framework_integrations` - Framework-specific CLI integration
- `Click Documentation <https://click.palletsprojects.com/>`_ - Underlying CLI framework
- `Rich-Click Documentation <https://github.com/ewels/rich-click>`_ - Enhanced CLI output
