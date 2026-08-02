Command Line Interface
======================

SQLSpec includes a CLI for managing migrations and inspecting configuration. Use it
when you want a fast, explicit workflow without additional tooling.

Every command needs a configuration reference. Pass it with ``--config``, set
``SQLSPEC_CONFIG``, or record it in ``[tool.sqlspec]`` -- see
:ref:`pointing-the-cli-at-your-configuration` for the details.

Core Commands
-------------

.. code-block:: console

   sqlspec --config database:database_config show-config
   sqlspec --config database:database_config init --no-prompt
   sqlspec --config database:database_config create-migration -m "add users" --no-prompt
   sqlspec --config database:database_config upgrade --no-prompt
   sqlspec --config database:database_config downgrade --no-prompt
   sqlspec --config database:database_config show-current-revision

Common Options
--------------

- ``--bind-key`` targets a specific database configuration.
- ``--no-prompt`` skips confirmation prompts.
- ``--format`` selects SQL vs Python migration files.
- ``--validate-config`` reports each configuration and whether it is async-capable.
- ``--use-logger`` emits migration output via structured logger.
- ``--no-echo`` disables console output for migration commands.
- ``--summary`` emits a single summary log entry when logger output is enabled.

Tips
----

- ``show-config`` verifies the CLI resolved the configuration you expected
  before you run anything that touches the database.
- Run ``sqlspec --help`` to see global options.
- Run ``sqlspec upgrade --help`` to see command-specific migration options.

Related Guides
--------------

- :doc:`migrations` for migration workflow details.
