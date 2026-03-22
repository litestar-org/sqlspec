Command Line Interface
======================

SQLSpec includes a CLI for managing migrations and inspecting configuration. Use it
when you want a fast, explicit workflow without additional tooling.

Configuration can come from ``--config``, ``SQLSPEC_CONFIG``, or
``[tool.sqlspec]`` in ``pyproject.toml``.

Core Commands
-------------

.. code-block:: console

   sqlspec init
   sqlspec create-migration -m "add users"
   sqlspec upgrade
   sqlspec downgrade

Common Options
--------------

- ``--bind-key`` targets a specific database configuration.
- ``--no-prompt`` skips confirmation prompts.
- ``--format`` selects SQL vs Python migration files.
- ``--use-logger`` emits migration output via structured logger.
- ``--no-echo`` disables console output for migration commands.
- ``--summary`` emits a single summary log entry when logger output is enabled.

Tips
----

- Run ``sqlspec --help`` to see global options.
- Run ``sqlspec upgrade --help`` to see command-specific migration options.

Related Guides
--------------

- :doc:`migrations` for migration workflow details.
