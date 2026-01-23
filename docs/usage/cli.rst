Command Line Interface
======================

SQLSpec includes a CLI for managing migrations and inspecting configuration. Use it
when you want a fast, explicit workflow without additional tooling.

Core Commands
-------------

.. code-block:: console

   sqlspec db init
   sqlspec db create-migration -m "add users"
   sqlspec db upgrade
   sqlspec db downgrade

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
- Run ``sqlspec db --help`` to see migration command details.

Related Guides
--------------

- :doc:`migrations` for migration workflow details.
