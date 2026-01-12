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
- ``--yes`` skips confirmation prompts.
- ``--format`` selects SQL vs Python migration files.

Tips
----

- Run ``sqlspec --help`` to see global options.
- Run ``sqlspec db --help`` to see migration command details.

Related Guides
--------------

- :doc:`migrations` for migration workflow details.
