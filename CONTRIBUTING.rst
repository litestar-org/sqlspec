Contribution guide
==================

Setting up the environment
--------------------------

1. Run ``make install-uv`` to install `uv <https://docs.astral.sh/uv/>`_ if not already installed
1. Run ``make install`` to install all dependencies and pre-commit hooks


Code contributions
------------------

Workflow
++++++++

1. `Fork <https://github.com/litestar-org/sqlspec/fork>`_ the `sqlspec repository <https://github.com/litestar-org/sqlspec>`_
2. Clone your fork locally with git
3. `Set up the environment <#setting-up-the-environment>`_
4. Make your changes
5. Run ``male lint`` to run linters and formatters. This step is optional and will be executed
   automatically by git before you make a commit, but you may want to run it manually in order to apply fixes
6. Commit your changes to git
7. Push the changes to your fork
8. Open a `pull request <https://docs.github.com/en/pull-requests>`_. Give the pull request a descriptive title
   indicating what it changes. If it has a corresponding open issue, the issue number should be included in the title as
   well. For example a pull request that fixes issue ``bug: Increased stack size making it impossible to find needle #100``
   could be titled ``fix(#100): Make needles easier to find by applying fire to haystack``

.. tip:: Pull requests and commits all need to follow the
    `Conventional Commit format <https://www.conventionalcommits.org>`_

Guidelines for writing code
----------------------------

- All code should be fully `typed <https://peps.python.org/pep-0484/>`_. This is enforced via
  `mypy <https://mypy.readthedocs.io/en/stable/>`_.
- All code should be tested. This is enforced via `pytest <https://docs.pytest.org/en/stable/>`_.
- All code should be properly formatted. This is enforced via `black <https://black.readthedocs.io/en/stable/>`_ and `Ruff <https://beta.ruff.rs/docs/>`_.

Writing and running tests
+++++++++++++++++++++++++

.. todo:: Write this section

Project documentation
---------------------

The documentation is located in the ``/docs`` directory and is `ReST <https://docutils.sourceforge.io/rst.html>`_ and
`Sphinx <https://www.sphinx-doc.org/en/master/>`_. If you're unfamiliar with any of those,
`ReStructuredText primer <https://www.sphinx-doc.org/en/master/lib/usage/restructuredtext/basics.html>`_ and
`Sphinx quickstart <https://www.sphinx-doc.org/en/master/lib/usage/quickstart.html>`_ are recommended reads.

Running the docs locally
++++++++++++++++++++++++

You can serve the documentation with ``make docs-serve``, or build them with ``make docs``.

Creating a new release
----------------------

1. Increment the version in `pyproject.toml <https://github.com/litestar-org/sqlspec/blob/main/pyproject.toml>`_.
    .. note:: The version should follow `semantic versioning <https://semver.org/>`_ and `PEP 440 <https://www.python.org/dev/peps/pep-0440/>`_.
2. `Draft a new release <https://github.com/litestar-org/sqlspec/releases/new>`_ on GitHub

   * Use ``vMAJOR.MINOR.PATCH`` (e.g. ``v1.2.3``) as both the tag and release title
   * Fill in the release description. You can use the "Generate release notes" function to get a draft for this
3. Commit your changes and push to ``main``
4. Publish the release
5. Go to `Actions <https://github.com/litestar-org/sqlspec/actions>`_ and approve the release workflow
6. Check that the workflow runs successfully
