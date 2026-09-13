==========
Developers
==========

Welcome to the SQLSpec developer documentation! SQLSpec is a type-safe SQL query mapper and connectivity layer designed for high performance with minimal abstraction. Whether you are building new database adapters, enhancing the query builder, fixing a bug, or improving documentation, this section provides the essential resources to get started.

.. grid:: 1 1 2 2
    :padding: 0
    :gutter: 2

    .. grid-item-card:: :octicon:`git-pull-request` Contribution Guide
      :link: ../contribution-guide
      :link-type: doc

      Environment setup with ``uv``, development workflow, coding standards (PEP 604, typing, docstrings), and repository quality gates.

    .. grid-item-card:: :octicon:`database` Creating Adapters
      :link: creating_adapters
      :link-type: doc

      Architecture guide for authoring new synchronous and asynchronous database adapters, driver base classes, statement configs, and feature flags.

    .. grid-item-card:: :octicon:`tag` Releases & Versioning
      :link: ../releases
      :link-type: doc

      Release lifecycle, Semantic Versioning standards, pre-release cadences, LTS deprecation policies, and the automated CI publishing pipeline.

    .. grid-item-card:: :octicon:`history` Changelog
      :link: ../changelog
      :link-type: doc

      Comprehensive version-by-version change log covering features, bug fixes, breaking changes, and migration notes across all releases.

.. toctree::
    :maxdepth: 2
    :hidden:

    ../contribution-guide
    creating_adapters
    ../releases
    ../changelog
