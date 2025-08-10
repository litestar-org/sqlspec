=======
SQLSpec
=======

.. container:: title-with-logo

    .. raw:: html

        <div class="brand-text">SQLSpec</div>

SQLSpec is a type-safe SQL query mapper designed for minimal abstraction between Python and SQL.
It is NOT an ORM but rather a flexible connectivity layer that provides consistent interfaces across multiple database systems.

SQLSpec provides :doc:`database adapters <usage/index>`, :doc:`statement compilation <reference/index>`,
and implementations of connection pooling, parameter handling, and result mapping patterns
to simplify your database operations while maintaining full control over your SQL.

.. container:: buttons wrap

  .. raw:: html

    <a href="usage/index.html" class="btn-no-wrap">Get Started</a>
    <a href="usage/index.html" class="btn-no-wrap">Usage Docs</a>
    <a href="reference/index.html" class="btn-no-wrap">API Docs</a>

.. grid:: 1 1 2 2
    :padding: 0
    :gutter: 2

    .. grid-item-card:: :octicon:`versions` Changelog
      :link: changelog
      :link-type: doc

      The latest updates and enhancements to SQLSpec

    .. grid-item-card:: :octicon:`comment-discussion` Discussions
      :link: https://github.com/litestar-org/sqlspec/discussions

      Join discussions, pose questions, or share insights.

    .. grid-item-card:: :octicon:`issue-opened` Issues
      :link: https://github.com/litestar-org/sqlspec/issues

      Report issues or suggest new features.

    .. grid-item-card:: :octicon:`beaker` Contributing
      :link: contribution-guide
      :link-type: doc

      Contribute to SQLSpec's growth with code, docs, and more.


.. _sponsor-github: https://github.com/sponsors/litestar-org
.. _sponsor-oc: https://opencollective.com/litestar
.. _sponsor-polar: https://polar.sh/litestar-org

.. toctree::
    :titlesonly:
    :caption: Documentation
    :hidden:

    usage/index
    reference/index

.. toctree::
    :titlesonly:
    :caption: Contributing
    :hidden:

    changelog
    contribution-guide
    Available Issues <https://github.com/search?q=user%3Alitestar-org+state%3Aopen+label%3A%22good+first+issue%22+++no%3Aassignee+repo%3A%22sqlspec%22&type=issues>
    Code of Conduct <https://github.com/litestar-org/.github?tab=coc-ov-file#readme>
