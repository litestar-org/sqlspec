# Documentation Style Guide

**Version:** 1.0
**Last Updated:** 2025-10-09
**Purpose:** Maintain consistent voice, tone, and terminology across all SQLSpec documentation

## Table of Contents

1. [Voice & Tone](#voice--tone)
2. [Terminology Standards](#terminology-standards)
3. [Writing Patterns](#writing-patterns)
4. [Code Examples](#code-examples)
5. [Good/Bad Patterns](#goodbad-patterns)
6. [Documentation Checklist](#documentation-checklist)
7. [reStructuredText Guidelines](#restructuredtext-guidelines)

---

## Voice & Tone

### Core Principles

**Professional • Technical • Helpful**

- **Professional:** Authoritative without being stuffy
- **Technical:** Precise, accurate, detail-oriented
- **Helpful:** Focus on enabling developers to succeed

### What to Use

✅ **Present Tense**

- "SQLSpec provides connection pooling"
- "The store implements cascade deletes"
- NOT: "SQLSpec will provide" or "The store would implement"

✅ **Active Voice**

- "Configure the database connection"
- "The adapter optimizes queries"
- NOT: "The database connection is configured" (passive)

✅ **Direct Language**

- "Set the pool size to 20"
- "Use AsyncPG for production"
- NOT: "You might want to consider possibly setting..."

### What to Avoid

❌ **Casual Language**

- NOT: "awesome", "cool", "super easy", "just", "simply"
- INSTEAD: "effective", "straightforward", "efficient"

❌ **Marketing Speak**

- NOT: "revolutionary", "game-changing", "cutting-edge"
- INSTEAD: "efficient", "optimized", "modern"

❌ **Hyperbole**

- NOT: "blazing fast", "incredibly powerful", "absolutely amazing"
- INSTEAD: "high-performance", "capable", "effective"

❌ **Apologetic Language**

- NOT: "sorry", "unfortunately", "sadly"
- INSTEAD: State facts directly, offer solutions

❌ **Excessive Exclamation Marks**

- Use sparingly and only for genuine emphasis
- NOT: "This is amazing!" "It works!"
- OK: "Note: This operation is destructive!"

❌ **Change-Focused Language (MANDATORY)**

**Always document "the way it is" NOT "what changed"**

This applies to ALL documentation: docstrings, guides, API docs, commit messages, PRs, and code comments.

- NOT: "Now works like this", "New enhancement", "Changed to", "Updated behavior"
- INSTEAD: Describe current behavior as if it's always been this way

**Why:** Documentation should describe the current state of the system, not its history. Readers don't care what it used to do—they need to know what it does NOW.

**Examples:**

❌ **Bad - Change-focused:**

```python
def execute(query: str) -> list[dict[str, Any]]:
    """Execute a query.

    Now supports parameter binding and returns typed results.
    The old behavior of returning raw tuples is no longer supported.
    """
```

✅ **Good - Current state:**

```python
def execute(query: str) -> list[dict[str, Any]]:
    """Execute a query with parameter binding.

    Args:
        query: SQL query string with parameter placeholders.

    Returns:
        Query results as list of dictionaries with typed values.
    """
```

❌ **Bad - Documentation:**
> "The pool size configuration has been updated. It now defaults to 10 instead of the old default of 5."

✅ **Good - Documentation:**
> "The pool size defaults to 10 connections. Configure `pool_config={'min_size': 10}` to adjust."

**Exceptions (use version annotations):**

Only reference changes in version-specific annotations:

```rst
.. versionadded:: 1.2.0
   Parameter binding support.

.. versionchanged:: 1.3.0
   Pool size now defaults to 10.

.. deprecated:: 1.4.0
   Use :meth:`execute` instead of :meth:`run_query`.
```

But the main documentation should ONLY describe current behavior.

### Perspective Guidelines

Choose perspective based on document type:

| Document Type | Perspective | Example |
|---------------|-------------|---------|
| **Tutorial/Quickstart** | Second person ("you") | "You can configure the pool size..." |
| **How-To Guide** | Imperative | "Configure the pool size by..." |
| **Reference/API** | Descriptive | "Configures the pool size for..." |
| **Conceptual/Overview** | Neutral descriptive | "Pool size determines..." |

**Consistency:** Once you choose a perspective for a section, maintain it throughout.

---

## Terminology Standards

### Core Concepts

Use these terms consistently. Do not mix alternatives.

#### Database Operations

| Use This | Not These | Context |
|----------|-----------|---------|
| **session** | connection (when referring to logical session) | "Acquire a database session" |
| **connection** | link, handle | "Raw database connection" (when referring to actual connection) |
| **pool** | connection pool | "Connection pool management" |
| **driver** | adapter (when referring to the driver class) | "AsyncPG driver" |
| **adapter** | driver (when referring to the SQLSpec adapter) | "AsyncPG adapter" |

#### SQLSpec Components

| Use This | Not These | Context |
|----------|-----------|---------|
| **store** | backend, repository, storage | "AsyncpgStore", "session store" |
| **configuration** (prose) | config, settings | "Database configuration" |
| **config** (code) | configuration, settings | `AsyncpgConfig`, `config.pool_size` |
| **extension** | plugin (except Litestar), module | "ADK extension", "Litestar extension" |

#### ADK Terminology (Google ADK)

| Use This | Not These | Context |
|----------|-----------|---------|
| **session** | conversation, chat, thread | "ADK session" |
| **event** | message, turn, interaction | "User event", "assistant event" |
| **state** | context, memory | "Session state" |
| **owner ID column** | user FK, tenant FK (except when naming) | Feature name |

#### Litestar Terminology

| Use This | Not These | Context |
|----------|-----------|---------|
| **plugin** | extension, module | "SQLSpecPlugin" |
| **dependency injection** | DI, injection | "Use dependency injection to..." |
| **middleware** | middleware layer | "Session middleware" |
| **route handler** | endpoint, route, handler | "In your route handler..." |

#### Action Verbs

| Use This | Avoid | Context |
|----------|-------|---------|
| **create** | generate, make, build | "Create a session" |
| **configure** | setup, set up, config | "Configure the database" |
| **initialize** | init, setup, start | "Initialize the store" |
| **execute** | run, perform | "Execute a query" |
| **implement** | code, write, build | "Implement the interface" |

#### SQLSpec Migrations (IMPORTANT - Avoid Confusion)

⚠️ **"migrations"** has a specific technical meaning in SQLSpec:

**The SQLSpec migrations feature** = Database schema versioning system that tracks and applies DDL changes

| Use This | Not This | Context |
|----------|----------|---------|
| **migrations** (the feature) | schema changes, database updates | "SQLSpec's migrations feature tracks DDL" |
| **migration** (singular) | revision, changeset | "Create a migration to add a column" |
| **migrate** (verb for DB) | upgrade, apply changes | "Migrate the database to version 5" |
| **migrating from/to** (feature change) | Use full context | "Migrating **from SQLAlchemy to SQLSpec**" |

**To avoid confusion when discussing non-database migrations:**

✅ **DO:** Be explicit about what's being migrated

```rst
Good examples:
- "Use SQLSpec's **migrations system** to version your database schema"
- "The **migrations feature** generates and applies DDL changes"
- "**Upgrading from** Python 3.10 to 3.11"  (not "migrating")
- "**Switching from** ADK v1 to v2 API"  (not "migrating")
- "**Moving to** the new session store format"  (not "migrating")
```

❌ **DON'T:** Use "migration" ambiguously

```rst
Bad examples:
- "Use migrations for upgrades"  (Which kind? DB schema or feature?)
- "Migration handles the changes"  (The feature? The process?)
- "Migrating to the new API"  (Confusing - sounds like DB migration)
```

**When to use migration terminology:**

- ✅ **Migrations feature**: "Generate a migration file"
- ✅ **Database versioning**: "Apply pending migrations"
- ✅ **DDL changes**: "The migration adds an index"
- ❌ **Feature changes**: Use "upgrade", "switch to", "move to" instead
- ❌ **Code refactoring**: Use "refactor", "update", "change" instead
- ❌ **API changes**: Use "upgrade to", "adopt", "transition to" instead

---

## Writing Patterns

### Document Structure

#### Overview Pages (index.rst)

```rst
Page Title
==========

[1-2 sentence description of what this is]

Overview
--------

[2-3 paragraphs explaining:
 - What it does
 - Why it exists
 - When to use it]

Key Features
------------

[Bulleted list of main features, organized by category]

Quick Example
-------------

[Minimal working example, 10-20 lines]

Architecture Overview
---------------------

[Diagram or text description of how components interact]

Use Cases
---------

[2-3 real-world scenarios]

Next Steps
----------

[Grid of links to related docs]
```

#### Tutorial/Quickstart Pages

```rst
Page Title
==========

[1 sentence: "This guide will get you X in Y minutes"]

Overview
--------

In this tutorial, you'll:

1. [Action 1]
2. [Action 2]
3. [Action 3]

Prerequisites
-------------

- [Requirement 1]
- [Requirement 2]

Step 1: [Action]
================

[Explanation]

.. code-block:: python

   # Code example

Step 2: [Action]
================

[Continue pattern...]

Complete Example
================

[Full working code]

Next Steps
----------

- Link to related docs
```

#### Reference Pages

```rst
Page Title
==========

[1-2 sentence description]

[Class/Function Name]
---------------------

.. autoclass:: module.ClassName
   :members:
   :show-inheritance:

   [Detailed description]

   **Parameters:**

   - parameter_name (type): Description

   **Returns:**

   - return_type: Description

   **Raises:**

   - ExceptionType: When this happens

   **Example:**

   .. code-block:: python

      # Usage example

   .. seealso::

      Related documentation links
```

### Headings

Use sentence case for headings:

✅ **Good:**

- "Installation guide"
- "Configure database connection"
- "Best practices for production"

❌ **Bad:**

- "Installation Guide" (title case)
- "Configure Database Connection" (title case)
- "CONFIGURE DATABASE" (all caps)

### Lists

**Bulleted Lists:** Use for unordered items

```rst
- Item one
- Item two
- Item three
```

**Numbered Lists:** Use for sequential steps

```rst
1. First step
2. Second step
3. Third step
```

**Definition Lists:** Use for term definitions

```rst
Term
   Definition of the term.

Another Term
   Another definition.
```

---

## Code Examples

### Code Block Standards

Always specify the language:

```rst
.. code-block:: python

   from sqlspec import SQLSpec
```

NOT:

```rst
.. code-block::

   from sqlspec import SQLSpec
```

### Example Structure

**Complete examples should:**

1. Import statements first
2. Configuration next
3. Core logic last
4. Include comments explaining non-obvious parts
5. Be runnable as-is (when possible)

```python
# Good example structure
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

# Configure database
spec = SQLSpec()
config = spec.add_config(
    AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/db"})
)

# Create store
store = AsyncpgStore(config)
await store.create_tables()

# Use the store
session = await store.create_session(
    session_id="sess-1",
    app_name="my_app",
    user_id="user_123",
    state={"key": "value"}
)
```

### Inline Code

Use inline code for:

- Class names: `` `AsyncpgConfig` ``
- Method names: `` `execute()` ``
- Parameters: `` `pool_size` ``
- File paths: `` `docs/index.rst` ``
- SQL keywords: `` `SELECT` ``

### Code Comments

**In examples, use comments to:**

- Explain why, not what (code shows what)
- Highlight important details
- Mark steps in a sequence

```python
# Good: Explains why
config = AsyncpgConfig(
    pool_config={
        "min_size": 10,  # Keep connections warm for fast response
        "max_size": 50   # Prevent pool exhaustion under load
    }
)

# Bad: Explains obvious what
config = AsyncpgConfig(  # Create config
    pool_config={  # Pool config
        "min_size": 10,  # Set min size to 10
```

---

## Good/Bad Patterns

### Pattern: DO/DON'T Comparison

**Use this pattern for showing correct vs incorrect usage:**

```rst
✅ **DO:**

.. code-block:: python

   # Good: Clear and efficient
   result = await session.execute("SELECT * FROM users WHERE id = $1", user_id)

❌ **DON'T:**

.. code-block:: python

   # Bad: SQL injection vulnerability
   result = await session.execute(f"SELECT * FROM users WHERE id = {user_id}")
```

**Why it works:** Visual, clear, immediately actionable

---

### Pattern: Progressive Examples

**Start simple, add complexity:**

```rst
Basic Usage
-----------

.. code-block:: python

   # Simplest case
   store = AsyncpgStore(config)

With Custom Table Names
-----------------------

.. code-block:: python

   # Add one concept
   store = AsyncpgStore(
       config,
       session_table="custom_sessions"
   )

With Owner ID Column
--------------------

.. code-block:: python

   # Add another concept
   store = AsyncpgStore(
       config,
       session_table="custom_sessions",
       owner_id_column="user_id INTEGER REFERENCES users(id)"
   )
```

---

### Pattern: Comparison Tables

**Use tables for feature comparisons:**

```rst
.. list-table::
   :header-rows: 1
   :widths: 20 20 20 40

   * - Feature
     - AsyncPG
     - SQLite
     - Notes
   * - Async Support
     - ✅ Native
     - ❌ Wrapped
     - AsyncPG is truly async
   * - JSON Type
     - JSONB
     - TEXT
     - JSONB is more efficient
```

---

### Pattern: Admonition Usage

Use Sphinx admonitions appropriately:

```rst
.. note::

   Contextual information that adds helpful detail but isn't critical.

.. tip::

   Optimization hints, best practices, professional advice.

.. warning::

   Important warnings about potential issues or limitations.

.. important::

   Critical information that must be understood before proceeding.

.. danger::

   Severe warnings about data loss, security, or breaking changes.

.. seealso::

   Cross-references to related documentation.
```

**Admonition Guidelines:**

- Use sparingly - too many reduce impact
- Keep content concise
- Don't nest admonitions
- Place near relevant content

---

### Pattern: Version Annotations

**When documenting version-specific features:**

```rst
.. versionadded:: 1.2.0
   The ``owner_id_column`` parameter enables referential integrity.

.. versionchanged:: 1.3.0
   Pool size now defaults to 10 instead of 5.

.. deprecated:: 1.4.0
   Use :meth:`create_session` instead of :meth:`make_session`.
```

---

## Documentation Checklist

Use this checklist when writing or reviewing documentation:

### Before Writing

- [ ] Identify document type (tutorial, reference, how-to, conceptual)
- [ ] Define target audience (beginner, intermediate, advanced)
- [ ] List prerequisites
- [ ] Outline main points

### While Writing

- [ ] Use consistent terminology (check glossary)
- [ ] Choose appropriate perspective and maintain it
- [ ] Write in present tense, active voice
- [ ] **MANDATORY: Document current state, NOT changes** (no "now works", "new feature", etc.)
- [ ] Include code examples where helpful
- [ ] Add cross-references to related docs
- [ ] Use appropriate admonitions

### After Writing

- [ ] Verify all code examples are runnable
- [ ] Check for casual/marketing language
- [ ] **Check for change-focused language** ("now", "updated", "new", "changed")
- [ ] Ensure proper heading hierarchy
- [ ] Add to table of contents (toctree)
- [ ] Build docs locally and check rendering
- [ ] Review links (internal and external)
- [ ] Proofread for typos and grammar

### Quality Checks

- [ ] Can a beginner understand this?
- [ ] Are prerequisites clearly stated?
- [ ] Do examples progress from simple to complex?
- [ ] Is the document scannable (good headings, lists, tables)?
- [ ] Are there "Next Steps" or related links?
- [ ] Is the tone professional and helpful?

---

## reStructuredText Guidelines

### Common Directives

**Code blocks:**

```rst
.. code-block:: python
   :caption: Example caption
   :linenos:
   :emphasize-lines: 2,3

   code here
```

**Includes:**

```rst
.. literalinclude:: ../examples/example.py
   :language: python
   :lines: 10-20
   :caption: Example from file
```

**Links:**

```rst
:doc:`quickstart`                    # Link to document
:ref:`section-label`                 # Link to section
:class:`sqlspec.base.SQLSpec`        # Link to class
:meth:`execute`                      # Link to method
:func:`create_config`                # Link to function
```

**Tables:**

```rst
.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Column 1
     - Column 2
   * - Data 1
     - Data 2
```

**Grids:**

```rst
.. grid:: 2
   :gutter: 3

   .. grid-item-card:: Title 1
      :link: page1
      :link-type: doc

      Description 1

   .. grid-item-card:: Title 2
      :link: page2
      :link-type: doc

      Description 2
```

### Cross-Referencing

**Internal Links:**

- Same document section: `` :ref:`section-label` ``
- Other document: `` :doc:`path/to/document` ``
- Specific section in other doc: `` :ref:`other-doc-section-label` ``

**External Links:**

```rst
`Link Text <https://example.com>`_
```

**API References:**

```rst
:class:`module.ClassName`
:meth:`ClassName.method_name`
:func:`function_name`
:data:`CONSTANT_NAME`
:exc:`ExceptionName`
```

---

## Examples: Before and After

### Example 1: Marketing Language

**Before:**
> "AsyncPG is the blazing fast, revolutionary PostgreSQL driver that will supercharge your AI agents with awesome performance!"

**After:**
> "AsyncPG is a high-performance PostgreSQL driver written in Cython, providing native async support and efficient connection pooling for production AI agent applications."

---

### Example 2: Casual Language

**Before:**
> "Just create a config and you're good to go! Super easy!"

**After:**
> "Create a configuration instance to connect to the database:"

---

### Example 3: Inconsistent Perspective

**Before:**
> "You should configure the pool size. One must ensure the size is appropriate. Configure min_size to 10."

**After:**
> "Configure the pool size based on your workload. Set `min_size` to 10 for typical production deployments."

---

### Example 4: Passive Voice

**Before:**
> "The configuration is created by the user. The pool is then initialized by the system."

**After:**
> "Create the configuration. SQLSpec initializes the connection pool automatically."

---

### Example 5: Change-Focused Language (MANDATORY FIX)

**Before:**
> "The session store has been updated to support cascade deletes. This new feature now allows automatic cleanup when the parent session is deleted. Previously, you had to manually delete child records."

**After:**
> "The session store supports cascade deletes. When a session is deleted, all associated child records are automatically removed."

---

**Before (commit message):**
> "Changed pool size default from 5 to 10"

**After (commit message):**
> "feat: set default pool size to 10 connections"

---

**Before (docstring):**

```python
def execute(query: str) -> list[dict]:
    """Execute query.

    This method has been enhanced to support parameter binding.
    The old tuple-based return format is now replaced with dicts.
    """
```

**After (docstring):**

```python
def execute(query: str) -> list[dict]:
    """Execute SQL query with parameter binding.

    Args:
        query: SQL query with parameter placeholders.

    Returns:
        Query results as list of dictionaries.
    """
```

---

## Quick Reference Card

### Voice

✅ Professional, technical, helpful
❌ Casual, marketing, apologetic, change-focused

### Tense

✅ Present tense, active voice
❌ Future tense, passive voice

### Perspective

- Tutorial: "you can configure..."
- Reference: "Configure the database..."
- API: "Configures the database..."

### Common Terms

- session (not connection, for logical session)
- store (not backend, in prose)
- configuration (prose) / config (code)
- create (not generate/make)
- owner ID column (not user FK, for feature)
- **migrations** = SQLSpec's DB schema versioning feature
    - Use "upgrade/switch to" for feature changes
    - Use "migrations" only for database DDL versioning

### Code Examples

- Always specify language
- Include imports
- Add helpful comments
- Make runnable when possible

### Admonitions

- `.. note::` - Extra context
- `.. tip::` - Best practice
- `.. warning::` - Important limitation
- `.. important::` - Critical information
- `.. danger::` - Severe warning

---

## Resources

### SQLSpec-Specific

- Terminology standards: See [Terminology Standards](#terminology-standards) section
- Example documentation: See adapter guides in `docs/guides/adapters/`

### General Documentation

- [Write the Docs Guide](https://www.writethedocs.org/guide/)
- [Google Developer Documentation Style Guide](https://developers.google.com/style)
- [Sphinx Documentation](https://www.sphinx-doc.org/)
- [reStructuredText Primer](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html)

### Tools

- `make docs` - Build documentation locally
- `make docs-serve` - Serve docs locally with live reload (if available)
- Spell checker: `codespell` or similar
- Link checker: `sphinx-build -b linkcheck`
