---
description: Re-bootstrap or align AI infrastructure with latest patterns
allowed-tools: Read, Write, Edit, Glob, Grep, Bash
---

# Bootstrap AI Infrastructure

Re-bootstrap in alignment mode.

## Purpose

This command validates and updates the AI infrastructure (agents, commands, skills) to ensure alignment with:
1. Current project standards (AGENTS.md, CLAUDE.md)
2. Latest agent patterns and workflows
3. Complete coverage of adapters, tools, and frameworks
4. Consistency across documentation

**Use when:**
- Adding new adapters, extensions, or features
- Updating agent workflows
- Aligning with new standards
- Validating infrastructure completeness

## Alignment Workflow

### Step 1: Inventory Existing Infrastructure

List all AI infrastructure components:

```bash
# A. List all agents
ls -1 /home/cody/code/litestar/sqlspec/.claude/agents/

# B. List all commands
ls -1 /home/cody/code/litestar/sqlspec/.claude/commands/

# C. List all skills
find /home/cody/code/litestar/sqlspec/.claude/skills -type f -name "*.md"

# D. List all adapters in codebase
ls -1d /home/cody/code/litestar/sqlspec/sqlspec/adapters/*/

# E. List all extensions
ls -1d /home/cody/code/litestar/sqlspec/sqlspec/extensions/*/

# F. Check guides structure
find /home/cody/code/litestar/sqlspec/docs/guides -type f -name "*.md"
```

**Expected output structure:**

```
.claude/
├── agents/
│   ├── prd.md
│   ├── expert.md
│   ├── testing.md
│   └── docs-vision.md
├── commands/
│   ├── prd.md
│   ├── implement.md
│   ├── test.md
│   ├── review.md
│   ├── explore.md
│   ├── fix-issue.md
│   └── bootstrap.md
└── skills/
    ├── sqlspec-usage/
    │   ├── skill.md
    │   ├── patterns/*.md
    │   └── examples/*.py
    └── sqlspec-adapters/
        ├── asyncpg.md
        ├── psycopg.md
        ├── oracledb.md
        ├── duckdb.md
        ├── sqlite.md
        ├── asyncmy.md
        ├── psqlpy.md
        ├── aiosqlite.md
        ├── adbc.md
        └── bigquery.md
```

### Step 2: Component Checklist

Compare existing components against required infrastructure:

#### A. Required Agents (4 total)

| Agent | File | Status | Purpose |
|-------|------|--------|---------|
| PRD | `.claude/agents/prd.md` | ☐ | Requirements planning |
| Expert | `.claude/agents/expert.md` | ☐ | Implementation |
| Testing | `.claude/agents/testing.md` | ☐ | Test creation |
| Docs & Vision | `.claude/agents/docs-vision.md` | ☐ | Documentation, QA, knowledge |

**Validation:**
```bash
for agent in prd expert testing docs-vision; do
    if [ -f ".claude/agents/${agent}.md" ]; then
        echo "✓ ${agent}.md exists"
    else
        echo "✗ ${agent}.md missing"
    fi
done
```

#### B. Required Commands (7 total)

| Command | File | Status | Purpose |
|---------|------|--------|---------|
| prd | `.claude/commands/prd.md` | ☐ | Create PRD workspace |
| implement | `.claude/commands/implement.md` | ☐ | Full implementation workflow |
| test | `.claude/commands/test.md` | ☐ | Standalone testing |
| review | `.claude/commands/review.md` | ☐ | Standalone docs/QA |
| explore | `.claude/commands/explore.md` | ☐ | Codebase exploration |
| fix-issue | `.claude/commands/fix-issue.md` | ☐ | GitHub issue workflow |
| bootstrap | `.claude/commands/bootstrap.md` | ☐ | Infrastructure alignment |

**Validation:**
```bash
for cmd in prd implement test review explore fix-issue bootstrap; do
    if [ -f ".claude/commands/${cmd}.md" ]; then
        echo "✓ ${cmd}.md exists"
    else
        echo "✗ ${cmd}.md missing"
    fi
done
```

#### C. Required Skills - Usage Patterns (8 total)

| Pattern | File | Status | Purpose |
|---------|------|--------|---------|
| Main skill | `.claude/skills/sqlspec-usage/skill.md` | ☐ | Overview and quick reference |
| Configuration | `.claude/skills/sqlspec-usage/patterns/configuration.md` | ☐ | Config patterns |
| Queries | `.claude/skills/sqlspec-usage/patterns/queries.md` | ☐ | Query execution |
| Frameworks | `.claude/skills/sqlspec-usage/patterns/frameworks.md` | ☐ | Extension integration |
| Migrations | `.claude/skills/sqlspec-usage/patterns/migrations.md` | ☐ | Migration tools |
| Testing | `.claude/skills/sqlspec-usage/patterns/testing.md` | ☐ | Test patterns |
| Performance | `.claude/skills/sqlspec-usage/patterns/performance.md` | ☐ | Optimization |
| Troubleshooting | `.claude/skills/sqlspec-usage/patterns/troubleshooting.md` | ☐ | Common issues |

**Validation:**
```bash
for pattern in configuration queries frameworks migrations testing performance troubleshooting; do
    if [ -f ".claude/skills/sqlspec-usage/patterns/${pattern}.md" ]; then
        echo "✓ ${pattern}.md exists"
    else
        echo "✗ ${pattern}.md missing"
    fi
done
```

#### D. Required Skills - Adapter Coverage (10 adapters)

| Adapter | File | Status | Codebase Path |
|---------|------|--------|---------------|
| asyncpg | `.claude/skills/sqlspec-adapters/asyncpg.md` | ☐ | `sqlspec/adapters/asyncpg/` |
| psycopg | `.claude/skills/sqlspec-adapters/psycopg.md` | ☐ | `sqlspec/adapters/psycopg/` |
| oracledb | `.claude/skills/sqlspec-adapters/oracledb.md` | ☐ | `sqlspec/adapters/oracledb/` |
| duckdb | `.claude/skills/sqlspec-adapters/duckdb.md` | ☐ | `sqlspec/adapters/duckdb/` |
| sqlite | `.claude/skills/sqlspec-adapters/sqlite.md` | ☐ | `sqlspec/adapters/sqlite/` |
| asyncmy | `.claude/skills/sqlspec-adapters/asyncmy.md` | ☐ | `sqlspec/adapters/asyncmy/` |
| psqlpy | `.claude/skills/sqlspec-adapters/psqlpy.md` | ☐ | `sqlspec/adapters/psqlpy/` |
| aiosqlite | `.claude/skills/sqlspec-adapters/aiosqlite.md` | ☐ | `sqlspec/adapters/aiosqlite/` |
| adbc | `.claude/skills/sqlspec-adapters/adbc.md` | ☐ | `sqlspec/adapters/adbc/` |
| bigquery | `.claude/skills/sqlspec-adapters/bigquery.md` | ☐ | `sqlspec/adapters/bigquery/` |

**Validation:**
```bash
# Check skill files exist
for adapter in asyncpg psycopg oracledb duckdb sqlite asyncmy psqlpy aiosqlite adbc bigquery; do
    if [ -f ".claude/skills/sqlspec-adapters/${adapter}.md" ]; then
        echo "✓ ${adapter}.md skill exists"
    else
        echo "✗ ${adapter}.md skill missing"
    fi
done

# Verify adapters exist in codebase
for adapter in asyncpg psycopg oracledb duckdb sqlite asyncmy psqlpy aiosqlite adbc bigquery; do
    if [ -d "sqlspec/adapters/${adapter}" ]; then
        echo "✓ ${adapter} adapter exists in codebase"
    else
        echo "✗ ${adapter} adapter missing in codebase"
    fi
done
```

#### E. Required Skills - Examples (4 total)

| Example | File | Status | Purpose |
|---------|------|--------|---------|
| Litestar | `.claude/skills/sqlspec-usage/examples/litestar-integration.py` | ☐ | Litestar framework |
| FastAPI | `.claude/skills/sqlspec-usage/examples/fastapi-integration.py` | ☐ | FastAPI framework |
| Multi-DB | `.claude/skills/sqlspec-usage/examples/multi-database.py` | ☐ | Multiple databases |
| Testing | `.claude/skills/sqlspec-usage/examples/testing-patterns.py` | ☐ | Test patterns |

**Validation:**
```bash
for example in litestar-integration fastapi-integration multi-database testing-patterns; do
    if [ -f ".claude/skills/sqlspec-usage/examples/${example}.py" ]; then
        echo "✓ ${example}.py exists"
    else
        echo "✗ ${example}.py missing"
    fi
done
```

#### F. Documentation Guides Coverage

| Guide Type | Path | Required Files |
|------------|------|----------------|
| Architecture | `docs/guides/architecture/` | architecture.md, data-flow.md, arrow-integration.md, patterns.md |
| Adapters | `docs/guides/adapters/` | One guide per adapter + parameter-profile-registry.md |
| Performance | `docs/guides/performance/` | mypyc.md, sqlglot.md |
| Extensions | `docs/guides/extensions/` | litestar.md, fastapi.md, starlette.md, flask.md |
| Testing | `docs/guides/testing/` | testing.md |
| Development | `docs/guides/development/` | code-standards.md, implementation-patterns.md |
| Quick Reference | `docs/guides/quick-reference/` | quick-reference.md |

**Validation:**
```bash
# Check guide directories exist
for dir in architecture adapters performance extensions testing development quick-reference; do
    if [ -d "docs/guides/${dir}" ]; then
        echo "✓ docs/guides/${dir}/ exists"
        ls -1 "docs/guides/${dir}/"
    else
        echo "✗ docs/guides/${dir}/ missing"
    fi
done
```

### Step 3: Gap Analysis

Identify missing or outdated components:

**A. Find missing adapter skills:**

```bash
# List adapters in codebase
CODEBASE_ADAPTERS=$(ls -1d sqlspec/adapters/*/ | xargs -n1 basename)

# List adapter skills
SKILL_ADAPTERS=$(ls -1 .claude/skills/sqlspec-adapters/*.md 2>/dev/null | xargs -n1 basename | sed 's/.md$//')

# Compare
echo "=== Adapters in codebase but missing skills ==="
for adapter in $CODEBASE_ADAPTERS; do
    if ! echo "$SKILL_ADAPTERS" | grep -q "^${adapter}$"; then
        echo "Missing skill: ${adapter}"
    fi
done

echo "=== Skills for adapters not in codebase ==="
for skill in $SKILL_ADAPTERS; do
    if ! echo "$CODEBASE_ADAPTERS" | grep -q "^${skill}$"; then
        echo "Orphaned skill: ${skill}"
    fi
done
```

**B. Find missing adapter guides:**

```bash
# List adapters in codebase
CODEBASE_ADAPTERS=$(ls -1d sqlspec/adapters/*/ | xargs -n1 basename)

# List adapter guides (exclude parameter-profile-registry.md)
GUIDE_ADAPTERS=$(ls -1 docs/guides/adapters/*.md 2>/dev/null | grep -v parameter-profile-registry | xargs -n1 basename | sed 's/.md$//')

echo "=== Adapters missing documentation guides ==="
for adapter in $CODEBASE_ADAPTERS; do
    if ! echo "$GUIDE_ADAPTERS" | grep -q "^${adapter}$"; then
        echo "Missing guide: docs/guides/adapters/${adapter}.md"
    fi
done
```

**C. Find missing extension guides:**

```bash
# List extensions in codebase
CODEBASE_EXTENSIONS=$(ls -1d sqlspec/extensions/*/ 2>/dev/null | xargs -n1 basename)

# List extension guides
GUIDE_EXTENSIONS=$(ls -1 docs/guides/extensions/*.md 2>/dev/null | xargs -n1 basename | sed 's/.md$//')

echo "=== Extensions missing documentation guides ==="
for ext in $CODEBASE_EXTENSIONS; do
    if ! echo "$GUIDE_EXTENSIONS" | grep -q "^${ext}$"; then
        echo "Missing guide: docs/guides/extensions/${ext}.md"
    fi
done
```

**D. Validate agent completeness:**

```bash
# Check each agent has required sections
for agent in prd expert testing docs-vision; do
    echo "=== Validating ${agent}.md ==="
    if [ -f ".claude/agents/${agent}.md" ]; then
        # Check for required sections
        grep -q "^## Core Responsibilities" ".claude/agents/${agent}.md" && echo "✓ Has Core Responsibilities" || echo "✗ Missing Core Responsibilities"
        grep -q "^## .*Workflow" ".claude/agents/${agent}.md" && echo "✓ Has Workflow section" || echo "✗ Missing Workflow section"
        grep -q "^## Success Criteria" ".claude/agents/${agent}.md" && echo "✓ Has Success Criteria" || echo "✗ Missing Success Criteria"
        grep -q "^## Tools Available" ".claude/agents/${agent}.md" && echo "✓ Has Tools Available" || echo "✗ Missing Tools Available"
    fi
done
```

**E. Validate command completeness:**

```bash
# Check each command has required sections
for cmd in prd implement test review explore fix-issue bootstrap; do
    echo "=== Validating ${cmd}.md ==="
    if [ -f ".claude/commands/${cmd}.md" ]; then
        # Check for workflow steps
        grep -q "^### Step" ".claude/commands/${cmd}.md" && echo "✓ Has workflow steps" || echo "✗ Missing workflow steps"
        grep -q "^## Success Criteria" ".claude/commands/${cmd}.md" && echo "✓ Has Success Criteria" || echo "✗ Missing Success Criteria"
        # Check frontmatter
        head -n 5 ".claude/commands/${cmd}.md" | grep -q "^description:" && echo "✓ Has description" || echo "✗ Missing description"
    fi
done
```

### Step 4: Apply Updates

Based on gap analysis, apply necessary updates:

#### A. Create Missing Adapter Skills

For each adapter missing a skill file:

```python
# Example: Create skill for new adapter
adapter_name = "newadapter"

Read(f"sqlspec/adapters/{adapter_name}/config.py")
Read(f"sqlspec/adapters/{adapter_name}/driver.py")

# Create skill file with template
Write(
    file_path=f".claude/skills/sqlspec-adapters/{adapter_name}.md",
    content=f"""# {adapter_name.capitalize()} Adapter

## Overview

Database-specific implementation for {adapter_name}.

## Configuration

```python
from sqlspec.adapters.{adapter_name} import {adapter_name.capitalize()}Config

config = {adapter_name.capitalize()}Config(
    pool_config={{"dsn": "..."}},
    driver_features={{}}
)
```

## Features

- Connection pooling: [Yes/No]
- Async support: [Yes/No]
- Transaction support: [Yes/No]
- Special types: [List]

## Usage Patterns

[Add specific patterns from driver.py analysis]

## Troubleshooting

[Add common issues]

## References

- Library: [link to library docs]
- Adapter guide: docs/guides/adapters/{adapter_name}.md
"""
)
```

#### B. Create Missing Adapter Guides

For each adapter missing a documentation guide:

```python
adapter_name = "newadapter"

# Read adapter implementation for context
Read(f"sqlspec/adapters/{adapter_name}/config.py")
Read(f"sqlspec/adapters/{adapter_name}/driver.py")

# Create guide
Write(
    file_path=f"docs/guides/adapters/{adapter_name}.md",
    content=f"""# {adapter_name.capitalize()} Adapter Guide

## Overview

This guide covers the {adapter_name} adapter for SQLSpec.

## Installation

```bash
pip install sqlspec[{adapter_name}]
```

## Basic Configuration

```python
from sqlspec.adapters.{adapter_name} import {adapter_name.capitalize()}Config

config = {adapter_name.capitalize()}Config(
    pool_config={{"dsn": "..."}}
)
```

## Connection Pooling

[Details about pooling configuration]

## Transaction Management

[Details about transactions]

## Type Handling

[Details about type conversion]

## Performance Considerations

[Optimization tips]

## Troubleshooting

[Common issues and solutions]

## API Reference

[Link to API docs]
"""
)
```

#### C. Update Agent Files with New Patterns

When new patterns emerge, update agent files:

```python
# Example: Add new pattern to expert.md
Read(".claude/agents/expert.md")

# Extract new pattern from AGENTS.md
Read("AGENTS.md")

# Update expert.md with new implementation pattern
Edit(
    file_path=".claude/agents/expert.md",
    old_string="## Database Adapter Implementation",
    new_string="""## Database Adapter Implementation

### New Pattern: [Pattern Name]

[Pattern description and example]

### Existing Patterns

"""
)
```

#### D. Synchronize Standards

Ensure AGENTS.md patterns are reflected in all agent files:

```python
# Read current standards
Read("AGENTS.md")

# Check each agent references AGENTS.md
for agent in ["expert", "testing", "docs-vision"]:
    Read(f".claude/agents/{agent}.md")

    # Verify MANDATORY CODE QUALITY RULES section references AGENTS.md
    # Update if outdated
```

#### E. Update Skills with Latest Patterns

When implementation patterns change, update skills:

```python
# Example: Update configuration pattern in skill
Read(".claude/skills/sqlspec-usage/patterns/configuration.md")
Read("sqlspec/adapters/asyncpg/config.py")  # Reference implementation

# Update skill with latest pattern
Edit(
    file_path=".claude/skills/sqlspec-usage/patterns/configuration.md",
    old_string="# Old pattern",
    new_string="# Updated pattern from asyncpg implementation"
)
```

### Step 5: Validation

After applying updates, validate infrastructure:

#### A. Syntax Validation

```bash
# Check all markdown files are valid
find .claude -name "*.md" -exec bash -c 'echo "Checking: $1"; head -1 "$1" | grep -q "^---$" || echo "  ✗ Missing frontmatter"' _ {} \;

# Check all Python examples are syntactically valid
find .claude/skills -name "*.py" -exec python -m py_compile {} \; && echo "✓ All Python examples valid"
```

#### B. Cross-Reference Validation

```bash
# Verify all adapter references are valid
echo "=== Checking adapter references in skills ==="
for adapter in asyncpg psycopg oracledb duckdb sqlite asyncmy psqlpy aiosqlite adbc bigquery; do
    # Check skill exists
    [ -f ".claude/skills/sqlspec-adapters/${adapter}.md" ] || echo "✗ Missing skill: ${adapter}.md"

    # Check adapter exists in codebase
    [ -d "sqlspec/adapters/${adapter}" ] || echo "✗ Adapter not in codebase: ${adapter}"

    # Check guide exists
    [ -f "docs/guides/adapters/${adapter}.md" ] || echo "✗ Missing guide: docs/guides/adapters/${adapter}.md"
done
```

#### C. Pattern Consistency Validation

```bash
# Check AGENTS.md patterns are referenced in agent files
echo "=== Validating pattern references ==="

# Extract pattern names from AGENTS.md
PATTERNS=$(grep "^### " AGENTS.md | sed 's/^### //' | sort)

# Check each agent references key patterns
for agent in expert testing docs-vision; do
    echo "Checking ${agent}.md for pattern references..."
    # This is a sample check - customize based on actual patterns
    grep -q "AGENTS.md" ".claude/agents/${agent}.md" && echo "✓ References AGENTS.md" || echo "✗ No AGENTS.md reference"
done
```

#### D. Tool Availability Validation

```bash
# Verify all agents declare their tools correctly
for agent in prd expert testing docs-vision; do
    echo "=== ${agent}.md tool declarations ==="
    if [ -f ".claude/agents/${agent}.md" ]; then
        # Check frontmatter has tools declaration
        sed -n '1,/^---$/p' ".claude/agents/${agent}.md" | grep -q "^tools:" && echo "✓ Has tools declaration" || echo "✗ Missing tools declaration"

        # Check Tools Available section exists
        grep -q "^## Tools Available" ".claude/agents/${agent}.md" && echo "✓ Has Tools Available section" || echo "✗ Missing Tools Available section"
    fi
done
```

### Step 6: Generate Report

Create comprehensive alignment report:

```bash
# Generate report
cat > /tmp/bootstrap-report.md <<'EOF'
# AI Infrastructure Bootstrap Report

Generated: $(date)

## Component Summary

### Agents
- [ ] prd.md
- [ ] expert.md
- [ ] testing.md
- [ ] docs-vision.md

### Commands
- [ ] prd.md
- [ ] implement.md
- [ ] test.md
- [ ] review.md
- [ ] explore.md
- [ ] fix-issue.md
- [ ] bootstrap.md

### Skills - Usage Patterns
- [ ] skill.md (main)
- [ ] configuration.md
- [ ] queries.md
- [ ] frameworks.md
- [ ] migrations.md
- [ ] testing.md
- [ ] performance.md
- [ ] troubleshooting.md

### Skills - Adapters
- [ ] asyncpg.md
- [ ] psycopg.md
- [ ] oracledb.md
- [ ] duckdb.md
- [ ] sqlite.md
- [ ] asyncmy.md
- [ ] psqlpy.md
- [ ] aiosqlite.md
- [ ] adbc.md
- [ ] bigquery.md

### Skills - Examples
- [ ] litestar-integration.py
- [ ] fastapi-integration.py
- [ ] multi-database.py
- [ ] testing-patterns.py

## Gaps Identified

[List of missing or outdated components]

## Updates Applied

[List of files created or updated]

## Validation Results

[Results from syntax, cross-reference, and pattern validation]

## Next Steps

[Recommended follow-up actions]
EOF

# Display report
cat /tmp/bootstrap-report.md
```

## Update Strategies

### Strategy 1: Preserve Custom Content

When updating existing files, preserve custom content:

```python
# Read existing file
content = Read(".claude/agents/expert.md")

# Extract custom sections (those not in template)
# Update only template sections
# Preserve custom additions

# Write updated file preserving custom content
```

### Strategy 2: Incremental Updates

For large infrastructure updates:

1. Update one component type at a time (agents → commands → skills)
2. Validate after each component type
3. Commit changes incrementally
4. Run tests after each major update

### Strategy 3: Breaking Change Detection

Before applying updates:

```bash
# Check for breaking changes
echo "=== Checking for breaking changes ==="

# Compare agent tool declarations
for agent in prd expert testing docs-vision; do
    # Extract tools from frontmatter
    OLD_TOOLS=$(git show HEAD:.claude/agents/${agent}.md | sed -n '/^tools:/p')
    NEW_TOOLS=$(sed -n '/^tools:/p' .claude/agents/${agent}.md)

    if [ "$OLD_TOOLS" != "$NEW_TOOLS" ]; then
        echo "⚠️  Tool changes in ${agent}.md:"
        echo "  Old: $OLD_TOOLS"
        echo "  New: $NEW_TOOLS"
    fi
done
```

## Success Criteria

Bootstrap is complete when:

✅ **All components present** - 4 agents, 7 commands, 10+ adapter skills, 8 usage patterns, 4 examples
✅ **Adapter coverage complete** - Every adapter in codebase has skill + guide
✅ **Extension coverage complete** - Every extension has guide
✅ **Pattern consistency** - AGENTS.md patterns reflected in all agents
✅ **Cross-references valid** - All file references resolve correctly
✅ **Syntax valid** - All markdown and Python files parse correctly
✅ **No breaking changes** - Existing functionality preserved
✅ **Documentation updated** - Guides reflect current implementation

## Example Execution

```bash
# Full bootstrap workflow

# 1. Inventory
ls -1 .claude/agents/
ls -1 .claude/commands/
find .claude/skills -name "*.md"

# 2. Gap analysis
./scripts/check-adapter-coverage.sh  # Custom script

# 3. Create missing adapter skill
cat > .claude/skills/sqlspec-adapters/newadapter.md <<'EOF'
# NewAdapter Skill
[Content...]
EOF

# 4. Create missing guide
cat > docs/guides/adapters/newadapter.md <<'EOF'
# NewAdapter Guide
[Content...]
EOF

# 5. Validate
python -m py_compile .claude/skills/sqlspec-usage/examples/*.py
make docs  # Verify docs build

# 6. Generate report
cat /tmp/bootstrap-report.md
```

## Maintenance Schedule

Recommended bootstrap frequency:

- **After adapter addition** - Immediate (create skill + guide)
- **After extension addition** - Immediate (create guide)
- **After AGENTS.md update** - Within 1 week (sync agents)
- **Quarterly** - Full validation and alignment check
- **Before major releases** - Complete bootstrap + validation
