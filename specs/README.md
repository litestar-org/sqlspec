# specs/ Workspace

Active specification planning and development workspace for AI coding agents working on SQLSpec.

## Structure

Each specification gets a dedicated folder:

```
specs/
├── active/                    # Active work (gitignored)
│   ├── {spec-slug}/
│   │   ├── prd.md            # Product Requirements Document
│   │   ├── tasks.md          # Implementation checklist
│   │   ├── recovery.md       # Session resume instructions
│   │   ├── research/         # Research findings, plans
│   │   │   └── plan.md      # Detailed planning output
│   │   └── tmp/              # Temporary files (cleaned by Docs & Vision)
│   └── .gitkeep
├── archive/                   # Completed work (gitignored)
│   └── {archived-spec}/
├── template-spec/             # Template structure (committed)
│   ├── prd.md
│   ├── tasks.md
│   ├── recovery.md
│   ├── README.md
│   ├── research/.gitkeep
│   └── tmp/.gitkeep
└── README.md                  # This file
```

## Workflow

### 1. Planning (`/plan`)

Planner agent creates spec folder:

```bash
specs/active/vector-search/
├── prd.md          # Created by Planner
├── tasks.md        # Created by Planner
├── recovery.md     # Created by Planner
├── research/       # Created by Planner
│   └── plan.md
└── tmp/            # Created by Planner
```

### 2. Implementation (`/implement`)

Expert agent orchestrates the **entire workflow automatically**:

- Reads prd.md, tasks.md, research/plan.md
- Implements feature following AGENTS.md standards
- Updates tasks.md (marks items complete)
- Updates recovery.md (current status)
- **Auto-invokes Testing agent** (creates comprehensive tests)
- **Auto-invokes Docs & Vision agent** (5-phase workflow):
  - Phase 1: Documentation
  - Phase 2: Quality gate
  - Phase 3: **Knowledge capture** (updates AGENTS.md + guides)
  - Phase 4: **Re-validation** (verifies consistency)
  - Phase 5: Cleanup and archive

**Result**: Feature implemented, tested, documented, patterns captured, and archived - all from ONE command!

### 3. Automatic Phases (No Manual Steps)

When you run `/implement`, everything happens automatically:

```
┌─────────────────────────────────────────────────────────────┐
│                      EXPERT AGENT                            │
│                                                              │
│  1. Read Plan & Research                                    │
│  2. Implement Feature                                       │
│  3. Self-Test & Verify                                      │
│  4. ──► Auto-Invoke Testing Agent                          │
│         └─► Creates comprehensive test suite               │
│  5. ──► Auto-Invoke Docs & Vision Agent                    │
│         ├─► Updates documentation                           │
│         ├─► Runs quality gate                              │
│         ├─► Captures patterns in AGENTS.md                 │
│         ├─► Re-validates consistency                        │
│         └─► Archives to specs/archive/                     │
│  6. Returns Complete Summary                                │
└─────────────────────────────────────────────────────────────┘
```

## Cleanup Protocol

**MANDATORY after every `/implement`:**

The Docs & Vision agent automatically:

1. **Removes tmp/ directories:**
   ```bash
   find specs/active/*/tmp -type d -exec rm -rf {} +
   ```

2. **Archives completed work:**
   ```bash
   mv specs/active/{spec} specs/archive/{spec}
   ```

3. **Updates knowledge base:**
   - Extracts patterns from implementation
   - Updates AGENTS.md with new patterns
   - Updates docs/guides/ with new techniques
   - Re-validates after updates

## Session Continuity

To resume work across sessions:

```python
# Read recovery.md to understand current state
Read("specs/active/{spec}/recovery.md")

# Check tasks.md for what's complete
Read("specs/active/{spec}/tasks.md")

# Review PRD for full context
Read("specs/active/{spec}/prd.md")

# Review research findings
Read("specs/active/{spec}/research/plan.md")
```

## Example Usage

```bash
# 1. Plan a new feature
/plan Add connection pooling for DuckDB

# 2. Implement (handles EVERYTHING automatically)
/implement duckdb-connection-pooling
# → Implementation complete
# → Tests created and passing
# → Documentation updated
# → Patterns captured in AGENTS.md
# → Spec archived to specs/archive/

# That's it! Feature is complete.
```

## Key Features

✅ **Automated Workflow** - One command (`/implement`) handles entire lifecycle
✅ **Knowledge Preservation** - Patterns captured in AGENTS.md and guides
✅ **Quality Assurance** - Multi-phase validation before completion
✅ **Session Resumability** - recovery.md enables cross-session work
✅ **Clean Workspace** - Automatic cleanup and archival
✅ **No Manual Steps** - Testing, docs, and knowledge capture all automatic

## Archive Management

Completed specs in `specs/archive/` are:

- Preserved for reference
- Searchable for patterns
- Available for recovery if needed
- Never deleted (historical record)

## Usage with AI Agents

All agents (Claude, Gemini, Codex) use this workspace:

- **Planner** creates the structure
- **Expert** implements and orchestrates workflow
- **Testing** adds comprehensive tests (auto-invoked)
- **Docs & Vision** handles docs, QA, knowledge capture, and cleanup (auto-invoked)

## Reference

See `specs/template-spec/` for the complete template structure with examples.
