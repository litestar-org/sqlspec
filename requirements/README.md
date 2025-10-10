# requirements/ Workspace

Active requirement planning and development workspace for AI coding agents.

## Structure

Each requirement gets a dedicated folder:

```
requirements/
├── {requirement-slug}/        # Active requirement
│   ├── prd.md                 # Product Requirements Document
│   ├── tasks.md               # Implementation checklist
│   ├── recovery.md            # Session resume instructions
│   ├── research/              # Research findings, plans
│   │   └── plan.md           # Detailed planning output
│   └── tmp/                   # Temporary files (cleaned by Docs & Vision)
├── archive/                   # Completed requirements
│   └── {old-requirement}/    # Archived when complete
└── README.md                  # This file
```

## Workflow

### 1. Planning (`/plan`)

Planner agent creates requirement folder:

```bash
requirements/vector-search/
├── prd.md          # Created by Planner
├── tasks.md        # Created by Planner
├── research/       # Created by Planner
│   └── plan.md
├── tmp/            # Created by Planner
└── recovery.md     # Created by Planner
```

### 2. Implementation (`/implement`)

Expert agent:

- Reads prd.md, tasks.md, research/plan.md
- Implements feature
- Updates tasks.md (marks items complete)
- Updates recovery.md (current status)
- May create tmp files (cleaned later)

### 3. Testing (`/test`)

Testing agent:

- Reads implementation
- Creates tests
- Updates tasks.md
- Updates recovery.md

### 4. Review (`/review`)

Docs & Vision agent:

- Writes documentation
- Runs quality gate
- **MANDATORY CLEANUP**:
    - Removes all tmp/ directories
    - Archives completed requirement
    - Keeps only last 3 active requirements

## Cleanup Protocol

**MANDATORY after every `/review`:**

1. **Remove tmp/ directories:**

   ```bash
   find requirements/*/tmp -type d -exec rm -rf {} +
   ```

2. **Archive completed work:**

   ```bash
   mv requirements/{requirement} requirements/archive/{requirement}
   ```

3. **Keep only last 3 active:**

   ```bash
   # Move oldest requirements to archive if more than 3 active
   ```

## Session Continuity

To resume work across sessions:

```python
# Read recovery.md to understand current state
Read("requirements/{requirement}/recovery.md")

# Check tasks.md for what's complete
Read("requirements/{requirement}/tasks.md")

# Review PRD for full context
Read("requirements/{requirement}/prd.md")

# Review research findings
Read("requirements/{requirement}/research/plan.md")
```

## Active Requirements Limit

**Keep only 3 active requirements** in requirements/ root:

- Prevents workspace clutter
- Forces completion of old work
- Maintains focus on current priorities
- Older requirements auto-archived

## Archive Management

Completed requirements in `requirements/archive/` are:

- Preserved for reference
- Searchable for patterns
- Available for recovery if needed
- Never deleted (historical record)

## Usage with AI Agents

All agents (Claude, Gemini, Codex) use this workspace:

- **Planner** creates the structure
- **Expert** implements and updates
- **Testing** adds test tracking
- **Docs & Vision** enforces cleanup

## Example

See `requirements/example-feature/` for reference structure.
