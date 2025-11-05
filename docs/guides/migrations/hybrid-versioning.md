(hybrid-versioning-guide)=

# Hybrid Versioning Guide

**Combine timestamp and sequential migration numbering for optimal development and production workflows.**

## Overview

Hybrid versioning is a migration strategy that uses different version formats for different stages of development:

- **Development**: Timestamp-based versions (e.g., `20251011120000`) avoid merge conflicts
- **Production**: Sequential versions (e.g., `0001`, `0002`) provide deterministic ordering

SQLSpec's `fix` command automates the conversion between these formats, enabling teams to work independently without version collisions while maintaining strict ordering in production.

## The Problem

Traditional migration versioning strategies have trade-offs:

### Sequential-Only Approach

```
migrations/
├── 0001_initial.sql
├── 0002_add_users.sql
├── 0003_add_products.sql  ← Alice creates this
└── 0003_add_orders.sql    ← Bob creates this (CONFLICT!)
```

**Problem**: When multiple developers create migrations simultaneously, they pick the same next number, causing merge conflicts.

### Timestamp-Only Approach

```
migrations/
├── 20251010120000_initial.sql
├── 20251011090000_add_users.sql
├── 20251011120000_add_products.sql  ← Alice (created at 12:00)
└── 20251011100000_add_orders.sql    ← Bob (created at 10:00, but merged later)
```

**Problem**: Migration order depends on timestamp, not merge order. Bob's migration runs first even though Alice's PR merged first.

## The Solution: Hybrid Versioning

Hybrid versioning combines the best of both approaches:

1. **Developers create migrations with timestamps** (no conflicts)
2. **CI converts timestamps to sequential before merge** (deterministic order)
3. **Production sees only sequential migrations** (clean, predictable)

```
Development (feature branch):
├── 0001_initial.sql
├── 0002_add_users.sql
└── 20251011120000_add_products.sql  ← Alice's new migration

                 ↓ PR merged, CI runs `sqlspec fix`

Main branch:
├── 0001_initial.sql
├── 0002_add_users.sql
└── 0003_add_products.sql  ← Converted to sequential
```

## Workflow

### 1. Development Phase

Developers create migrations normally:

```bash
# Alice on feature/products
sqlspec --config myapp.config create-migration -m "add products table"
# Creates: 20251011120000_add_products.sql

# Bob on feature/orders (same time)
sqlspec --config myapp.config create-migration -m "add orders table"
# Creates: 20251011120500_add_orders.sql
```

No conflicts! Timestamps are unique.

### 2. Pre-Merge CI Check

Before merging to main, CI converts timestamps to sequential:

```yaml
# .github/workflows/migrations.yml
name: Fix Migrations
on:
  pull_request:
    branches: [main]
    paths: ['migrations/**']

jobs:
  fix-migrations:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ github.head_ref }}

      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install SQLSpec
        run: pip install sqlspec[cli]

      - name: Convert migrations to sequential
        run: |
          sqlspec --config myapp.config fix --yes --no-database

      - name: Commit changes
        run: |
          git config user.name "GitHub Actions"
          git config user.email "actions@github.com"
          git add migrations/
          if ! git diff --quiet && ! git diff --staged --quiet; then
            git commit -m "fix: convert migrations to sequential format"
            git push
          fi
```

### 3. Production Deployment

Production only sees sequential migrations with deterministic ordering:

```
migrations/
├── 0001_initial.sql
├── 0002_add_users.sql
├── 0003_add_products.sql   ← Alice's migration (merged first)
└── 0004_add_orders.sql     ← Bob's migration (merged second)
```

Order is determined by merge order, not timestamp.

## Command Reference

### Preview Changes

See what would be converted without applying:

```bash
sqlspec --config myapp.config fix --dry-run
```

Output:

```
╭─────────────────────────────────────────────────────────╮
│ Migration Conversions                                    │
├───────────────┬───────────────┬─────────────────────────┤
│ Current Ver   │ New Version   │ File                     │
├───────────────┼───────────────┼─────────────────────────┤
│ 20251011120000│ 0003          │ 20251011120000_prod.sql  │
│ 20251012130000│ 0004          │ 20251012130000_ord.sql   │
╰───────────────┴───────────────┴─────────────────────────╯

2 migrations will be converted
[Preview Mode - No changes made]
```

### Apply Conversion

Convert with confirmation:

```bash
sqlspec --config myapp.config fix
```

You'll be prompted:

```
Proceed with conversion? [y/N]: y

✓ Created backup in .backup_20251012_143022
✓ Renamed 20251011120000_add_products.sql → 0003_add_products.sql
✓ Renamed 20251012130000_add_orders.sql → 0004_add_orders.sql
✓ Updated 2 database records
✓ Conversion complete!
```

### CI/CD Mode

Auto-approve for automation:

```bash
sqlspec --config myapp.config fix --yes
```

### Files Only (Skip Database)

Useful when database is not accessible:

```bash
sqlspec --config myapp.config fix --no-database
```

## What Gets Updated

The `fix` command updates three things:

### 1. File Names

```
Before: 20251011120000_add_products.sql
After:  0003_add_products.sql
```

### 2. SQL Query Names (inside .sql files)

```sql
-- Before
-- name: migrate-20251011120000-up
CREATE TABLE products (id INT);

-- name: migrate-20251011120000-down
DROP TABLE products;

-- After
-- name: migrate-0003-up
CREATE TABLE products (id INT);

-- name: migrate-0003-down
DROP TABLE products;
```

### 3. Database Records

Migration tracking table is updated:

```sql
-- Before
INSERT INTO sqlspec_versions (version_num, ...) VALUES ('20251011120000', ...);

-- After
UPDATE sqlspec_versions SET version_num = '0003' WHERE version_num = '20251011120000';
```

## Safety Features

### Automatic Backups

Before any changes, a timestamped backup is created:

```
migrations/
├── .backup_20251012_143022/  ← Automatic backup
│   ├── 20251011120000_add_products.sql
│   └── 20251012130000_add_orders.sql
├── 0003_add_products.sql
└── 0004_add_orders.sql
```

### Automatic Rollback

If conversion fails, files are automatically restored:

```
Error: Target file already exists: 0003_add_products.sql
Restored files from backup
```

### Dry Run Mode

Always preview before applying:

```bash
sqlspec --config myapp.config fix --dry-run
```

## Programmatic API

For Python-based migration automation, use the config method directly instead of CLI commands:

### Async Configuration

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://user:pass@localhost/mydb"},
    migration_config={
        "enabled": True,
        "script_location": "migrations",
    }
)

# Preview conversions
await config.fix_migrations(dry_run=True)

# Apply conversions (auto-approve)
await config.fix_migrations(dry_run=False, update_database=True, yes=True)

# Files only (skip database update)
await config.fix_migrations(dry_run=False, update_database=False, yes=True)
```

### Sync Configuration

```python
from sqlspec.adapters.sqlite import SqliteConfig

config = SqliteConfig(
    pool_config={"database": "myapp.db"},
    migration_config={
        "enabled": True,
        "script_location": "migrations",
    }
)

# Preview conversions (no await needed)
config.fix_migrations(dry_run=True)

# Apply conversions (auto-approve)
config.fix_migrations(dry_run=False, update_database=True, yes=True)

# Files only (skip database update)
config.fix_migrations(dry_run=False, update_database=False, yes=True)
```

### Use Cases

The programmatic API is useful for:

- **Custom deployment scripts** - Integrate migration fixing into deployment automation
- **Testing workflows** - Automate migration testing in CI/CD pipelines
- **Framework integrations** - Build migration support into web framework startup hooks
- **Monitoring tools** - Track migration conversions programmatically

### Example: Custom Deployment Script

```python
import asyncio
from sqlspec.adapters.asyncpg import AsyncpgConfig

async def deploy():
    config = AsyncpgConfig(
        pool_config={"dsn": "postgresql://..."},
        migration_config={"script_location": "migrations"}
    )

    # Step 1: Convert migrations to sequential
    print("Converting migrations to sequential format...")
    await config.fix_migrations(dry_run=False, update_database=True, yes=True)

    # Step 2: Apply all pending migrations
    print("Applying migrations...")
    await config.upgrade("head")

    # Step 3: Verify current version
    current = await config.get_current_migration(verbose=True)
    print(f"Deployed to version: {current}")

asyncio.run(deploy())
```

## Best Practices

### 1. Always Use Version Control

Commit migration files before running `fix`:

```bash
git add migrations/
git commit -m "feat: add products migration"

# Then run fix
sqlspec --config myapp.config fix
```

### 2. Run Fix in CI, Not Locally

Let CI handle conversion to avoid inconsistencies:

```yaml
# Good: CI converts before merge
on:
  pull_request:
    branches: [main]

# Bad: Manual conversion on developer machines
```

### 3. Test Migrations Before Fix

Ensure migrations work before converting:

```bash
# Test on development database
sqlspec --config test.config upgrade
sqlspec --config test.config downgrade

# Then convert
sqlspec --config myapp.config fix
```

### 4. Keep Backup Until Verified

Don't delete backup immediately:

```bash
# Convert
sqlspec --config myapp.config fix

# Test deployment
sqlspec --config prod.config upgrade

# Only then remove backup
rm -rf migrations/.backup_*
```

### 5. Document Your Workflow

Add to your project's CONTRIBUTING.md:

```markdown
## Migrations

- Create migrations with: `sqlspec --config myapp.config create-migration -m "description"`
- Migrations use timestamp format during development
- CI automatically converts to sequential before merge
- Never manually rename migration files
```

## Example Workflows

### GitHub Actions (Recommended)

```yaml
# .github/workflows/fix-migrations.yml
name: Fix Migrations

on:
  pull_request:
    branches: [main]
    paths: ['migrations/**']

jobs:
  fix:
    runs-on: ubuntu-latest

    permissions:
      contents: write

    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ github.head_ref }}
          token: ${{ secrets.GITHUB_TOKEN }}

      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: |
          pip install sqlspec[cli]
          pip install -e .

      - name: Fix migrations
        run: |
          sqlspec --config myapp.config fix --yes --no-database

      - name: Commit and push
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add migrations/
          if ! git diff --quiet HEAD migrations/; then
            git commit -m "fix: convert migrations to sequential format"
            git push
          else
            echo "No changes to commit"
          fi
```

### GitLab CI

```yaml
# .gitlab-ci.yml
fix-migrations:
  stage: migrate
  image: python:3.12
  before_script:
    - pip install sqlspec[cli]
  script:
    - sqlspec --config myapp.config fix --yes --no-database
    - |
      if ! git diff --quiet migrations/; then
        git config user.name "GitLab CI"
        git config user.email "ci@gitlab.com"
        git add migrations/
        git commit -m "fix: convert migrations to sequential"
        git push origin HEAD:$CI_COMMIT_REF_NAME
      fi
  only:
    refs:
      - merge_requests
    changes:
      - migrations/**
```

### Manual Workflow

If you can't use CI:

```bash
# 1. Create feature branch
git checkout -b feature/new-stuff

# 2. Create migration
sqlspec --config myapp.config create-migration -m "add new stuff"
# Creates: 20251012120000_add_new_stuff.sql

# 3. Commit
git add migrations/
git commit -m "feat: add new stuff migration"

# 4. Before merging, convert to sequential
sqlspec --config myapp.config fix --dry-run  # Preview
sqlspec --config myapp.config fix            # Apply

# 5. Commit converted files
git add migrations/
git commit -m "fix: convert to sequential format"

# 6. Merge to main
git checkout main
git merge feature/new-stuff
```

## Troubleshooting

### Version Collision

**Problem**: `Target file already exists: 0003_add_products.sql`

**Solution**: Someone already has a migration with that number. Pull latest from main:

```bash
git pull origin main
# Then fix will assign next available number
sqlspec --config myapp.config fix
```

### Database Out of Sync

**Problem**: Database has old timestamp versions after fix

**Solution**: Run fix with database updates:

```bash
sqlspec --config myapp.config fix --yes
```

Or manually update tracking table:

```sql
UPDATE sqlspec_versions
SET version_num = '0003'
WHERE version_num = '20251011120000';
```

### After Pulling Fixed Migrations

**Problem**: Teammate ran `fix` and merged to main. You pull changes and your local database still has timestamp version.

**Example**:

- Your database: `version_num = '20251011120000'`
- Migration file (after pull): `0003_add_users.sql`

**Solution (Automatic)**: Just run `upgrade` - auto-sync handles it:

```bash
git pull origin main              # Get renamed migration files
sqlspec --config myapp.config upgrade # Auto-sync updates: 20251011120000 → 0003
```

**Solution (Manual)**: If you disabled auto-sync, run `fix`:

```bash
git pull origin main              # Get renamed migration files
sqlspec --config myapp.config fix # Updates your database: 20251011120000 → 0003
sqlspec --config myapp.config upgrade # Now sees 0003 already applied
```

**Why this happens**: Migration files were renamed but your local database still references the old timestamp version.

**Best Practice**: Enable auto-sync (default) for automatic reconciliation. See [Auto-Sync section](#auto-sync-the-fix-command-on-autopilot) for details.

### CI Fails to Push

**Problem**: CI can't push converted migrations

**Solution**: Check repository permissions:

- GitHub: Enable "Allow GitHub Actions to create and approve pull requests"
- GitLab: Use access token with `write_repository` scope

### Mixed Formats After Merge

**Problem**: Some migrations are timestamp, some sequential

**Solution**: This is normal during transition. Run fix to convert remaining:

```bash
sqlspec --config myapp.config fix
```

## Migration from Sequential-Only

If you're currently using sequential-only migrations:

1. Continue using sequential for existing migrations
2. New migrations can use timestamps
3. Run `fix` before each merge to convert

No migration history is lost - the command only converts timestamps that exist.

## Migration from Timestamp-Only

If you're currently using timestamp-only migrations:

1. Run `fix` once to convert all existing timestamps
2. Continue using timestamps for new migrations
3. Run `fix` in CI for future conversions

```bash
# One-time conversion
sqlspec --config myapp.config fix --dry-run  # Preview
sqlspec --config myapp.config fix            # Convert all

git add migrations/
git commit -m "chore: convert all migrations to sequential"
git push
```

## Auto-Sync: The Fix Command on Autopilot

SQLSpec now automatically reconciles renamed migrations when you run `upgrade`. No more manual `fix` commands after pulling changes.

### How It Works

When you run `upgrade`, SQLSpec:

1. Checks if migration files have been renamed (timestamp → sequential)
2. Validates checksums match between old and new versions
3. Auto-updates your database tracking to match the renamed files
4. Proceeds with normal migration workflow

This happens transparently - you just run `upgrade` and it works.

### Usage Scenarios

#### Scenario 1: Pull and Go (The Happy Path)

Your teammate merged a PR that converted migrations to sequential format.

```bash
# Your database before pull
SELECT version_num FROM ddl_migrations;
# 20251011120000  ← timestamp format

git pull origin main

# Migration files after pull
ls migrations/
# 0001_initial.sql
# 0002_add_users.sql
# 0003_add_products.sql  ← was 20251011120000_add_products.sql

# Just run upgrade - auto-sync handles everything
sqlspec --config myapp.config upgrade

# Output:
# Reconciled 1 version record(s)
# Already at latest version

# Your database after upgrade
SELECT version_num FROM ddl_migrations;
# 0003  ← automatically updated!
```

**Before auto-sync**: You'd need to manually run `fix` or update the database yourself.

**With auto-sync**: Just `upgrade` and continue working.

#### Scenario 2: Team Workflow (Multiple PRs)

Three developers working on different features.

```bash
# Alice (feature/products branch)
sqlspec --config myapp.config create-migration -m "add products table"
# Creates: 20251011120000_add_products.sql

# Bob (feature/orders branch)
sqlspec --config myapp.config create-migration -m "add orders table"
# Creates: 20251011121500_add_orders.sql

# Carol (feature/invoices branch)
sqlspec --config myapp.config create-migration -m "add invoices table"
# Creates: 20251011123000_add_invoices.sql
```

**Alice's PR merges first:**

```bash
# CI runs: sqlspec --config myapp.config fix --yes --no-database
# Renames: 20251011120000_add_products.sql → 0003_add_products.sql
# Merged to main
```

**Bob pulls and continues:**

```bash
git pull origin main

# Bob's local database still has: 20251011120000 (Alice's old timestamp)
# Bob's migration files now have: 0003_add_products.sql (Alice's renamed)

# Bob just runs upgrade to apply his changes
sqlspec --config myapp.config upgrade

# Output:
# Reconciled 1 version record(s)  ← Alice's migration auto-synced
# Found 1 pending migrations
# Applying 20251011121500: add orders table
# ✓ Applied in 15ms
```

**Bob's PR merges second:**

```bash
# CI converts Bob's timestamp → 0004
# Merged to main
```

**Carol pulls and continues:**

```bash
git pull origin main

# Carol's local database has:
# - 20251011120000 (Alice's old timestamp)
# - 20251011121500 (Bob's old timestamp)

# Carol's migration files now have:
# - 0003_add_products.sql (Alice's renamed)
# - 0004_add_orders.sql (Bob's renamed)

sqlspec --config myapp.config upgrade

# Output:
# Reconciled 2 version record(s)  ← Both auto-synced!
# Found 1 pending migrations
# Applying 20251011123000: add invoices table
# ✓ Applied in 12ms
```

**Key takeaway**: No manual intervention needed. Each developer just pulls and runs `upgrade`.

#### Scenario 3: Production Deployment

Your production database has never seen timestamp versions.

```bash
# Production database
SELECT version_num FROM ddl_migrations;
# 0001
# 0002
# No timestamps - only sequential

# Deploy new version with migrations 0003, 0004, 0005
sqlspec --config prod.config upgrade

# Output:
# Found 3 pending migrations
# Applying 0003: add products table
# ✓ Applied in 45ms
# Applying 0004: add orders table
# ✓ Applied in 32ms
# Applying 0005: add invoices table
# ✓ Applied in 28ms
```

**Key takeaway**: Production never sees timestamps. Auto-sync is a no-op when all versions are already sequential.

#### Scenario 4: Staging Environment Sync

Staging database has old timestamp versions from before you adopted hybrid versioning.

```bash
# Staging database (mixed state)
SELECT version_num FROM ddl_migrations;
# 0001
# 0002
# 20251008100000  ← old timestamp from before hybrid versioning
# 20251009150000  ← old timestamp
# 20251010180000  ← old timestamp

# Migration files (after fix command ran in CI)
ls migrations/
# 0001_initial.sql
# 0002_add_users.sql
# 0003_add_feature_x.sql  ← was 20251008100000
# 0004_add_feature_y.sql  ← was 20251009150000
# 0005_add_feature_z.sql  ← was 20251010180000
# 0006_new_feature.sql    ← new migration

sqlspec --config staging.config upgrade

# Output:
# Reconciled 3 version record(s)
# Found 1 pending migrations
# Applying 0006: new feature
# ✓ Applied in 38ms

# Staging database (cleaned up)
SELECT version_num FROM ddl_migrations;
# 0001
# 0002
# 0003  ← auto-synced from 20251008100000
# 0004  ← auto-synced from 20251009150000
# 0005  ← auto-synced from 20251010180000
# 0006  ← newly applied
```

**Key takeaway**: Auto-sync gradually cleans up old timestamp versions as you deploy. No manual database updates needed.

#### Scenario 5: Checksum Validation (Safety Check)

Someone manually edited a migration file after it was applied.

```bash
# Database has timestamp version
SELECT version_num, checksum FROM ddl_migrations WHERE version_num = '20251011120000';
# 20251011120000 | a1b2c3d4e5f6...

# Migration file renamed but content changed
cat migrations/0003_add_products.sql
# Different SQL than what was originally applied

sqlspec --config myapp.config upgrade

# Output:
# Checksum mismatch for 20251011120000 → 0003, skipping auto-sync
# Found 0 pending migrations

# Database unchanged - safely prevented incorrect sync
SELECT version_num FROM ddl_migrations;
# 20251011120000  ← still has old version (not auto-synced)
```

**Key takeaway**: Auto-sync validates checksums before updating. Protects against corruption or incorrect renames.

### Configuration Options

#### Enable/Disable Auto-Sync

Auto-sync is enabled by default. Disable via config:

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/mydb"},
    migration_config={
        "script_location": "migrations",
        "enabled": True,
        "auto_sync": False  # Disable auto-sync
    }
)
```

#### Disable Per-Command

Disable for a single migration run:

```bash
sqlspec --config myapp.config upgrade --no-auto-sync
```

Useful when you want explicit control over version reconciliation.

### When to Disable Auto-Sync

Auto-sync is safe for most workflows, but disable if:

1. **You want explicit control**: Run `fix` manually to see exactly what's being updated
2. **Custom migration workflows**: You're using non-standard migration file organization
3. **Debugging**: Isolate whether auto-sync is causing unexpected behavior

### Troubleshooting Auto-Sync

#### Auto-Sync Not Reconciling

**Problem**: Auto-sync reports 0 reconciled records but you expected some.

**Possible causes:**

1. **Already synced**: Database already has sequential versions
2. **No conversion map**: No timestamp migrations found in files
3. **Version already exists**: New version already applied (edge case from parallel execution)

**Debug steps:**

```bash
# Check what's in your database
sqlspec --config myapp.config current --verbose

# Check what's in your migration files
ls -la migrations/

# Try explicit fix to see what would convert
sqlspec --config myapp.config fix --dry-run
```

#### Checksum Mismatch Warnings

**Problem**: `Checksum mismatch for X → Y, skipping auto-sync`

**Cause**: Migration content changed between when it was applied and when it was renamed.

**Solution:**

```bash
# Option 1: Manual fix (if change was intentional)
sqlspec --config myapp.config fix --yes

# Option 2: Revert file changes (if change was accidental)
git checkout migrations/0003_add_products.sql
```

### Migration from Manual Fix Workflow

If you're currently using the manual `fix` workflow, auto-sync is backward-compatible:

```bash
# Old workflow (still works)
git pull origin main
sqlspec --config myapp.config fix  # Manual sync
sqlspec --config myapp.config upgrade

# New workflow (auto-sync handles it)
git pull origin main
sqlspec --config myapp.config upgrade  # Auto-sync runs automatically
```

Both workflows produce identical results. Auto-sync just eliminates the manual step.

### Best Practices with Auto-Sync

1. **Trust auto-sync in dev/staging**: Let it handle reconciliation automatically
2. **Monitor in production**: Check reconciliation output in deployment logs
3. **Use --no-auto-sync for debugging**: Disable temporarily to isolate issues
4. **Keep checksums intact**: Don't edit migration files after they're applied

## Advanced Topics

### Extension Migrations

**Important**: The `fix` command only affects **user-created** migrations, not packaged extension migrations that ship with SQLSpec.

#### Packaged Extension Migrations (NOT affected by `fix`)

Migrations included with SQLSpec extensions are **always sequential**:

```
sqlspec/extensions/
├── adk/migrations/
│   └── 0001_create_adk_tables.py      ← Always sequential
└── litestar/migrations/
    └── 0001_create_session_table.py   ← Always sequential

Database tracking:
- ext_adk_0001
- ext_litestar_0001
```

These are pre-built migrations that ship with the library and are never converted.

#### User-Created Extension Migrations (Affected by `fix`)

If you create custom migrations for extension functionality, they follow the standard hybrid workflow:

```
Before fix (your development branch):
├── 0001_initial.sql
├── ext_adk_0001_create_adk_tables.sql         ← Packaged (sequential)
├── 20251011120000_custom_adk_columns.sql      ← Your custom migration (timestamp)

After fix (merged to main):
├── 0001_initial.sql
├── ext_adk_0001_create_adk_tables.sql         ← Unchanged (packaged)
├── 0002_custom_adk_columns.sql                ← Converted to sequential
```

Each extension has its own sequence counter for user-created migrations.

### Multiple Databases

When using multiple database configurations:

```bash
# Fix migrations for specific database
sqlspec --config myapp.config fix --bind-key postgres

# Or fix all
sqlspec --config myapp.config fix
```

### Custom Migration Paths

Works with custom migration directories:

```python
# config.py
AsyncpgConfig(
    pool_config={"dsn": "..."},
    migration_config={
        "script_location": "db/migrations",  # Custom path
        "enabled": True
    }
)
```

```bash
sqlspec --config myapp.config fix
# Converts migrations in db/migrations/
```

## Performance

The `fix` command is designed for fast execution:

- File operations are atomic (rename only)
- Database updates use single transaction
- Backup is file-system copy (instant)
- No migration re-execution

Typical conversion time: < 1 second for 100 migrations.

## See Also

- [CLI Reference](../../usage/cli.rst) - Complete `fix` command documentation
- [Configuration Guide](../../usage/configuration.rst) - Migration configuration options
- [Best Practices](../best-practices.md) - General migration best practices

## Summary

Hybrid versioning with the `fix` command provides:

- **Zero merge conflicts** - Timestamps during development
- **Deterministic ordering** - Sequential in production
- **Automatic conversion** - CI handles the switch
- **Safe operations** - Automatic backup and rollback
- **Database sync** - Version tracking stays current

Start using hybrid versioning today:

```bash
# Preview conversion
sqlspec --config myapp.config fix --dry-run

# Apply conversion
sqlspec --config myapp.config fix

# Set up CI workflow (see examples above)
```
