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
- Running `migrate` tries to apply `0003` again (fails or causes duplicates)

**Solution**: Run `fix` locally to update your database:

```bash
git pull origin main              # Get renamed migration files
sqlspec --config myapp.config fix # Updates your database: 20251011120000 → 0003
sqlspec --config myapp.config migrate # Now sees 0003 already applied
```

**Why this happens**: The `fix` command is idempotent - it safely detects that `0003` already exists in your database and just logs it without errors. This keeps your local database synchronized with the renamed files.

**Best Practice**: Always run `fix` after pulling changes that include renamed migrations.

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

## Advanced Topics

### Extension Migrations

Extension migrations maintain separate numbering:

```
Before fix:
├── 0001_initial.sql
├── ext_litestar_20251011120000_feature.sql
├── ext_adk_20251012130000_other.sql

After fix:
├── 0001_initial.sql
├── ext_litestar_0001_feature.sql       ← Converted
├── ext_adk_0001_other.sql              ← Converted
```

Each extension has its own sequence counter.

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
