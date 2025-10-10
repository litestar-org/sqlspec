# Documentation Preview Setup

This repository uses GitHub Actions to automatically build and deploy documentation previews for pull requests.

## How It Works

When a PR is opened or updated:

1. **CI Workflow** ([ci.yml](.github/workflows/ci.yml)):
   - Builds documentation using `make docs`
   - Saves the PR number to `docs/.pr_number`
   - Uploads docs as an artifact (retained for 1 day)

2. **Preview Deployment** ([docs-preview.yml](.github/workflows/docs-preview.yml)):
   - Triggers when CI completes successfully on a PR
   - Downloads the docs artifact
   - Validates the PR number (security measure)
   - Deploys to `litestar-org/sqlspec-docs-preview` repository
   - Posts a comment on the PR with the preview URL

## Preview URL Format

```
https://litestar-org.github.io/sqlspec-docs-preview/[PR_NUMBER]
```

## Required Setup (One-Time)

### 1. Create the Preview Repository

Create a new repository: `litestar-org/sqlspec-docs-preview`

**Repository Settings:**

- Public repository
- Enable GitHub Pages:
    - Settings → Pages
    - Source: Deploy from a branch
    - Branch: `gh-pages` / `(root)`
- Add description: "Documentation previews for SQLSpec pull requests"

### 2. Create Deploy Token

Generate a GitHub Personal Access Token (classic):

1. Go to <https://github.com/settings/tokens>
2. Click "Generate new token (classic)"
3. Name: `SQLSpec Docs Preview Deploy Token`
4. Expiration: No expiration (or set a reminder to rotate)
5. Scopes:
   - ✅ `repo` (Full control of private repositories)
6. Click "Generate token"
7. **Copy the token immediately** (you won't see it again)

### 3. Add Secret to SQLSpec Repository

1. Go to <https://github.com/litestar-org/sqlspec/settings/secrets/actions>
2. Click "New repository secret"
3. Name: `DOCS_PREVIEW_DEPLOY_TOKEN`
4. Value: Paste the token from step 2
5. Click "Add secret"

## Security

This implementation uses the secure `workflow_run` pattern to prevent "pwn requests":

- **CI workflow** runs with PR code (untrusted) but has **no access to secrets**
- **Preview workflow** runs with main branch code (trusted) and has **access to secrets**
- PR number is validated before deployment to prevent injection attacks

This ensures malicious code from forks cannot access deployment credentials.

## Testing

To test the setup:

1. Create a test PR
2. Wait for CI to complete
3. Check that the preview deployment workflow runs
4. Verify a comment is posted with the preview URL
5. Visit the URL to confirm docs are accessible

## Troubleshooting

### Preview workflow doesn't trigger

- Check that CI workflow completed successfully
- Verify the artifact was uploaded (Actions tab → CI run → Artifacts section)
- Ensure `DOCS_PREVIEW_DEPLOY_TOKEN` secret is set

### Deployment fails

- Verify the token has `repo` scope
- Check that `litestar-org/sqlspec-docs-preview` repository exists
- Ensure GitHub Pages is enabled on the `gh-pages` branch

### Comment not posted

- Check workflow permissions in [docs-preview.yml](.github/workflows/docs-preview.yml)
- Verify `issues: write` and `pull-requests: write` permissions are set

## Cleanup

Preview deployments persist after PR merge. To clean up old previews:

```bash
# Clone the preview repository
git clone https://github.com/litestar-org/sqlspec-docs-preview.git
cd sqlspec-docs-preview

# Switch to gh-pages branch
git checkout gh-pages

# Remove old PR folders (e.g., PR #123)
git rm -r 123
git commit -m "Clean up PR #123 preview"
git push
```

Future enhancement: Automate cleanup when PRs are closed/merged.

## References

- Litestar implementation: [litestar/.github/workflows/docs-preview.yml](https://github.com/litestar-org/litestar/blob/main/.github/workflows/docs-preview.yml)
- Advanced Alchemy implementation: [advanced-alchemy/.github/workflows/docs-preview.yml](https://github.com/litestar-org/advanced-alchemy/blob/main/.github/workflows/docs-preview.yml)
- GitHub Actions security: [Keeping your GitHub Actions secure](https://docs.github.com/en/actions/security-guides/security-hardening-for-github-actions)
