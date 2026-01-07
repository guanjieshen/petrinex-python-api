# GitHub Actions Workflows

This directory contains automated workflows for the Petrinex Python API.

## Workflows

### 1. `test.yml` - Continuous Integration
**Triggers:** Push to main/master, Pull requests
**Purpose:** Tests the package on multiple Python versions

**What it does:**
- Tests on Python 3.7, 3.8, 3.9, 3.10, 3.11
- Installs package and dependencies
- Runs linting with flake8
- Tests package imports

### 2. `publish-to-test-pypi.yml` - Test Publishing
**Triggers:** Tags matching `v*-test` (e.g., v0.1.0-test), Manual dispatch
**Purpose:** Publish to Test PyPI for validation

**What it does:**
- Builds the package
- Validates the build
- Publishes to Test PyPI

### 3. `publish-to-pypi.yml` - Production Publishing
**Triggers:** GitHub releases, Manual dispatch
**Purpose:** Publish to PyPI

**What it does:**
- Builds the package
- Validates the build
- Publishes to PyPI

## Setup Instructions

### 1. Set up PyPI API Tokens

#### For Production PyPI:
1. Go to https://pypi.org/manage/account/
2. Create an API token
3. In your GitHub repo: Settings → Secrets → Actions
4. Add secret: `PYPI_API_TOKEN` = your token

#### For Test PyPI (optional but recommended):
1. Go to https://test.pypi.org/manage/account/
2. Create an API token
3. In your GitHub repo: Settings → Secrets → Actions
4. Add secret: `TEST_PYPI_API_TOKEN` = your token

### 2. Publishing Workflow

#### Test Publishing (recommended first):
```bash
# Tag with -test suffix
git tag v0.1.0-test
git push origin v0.1.0-test

# This triggers publish-to-test-pypi.yml
# Install from Test PyPI to verify:
pip install --index-url https://test.pypi.org/simple/ petrinex
```

#### Production Publishing:
```bash
# Create a release on GitHub
# Go to: Releases → Create new release
# Tag: v0.1.0
# Title: v0.1.0
# Click "Publish release"

# This triggers publish-to-pypi.yml
# Install from PyPI:
pip install petrinex
```

#### Manual Publishing (alternative):
```bash
# Go to: Actions → Publish to PyPI → Run workflow
```

## Version Management

Update version in `pyproject.toml` before publishing:

```toml
[project]
name = "petrinex"
version = "0.1.0"  # Update this
```

## Testing Before Publishing

Always test on Test PyPI first:

```bash
# 1. Update version in pyproject.toml
# 2. Tag and push
git tag v0.1.0-test
git push origin v0.1.0-test

# 3. Wait for workflow to complete
# 4. Test install
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ petrinex

# 5. Verify it works
python -c "from petrinex import PetrinexVolumetricsClient; print('✓')"

# 6. If successful, create production release
```

## Troubleshooting

### Build fails
- Check Python version compatibility
- Ensure all dependencies are in pyproject.toml

### Publish fails with 403
- Check API token is correct
- Ensure token has upload permissions
- Verify token secret name matches workflow

### Version already exists
- PyPI doesn't allow re-uploading same version
- Increment version in pyproject.toml
- Create new tag/release

## Local Testing

Test the build locally before pushing:

```bash
# Build
python -m build

# Check
twine check dist/*

# Upload to Test PyPI (requires credentials)
twine upload --repository testpypi dist/*
```

