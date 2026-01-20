# Changelog

All notable changes to the Petrinex Python API will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2026-01-19

### Changed
- Updated Python version requirement from 3.7+ to 3.8+ (Python 3.7 reached EOL June 2023)
- GitHub Actions now tests on Python 3.8, 3.9, 3.10, 3.11, and 3.12
- Fixed `.gitignore` to allow test files in `tests/` directory while ignoring temporary test files in root
- Added comprehensive badges to README (PyPI version, downloads, build status)
- Updated copyright to Guanjie Shen 2026
- Improved package metadata in `setup.py` and `pyproject.toml`

### Added
- Unit tests now run in CI/CD pipeline
- `requirements.txt` for explicit dependency management
- Better README badges and documentation
- Databricks Serverless compatibility note

### Fixed
- GitHub Actions workflow updated to work with modern Ubuntu runners
- Test import updated to use `PetrinexClient` instead of deprecated `PetrinexVolumetricsClient`

## [0.3.0] - 2026-01-19

### Added
- **Multi-Data-Type Support**: Added support for NGL and Marketable Gas Volumes in addition to Volumetrics
- New `PetrinexClient` class with `data_type` parameter (replaces `PetrinexVolumetricsClient` as primary interface)
- `SUPPORTED_DATA_TYPES` constant exported from package for runtime validation
- Support for two data types:
  - `Vol` - Conventional Volumetrics (oil & gas production)
  - `NGL` - NGL and Marketable Gas Volumes
- Both data types use the same URL structure: `.../Files/{JURISDICTION}/{DATA_TYPE}/{YYYY-MM}/CSV`
- Examples for loading both data types in README, example.py, and databricks_example.ipynb
- Tested and verified with real Petrinex NGL data downloads

### Changed
- Refactored `PetrinexVolumetricsClient` → `PetrinexClient` as the main class
- Updated `_build_download_url()` to use dynamic `data_type` instead of hardcoded "Vol"
- Updated all documentation to reflect multi-data-type support
- Bumped version to 0.3.0

### Deprecated
- `PetrinexVolumetricsClient` is now an alias for backward compatibility
  - Automatically uses `data_type="Vol"`
  - Recommended to migrate to `PetrinexClient(data_type="Vol")` for new code

### Backward Compatibility
- ✅ Existing code using `PetrinexVolumetricsClient` continues to work unchanged
- ✅ All existing methods (`read_spark_df`, `read_pandas_df`, etc.) remain unchanged
- ✅ No breaking changes to API signatures

## [0.2.0] - 2026-01-18

### Added
- Flexible date filtering with `updated_after`, `since`, and `from_date` parameters
- New `read_pandas_df()` method for pandas DataFrame output
- Renamed `read_updated_after_as_spark_df_via_pandas()` to `read_spark_df()` for simplicity

### Changed
- Simplified API by hiding `pandas_read_kwargs` (sensible defaults now built-in)
- Improved date parameter handling with better error messages
- Enhanced documentation and examples

### Deprecated
- `read_updated_after_as_spark_df_via_pandas()` kept as deprecated alias

## [0.1.0] - 2026-01-17

### Added
- Initial release
- Unity Catalog compatible data loading
- Memory-efficient incremental loading with auto-checkpointing
- Automatic error handling (404s, malformed CSV, encoding issues)
- Support for Conventional Volumetrics data only
- ZIP file extraction (including nested ZIPs)
- Provenance tracking columns
- Progress tracking during data loading

