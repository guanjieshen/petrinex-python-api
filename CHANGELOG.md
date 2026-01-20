# Changelog

All notable changes to the Petrinex Python API will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.1] - 2026-01-20

### Fixed
- **VoidType Column Handling**: Automatically detects and drops columns with `VoidType` (all-NULL columns)
  - Prevents "Could not find column" errors when querying Delta tables
  - Happens when pandas DataFrame has all-NULL columns that Spark can't infer type for
  - Logs which columns are dropped for transparency
  - Example: `⚠️  Dropping 1 void column(s) (all NULL): ['Heat']`

### Changed
- **Enhanced Delta Schema Evolution**: Added `spark.databricks.delta.schema.autoMerge.enabled="true"` option
  - Provides additional safety for schema changes
  - Works alongside existing `mergeSchema="true"` setting
- **Improved Error Messages**: Schema mismatch errors now include:
  - Clear explanation of the issue
  - 3 specific solutions with example commands
  - Guidance on how to fix incompatible schemas

## [1.1.0] - 2026-01-20

### Added
- **Test Coverage for download_files()**: Added 8 new unit tests for file download functionality
  - Tests for basic download, month organization, date range filtering
  - Tests for 404 error handling, directory creation, parameter validation
  - Total test suite now includes 42 unit tests + 3 integration tests

### Changed
- **Simplified download_files() API**: Removed optional parameters for clearer, more consistent behavior
  - Always extracts CSV from ZIP (removed `extract_zip` parameter)
  - Always organizes files in month subdirectories (removed `organize_by_month` parameter)
  - Simpler API with only essential date filtering parameters
- **Streamlined Documentation**: README reduced by 36% (219 → 140 lines)
  - Removed duplicate sections
  - Consolidated examples
  - Clearer organization and hierarchy
  - More focused on key features
- **Repository Cleanup**: Removed redundant files
  - Removed `tests/README.md` (testing info now in main README)
  - Cleaner, more maintainable repository structure

### Fixed
- Fixed pre-existing indentation issues in `read_spark_df()` method

## [1.0.1] - 2026-01-19

### Added
- **Local File Download API**: New `download_files()` method to download Petrinex files to local directory
  - Supports all date parameters: `updated_after`, `since`, `from_date`, `end_date`
  - Files extracted from ZIP format and saved as CSVs
  - Organized in subdirectories by production month (e.g., `output_dir/2025-12/Vol_2025-12.csv`)
  - Example: `paths = client.download_files(output_dir="./data", updated_after="2025-12-01")`
  - Graceful error handling with progress tracking
  - Returns list of downloaded file paths
- **Unity Catalog Direct Write**: New `uc_table` parameter for `read_spark_df()`
  - Write each file directly to a Unity Catalog Delta table instead of accumulating in memory
  - Solves Spark Connect timeout issues for large data loads (20+ files)
  - Example: `client.read_spark_df(from_date="2020-01-01", uc_table="main.petrinex.volumetrics")`
  - Always uses append mode (safer, no accidental overwrites)
  - **Safety validation**: Only appends to tables created by this library (checks for provenance columns)
  - **Schema validation**: Validates existing table columns are present in new data before writing
  - **Schema evolution**: Supports adding new columns automatically (via mergeSchema)
  - Prevents accidentally appending to wrong tables or incompatible schemas
  - Clear error messages if schema mismatch detected
  - To replace data, truncate the table first: `spark.sql("TRUNCATE TABLE ...")`
  - Can handle 100+ files without memory or timeout issues
- **Date Range Support**: New optional `end_date` parameter for `read_spark_df()` and `read_pandas_df()`
  - Use with `from_date` to load data for specific date ranges
  - Example: `client.read_spark_df(from_date="2021-01-01", end_date="2023-12-31")`
  - Filters production months to only include data up to the specified end date
- Unit tests for `end_date` validation and filtering logic

### Changed
- Updated Python version requirement from 3.7+ to 3.8+ (Python 3.7 reached EOL June 2023)
- GitHub Actions now tests on Python 3.8, 3.9, 3.10, 3.11, and 3.12
- Fixed `.gitignore` to allow test files in `tests/` directory while ignoring temporary test files in root
- Added comprehensive badges to README (PyPI version, downloads, build status)
- Updated copyright to Guanjie Shen 2026
- Improved package metadata in `setup.py` and `pyproject.toml`
- Enhanced documentation with date range examples in README, example.py, and databricks_example.ipynb

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

