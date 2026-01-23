# Changelog

All notable changes to the Petrinex Python API will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.4] - 2026-01-23

### Fixed
- **Schema Type Conversion**: Fixed Arrow conversion errors for DateType columns
  - Previously caused error: `Exception thrown when converting pandas.Series (object) to Arrow Array (date32[day])` for SubmissionDate
  - Now converts DateType columns to pandas datetime before Spark DataFrame creation
  - Numeric columns (DecimalType, IntegerType) remain as strings - Spark converts them via schema
  - Why this approach:
    - Arrow can convert: `string → decimal128` ✅
    - Arrow cannot convert: `float64 → decimal128` ❌
    - Converting to float64 with `pd.to_numeric()` would cause new Arrow errors
  - Fixes: `ValueError: No data loaded. All X file(s) failed or were skipped`
  - Schema remains unchanged (types defined per official specification)
  - Root cause: Reading CSV with `dtype=str` to avoid mixed-type issues, but DateType columns need datetime objects

### Added
- **Schema Conversion Tests**: New test suite (`tests/test_schema_conversion.py`) to catch pandas → Spark conversion issues
  - Tests date column conversion from string to datetime
  - Verifies numeric columns remain as strings (for Spark to convert)
  - Verifies proper type handling before Spark DataFrame creation
  - Prevents regression of Arrow conversion errors
  - These tests would have caught the schema conversion bugs before release

### Technical Notes
- **Why not convert numeric columns to float64?**
  - Attempted: `pd.to_numeric()` converts to float64
  - Result: Arrow error `Exception thrown when converting pandas.Series (float64) to Arrow Array (decimal128(13, 3))`
  - Solution: Keep numeric columns as strings, let Spark handle conversion via schema
  - Arrow conversions: string→decimal128 ✅, float64→decimal128 ❌

## [1.1.3] - 2026-01-23 (Unreleased - Superseded by 1.1.4)

### Fixed
- **SubmissionDate Conversion**: Partial fix for date column conversion
  - Fixed SubmissionDate but missed numeric columns (Volume, Energy, Hours)
  - Superseded by v1.1.4 which fixes all typed columns

## [1.1.2] - 2026-01-23

### Fixed
- **Date Filtering Bug**: Fixed `from_date` and `end_date` parameters to correctly filter by production month
  - Previously, `from_date` was incorrectly filtering by file update timestamp instead of production month
  - This caused `from_date="2025-01-01"` to load all files from 2022 onwards (incorrect)
  - Now correctly filters to only load files with production months >= `from_date` and <= `end_date`
  - Affects both `read_spark_df()` and `download_files()` methods
  - Example: `from_date="2025-01-01", end_date="2025-01-02"` now correctly loads only January 2025 data
  - Existing `updated_after` and `since` parameters continue to work correctly (filter by file update timestamp)

### Changed
- **Repository Cleanup**: Streamlined repository structure for better maintainability
  - Moved `manual_e2e_test.py` to `tests/` directory
  - Removed redundant `example.py` (functionality covered by `databricks_example.ipynb`)
  - Removed large PDF files from repository, added links to official AER documentation in README
  - Removed unnecessary `tests/__init__.py`
  - Updated README with schema documentation and official documentation links

### Documentation
- Added schema documentation section to README with links to official AER ST39 reports
- Updated data types table with official documentation references
- Added detailed schema information (column counts, data types) for Volumetrics and NGL

## [1.1.1] - 2026-01-20

### Fixed
- **Explicit Schema Application**: Now applies official Petrinex schema from PDF documentation
  - Fixes "Could not find Heat#..." and similar schema errors
  - All columns from official schema are present in every DataFrame (with NULL if not in file)
  - Heat column defined as `Decimal(5,2)` per spec, even when NULL
  - Volume, Energy defined as `Decimal(13,3)`, Hours as `Integer`, etc.
  - Ensures consistent schema across all files regardless of which columns have data
  - Explicit types ensure correct data types (no type inference issues)
  - Reference: PD_Conventional_Volumetrics_Report.pdf (Data Fields section)

### Changed
- **Schema Module**: Separated schema definitions into dedicated `petrinex/schema.py`
  - Better code organization and maintainability
  - Schema definitions in one place, easy to update
  - New public functions: `get_volumetrics_schema()`, `get_ngl_schema()`, `get_schema_for_data_type()`
  - Users can now import schemas directly: `from petrinex import get_volumetrics_schema`
  - **NGL Schema**: Implemented proper NGL schema (26 columns) per PDF documentation
  - Volumetrics: 30 columns, NGL: 26 columns (completely different schemas)
  - No breaking changes - backward compatible

### Added
- **Schema Tests**: Comprehensive test suite in `tests/test_schema.py` (15 tests)
  - Tests schema structure and column definitions
  - Verifies all columns have explicit types
  - Tests data type correctness (Decimal, Integer, Date, String)
  - Integration tests with real Petrinex data downloads
  - Verifies both Volumetrics and NGL schemas work correctly
  - Explicit schemas ensure correct data types in all cases
- **Enhanced Delta Schema Evolution**: Added `spark.databricks.delta.schema.autoMerge.enabled="true"` option
  - Provides additional safety for schema changes
  - Works alongside existing `mergeSchema="true"` setting
  - Handles both forward (new columns) and backward (missing columns) schema evolution
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

