# petrinex-python-api

A Python client for accessing Petrinex Conventional Volumetric data with Spark integration.

## Features

- 🔍 Lists Petrinex files updated after a specific date
- 📊 Two read modes:
  1. **Spark direct read** from HTTPS URLs
  2. **Pandas-based read** (driver-side) that avoids Spark file permissions
- 📝 Automatic provenance tracking (production month, update timestamp, source URL)
- 🔄 Handles schema drift across months with `union_by_name`
- 🔐 **Unity Catalog friendly** - no `SELECT ON ANY FILE` privilege required

## Installation

### From Git Repository

```bash
pip install git+https://github.com/yourusername/petrinex-python-api.git
```

### Local Development Install

```bash
git clone https://github.com/yourusername/petrinex-python-api.git
cd petrinex-python-api
pip install -e .
```

### Dependencies

The package automatically installs:
- `beautifulsoup4` - HTML parsing
- `lxml` - XML parsing
- `pandas` - Data manipulation
- `requests` - HTTP client
- `pyspark` - Spark integration

## Quick Start

```python
from petrinex import PetrinexVolumetricsClient

# Initialize client with your Spark session
client = PetrinexVolumetricsClient(
    spark=spark,
    jurisdiction="AB",
    file_format="CSV"
)

# Recommended in UC-governed env:
df = client.read_updated_after_as_spark_df_via_pandas(
    "2026-01-01",
    pandas_read_kwargs={
        "dtype": str,      # avoid mixed-type issues
        "encoding": "latin1"
    }
)
```

## Usage Guide

### 1. Reading Data (Recommended for Unity Catalog)

This mode downloads files on the driver and avoids Spark file permission issues.

```python
# Read data updated after a specific date
df = client.read_updated_after_as_spark_df_via_pandas(
    "2026-01-01",
    pandas_read_kwargs={
        "dtype": str,           # avoid mixed-type issues
        "encoding": "latin1"    # handle special characters
    }
)

# Display the DataFrame
df.show(10)
df.printSchema()
```

**Why this approach is recommended:**
- ✅ Avoids Unity Catalog `ANY FILE` privilege requirements
- ✅ Handles mixed-type columns gracefully with `dtype: str`
- ✅ Properly decodes special characters with `encoding: "latin1"`
- ✅ Automatically handles schema drift across months
- ✅ No explicit disk writes required

### 2. Alternative: Spark Direct Read

```python
# Direct Spark read (requires SELECT ON ANY FILE in UC)
df = client.read_updated_after_as_spark_df(
    "2026-01-01",
    infer_schema=True,
    header=True,
    add_provenance_columns=True
)
```

**Note:** This mode requires Spark to fetch HTTPS URLs directly, which may be blocked in Unity Catalog environments without appropriate permissions.

### 3. Listing Files Only

```python
# Get list of files with metadata
files = client.list_updated_after("2026-01-01")
for f in files:
    print(f"{f.production_month}: Updated {f.updated_ts}")
    print(f"  URL: {f.url}")

# Get URLs only
urls = client.urls_updated_after("2026-01-01")
```

## API Reference

### PetrinexVolumetricsClient

#### Constructor Parameters

```python
PetrinexVolumetricsClient(
    spark,                          # SparkSession (required)
    jurisdiction="AB",              # Jurisdiction code
    file_format="CSV",              # "CSV" or "XML"
    publicdata_url=None,            # Custom PublicData page URL
    files_base_url="https://...",   # Base URL for downloads
    request_timeout_s=60,           # HTTP timeout in seconds
    user_agent="Mozilla/5.0",       # User-Agent header
    html_parser="html.parser"       # BeautifulSoup parser
)
```

#### Methods

##### `list_updated_after(updated_after: str) -> List[PetrinexFile]`

Returns list of files with updated dates after the specified cutoff.

**Parameters:**
- `updated_after` (str): Date string in "YYYY-MM-DD" format

**Returns:**
- List of `PetrinexFile` objects with:
  - `production_month` (str): "YYYY-MM"
  - `updated_ts` (datetime): When the file was last updated
  - `url` (str): Download URL

##### `urls_updated_after(updated_after: str) -> List[str]`

Returns list of URLs for files updated after the specified cutoff.

##### `read_updated_after_as_spark_df_via_pandas(...) -> DataFrame`

**Recommended method** - Downloads CSVs via requests on the driver, loads into pandas, then converts to Spark DataFrame.

**Parameters:**
- `updated_after` (str): Date string "YYYY-MM-DD"
- `pandas_read_kwargs` (dict, optional): Parameters passed to `pd.read_csv()`
  - Recommended: `{"dtype": str, "encoding": "latin1"}`
- `add_provenance_columns` (bool): Add tracking columns (default: True)
- `union_by_name` (bool): Align columns across months (default: True)

**Example:**
```python
df = client.read_updated_after_as_spark_df_via_pandas(
    "2026-01-01",
    pandas_read_kwargs={
        "dtype": str,               # Force all columns to string
        "encoding": "latin1",       # Handle special characters
        "na_values": ["", "NA"],    # Custom NA values
        "skipinitialspace": True    # Trim whitespace
    }
)
```

##### `read_updated_after_as_spark_df(...) -> DataFrame`

Reads selected months by letting Spark fetch HTTPS URLs directly.

**Parameters:**
- `updated_after` (str): Date string "YYYY-MM-DD"
- `infer_schema` (bool): Infer column types (default: True)
- `header` (bool): First row is header (default: True)
- `add_provenance_columns` (bool): Add tracking columns (default: True)

## Provenance Columns

When `add_provenance_columns=True` (default), these columns are added:

| Column | Description |
|--------|-------------|
| `production_month` | The YYYY-MM production period |
| `file_updated_ts` | Timestamp when file was last updated |
| `source_url` | The download URL for the file |

## Best Practices

1. ✅ **Use pandas mode in Unity Catalog environments** to avoid file permission issues
2. ✅ **Set `dtype: str`** to prevent mixed-type column errors during ingestion
3. ✅ **Use `encoding: "latin1"`** to properly handle special characters in Alberta data
4. ✅ **Enable `union_by_name`** (default) to handle schema changes across months
5. ✅ **Monitor driver memory** when processing large date ranges in pandas mode
6. ✅ **Cache or persist** the DataFrame if you'll use it multiple times

## Example: Complete Databricks Workflow

```python
from pyspark.sql import SparkSession
from petrinex import PetrinexVolumetricsClient

# Create Spark session
spark = SparkSession.builder.appName("PetrinexData").getOrCreate()

# Initialize client
client = PetrinexVolumetricsClient(
    spark=spark,
    jurisdiction="AB",
    file_format="CSV"
)

# Read data updated in the last month
df = client.read_updated_after_as_spark_df_via_pandas(
    "2026-01-01",
    pandas_read_kwargs={
        "dtype": str,
        "encoding": "latin1"
    }
)

# Cache for reuse
df.cache()

# Show schema and sample data
df.printSchema()
df.show(10, truncate=False)

# Write to Delta table
df.write.format("delta").mode("overwrite").saveAsTable("petrinex.volumetrics")

print(f"✅ Loaded {df.count():,} records")
```

## File Format Notes

- 📦 Petrinex files are **double-zipped** (ZIP within ZIP)
- 📝 CSV files use **uppercase extension** (.CSV)
- 💾 Files can be **large** (100+ MB uncompressed)
- 🔄 Schema may vary slightly across months (handled by `union_by_name`)

## Troubleshooting

### Error: "SELECT ON ANY FILE privilege required"

**Solution:** Use the pandas-based read method instead:
```python
df = client.read_updated_after_as_spark_df_via_pandas(...)
```

### Error: "Mixed type column detected"

**Solution:** Set all columns to string type:
```python
pandas_read_kwargs={"dtype": str}
```

### Error: "UnicodeDecodeError"

**Solution:** Use latin1 encoding:
```python
pandas_read_kwargs={"encoding": "latin1"}
```

### Memory Issues with Large Date Ranges

**Solution:** Process in smaller batches:
```python
# Process one month at a time
for month in ["2026-01-01", "2026-02-01", "2026-03-01"]:
    df = client.read_updated_after_as_spark_df_via_pandas(month, ...)
    # Process and write each month
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
- 🐛 [Report bugs](https://github.com/yourusername/petrinex-python-api/issues)
- 💡 [Request features](https://github.com/yourusername/petrinex-python-api/issues)
- 📖 [View documentation](https://github.com/yourusername/petrinex-python-api)
