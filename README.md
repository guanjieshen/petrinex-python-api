# Petrinex Python API

A Python client for loading Alberta Petrinex volumetric data into Spark DataFrames, designed for Databricks and Unity Catalog environments.

[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- ✅ **Unity Catalog Compatible** - No `SELECT ON ANY FILE` privilege required
- ✅ **Memory Efficient** - Incremental loading with automatic checkpointing
- ✅ **Robust** - Handles ZIP files, malformed data, missing files automatically
- ✅ **Schema Drift** - Automatically aligns columns across months
- ✅ **Provenance Tracking** - Tracks source files and update timestamps
- ✅ **Databricks Ready** - Direct import from repos, no pip install needed

## Quick Start

### Option 1: Databricks Notebook (Recommended)

1. **Sync this repo to Databricks Repos**
2. **Open `databricks_example.ipynb`**
3. **Run all cells**

That's it! The notebook imports directly from the repo directory.

### Option 2: Python Script

```python
from petrinex import PetrinexVolumetricsClient

# Initialize
client = PetrinexVolumetricsClient(spark=spark, jurisdiction="AB")

# Load data (memory efficient, shows progress)
df = client.read_updated_after_as_spark_df_via_pandas(
    "2025-12-01",
    pandas_read_kwargs={"dtype": str, "encoding": "latin1"}
)

# Display
df.show()
```

## Installation

### For Databricks

No installation needed! Just sync the repo and import:

```python
import sys, os
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
sys.path.insert(0, os.path.dirname(notebook_path))

from petrinex import PetrinexVolumetricsClient
```

### For Local Development

```bash
git clone https://github.com/yourusername/petrinex-python-api.git
cd petrinex-python-api
pip install -e .
```

## Usage

### Basic Example

```python
from petrinex import PetrinexVolumetricsClient

client = PetrinexVolumetricsClient(
    spark=spark,
    jurisdiction="AB",
    file_format="CSV"
)

# Load all files updated after a date
df = client.read_updated_after_as_spark_df_via_pandas(
    "2025-12-01",
    pandas_read_kwargs={"dtype": str, "encoding": "latin1"}
)

print(f"Loaded {df.count():,} rows")
```

### What You'll See

```
Loading 1/60: 2021-01... ✓ (544,221 rows)
Loading 2/60: 2021-02... ✓ (498,832 rows)
...
Loading 10/60: 2021-10... ✓ (487,934 rows)
  → Checkpointed at 10 files (5,234,891 total rows)
...
✓ Successfully loaded 60 file(s)

Loaded 31,456,789 rows
```

### List Files Only

```python
files = client.list_updated_after("2025-12-01")
for f in files:
    print(f"{f.production_month} | {f.updated_ts}")
```

### Save to Delta Table

```python
# Add partitioning columns
df_partitioned = df.withColumn("year", F.substring("production_month", 1, 4)) \
                   .withColumn("month", F.substring("production_month", 6, 2))

# Write to Delta
df_partitioned.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .saveAsTable("main.petrinex.volumetrics_raw")
```

## API Reference

### `PetrinexVolumetricsClient`

#### Constructor

```python
PetrinexVolumetricsClient(
    spark,                    # SparkSession (required)
    jurisdiction="AB",        # Jurisdiction code
    file_format="CSV",        # File format
    request_timeout_s=60,     # HTTP timeout
    user_agent="Mozilla/5.0"  # User agent string
)
```

#### Methods

##### `read_updated_after_as_spark_df_via_pandas(updated_after, pandas_read_kwargs=None)`

**Recommended method** - Memory efficient, Unity Catalog compatible.

**Parameters:**
- `updated_after` (str): Date in "YYYY-MM-DD" format
- `pandas_read_kwargs` (dict): Options for `pd.read_csv()`
  - Recommended: `{"dtype": str, "encoding": "latin1"}`

**Returns:** Spark DataFrame with provenance columns

**Features:**
- Incremental union (memory efficient)
- Automatic checkpointing every 10 files
- Progress tracking
- Skips missing files (404)
- Handles nested ZIPs automatically

##### `list_updated_after(updated_after)`

Lists files with update dates after the cutoff.

**Returns:** List of `PetrinexFile` objects with:
- `production_month`: "YYYY-MM" string
- `updated_ts`: datetime
- `url`: download URL

## Provenance Columns

All DataFrames include:

| Column | Type | Description |
|--------|------|-------------|
| `production_month` | string | Production period (YYYY-MM) |
| `file_updated_ts` | string | When file was last updated |
| `source_url` | string | Download URL |

## How It Works

1. **Queries Petrinex API** - Scrapes the PublicData page for file metadata
2. **Filters by date** - Returns only files updated after your cutoff
3. **Downloads files** - Fetches via HTTP (driver-side)
4. **Extracts ZIPs** - Handles nested ZIP files automatically
5. **Loads to pandas** - Reads CSV with robust error handling
6. **Converts to Spark** - Creates Spark DataFrame
7. **Unions incrementally** - Memory-efficient union with checkpointing
8. **Adds provenance** - Tracks source files and timestamps

## File Format Details

- **Compression**: Double-zipped (ZIP within ZIP) - automatically handled
- **Format**: CSV with header row
- **Encoding**: Latin-1 (for special characters)
- **Size**: ~7-8 MB compressed → ~120 MB uncompressed per month
- **Rows**: ~500K-600K rows per month
- **Schema**: May vary slightly across months (handled automatically)

## Memory Considerations

**Incremental union approach:**
- Unions DataFrames as they load (not all at once)
- Checkpoints every 10 files to avoid long lineage
- Typical usage: 16-32 GB driver memory for 60 files

**If you hit OOM:**
```python
# Increase driver memory in cluster config
spark.conf.set("spark.driver.memory", "32g")
spark.conf.set("spark.driver.maxResultSize", "8g")
```

## Error Handling

All errors are handled gracefully:

| Error | Behavior |
|-------|----------|
| 404 Not Found | Skips file, continues loading others |
| Malformed CSV | Skips bad lines with `on_bad_lines='skip'` |
| ZIP issues | Auto-extracts nested ZIPs |
| Encoding errors | Uses latin-1 encoding |
| No data found | Clear error message with suggestions |

## Best Practices

1. ✅ Use realistic date ranges (files may not be published for recent months)
2. ✅ Always use `dtype: str` to avoid mixed-type column errors
3. ✅ Use `encoding: "latin1"` for Alberta data
4. ✅ Cache the final DataFrame if using it multiple times
5. ✅ Monitor the progress output to track loading
6. ✅ For very large loads (100+ files), increase driver memory

## Examples

### Load Last 6 Months

```python
from datetime import datetime, timedelta

six_months_ago = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
df = client.read_updated_after_as_spark_df_via_pandas(
    six_months_ago,
    pandas_read_kwargs={"dtype": str, "encoding": "latin1"}
)
```

### Filter After Loading

```python
# Load data
df = client.read_updated_after_as_spark_df_via_pandas("2025-01-01", ...)

# Filter to specific operator
df_filtered = df.filter(F.col("OperatorBAID") == "12345")

# Aggregate
summary = df.groupBy("production_month", "ProductType") \
    .agg(F.sum("Volume").alias("total_volume"))
```

### Incremental Updates

```python
# Get last load date from existing table
last_update = spark.sql("""
    SELECT MAX(file_updated_ts) as last_ts
    FROM main.petrinex.volumetrics_raw
""").collect()[0]["last_ts"]

# Load only new/updated files
df_new = client.read_updated_after_as_spark_df_via_pandas(
    last_update.split()[0],  # Convert datetime to date
    pandas_read_kwargs={"dtype": str, "encoding": "latin1"}
)

# Append to existing table
df_new.write.format("delta").mode("append").saveAsTable("...")
```

## Troubleshooting

### "No data loaded" error
- Use an earlier date (e.g., 6 months ago)
- Files may not be published yet for recent months

### Memory errors
- Increase driver memory configuration
- Load smaller date ranges

### Encoding errors
- Already handled with `encoding="latin1"` default
- If issues persist, try `encoding="utf-8"`

## Repository Structure

```
petrinex-python-api/
├── petrinex/                    # Main package
│   ├── __init__.py             # Package exports
│   └── client.py               # PetrinexVolumetricsClient
├── databricks_example.ipynb    # Example notebook
├── example.py                  # Python script example
├── setup.py                    # Package setup
├── pyproject.toml              # Modern Python config
└── README.md                   # This file
```

## Requirements

- Python 3.7+
- PySpark 3.0+
- pandas 1.2+
- requests 2.25+
- beautifulsoup4 4.9+
- lxml 4.6+

## License

MIT License - see [LICENSE](LICENSE) file.

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Support

- 📖 [Full documentation](README.md)
- 💻 [Example notebook](databricks_example.ipynb)
- 🐛 [Report issues](https://github.com/yourusername/petrinex-python-api/issues)

## Changelog

### v0.1.0 (2026-01)
- Initial release
- Unity Catalog compatible
- Memory-efficient incremental loading
- Automatic error handling
- Databricks notebook example

---

**Made with ❤️ for the Alberta energy data community**
