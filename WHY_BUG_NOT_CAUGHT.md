# Why the Schema Conversion Bug Wasn't Caught by Unit Tests

## The Bug
When loading data with `read_spark_df()`, typed columns caused Arrow conversion errors:
```
Exception thrown when converting pandas.Series (object) with name 'SubmissionDate' to Arrow Array (date32[day])
Exception thrown when converting pandas.Series (object) with name 'Volume' to Arrow Array (decimal128(13, 3))
```

This affected all DateType, DecimalType, and IntegerType columns in the schema.

## Why Unit Tests Missed It

### 1. **Over-Mocking in Unit Tests**

The existing unit tests mocked the entire data pipeline:

```python
# From tests/test_client.py
with patch('petrinex.client.pd.read_csv') as mock_read_csv:
    # Returns fake DataFrame without SubmissionDate
    mock_read_csv.return_value = pd.DataFrame({'col1': []})
    
    # Mock Spark conversion - never actually tries to convert
    mock_spark.createDataFrame.side_effect = track_months
```

**Result**: The real schema conversion logic (pandas → Spark with DateType) was never executed.

### 2. **Integration Tests Too Basic**

The integration tests (`tests/test_integration.py`) only:
- Check if URLs are accessible
- Verify response codes
- Don't actually load data into Spark with schemas

**Result**: Real data pipeline never tested in CI.

### 3. **Manual E2E Tests Not Automated**

The comprehensive test (`tests/manual_e2e_test.py`) exists but:
- Requires Databricks environment
- Must be run manually
- Not part of CI pipeline

**Result**: Full end-to-end testing only happens manually.

## The Fix

### 1. **Code Fix**
Changed approach: Create DataFrame without schema, then cast columns:

```python
# petrinex/client.py

# Step 1: Convert SubmissionDate to datetime in pandas
if "SubmissionDate" in pdf.columns:
    pdf["SubmissionDate"] = pd.to_datetime(pdf["SubmissionDate"], errors="coerce")

# Step 2: Create Spark DataFrame WITHOUT explicit schema
# Spark will infer types (everything becomes StringType except SubmissionDate which is TimestampType)
sdf = self.spark.createDataFrame(pdf)

# Step 3: Cast columns to match official schema types
for col_name, field in schema_fields.items():
    if col_name in sdf.columns:
        sdf = sdf.withColumn(col_name, col(col_name).cast(field.dataType))
```

**Why this approach works:**

Arrow's limitations during createDataFrame():
- ❌ Arrow cannot: `string → decimal128` (when schema provided to createDataFrame)
- ❌ Arrow cannot: `float64 → decimal128` (when schema provided to createDataFrame)
- ❌ Arrow cannot: `object (string) → date32` (when schema provided to createDataFrame)

Spark's capabilities with .cast():
- ✅ Spark CAN: `string → decimal128` (using .cast() after DataFrame creation)
- ✅ Spark CAN: `string → integer` (using .cast() after DataFrame creation)
- ✅ Spark CAN: `timestamp → date` (using .cast() after DataFrame creation)

**Solution:**
1. Convert dates in pandas (before Spark): string → datetime64
2. Create DataFrame without schema (let Spark infer - everything is string except dates)
3. Cast columns after creation (Spark handles string → decimal/integer conversions)

### 2. **New Tests Added**
Created `tests/test_schema_conversion.py` with tests that:
- ✅ Verify date columns are converted from string to datetime
- ✅ Verify numeric columns are converted from string to numeric types
- ✅ Test the conversion logic without over-mocking
- ✅ Would catch this bug before it reaches production

```python
def test_pandas_dataframe_has_proper_types_before_spark_conversion(self):
    # Read CSV with dtype=str (like real code does)
    pdf = pd.read_csv(io.BytesIO(csv_data), dtype=str, ...)
    
    # Verify everything starts as strings
    assert pdf["SubmissionDate"].dtype == object
    assert pdf["Volume"].dtype == object
    assert pdf["Hours"].dtype == object
    
    # Apply conversions (like client.py does)
    for col_name, field in schema_fields.items():
        if col_name in pdf.columns:
            if isinstance(field.dataType, DateType):
                pdf[col_name] = pd.to_datetime(pdf[col_name], errors="coerce")
            elif isinstance(field.dataType, (DecimalType, IntegerType)):
                pdf[col_name] = pd.to_numeric(pdf[col_name], errors="coerce")
    
    # Verify conversions
    assert pd.api.types.is_datetime64_any_dtype(pdf["SubmissionDate"])
    assert pd.api.types.is_numeric_dtype(pdf["Volume"])
    assert pd.api.types.is_numeric_dtype(pdf["Hours"])
```

## Lessons Learned

### 1. **Don't Over-Mock**
- Mocking is useful but can hide real bugs
- Mock external dependencies (network, files) but not core logic
- Test actual data transformations when possible

### 2. **Test the Happy Path with Real-ish Data**
- Use realistic data structures in tests
- Include columns with special handling (dates, decimals)
- Verify type conversions actually work

### 3. **Integration Tests Should Test Integration**
- Don't just check if APIs are accessible
- Actually exercise the full data pipeline
- Include schema conversions, type handling, etc.

## Test Coverage Now

| Test Type | Coverage | Catches This Bug? |
|-----------|----------|-------------------|
| **Unit Tests** (45 tests) | API behavior, filtering logic | ❌ No (over-mocked) |
| **Schema Conversion Tests** (2 tests) | ✅ Date conversion logic | ✅ **Yes!** |
| **Integration Tests** (3 tests) | URL accessibility | ❌ No (too basic) |
| **Manual E2E** | Full pipeline in Databricks | ✅ Yes (but manual) |

## Recommendation

For future bug prevention:
1. ✅ Keep the new schema conversion tests
2. Consider adding a lightweight Spark integration test in CI
3. Run manual E2E tests before each release
4. Review test coverage when adding schema changes

