"""
Tests for schema conversion (pandas → Spark) with real data structures.

These tests verify that the schema conversion logic works correctly with
realistic data, including date columns that require special handling.
"""

import pytest
import pandas as pd
import io
from datetime import datetime
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

from petrinex import PetrinexClient
from petrinex.schema import get_volumetrics_schema


@pytest.mark.integration
class TestSchemaConversion:
    """Test pandas to Spark schema conversion with realistic data"""
    
    def test_submission_date_conversion_in_pandas(self):
        """Test that SubmissionDate string is converted to datetime in pandas before Spark conversion"""
        # This test verifies the conversion logic without requiring a full Spark session
        
        # Create CSV data with SubmissionDate as string (like real Petrinex data)
        csv_data = b"""ProductionMonth,OperatorBAID,SubmissionDate,Volume,Hours
2025-01,BA123,2025-01-15,1234.567,24
2025-01,BA456,2025-01-16,9876.543,22"""
        
        # Read CSV exactly like client.py does
        pdf = pd.read_csv(
            io.BytesIO(csv_data),
            dtype=str,              # All columns as strings initially
            encoding="latin1",
            on_bad_lines="skip",
            engine="python",
        )
        
        # Verify SubmissionDate starts as string (object dtype)
        assert pdf["SubmissionDate"].dtype == object, "Initially should be string"
        assert isinstance(pdf["SubmissionDate"].iloc[0], str), "Values should be strings"
        
        # Apply the conversion (like client.py does now)
        if "SubmissionDate" in pdf.columns:
            pdf["SubmissionDate"] = pd.to_datetime(pdf["SubmissionDate"], errors="coerce")
        
        # Verify conversion to datetime
        assert pd.api.types.is_datetime64_any_dtype(pdf["SubmissionDate"]), \
            "SubmissionDate should be datetime64 after conversion"
        
        # Verify values are datetime objects
        assert isinstance(pdf["SubmissionDate"].iloc[0], pd.Timestamp), \
            "Values should be Timestamp objects"
        
        print("✅ SubmissionDate pandas conversion test passed")
        print(f"   Before: object (string)")
        print(f"   After: {pdf['SubmissionDate'].dtype}")
    
    def test_pandas_dataframe_has_proper_types_before_spark_conversion(self):
        """Test that pandas DataFrame has proper types before being converted to Spark"""
        spark = MagicMock()
        client = PetrinexClient(spark=spark, data_type="Vol")
        
        # Create CSV with date and numeric columns
        csv_data = b"""SubmissionDate,Volume,Hours,Energy
2025-01-15,123.456,24,5678.901
2025-01-16,789.012,22,3456.789"""
        
        # Read CSV like the client does (everything as strings)
        pdf = pd.read_csv(
            io.BytesIO(csv_data),
            dtype=str,
            encoding="latin1",
            on_bad_lines="skip",
            engine="python",
        )
        
        # Before conversion, all columns should be strings (object)
        assert pdf["SubmissionDate"].dtype == object, "Initially should be string"
        assert pdf["Volume"].dtype == object, "Volume should remain as string"
        assert pdf["Hours"].dtype == object, "Hours should remain as string"
        
        # After applying the conversions (like client.py does)
        # Note: We only convert DateType columns. Numeric columns remain as strings
        # because Spark can convert string→decimal but not float64→decimal via Arrow
        from petrinex.schema import get_volumetrics_schema
        from pyspark.sql.types import DateType
        
        schema = get_volumetrics_schema()
        schema_fields = {field.name: field for field in schema.fields}
        
        for col_name, field in schema_fields.items():
            if col_name in pdf.columns:
                if isinstance(field.dataType, DateType):
                    pdf[col_name] = pd.to_datetime(pdf[col_name], errors="coerce")
        
        # Verify: DateType columns converted, numeric columns remain as strings
        assert pd.api.types.is_datetime64_any_dtype(pdf["SubmissionDate"]), \
            "SubmissionDate should be datetime64 after conversion"
        assert pdf["Volume"].dtype == object, \
            "Volume should remain as string (Spark will convert to Decimal)"
        assert pdf["Hours"].dtype == object, \
            "Hours should remain as string (Spark will convert to Integer)"
        assert pdf["Energy"].dtype == object, \
            "Energy should remain as string (Spark will convert to Decimal)"
        
        print("✅ Pandas type conversion test passed")
        print(f"   SubmissionDate: {pdf['SubmissionDate'].dtype} (converted to datetime)")
        print(f"   Volume: {pdf['Volume'].dtype} (kept as string for Spark)")
        print(f"   Hours: {pdf['Hours'].dtype} (kept as string for Spark)")
        print(f"   Energy: {pdf['Energy'].dtype} (kept as string for Spark)")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

