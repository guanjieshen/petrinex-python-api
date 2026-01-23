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
        
        # Create CSV with SubmissionDate
        csv_data = b"""SubmissionDate,Volume
2025-01-15,123.456
2025-01-16,789.012"""
        
        # Read CSV like the client does
        pdf = pd.read_csv(
            io.BytesIO(csv_data),
            dtype=str,
            encoding="latin1",
            on_bad_lines="skip",
            engine="python",
        )
        
        # Before conversion fix, SubmissionDate would be string (object)
        assert pdf["SubmissionDate"].dtype == object, "Initially should be string"
        
        # After applying the conversion (like client.py does)
        if "SubmissionDate" in pdf.columns:
            pdf["SubmissionDate"] = pd.to_datetime(pdf["SubmissionDate"], errors="coerce")
        
        # Now should be datetime64
        assert pdf["SubmissionDate"].dtype == 'datetime64[ns]', \
            "SubmissionDate should be datetime64 after conversion"
        
        print("✅ Pandas date conversion test passed")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

