#!/usr/bin/env python3
"""
Example usage of the Petrinex Python API

This example shows how to use the PetrinexVolumetricsClient to fetch
Alberta volumetric data and load it into Spark.
"""

from pyspark.sql import SparkSession
from petrinex import PetrinexVolumetricsClient

def main():
    # Create Spark session
    print("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("PetrinexExample") \
        .master("local[*]") \
        .getOrCreate()
    
    # Initialize Petrinex client
    print("\nInitializing Petrinex client...")
    client = PetrinexVolumetricsClient(
        spark=spark,
        jurisdiction="AB",
        file_format="CSV"
    )
    
    # Example 1: List files updated after a specific date
    print("\n" + "="*70)
    print("EXAMPLE 1: Listing files updated after 2025-12-01")
    print("="*70)
    
    files = client.list_updated_after("2025-12-01")
    print(f"\nFound {len(files)} file(s):")
    for f in files[:5]:  # Show first 5
        print(f"  • {f.production_month}: Updated {f.updated_ts}")
    if len(files) > 5:
        print(f"  ... and {len(files) - 5} more")
    
    # Example 2: Read data using pandas (recommended for UC)
    print("\n" + "="*70)
    print("EXAMPLE 2: Reading data via pandas (UC-friendly)")
    print("="*70)
    
    df = client.read_updated_after_as_spark_df_via_pandas(
        "2026-01-01",
        pandas_read_kwargs={
            "dtype": str,           # avoid mixed-type issues
            "encoding": "latin1"    # handle special characters
        }
    )
    
    print(f"\n✓ Loaded DataFrame:")
    print(f"  Rows: {df.count():,}")
    print(f"  Columns: {len(df.columns)}")
    
    print("\nSchema (first 10 columns):")
    for field in df.schema.fields[:10]:
        print(f"  • {field.name}: {field.dataType}")
    
    print("\nSample data (first 5 rows):")
    df.show(5, truncate=False)
    
    # Example 3: Query the data
    print("\n" + "="*70)
    print("EXAMPLE 3: Querying the data")
    print("="*70)
    
    # Create temporary view for SQL queries
    df.createOrReplaceTempView("volumetrics")
    
    # Example query: Count by production month
    result = spark.sql("""
        SELECT production_month, COUNT(*) as record_count
        FROM volumetrics
        GROUP BY production_month
        ORDER BY production_month
    """)
    
    print("\nRecords per production month:")
    result.show(truncate=False)
    
    print("\n" + "="*70)
    print("✅ Example completed successfully!")
    print("="*70)
    
    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()

