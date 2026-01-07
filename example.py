#!/usr/bin/env python3
"""
Example: Load Alberta Petrinex volumetric data

This example demonstrates the memory-efficient loading of Petrinex data
using the PetrinexVolumetricsClient.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from petrinex import PetrinexVolumetricsClient

def main():
    # Create Spark session
    print("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("PetrinexExample") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
    
    # Initialize Petrinex client
    print("Initializing client...")
    client = PetrinexVolumetricsClient(
        spark=spark,
        jurisdiction="AB",
        file_format="CSV"
    )
    
    # Example 1: List available files
    print("\n" + "="*70)
    print("EXAMPLE 1: List files updated in the last 30 days")
    print("="*70)
    
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    files = client.list_updated_after(cutoff)
    print(f"\nFound {len(files)} file(s):")
    for f in files[:5]:
        print(f"  • {f.production_month}: Updated {f.updated_ts}")
    if len(files) > 5:
        print(f"  ... and {len(files) - 5} more")
    
    # Example 2: Load data (memory efficient)
    print("\n" + "="*70)
    print("EXAMPLE 2: Load data with progress tracking")
    print("="*70)
    
    df = client.read_updated_after_as_spark_df_via_pandas(
        "2025-12-01",  # Change as needed
        pandas_read_kwargs={
            "dtype": str,           # Avoid mixed-type issues
            "encoding": "latin1"    # Handle special characters
        }
    )
    
    # Cache for reuse
    df.cache()
    
    print(f"\n✅ Loaded {df.count():,} rows × {len(df.columns)} columns")
    
    # Example 3: Display sample data
    print("\n" + "="*70)
    print("EXAMPLE 3: Display data")
    print("="*70)
    
    print("\nSchema (first 10 columns):")
    for field in df.schema.fields[:10]:
        print(f"  • {field.name}: {field.dataType}")
    if len(df.schema.fields) > 10:
        print(f"  ... and {len(df.schema.fields) - 10} more columns")
    
    print("\nSample data:")
    df.show(5, truncate=False)
    
    # Example 4: Aggregate by month
    print("\n" + "="*70)
    print("EXAMPLE 4: Aggregate by production month")
    print("="*70)
    
    monthly_summary = df.groupBy("production_month") \
        .agg(F.count("*").alias("records")) \
        .orderBy("production_month")
    
    monthly_summary.show(truncate=False)
    
    # Example 5: Save to Delta (optional)
    print("\n" + "="*70)
    print("EXAMPLE 5: Save to Delta table (commented)")
    print("="*70)
    
    print("\nTo save to Delta, uncomment:")
    print("""
    # Add partitioning columns
    df_partitioned = df.withColumn("year", F.substring("production_month", 1, 4)) \\
                       .withColumn("month", F.substring("production_month", 6, 2))
    
    # Write to Delta
    df_partitioned.write \\
        .format("delta") \\
        .mode("overwrite") \\
        .partitionBy("year", "month") \\
        .saveAsTable("petrinex.volumetrics_raw")
    """)
    
    print("\n" + "="*70)
    print("✅ All examples completed!")
    print("="*70)
    
    # Cleanup
    spark.stop()


if __name__ == "__main__":
    main()
