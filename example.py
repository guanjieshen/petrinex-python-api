#!/usr/bin/env python3
"""
Petrinex API Example - Load Volumetrics and NGL Data

Demonstrates:
- Loading different data types (Vol, NGL)
- Incremental updates
- Historical backfills
- Data exploration
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from petrinex import PetrinexClient, SUPPORTED_DATA_TYPES
from datetime import datetime, timedelta


def main():
    # Create Spark session
    print("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("PetrinexExample") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
    
    # Show supported data types
    print("\n" + "="*70)
    print("SUPPORTED DATA TYPES")
    print("="*70)
    for data_type, description in SUPPORTED_DATA_TYPES.items():
        print(f"  • {data_type}: {description}")
    
    # Example 1: List available files
    print("\n" + "="*70)
    print("EXAMPLE 1: List available files")
    print("="*70)
    
    client = PetrinexClient(spark=spark, data_type="Vol")
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    files = client.list_updated_after(cutoff)
    print(f"\nFound {len(files)} file(s) updated after {cutoff}:")
    for f in files[:5]:
        print(f"  • {f.production_month}: Updated {f.updated_ts}")
    if len(files) > 5:
        print(f"  ... and {len(files) - 5} more")
    
    # Example 2: Load data (incremental)
    print("\n" + "="*70)
    print("EXAMPLE 2: Load data (incremental)")
    print("="*70)
    
    if files:
        print(f"\nLoading files updated after {cutoff}...")
        df = client.read_spark_df(updated_after=cutoff)
        
        # Note: .cache() not used - not compatible with Databricks Serverless
        print(f"✓ Loaded {df.count():,} rows")
        print(f"✓ Columns: {len(df.columns)}")
        
        print("\nSchema (first 5 columns):")
        for field in df.schema.fields[:5]:
            print(f"  • {field.name}: {field.dataType}")
        
        print("\nSample data:")
        df.show(5, truncate=True)
    else:
        print(f"\n⚠️  No files found. Try an earlier date.")
    
    # Example 3: Load NGL data
    print("\n" + "="*70)
    print("EXAMPLE 3: Load NGL data")
    print("="*70)
    
    print("\nTo load NGL and Marketable Gas data:")
    print("""
    ngl_client = PetrinexClient(spark=spark, data_type="NGL")
    ngl_df = ngl_client.read_spark_df(updated_after="2025-12-01")
    print(f"NGL data: {ngl_df.count():,} rows")
    """)
    
    # Example 4: Historical backfill
    print("\n" + "="*70)
    print("EXAMPLE 4: Historical backfill")
    print("="*70)
    
    print("\nTo load all historical data from a date:")
    print("""
    # Load all data from 2020 onwards
    df_historical = client.read_spark_df(from_date="2020-01-01")
    
    # Save to Delta table
    df_historical.write.format("delta") \\
        .mode("overwrite") \\
        .saveAsTable("main.petrinex.volumetrics")
    """)
    
    print("\n" + "="*70)
    print("✓ Examples complete!")
    print("="*70)
    
    spark.stop()


if __name__ == "__main__":
    main()
