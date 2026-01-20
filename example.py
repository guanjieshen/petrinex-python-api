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
    
    # Example 4: Download files to local directory
    print("\n" + "="*70)
    print("EXAMPLE 4: Download files to local directory")
    print("="*70)
    
    print("\nDownload Petrinex files to your local machine:")
    print("""
    # Download recent updates (extracted CSVs in organized folders)
    paths = client.download_files(
        output_dir="./petrinex_data",
        updated_after="2025-12-01"
    )
    # Creates: ./petrinex_data/2025-12/Vol_2025-12.csv
    print(f"✓ Downloaded {len(paths)} file(s)")
    
    # Download historical range
    paths = client.download_files(
        output_dir="./petrinex_data",
        from_date="2021-01-01",
        end_date="2023-12-31"
    )
    # Creates organized subdirectories: 2021-01/, 2021-02/, etc.
    """)
    
    # Example 5: Historical backfill
    print("\n" + "="*70)
    print("EXAMPLE 5: Historical backfill")
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
    
    # Example 6: Date range loading
    print("\n" + "="*70)
    print("EXAMPLE 6: Date range loading")
    print("="*70)
    
    print("\nTo load data for a specific date range:")
    print("""
    # Load data from 2021 to 2023 only
    df_range = client.read_spark_df(
        from_date="2021-01-01", 
        end_date="2023-12-31"
    )
    print(f"Data from 2021-2023: {df_range.count():,} rows")
    """)
    
    # Example 7: Large data loads with Unity Catalog
    print("\n" + "="*70)
    print("EXAMPLE 7: Large data loads (Unity Catalog)")
    print("="*70)
    
    print("\nFor large data loads (20+ files), write directly to UC table:")
    print("""
    # Historical backfill - avoids memory issues and timeouts
    # Creates table if doesn't exist, appends if it does
    df = client.read_spark_df(
        from_date="2020-01-01",
        uc_table="main.petrinex.volumetrics"
    )
    print(f"✓ Loaded to UC table: {df.count():,} rows")
    
    # Incremental update - appends new data
    df = client.read_spark_df(
        updated_after="2025-12-01",
        uc_table="main.petrinex.volumetrics"
    )
    
    # To replace existing data, truncate first:
    spark.sql("TRUNCATE TABLE main.petrinex.volumetrics")
    df = client.read_spark_df(
        from_date="2020-01-01",
        uc_table="main.petrinex.volumetrics"
    )
    
    # Benefits:
    # - No memory accumulation (each file written immediately)
    # - No Spark Connect timeouts
    # - Automatic schema evolution
    # - Can handle 100+ files
    # - Always appends (no accidental overwrites)
    """)
    
    print("\n" + "="*70)
    print("✓ Examples complete!")
    print("="*70)
    
    spark.stop()


if __name__ == "__main__":
    main()
