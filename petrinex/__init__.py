"""
Petrinex Python API
===================

Copyright (c) 2026 Guanjie Shen

A Python client for accessing Petrinex data (Volumetrics, NGL) with Spark or pandas.

Usage:
    from petrinex import PetrinexClient
    
    # Volumetrics (Conventional Oil & Gas Production)
    client = PetrinexClient(spark=spark, jurisdiction="AB", data_type="Vol")
    df = client.read_spark_df(updated_after="2025-12-01")      # Incremental updates
    df = client.read_spark_df(from_date="2021-01-01")          # All historical data
    
    # NGL and Marketable Gas Volumes
    ngl_client = PetrinexClient(spark=spark, data_type="NGL")
    ngl_df = ngl_client.read_spark_df(updated_after="2025-12-01")
    
    # For pandas DataFrame (any data type):
    pdf = client.read_pandas_df(updated_after="2025-12-01")
    
    # Download files to local directory:
    paths = client.download_files(output_dir="./data", updated_after="2025-12-01")
    
    # Backward compatibility: PetrinexVolumetricsClient still works
    from petrinex import PetrinexVolumetricsClient
    client = PetrinexVolumetricsClient(spark=spark)  # Defaults to Vol data
    
    # Automatic optimizations for Petrinex CSV files built-in!
"""

from petrinex.client import (
    PetrinexClient,
    PetrinexVolumetricsClient,
    PetrinexFile,
    SUPPORTED_DATA_TYPES,
)

__version__ = "1.1.1"
__all__ = [
    "PetrinexClient",
    "PetrinexVolumetricsClient",  # Backward compatibility
    "PetrinexFile",
    "SUPPORTED_DATA_TYPES",
]

