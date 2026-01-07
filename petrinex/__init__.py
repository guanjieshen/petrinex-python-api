"""
Petrinex Python API
===================

A Python client for accessing Petrinex Conventional Volumetric data with Spark or pandas.

Usage:
    from petrinex import PetrinexVolumetricsClient
    
    client = PetrinexVolumetricsClient(spark=spark, jurisdiction="AB")
    
    # For Spark DataFrame - two date options:
    df = client.read_spark_df(updated_after="2025-12-01")      # Incremental updates
    df = client.read_spark_df(from_date="2021-01-01")          # All historical data
    
    # For pandas DataFrame:
    pdf = client.read_pandas_df(updated_after="2025-12-01")
    
    # Automatic optimizations for Petrinex CSV files built-in!
"""

from petrinex.client import PetrinexVolumetricsClient, PetrinexFile

__version__ = "0.2.0"
__all__ = ["PetrinexVolumetricsClient", "PetrinexFile"]

