"""
Petrinex Python API
===================

A Python client for accessing Petrinex Conventional Volumetric data with Spark or pandas.

Usage:
    from petrinex import PetrinexVolumetricsClient
    
    client = PetrinexVolumetricsClient(spark=spark, jurisdiction="AB")
    
    # For Spark DataFrame (recommended for large data):
    df = client.read_spark_df("2025-12-01")
    
    # For pandas DataFrame (for smaller data):
    pdf = client.read_pandas_df("2025-12-01")
    
    # Automatic optimizations for Petrinex CSV files built-in!
"""

from petrinex.client import PetrinexVolumetricsClient, PetrinexFile

__version__ = "0.2.0"
__all__ = ["PetrinexVolumetricsClient", "PetrinexFile"]

