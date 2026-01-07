"""
Petrinex Python API
===================

A Python client for accessing Petrinex Conventional Volumetric data with Spark integration.

Usage:
    from petrinex import PetrinexVolumetricsClient
    
    client = PetrinexVolumetricsClient(spark=spark, jurisdiction="AB")
    
    # Recommended for Unity Catalog environments:
    df = client.read_updated_after_as_spark_df_via_pandas(
        "2026-01-01",
        pandas_read_kwargs={
            "dtype": str,
            "encoding": "latin1"
        }
    )
"""

from petrinex.client import PetrinexVolumetricsClient, PetrinexFile

__version__ = "0.1.0"
__all__ = ["PetrinexVolumetricsClient", "PetrinexFile"]

