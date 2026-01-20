"""
Unit tests for PetrinexClient
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from petrinex.client import PetrinexClient, PetrinexFile, SUPPORTED_DATA_TYPES


class TestPetrinexClient:
    """Test PetrinexClient initialization and configuration"""
    
    def test_supported_data_types(self):
        """Test that supported data types are defined"""
        assert "Vol" in SUPPORTED_DATA_TYPES
        assert "NGL" in SUPPORTED_DATA_TYPES
        assert len(SUPPORTED_DATA_TYPES) == 2
    
    def test_client_initialization_vol(self):
        """Test client initialization with Vol data type"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        assert client.data_type == "Vol"
        assert client.jurisdiction == "AB"
        assert client.file_format == "CSV"
    
    def test_client_initialization_ngl(self):
        """Test client initialization with NGL data type"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="NGL")
        
        assert client.data_type == "NGL"
        assert client.jurisdiction == "AB"
    
    def test_invalid_data_type(self):
        """Test that invalid data type raises ValueError"""
        mock_spark = MagicMock()
        
        with pytest.raises(ValueError, match="data_type must be one of"):
            PetrinexClient(spark=mock_spark, data_type="INVALID")
    
    def test_invalid_file_format(self):
        """Test that invalid file format raises ValueError"""
        mock_spark = MagicMock()
        
        with pytest.raises(ValueError, match="file_format must be"):
            PetrinexClient(spark=mock_spark, file_format="JSON")


class TestURLBuilding:
    """Test URL building for different data types"""
    
    def test_vol_url_pattern(self):
        """Test Vol URL pattern"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        url = client._build_download_url("2025-11")
        expected = "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2025-11/CSV"
        
        assert url == expected
    
    def test_ngl_url_pattern(self):
        """Test NGL URL pattern (same as Vol)"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="NGL")
        
        url = client._build_download_url("2025-11")
        expected = "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/NGL/2025-11/CSV"
        
        assert url == expected
    
    def test_url_with_xml_format(self):
        """Test URL building with XML format"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol", file_format="XML")
        
        url = client._build_download_url("2025-11")
        expected = "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2025-11/XML"
        
        assert url == expected


class TestPetrinexFile:
    """Test PetrinexFile dataclass"""
    
    def test_petrinex_file_creation(self):
        """Test creating a PetrinexFile"""
        ts = datetime(2025, 11, 15, 10, 30, 0)
        file = PetrinexFile(
            production_month="2025-11",
            updated_ts=ts,
            url="https://example.com/file.csv"
        )
        
        assert file.production_month == "2025-11"
        assert file.updated_ts == ts
        assert file.url == "https://example.com/file.csv"
    
    def test_petrinex_file_immutable(self):
        """Test that PetrinexFile is immutable (frozen)"""
        ts = datetime(2025, 11, 15, 10, 30, 0)
        file = PetrinexFile(
            production_month="2025-11",
            updated_ts=ts,
            url="https://example.com/file.csv"
        )
        
        with pytest.raises(AttributeError):
            file.production_month = "2025-12"


class TestBackwardCompatibility:
    """Test backward compatibility with PetrinexVolumetricsClient"""
    
    def test_volumetrics_client_alias(self):
        """Test that PetrinexVolumetricsClient works as alias"""
        from petrinex.client import PetrinexVolumetricsClient
        
        mock_spark = MagicMock()
        client = PetrinexVolumetricsClient(spark=mock_spark)
        
        # Should default to Vol data type
        assert client.data_type == "Vol"
        assert client.jurisdiction == "AB"
    
    def test_volumetrics_client_ignores_data_type(self):
        """Test that PetrinexVolumetricsClient ignores data_type parameter"""
        from petrinex.client import PetrinexVolumetricsClient
        
        mock_spark = MagicMock()
        # Even if we pass data_type, it should be forced to "Vol"
        client = PetrinexVolumetricsClient(spark=mock_spark, data_type="NGL")
        
        assert client.data_type == "Vol"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

