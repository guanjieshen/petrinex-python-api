"""
Integration tests for Petrinex API (requires network access)

Run with: pytest tests/test_integration.py -v
Skip with: pytest tests/ -v -m "not integration"
"""

import pytest
import requests
from datetime import datetime, timedelta

from petrinex.client import SUPPORTED_DATA_TYPES


@pytest.mark.integration
class TestPetrinexAPIIntegration:
    """Integration tests that hit real Petrinex API"""
    
    def test_vol_url_accessible(self):
        """Test that Vol URLs are accessible"""
        # Try a recent month
        ym = (datetime.now() - timedelta(days=60)).strftime("%Y-%m")
        url = f"https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/{ym}/CSV"
        
        response = requests.get(url, timeout=10)
        
        # Should be 200 (found) or 404 (not published yet)
        assert response.status_code in [200, 404]
        
        if response.status_code == 200:
            # Should be a ZIP file
            assert response.content[:4] == b'PK\x03\x04', "Expected ZIP file"
            assert len(response.content) > 1000, "File too small"
    
    def test_ngl_url_accessible(self):
        """Test that NGL URLs are accessible"""
        # Try a recent month
        ym = (datetime.now() - timedelta(days=60)).strftime("%Y-%m")
        url = f"https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/NGL/{ym}/CSV"
        
        response = requests.get(url, timeout=10)
        
        # Should be 200 (found) or 404 (not published yet)
        assert response.status_code in [200, 404]
        
        if response.status_code == 200:
            # Should be a ZIP file
            assert response.content[:4] == b'PK\x03\x04', "Expected ZIP file"
            assert len(response.content) > 1000, "File too small"
    
    def test_publicdata_page_accessible(self):
        """Test that Petrinex PublicData page is accessible"""
        url = "https://www.petrinex.gov.ab.ca/PublicData?Jurisdiction=AB"
        
        response = requests.get(url, timeout=10)
        
        assert response.status_code == 200
        assert "Petrinex" in response.text or "PublicData" in response.text


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"])

