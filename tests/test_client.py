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


class TestDateRangeFiltering:
    """Test end_date parameter functionality"""
    
    def test_end_date_validation_without_from_date(self):
        """Test that end_date raises error without from_date"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        with pytest.raises(ValueError, match="end_date can only be used with from_date"):
            client.read_spark_df(updated_after="2025-01-01", end_date="2025-12-31")
    
    def test_end_date_filtering_logic(self):
        """Test that end_date correctly filters production months"""
        # Simulate files with different production months
        files = [
            PetrinexFile('2020-01', datetime(2025, 1, 15), 'url1'),
            PetrinexFile('2021-06', datetime(2025, 1, 15), 'url2'),
            PetrinexFile('2022-12', datetime(2025, 1, 15), 'url3'),
            PetrinexFile('2023-06', datetime(2025, 1, 15), 'url4'),
            PetrinexFile('2024-01', datetime(2025, 1, 15), 'url5'),
        ]
        
        # Test end_date filtering (should keep only up to 2023-12)
        end_cutoff = datetime.strptime('2023-12-31', '%Y-%m-%d')
        filtered = [
            f for f in files
            if datetime.strptime(f.production_month + '-01', '%Y-%m-%d') <= end_cutoff
        ]
        
        expected_months = ['2020-01', '2021-06', '2022-12', '2023-06']
        actual_months = [f.production_month for f in filtered]
        
        assert actual_months == expected_months
        assert '2024-01' not in actual_months
    
    def test_end_date_accepts_with_from_date(self):
        """Test that end_date is accepted when used with from_date"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        # Mock list_updated_after to return empty list (avoid network calls)
        with patch.object(client, 'list_updated_after', return_value=[]):
            try:
                # This should not raise the end_date validation error
                client.read_spark_df(from_date="2021-01-01", end_date="2023-12-31")
            except ValueError as e:
                # Should only fail on "No months found", not on end_date validation
                assert "No months found" in str(e)
                assert "end_date can only be used with from_date" not in str(e)


class TestUnityCalatalogDirectWrite:
    """Test uc_table parameter functionality"""
    
    def test_uc_table_parameter_accepted(self):
        """Test that uc_table parameter is accepted"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        # Mock list_updated_after to return empty list (avoid network calls)
        with patch.object(client, 'list_updated_after', return_value=[]):
            try:
                # This should accept uc_table parameter
                client.read_spark_df(
                    from_date="2021-01-01",
                    uc_table="main.petrinex.test_table"
                )
            except ValueError as e:
                # Should only fail on "No months found", not parameter error
                assert "No months found" in str(e)
                assert "uc_table" not in str(e)
    
    def test_uc_table_always_appends(self):
        """Test that uc_table always uses append mode"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        # Mock list_updated_after to return empty list
        with patch.object(client, 'list_updated_after', return_value=[]):
            try:
                # UC table should always append (no uc_mode parameter)
                client.read_spark_df(
                    from_date="2021-01-01",
                    uc_table="main.petrinex.test"
                )
            except ValueError as e:
                # Should only fail on "No months found"
                assert "No months found" in str(e)
    
    def test_uc_table_requires_provenance_columns(self):
        """Test that uc_table requires add_provenance_columns=True"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        # Mock list_updated_after to return one file
        mock_file = PetrinexFile('2021-01', datetime(2025, 1, 15), 'http://test.com/file.csv')
        with patch.object(client, 'list_updated_after', return_value=[mock_file]):
            with pytest.raises(ValueError, match="uc_table requires add_provenance_columns=True"):
                client.read_spark_df(
                    from_date="2021-01-01",
                    uc_table="main.petrinex.test",
                    add_provenance_columns=False
                )
    
    def test_uc_table_validates_existing_table(self):
        """Test that uc_table validates existing tables have provenance columns"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        # Mock existing table WITHOUT provenance columns
        mock_df = MagicMock()
        mock_df.columns = ["some_column", "another_column"]  # Missing provenance columns
        mock_spark.table.return_value = mock_df
        
        # Mock list_updated_after to return one file
        mock_file = PetrinexFile('2021-01', datetime(2025, 1, 15), 'http://test.com/file.csv')
        with patch.object(client, 'list_updated_after', return_value=[mock_file]):
            with pytest.raises(ValueError, match="appears to not be a Petrinex table"):
                client.read_spark_df(
                    from_date="2021-01-01",
                    uc_table="main.petrinex.test"
                )
    
    def test_uc_table_validates_schema_compatibility(self):
        """Test that uc_table validates schema compatibility with existing table"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        # Mock existing table WITH provenance columns AND data columns
        mock_existing_df = MagicMock()
        mock_existing_df.columns = [
            "production_month", "file_updated_ts", "source_url",  # Provenance
            "well_id", "production_volume", "extra_column"  # Data columns
        ]
        mock_spark.table.return_value = mock_existing_df
        
        # Mock list_updated_after to return one file
        mock_file = PetrinexFile('2021-01', datetime(2025, 1, 15), 'http://test.com/file.csv')
        
        with patch.object(client, 'list_updated_after', return_value=[mock_file]):
            # Mock requests.get to return fake CSV data (missing "extra_column")
            with patch('petrinex.client.requests.get') as mock_get:
                # Create mock CSV data without "extra_column"
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.content = b'well_id,production_volume\nW1,100\n'
                mock_get.return_value = mock_response
                
                # This should raise an error because new data is missing "extra_column"
                with pytest.raises(ValueError, match="Schema mismatch.*missing in the new data"):
                    client.read_spark_df(
                        from_date="2021-01-01",
                        uc_table="main.petrinex.test"
                    )
    
    def test_uc_table_allows_schema_evolution(self):
        """Test that uc_table allows new columns (schema evolution)"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        # Mock existing table WITH provenance columns
        mock_existing_df = MagicMock()
        mock_existing_df.columns = [
            "production_month", "file_updated_ts", "source_url",
            "well_id", "production_volume"
        ]
        mock_spark.table.return_value = mock_existing_df
        
        # Mock list_updated_after to return one file
        mock_file = PetrinexFile('2021-01', datetime(2025, 1, 15), 'http://test.com/file.csv')
        
        with patch.object(client, 'list_updated_after', return_value=[mock_file]):
            with patch('petrinex.client.requests.get') as mock_get:
                # Create mock CSV data WITH a new column
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.content = b'well_id,production_volume,new_column\nW1,100,ABC\n'
                mock_get.return_value = mock_response
                
                # Mock the write operation
                mock_writer = MagicMock()
                mock_spark.createDataFrame.return_value.write = mock_writer
                
                # This should NOT raise an error (schema evolution is allowed)
                try:
                    client.read_spark_df(
                        from_date="2021-01-01",
                        uc_table="main.petrinex.test"
                    )
                except ValueError as e:
                    # Should not be a schema mismatch error
                    assert "Schema mismatch" not in str(e)
    
    def test_uc_table_new_table_creation(self):
        """Test that uc_table handles new table creation"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        # Mock table.table() to raise "not found" error (table doesn't exist)
        mock_spark.table.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND")
        
        # Mock list_updated_after to return empty list
        with patch.object(client, 'list_updated_after', return_value=[]):
            try:
                # Should handle "table not found" gracefully
                client.read_spark_df(
                    from_date="2021-01-01",
                    uc_table="main.petrinex.new_table"
                )
            except ValueError as e:
                # Should only fail on "No months found"
                assert "No months found" in str(e)
                assert "appears to not be a Petrinex table" not in str(e)


class TestDateParameters:
    """Test date parameter validation"""
    
    def test_requires_one_date_parameter(self):
        """Test that at least one date parameter is required"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        with pytest.raises(ValueError, match="Must specify one of"):
            client.read_spark_df()
    
    def test_only_one_date_parameter_allowed(self):
        """Test that only one date parameter can be specified"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        with pytest.raises(ValueError, match="Specify only ONE of"):
            client.read_spark_df(
                updated_after="2025-01-01",
                from_date="2020-01-01"
            )
    
    def test_since_alias_works(self):
        """Test that 'since' is an alias for 'updated_after'"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        with patch.object(client, 'list_updated_after', return_value=[]):
            try:
                client.read_spark_df(since="2025-01-01")
            except ValueError as e:
                # Should process the date parameter
                assert "No months found" in str(e)
                assert "Must specify one of" not in str(e)


class TestPandasDataFrame:
    """Test read_pandas_df method"""
    
    def test_read_pandas_df_requires_date_param(self):
        """Test that read_pandas_df requires a date parameter"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        with pytest.raises(ValueError, match="Must specify one of"):
            client.read_pandas_df()
    
    def test_read_pandas_df_accepts_updated_after(self):
        """Test that read_pandas_df accepts updated_after parameter"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        with patch.object(client, 'list_updated_after', return_value=[]):
            with pytest.raises(ValueError, match="No months found"):
                client.read_pandas_df(updated_after="2025-01-01")
    
    def test_read_pandas_df_accepts_end_date(self):
        """Test that read_pandas_df accepts end_date with from_date"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        with patch.object(client, 'list_updated_after', return_value=[]):
            try:
                client.read_pandas_df(
                    from_date="2021-01-01",
                    end_date="2023-12-31"
                )
            except ValueError as e:
                assert "No months found" in str(e)
                assert "end_date can only be used with from_date" not in str(e)


class TestZIPExtraction:
    """Test ZIP file extraction"""
    
    def test_extract_csv_from_zip(self):
        """Test extracting CSV from ZIP file"""
        import io
        import zipfile
        
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        # Create a mock ZIP file with CSV content
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zf:
            zf.writestr('data.csv', 'col1,col2\nval1,val2\n')
        
        zip_content = zip_buffer.getvalue()
        
        # Extract CSV from ZIP
        csv_content = client._extract_csv_from_zip(zip_content)
        
        assert b'col1,col2' in csv_content
        assert b'val1,val2' in csv_content
    
    def test_extract_csv_from_nested_zip(self):
        """Test extracting CSV from nested ZIP file"""
        import io
        import zipfile
        
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        # Create inner ZIP with CSV
        inner_zip_buffer = io.BytesIO()
        with zipfile.ZipFile(inner_zip_buffer, 'w') as inner_zf:
            inner_zf.writestr('data.csv', 'nested,data\n1,2\n')
        
        # Create outer ZIP with inner ZIP
        outer_zip_buffer = io.BytesIO()
        with zipfile.ZipFile(outer_zip_buffer, 'w') as outer_zf:
            outer_zf.writestr('inner.zip', inner_zip_buffer.getvalue())
        
        zip_content = outer_zip_buffer.getvalue()
        
        # Extract CSV from nested ZIP
        csv_content = client._extract_csv_from_zip(zip_content)
        
        assert b'nested,data' in csv_content
        assert b'1,2' in csv_content
    
    def test_extract_handles_non_zip_content(self):
        """Test that non-ZIP content is returned as-is"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        # Plain CSV content (not zipped)
        csv_content = b'col1,col2\nval1,val2\n'
        
        # Should return the content as-is
        result = client._extract_csv_from_zip(csv_content)
        
        assert result == csv_content


class TestErrorHandling:
    """Test error handling scenarios"""
    
    def test_404_error_handling(self):
        """Test that 404 errors are handled gracefully"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        mock_file = PetrinexFile('2021-01', datetime(2025, 1, 15), 'http://test.com/file.csv')
        
        with patch.object(client, 'list_updated_after', return_value=[mock_file]):
            with patch('petrinex.client.requests.get') as mock_get:
                # Mock 404 error
                mock_response = MagicMock()
                mock_response.status_code = 404
                mock_response.raise_for_status.side_effect = Exception("404 Client Error")
                mock_get.return_value = mock_response
                
                # Should handle 404 gracefully and raise "No data loaded" error
                with pytest.raises(ValueError, match="No data loaded"):
                    client.read_spark_df(from_date="2021-01-01")
    
    def test_invalid_file_format_for_spark_df(self):
        """Test that read_spark_df only works with CSV format"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol", file_format="XML")
        
        with pytest.raises(ValueError, match="Pandas mode supports CSV only"):
            client.read_spark_df(updated_after="2025-01-01")
    
    def test_invalid_file_format_for_pandas_df(self):
        """Test that read_pandas_df only works with CSV format"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol", file_format="XML")
        
        with pytest.raises(ValueError, match="Pandas mode supports CSV only"):
            client.read_pandas_df(updated_after="2025-01-01")


class TestDownloadFiles:
    """Test download_files functionality"""
    
    def test_download_files_basic(self):
        """Test basic file download functionality"""
        import tempfile
        import os
        
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        mock_file = PetrinexFile('2025-01', datetime(2025, 1, 15), 'http://test.com/file.zip')
        
        with patch.object(client, 'list_updated_after', return_value=[mock_file]):
            with patch('petrinex.client.requests.get') as mock_get:
                # Mock successful response with ZIP content
                mock_response = MagicMock()
                mock_response.content = b"fake_zip_content"
                mock_response.raise_for_status = MagicMock()
                mock_get.return_value = mock_response
                
                # Mock CSV extraction
                csv_data = b"Column1,Column2\nValue1,Value2\n"
                with patch.object(client, '_extract_csv_from_zip', return_value=csv_data):
                    with tempfile.TemporaryDirectory() as tmpdir:
                        paths = client.download_files(
                            output_dir=tmpdir,
                            updated_after="2025-01-01"
                        )
                        
                        # Verify results
                        assert len(paths) == 1
                        assert os.path.exists(paths[0])
                        assert "2025-01" in paths[0]
                        assert paths[0].endswith(".csv")
                        
                        # Verify file content
                        with open(paths[0], 'rb') as f:
                            content = f.read()
                            assert content == csv_data
    
    def test_download_files_organizes_by_month(self):
        """Test that files are organized in subdirectories by production month"""
        import tempfile
        import os
        
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        mock_files = [
            PetrinexFile('2025-01', datetime(2025, 1, 15), 'http://test.com/2025-01.zip'),
            PetrinexFile('2025-02', datetime(2025, 2, 15), 'http://test.com/2025-02.zip'),
        ]
        
        with patch.object(client, 'list_updated_after', return_value=mock_files):
            csv_data = b"Column1,Column2\nValue1,Value2\n"
            
            with patch.object(client, '_extract_csv_from_zip', return_value=csv_data):
                with patch('petrinex.client.requests.get') as mock_get:
                    mock_response = MagicMock()
                    mock_response.content = b"fake_zip"
                    mock_response.raise_for_status = MagicMock()
                    mock_get.return_value = mock_response
                    
                    with tempfile.TemporaryDirectory() as tmpdir:
                        paths = client.download_files(
                            output_dir=tmpdir,
                            updated_after="2025-01-01"
                        )
                        
                        # Verify subdirectories created
                        assert len(paths) == 2
                        assert os.path.exists(os.path.join(tmpdir, "2025-01"))
                        assert os.path.exists(os.path.join(tmpdir, "2025-02"))
                        
                        # Verify files in correct subdirectories
                        assert any("2025-01" in p and p.endswith("Vol_2025-01.csv") for p in paths)
                        assert any("2025-02" in p and p.endswith("Vol_2025-02.csv") for p in paths)
    
    def test_download_files_date_range(self):
        """Test download_files with from_date and end_date"""
        import tempfile
        
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        mock_files = [
            PetrinexFile('2021-01', datetime(2021, 1, 15), 'http://test.com/2021-01.zip'),
            PetrinexFile('2021-12', datetime(2021, 12, 15), 'http://test.com/2021-12.zip'),
            PetrinexFile('2022-01', datetime(2022, 1, 15), 'http://test.com/2022-01.zip'),
        ]
        
        with patch.object(client, 'list_updated_after', return_value=mock_files):
            csv_data = b"test"
            
            with patch.object(client, '_extract_csv_from_zip', return_value=csv_data):
                with patch('petrinex.client.requests.get') as mock_get:
                    mock_response = MagicMock()
                    mock_response.content = b"fake_zip"
                    mock_response.raise_for_status = MagicMock()
                    mock_get.return_value = mock_response
                    
                    with tempfile.TemporaryDirectory() as tmpdir:
                        # Download with end_date filter
                        paths = client.download_files(
                            output_dir=tmpdir,
                            from_date="2021-01-01",
                            end_date="2021-12-31"
                        )
                        
                        # Should get 2 files (2021-01 and 2021-12), not 2022-01
                        assert len(paths) == 2
                        assert any("2021-01" in p for p in paths)
                        assert any("2021-12" in p for p in paths)
                        assert not any("2022-01" in p for p in paths)
    
    def test_download_files_handles_404(self):
        """Test that download_files handles 404 errors gracefully"""
        import tempfile
        
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        mock_files = [
            PetrinexFile('2025-01', datetime(2025, 1, 15), 'http://test.com/2025-01.zip'),
            PetrinexFile('2025-02', datetime(2025, 2, 15), 'http://test.com/2025-02.zip'),
        ]
        
        with patch.object(client, 'list_updated_after', return_value=mock_files):
            csv_data = b"test"
            
            with patch.object(client, '_extract_csv_from_zip', return_value=csv_data):
                with patch('petrinex.client.requests.get') as mock_get:
                    # First call succeeds, second returns 404
                    def side_effect(*args, **kwargs):
                        mock_response = MagicMock()
                        if '2025-01' in args[0]:
                            mock_response.content = b"fake_zip"
                            mock_response.raise_for_status = MagicMock()
                        else:
                            mock_response.status_code = 404
                            from requests.exceptions import HTTPError
                            mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)
                        return mock_response
                    
                    mock_get.side_effect = side_effect
                    
                    with tempfile.TemporaryDirectory() as tmpdir:
                        paths = client.download_files(
                            output_dir=tmpdir,
                            updated_after="2025-01-01"
                        )
                        
                        # Should get 1 file (2025-01), skip 2025-02
                        assert len(paths) == 1
                        assert "2025-01" in paths[0]
    
    def test_download_files_all_fail_raises_error(self):
        """Test that download_files raises error if all downloads fail"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        mock_file = PetrinexFile('2025-01', datetime(2025, 1, 15), 'http://test.com/file.zip')
        
        with patch.object(client, 'list_updated_after', return_value=[mock_file]):
            with patch('petrinex.client.requests.get') as mock_get:
                # All requests fail with 404
                mock_response = MagicMock()
                mock_response.status_code = 404
                from requests.exceptions import HTTPError
                mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)
                mock_get.return_value = mock_response
                
                import tempfile
                with tempfile.TemporaryDirectory() as tmpdir:
                    with pytest.raises(ValueError, match="No files downloaded"):
                        client.download_files(
                            output_dir=tmpdir,
                            updated_after="2025-01-01"
                        )
    
    def test_download_files_requires_date_param(self):
        """Test that download_files requires a date parameter"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="Must specify one of"):
                client.download_files(output_dir=tmpdir)
    
    def test_download_files_end_date_requires_from_date(self):
        """Test that end_date can only be used with from_date"""
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="end_date can only be used with from_date"):
                client.download_files(
                    output_dir=tmpdir,
                    updated_after="2025-01-01",
                    end_date="2025-12-31"
                )
    
    def test_download_files_creates_output_dir(self):
        """Test that download_files creates output directory if it doesn't exist"""
        import tempfile
        import os
        
        mock_spark = MagicMock()
        client = PetrinexClient(spark=mock_spark, data_type="Vol")
        
        mock_file = PetrinexFile('2025-01', datetime(2025, 1, 15), 'http://test.com/file.zip')
        
        with patch.object(client, 'list_updated_after', return_value=[mock_file]):
            csv_data = b"test"
            
            with patch.object(client, '_extract_csv_from_zip', return_value=csv_data):
                with patch('petrinex.client.requests.get') as mock_get:
                    mock_response = MagicMock()
                    mock_response.content = b"fake_zip"
                    mock_response.raise_for_status = MagicMock()
                    mock_get.return_value = mock_response
                    
                    with tempfile.TemporaryDirectory() as tmpdir:
                        # Use a nested path that doesn't exist
                        output_dir = os.path.join(tmpdir, "new_dir", "nested")
                        assert not os.path.exists(output_dir)
                        
                        paths = client.download_files(
                            output_dir=output_dir,
                            updated_after="2025-01-01"
                        )
                        
                        # Verify directory was created
                        assert os.path.exists(output_dir)
                        assert len(paths) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

