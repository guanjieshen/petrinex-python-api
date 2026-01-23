"""
Tests for schema definitions and data type correctness

These tests verify that:
1. Official schemas are correctly defined with all columns
2. All columns have explicit types (DecimalType, IntegerType, StringType, DateType)
3. No VoidType can occur because all columns are explicitly typed
4. Both Volumetrics and NGL schemas match PDF specifications
5. Real data downloads work correctly with proper types
"""

import pytest
from pyspark.sql.types import (
    StructType, StringType, DecimalType, IntegerType, DateType
)

from petrinex.schema import (
    get_volumetrics_schema,
    get_ngl_schema,
    get_schema_for_data_type,
)


class TestSchemaDefinitions:
    """Test schema structure and field definitions"""
    
    def test_volumetrics_schema_structure(self):
        """Test Volumetrics schema has 30 columns"""
        schema = get_volumetrics_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 30
    
    def test_volumetrics_schema_has_heat_column(self):
        """Test that Heat column exists with correct type"""
        schema = get_volumetrics_schema()
        
        heat_field = [f for f in schema.fields if f.name == "Heat"]
        assert len(heat_field) == 1
        
        heat_field = heat_field[0]
        assert isinstance(heat_field.dataType, DecimalType)
        assert heat_field.dataType.precision == 5
        assert heat_field.dataType.scale == 2
    
    def test_volumetrics_schema_key_columns(self):
        """Test that key Volumetrics columns exist with correct types"""
        schema = get_volumetrics_schema()
        field_map = {f.name: f for f in schema.fields}
        
        # Test string columns
        assert field_map["ProductionMonth"].dataType == StringType()
        assert field_map["OperatorBAID"].dataType == StringType()
        assert field_map["ActivityID"].dataType == StringType()
        assert field_map["ProductID"].dataType == StringType()
        
        # Test numeric columns
        assert isinstance(field_map["Volume"].dataType, DecimalType)
        assert field_map["Volume"].dataType.precision == 13
        assert field_map["Volume"].dataType.scale == 3
        
        assert isinstance(field_map["Energy"].dataType, DecimalType)
        assert field_map["Energy"].dataType.precision == 13
        assert field_map["Energy"].dataType.scale == 3
        
        assert isinstance(field_map["Hours"].dataType, IntegerType)
        
        assert isinstance(field_map["ProrationFactor"].dataType, DecimalType)
        assert field_map["ProrationFactor"].dataType.precision == 6
        assert field_map["ProrationFactor"].dataType.scale == 5
        
        # Test date column
        assert isinstance(field_map["SubmissionDate"].dataType, DateType)
    
    def test_ngl_schema_structure(self):
        """Test NGL schema has 26 columns"""
        schema = get_ngl_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 26
    
    def test_ngl_schema_no_heat_column(self):
        """Test that NGL schema does NOT have Heat column"""
        schema = get_ngl_schema()
        
        heat_fields = [f for f in schema.fields if f.name == "Heat"]
        assert len(heat_fields) == 0
    
    def test_ngl_schema_key_columns(self):
        """Test that key NGL columns exist with correct types"""
        schema = get_ngl_schema()
        field_map = {f.name: f for f in schema.fields}
        
        # Test infrastructure columns
        assert field_map["FacilityID"].dataType == StringType()
        assert field_map["WellID"].dataType == StringType()
        assert field_map["Field"].dataType == StringType()
        assert field_map["Pool"].dataType == StringType()
        
        # Test production columns (all Decimal 10,1)
        assert isinstance(field_map["GasProduction"].dataType, DecimalType)
        assert field_map["GasProduction"].dataType.precision == 10
        assert field_map["GasProduction"].dataType.scale == 1
        
        assert isinstance(field_map["OilProduction"].dataType, DecimalType)
        assert isinstance(field_map["CondensateProduction"].dataType, DecimalType)
        assert isinstance(field_map["WaterProduction"].dataType, DecimalType)
        
        # Test allocation columns
        assert isinstance(field_map["ResidueGasVolume"].dataType, DecimalType)
        assert isinstance(field_map["Energy"].dataType, DecimalType)
        
        # Test NGL product columns
        assert isinstance(field_map["EthaneMixVolume"].dataType, DecimalType)
        assert isinstance(field_map["PropaneSpecVolume"].dataType, DecimalType)
        assert isinstance(field_map["ButaneMixVolume"].dataType, DecimalType)
        assert isinstance(field_map["PentaneSpecVolume"].dataType, DecimalType)
        assert isinstance(field_map["LiteMixVolume"].dataType, DecimalType)
        
        # Test Hours is Integer
        assert isinstance(field_map["Hours"].dataType, IntegerType)
    
    def test_get_schema_for_data_type_vol(self):
        """Test get_schema_for_data_type returns correct schema for Vol"""
        schema = get_schema_for_data_type("Vol")
        
        assert len(schema.fields) == 30
        assert any(f.name == "Heat" for f in schema.fields)
    
    def test_get_schema_for_data_type_ngl(self):
        """Test get_schema_for_data_type returns correct schema for NGL"""
        schema = get_schema_for_data_type("NGL")
        
        assert len(schema.fields) == 26
        assert any(f.name == "WellID" for f in schema.fields)
    
    def test_get_schema_for_data_type_invalid(self):
        """Test get_schema_for_data_type raises error for invalid type"""
        with pytest.raises(ValueError, match="Unsupported data_type"):
            get_schema_for_data_type("INVALID")
    
    def test_volumetrics_ngl_schemas_are_different(self):
        """Test that Volumetrics and NGL schemas are completely different"""
        vol_schema = get_volumetrics_schema()
        ngl_schema = get_ngl_schema()
        
        # Different number of columns
        assert len(vol_schema.fields) != len(ngl_schema.fields)
        
        # Different column sets
        vol_columns = {f.name for f in vol_schema.fields}
        ngl_columns = {f.name for f in ngl_schema.fields}
        
        # Vol has columns NGL doesn't
        assert "Heat" in vol_columns and "Heat" not in ngl_columns
        assert "ActivityID" in vol_columns and "ActivityID" not in ngl_columns
        assert "ProductID" in vol_columns and "ProductID" not in ngl_columns
        
        # NGL has columns Vol doesn't
        assert "WellID" in ngl_columns and "WellID" not in vol_columns
        assert "GasProduction" in ngl_columns and "GasProduction" not in vol_columns
        assert "EthaneMixVolume" in ngl_columns and "EthaneMixVolume" not in vol_columns


class TestSchemaApplication:
    """Test that schemas are correctly structured"""
    
    def test_all_volumetrics_columns_have_types(self):
        """Test that all Volumetrics columns have explicit types (no VoidType possible)"""
        schema = get_volumetrics_schema()
        
        # Verify every field has a concrete type
        for field in schema.fields:
            assert field.dataType is not None
            assert not isinstance(field.dataType, VoidType)
            # All types should be StringType, DecimalType, IntegerType, or DateType
            assert isinstance(field.dataType, (StringType, DecimalType, IntegerType, DateType))
    
    def test_all_ngl_columns_have_types(self):
        """Test that all NGL columns have explicit types (no VoidType possible)"""
        schema = get_ngl_schema()
        
        # Verify every field has a concrete type
        for field in schema.fields:
            assert field.dataType is not None
            assert not isinstance(field.dataType, VoidType)
            # All types should be StringType, DecimalType, IntegerType, or DateType
            assert isinstance(field.dataType, (StringType, DecimalType, IntegerType, DateType))
    
    def test_ngl_schema_all_decimals_correct(self):
        """Test that NGL schema has correct Decimal types for all numeric columns"""
        schema = get_ngl_schema()
        
        # All production and allocation volumes should be Decimal(10,1)
        numeric_columns = [
            "GasProduction", "OilProduction", "CondensateProduction", 
            "WaterProduction", "ResidueGasVolume", "Energy",
            "EthaneMixVolume", "EthaneSpecVolume",
            "PropaneMixVolume", "PropaneSpecVolume",
            "ButaneMixVolume", "ButaneSpecVolume",
            "PentaneMixVolume", "PentaneSpecVolume",
            "LiteMixVolume"
        ]
        
        field_map = {f.name: f for f in schema.fields}
        
        for col in numeric_columns:
            assert col in field_map, f"Column {col} missing from schema"
            field = field_map[col]
            assert isinstance(field.dataType, DecimalType), f"{col} should be DecimalType"
            assert field.dataType.precision == 10, f"{col} precision should be 10"
            assert field.dataType.scale == 1, f"{col} scale should be 1"


@pytest.mark.integration
class TestSchemaWithRealData:
    """Integration tests using real Petrinex data (requires Spark and network)"""
    
    def test_volumetrics_real_download_and_schema(self):
        """Test downloading real Vol data and verifying correct data types"""
        pytest.skip("Requires Spark session and network - run manually if needed")
        
        from pyspark.sql import SparkSession
        from petrinex import PetrinexClient
        from datetime import datetime, timedelta
        
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        
        try:
            client = PetrinexClient(spark=spark, data_type="Vol")
            
            # Try to load one recent month
            recent_month = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-01")
            
            try:
                df = client.read_spark_df(from_date=recent_month, end_date=recent_month)
                
                # Verify schema has correct types
                field_map = {f.name: f.dataType for f in df.schema.fields}
                
                # Check key columns exist with correct types
                assert "Heat" in field_map
                assert isinstance(field_map["Heat"], DecimalType)
                assert field_map["Heat"].precision == 5
                assert field_map["Heat"].scale == 2
                
                assert "Volume" in field_map
                assert isinstance(field_map["Volume"], DecimalType)
                assert field_map["Volume"].precision == 13
                assert field_map["Volume"].scale == 3
                
                assert "Hours" in field_map
                assert isinstance(field_map["Hours"], IntegerType)
                
                # All columns should have explicit types (no VoidType)
                for field in df.schema.fields:
                    assert not isinstance(field.dataType, VoidType), \
                        f"Column {field.name} has VoidType (should never happen with explicit schema)"
                
                print(f"✅ Successfully loaded Vol data with correct schema")
                print(f"   Rows: {df.count()}")
                print(f"   Columns: {len(df.columns)}")
                
            except ValueError as e:
                if "No months found" in str(e):
                    pytest.skip(f"No data available for {recent_month}")
                raise
        
        finally:
            spark.stop()
    
    def test_ngl_real_download_and_schema(self):
        """Test downloading real NGL data and verifying correct data types"""
        pytest.skip("Requires Spark session and network - run manually if needed")
        
        from pyspark.sql import SparkSession
        from petrinex import PetrinexClient
        from datetime import datetime, timedelta
        
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        
        try:
            client = PetrinexClient(spark=spark, data_type="NGL")
            
            # Try to load one recent month
            recent_month = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-01")
            
            try:
                df = client.read_spark_df(from_date=recent_month, end_date=recent_month)
                
                # Verify schema has correct types
                field_map = {f.name: f.dataType for f in df.schema.fields}
                
                # Check key NGL columns exist with correct types
                assert "GasProduction" in field_map
                assert isinstance(field_map["GasProduction"], DecimalType)
                assert field_map["GasProduction"].precision == 10
                assert field_map["GasProduction"].scale == 1
                
                assert "EthaneMixVolume" in field_map
                assert isinstance(field_map["EthaneMixVolume"], DecimalType)
                
                assert "Hours" in field_map
                assert isinstance(field_map["Hours"], IntegerType)
                
                # All columns should have explicit types (no VoidType)
                for field in df.schema.fields:
                    assert not isinstance(field.dataType, VoidType), \
                        f"Column {field.name} has VoidType (should never happen with explicit schema)"
                
                # Verify Heat does NOT exist in NGL data
                assert "Heat" not in field_map, "NGL schema should not have Heat column"
                
                print(f"✅ Successfully loaded NGL data with correct schema")
                print(f"   Rows: {df.count()}")
                print(f"   Columns: {len(df.columns)}")
                
            except ValueError as e:
                if "No months found" in str(e):
                    pytest.skip(f"No data available for {recent_month}")
                raise
        
        finally:
            spark.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

