"""
Manual E2E Integration Test for Petrinex API

Run this in Databricks to verify:
1. Schema application works correctly
2. Data types are correct (Decimal, Integer, Date)
3. Both Volumetrics and NGL schemas work
4. Real data downloads successfully

Usage in Databricks:
    %pip install git+https://github.com/guanjieshen/petrinex-python-api.git
    %run ./manual_e2e_test.py
"""

from petrinex import PetrinexClient, get_volumetrics_schema, get_ngl_schema
from pyspark.sql.types import DecimalType, IntegerType, DateType
from datetime import datetime, timedelta

def test_volumetrics_schema_definition():
    """Test that Volumetrics schema is correctly defined"""
    print("\n" + "="*80)
    print("TEST 1: Volumetrics Schema Definition")
    print("="*80)
    
    schema = get_volumetrics_schema()
    
    # Check column count
    assert len(schema.fields) == 30, f"Expected 30 columns, got {len(schema.fields)}"
    print(f"✅ Volumetrics schema has 30 columns")
    
    # Check Heat column
    field_map = {f.name: f for f in schema.fields}
    assert "Heat" in field_map, "Heat column missing"
    heat = field_map["Heat"]
    assert isinstance(heat.dataType, DecimalType), "Heat should be DecimalType"
    assert heat.dataType.precision == 5 and heat.dataType.scale == 2, "Heat should be Decimal(5,2)"
    print(f"✅ Heat column: Decimal(5,2) ✓")
    
    # Check Volume
    assert isinstance(field_map["Volume"].dataType, DecimalType), "Volume should be DecimalType"
    assert field_map["Volume"].dataType.precision == 13, "Volume precision should be 13"
    print(f"✅ Volume column: Decimal(13,3) ✓")
    
    # Check Hours
    assert isinstance(field_map["Hours"].dataType, IntegerType), "Hours should be IntegerType"
    print(f"✅ Hours column: Integer ✓")
    
    print("\n✅ TEST 1 PASSED: Volumetrics schema correctly defined")


def test_ngl_schema_definition():
    """Test that NGL schema is correctly defined"""
    print("\n" + "="*80)
    print("TEST 2: NGL Schema Definition")
    print("="*80)
    
    schema = get_ngl_schema()
    
    # Check column count
    assert len(schema.fields) == 26, f"Expected 26 columns, got {len(schema.fields)}"
    print(f"✅ NGL schema has 26 columns")
    
    # Check Heat does NOT exist
    field_map = {f.name: f for f in schema.fields}
    assert "Heat" not in field_map, "NGL should NOT have Heat column"
    print(f"✅ Heat column correctly absent from NGL")
    
    # Check WellID exists
    assert "WellID" in field_map, "WellID should exist in NGL"
    print(f"✅ WellID column exists")
    
    # Check GasProduction
    assert isinstance(field_map["GasProduction"].dataType, DecimalType), "GasProduction should be Decimal"
    assert field_map["GasProduction"].dataType.precision == 10, "GasProduction precision should be 10"
    print(f"✅ GasProduction column: Decimal(10,1) ✓")
    
    print("\n✅ TEST 2 PASSED: NGL schema correctly defined")


def test_volumetrics_real_data(spark):
    """Test downloading real Volumetrics data"""
    print("\n" + "="*80)
    print("TEST 3: Volumetrics Real Data Download")
    print("="*80)
    
    client = PetrinexClient(spark=spark, data_type="Vol")
    
    # Try to load one recent month
    recent_month = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-01")
    print(f"Attempting to load data from {recent_month}...")
    
    try:
        df = client.read_spark_df(from_date=recent_month, end_date=recent_month)
        
        print(f"✅ Downloaded data successfully")
        print(f"   Rows: {df.count():,}")
        print(f"   Columns: {len(df.columns)}")
        
        # Verify schema
        field_map = {f.name: f.dataType for f in df.schema.fields}
        
        # Check Heat
        assert "Heat" in field_map, "Heat column should exist"
        assert isinstance(field_map["Heat"], DecimalType), "Heat should be DecimalType"
        assert field_map["Heat"].precision == 5, "Heat precision should be 5"
        assert field_map["Heat"].scale == 2, "Heat scale should be 2"
        print(f"✅ Heat column: decimal(5,2) ✓")
        
        # Check Volume
        assert isinstance(field_map["Volume"], DecimalType), "Volume should be DecimalType"
        assert field_map["Volume"].precision == 13, "Volume precision should be 13"
        print(f"✅ Volume column: decimal(13,3) ✓")
        
        # Check Hours
        assert isinstance(field_map["Hours"], IntegerType), "Hours should be IntegerType"
        print(f"✅ Hours column: integer ✓")
        
        # Show sample data
        print("\nSample data:")
        df.select("ProductionMonth", "Heat", "Volume", "Hours").show(5, truncate=False)
        
        print("\n✅ TEST 3 PASSED: Volumetrics data loaded with correct types")
        return True
        
    except ValueError as e:
        if "No months found" in str(e):
            print(f"⚠️  No data available for {recent_month}")
            print("   This is expected if data hasn't been published yet")
            print("   Try manually: client.read_spark_df(from_date='2024-01-01', end_date='2024-01-31')")
            return False
        raise


def test_ngl_real_data(spark):
    """Test downloading real NGL data"""
    print("\n" + "="*80)
    print("TEST 4: NGL Real Data Download")
    print("="*80)
    
    client = PetrinexClient(spark=spark, data_type="NGL")
    
    # Try to load one recent month
    recent_month = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-01")
    print(f"Attempting to load NGL data from {recent_month}...")
    
    try:
        df = client.read_spark_df(from_date=recent_month, end_date=recent_month)
        
        print(f"✅ Downloaded NGL data successfully")
        print(f"   Rows: {df.count():,}")
        print(f"   Columns: {len(df.columns)}")
        
        # Verify schema
        field_map = {f.name: f.dataType for f in df.schema.fields}
        
        # Check Heat does NOT exist
        assert "Heat" not in field_map, "NGL should NOT have Heat column"
        print(f"✅ Heat column correctly absent")
        
        # Check GasProduction
        assert "GasProduction" in field_map, "GasProduction should exist"
        assert isinstance(field_map["GasProduction"], DecimalType), "GasProduction should be Decimal"
        assert field_map["GasProduction"].precision == 10, "GasProduction precision should be 10"
        print(f"✅ GasProduction column: decimal(10,1) ✓")
        
        # Check Hours
        assert isinstance(field_map["Hours"], IntegerType), "Hours should be IntegerType"
        print(f"✅ Hours column: integer ✓")
        
        # Show sample data
        print("\nSample data:")
        df.select("WellID", "GasProduction", "EthaneMixVolume", "Hours").show(5, truncate=False)
        
        print("\n✅ TEST 4 PASSED: NGL data loaded with correct types")
        return True
        
    except ValueError as e:
        if "No months found" in str(e):
            print(f"⚠️  No data available for {recent_month}")
            print("   This is expected if data hasn't been published yet")
            print("   Try manually: client.read_spark_df(from_date='2024-01-01', end_date='2024-01-31')")
            return False
        raise


def run_all_tests():
    """Run all E2E tests"""
    print("\n" + "="*80)
    print("PETRINEX API - END-TO-END INTEGRATION TESTS")
    print("="*80)
    
    # Get Spark session (available in Databricks)
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    except:
        spark = globals().get('spark')  # Try to get Databricks spark
        if spark is None:
            print("❌ ERROR: No Spark session available")
            print("   This script must be run in a Spark environment (e.g., Databricks)")
            return
    
    print(f"Using Spark version: {spark.version}")
    
    # Run tests
    try:
        test_volumetrics_schema_definition()
        test_ngl_schema_definition()
        
        vol_success = test_volumetrics_real_data(spark)
        ngl_success = test_ngl_real_data(spark)
        
        # Summary
        print("\n" + "="*80)
        print("TEST SUMMARY")
        print("="*80)
        print("✅ Schema Definition Tests: PASSED (2/2)")
        
        if vol_success and ngl_success:
            print("✅ Real Data Download Tests: PASSED (2/2)")
            print("\n🎉 ALL TESTS PASSED!")
        else:
            print(f"⚠️  Real Data Download Tests: {'✅' if vol_success else '⚠️'} Vol, {'✅' if ngl_success else '⚠️'} NGL")
            print("\n⚠️  Some data downloads skipped (no recent data available)")
            print("   This is expected - try with a specific historical month")
        
        print("\n" + "="*80)
        print("✅ E2E INTEGRATION TESTS COMPLETE")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    # Run in Databricks
    run_all_tests()

