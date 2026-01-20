"""Petrinex data schemas based on official PDF documentation.

Copyright (c) 2026 Guanjie Shen

Schema definitions match the official Petrinex data specifications:
- Conventional Volumetrics: PD_Conventional_Volumetrics_Report.pdf (30 columns)
- NGL and Marketable Gas Volumes: PD_NGL_Marketable_Gas_Volumes_Report.pdf (26 columns)

Each schema is defined with explicit Spark types (Decimal, Integer, String, Date) to ensure
correct data types are applied when converting pandas DataFrames to Spark DataFrames.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    IntegerType,
    DateType,
)


def get_volumetrics_schema():
    """
    Returns the Spark schema for Petrinex Conventional Volumetrics data.
    
    Reference: PD_Conventional_Volumetrics_Report.pdf (Data Fields section, pages 6-7)
    
    This schema defines explicit types for all columns, ensuring correct data types
    are applied when converting pandas DataFrames to Spark DataFrames.
    
    Returns
    -------
    pyspark.sql.types.StructType
        Schema with 30 columns matching official Petrinex specification
    """
    return StructType([
        # Production Month - YearMonth (7)
        # Year month (YYYY-MM) for production month
        StructField("ProductionMonth", StringType(), True),
        
        # Operator BA ID - String (20)
        # Operator ID (Code) of the reporting facility for the production month
        StructField("OperatorBAID", StringType(), True),
        
        # Operator Name - String (150)
        # BA name of the reporting facility operator for the production month
        StructField("OperatorName", StringType(), True),
        
        # Reporting Facility ID - String (20)
        # Unique identifier of the reporting facility
        StructField("ReportingFacilityID", StringType(), True),
        
        # Reporting Facility Province/State - String (2)
        # Province/State for the Reporting Facility
        StructField("ReportingFacilityProvinceState", StringType(), True),
        
        # Reporting Facility Type - String (2)
        # Type for the Reporting Facility
        StructField("ReportingFacilityType", StringType(), True),
        
        # Reporting Facility Identifier - String (7)
        # Numeric component of the unique identifier for the Reporting Facility
        StructField("ReportingFacilityIdentifier", StringType(), True),
        
        # Reporting Facility Name - String (60)
        # Facility Name of the reporting facility
        StructField("ReportingFacilityName", StringType(), True),
        
        # Reporting Facility Subtype - String (3)
        # Subtype Code indicating purpose of facility
        StructField("ReportingFacilitySubType", StringType(), True),
        
        # Reporting Facility Subtype Desc - String (60)
        # Subtype description
        StructField("ReportingFacilitySubTypeDesc", StringType(), True),
        
        # Reporting Facility Location - String (20)
        # Facility Location is made up of: legal subdivision-section-township-range-meridian
        StructField("ReportingFacilityLocation", StringType(), True),
        
        # Facility Legal Subdivision - String (2)
        # The DLS Legal Subdivision designation for the location of a facility
        StructField("FacilityLegalSubdivision", StringType(), True),
        
        # Facility Section - String (2)
        # The DLS Section designation for the location of a facility
        StructField("FacilitySection", StringType(), True),
        
        # Facility Township - String (3)
        # The DLS Township designation for the location of a facility
        StructField("FacilityTownship", StringType(), True),
        
        # Facility Range - String (2)
        # The DLS Range designation for the location of a well
        StructField("FacilityRange", StringType(), True),
        
        # Facility Meridian - String (2)
        # The DLS Meridian designation for the location of a facility
        StructField("FacilityMeridian", StringType(), True),
        
        # Submission Date - Date (10)
        # Last updated date (YYYY-MM-DD)
        StructField("SubmissionDate", DateType(), True),
        
        # Activity ID - String (12)
        # Activity code
        StructField("ActivityID", StringType(), True),
        
        # Product ID - String (12)
        # Product code
        StructField("ProductID", StringType(), True),
        
        # From/To ID - String (20)
        # Unique identifier of the From/To facility or well
        StructField("FromToID", StringType(), True),
        
        # From/To ID Province/State - String (2)
        # Province/State for the From/To ID
        StructField("FromToIDProvinceState", StringType(), True),
        
        # From/To ID Type - String (2)
        # Type for the From/To ID
        StructField("FromToIDType", StringType(), True),
        
        # From/To ID Identifier - String (16)
        # Numeric component of the unique identifier for the From/To ID
        StructField("FromToIDIdentifier", StringType(), True),
        
        # Volume - Decimal (13,3)
        # Volume of product (m3 for liquids, e3m3 for gas)
        StructField("Volume", DecimalType(13, 3), True),
        
        # Energy - Decimal (13,3)
        # Energy of gas (GJ)
        StructField("Energy", DecimalType(13, 3), True),
        
        # Hours - Integer (3)
        # Hours of production or injection
        StructField("Hours", IntegerType(), True),
        
        # CCI Code - string (1)
        # Consecutive Concurrent Injection indicator
        StructField("CCICode", StringType(), True),
        
        # Proration Product - string (12)
        # Product which Proration Factor is applied to
        StructField("ProrationProduct", StringType(), True),
        
        # Proration Factor - Decimal (6,5)
        # Proration Factor for the product
        StructField("ProrationFactor", DecimalType(6, 5), True),
        
        # Heat - Decimal (5,2)
        # Wellhead heat value of gas, if reported (MJ/m3)
        # Note: "if reported" means it can be NULL, but column should always be present
        StructField("Heat", DecimalType(5, 2), True),
    ])


def get_ngl_schema():
    """
    Returns the Spark schema for Petrinex NGL and Marketable Gas Volumes data.
    
    Reference: PD_NGL_Marketable_Gas_Volumes_Report.pdf (Data Fields section, pages 6-7)
    
    This schema represents NGL volumetric submissions and stream allocation information,
    including production volumes and allocation data for various NGL products.
    
    Returns
    -------
    pyspark.sql.types.StructType
        Schema with 26 columns for NGL and Marketable Gas Volumes data
    """
    return StructType([
        # Facility ID - String (20)
        # Unique identifier of the reporting facility
        StructField("FacilityID", StringType(), True),
        
        # Facility Name - String (60)
        # Facility Name of the reporting facility
        StructField("FacilityName", StringType(), True),
        
        # Operator BA ID - String (20)
        # Operator ID (Code) of the reporting facility for the production month
        StructField("OperatorBAID", StringType(), True),
        
        # Operator Name - String (150)
        # BA name of the reporting facility operator for the production month
        StructField("OperatorName", StringType(), True),
        
        # Production Month - YearMonth (7)
        # The production month for which the information was submitted
        StructField("ProductionMonth", StringType(), True),
        
        # Well ID - String (20)
        # Unique identifier of the well, units and well groups
        StructField("WellID", StringType(), True),
        
        # Well License Number - String (9)
        # Unique number identifying the well license
        StructField("WellLicenseNumber", StringType(), True),
        
        # Field - String (12)
        # Unique Identifier for the field
        StructField("Field", StringType(), True),
        
        # Pool - String (12)
        # Unique Identifier for the pool in which the well event is producing or injecting
        StructField("Pool", StringType(), True),
        
        # Area - String (12)
        # Unique identifier for the area
        StructField("Area", StringType(), True),
        
        # Hours - Integer (3)
        # Hours of production or injection
        # Source: Production - AER (Alberta Energy Regulator)
        StructField("Hours", IntegerType(), True),
        
        # Gas Production - Decimal (10,1)
        # Volume of gas (e3m3)
        # Source: Production - AER
        StructField("GasProduction", DecimalType(10, 1), True),
        
        # Oil Production - Decimal (10,1)
        # Volume of oil (m3)
        # Source: Production - AER
        StructField("OilProduction", DecimalType(10, 1), True),
        
        # Condensate Production - Decimal (10,1)
        # Volume of condensate (m3)
        # Source: Production - AER
        StructField("CondensateProduction", DecimalType(10, 1), True),
        
        # Water Production - Decimal (10,1)
        # Volume of water (m3)
        # Source: Production - AER
        StructField("WaterProduction", DecimalType(10, 1), True),
        
        # Residue Gas Volume - Decimal (10,1)
        # Volume of residue (e3m3)
        # Source: Allocation - AEM (Alberta Energy and Minerals)
        StructField("ResidueGasVolume", DecimalType(10, 1), True),
        
        # Energy - Decimal (10,1)
        # Total of all Energy of gas (GJ)
        # Source: Allocation - AEM
        StructField("Energy", DecimalType(10, 1), True),
        
        # Ethane Mix Volume - Decimal (10,1)
        # Volume of ethane mix (m3)
        # Source: Allocation - AEM
        StructField("EthaneMixVolume", DecimalType(10, 1), True),
        
        # Ethane Spec Volume - Decimal (10,1)
        # Volume of ethane spec (m3)
        # Source: Allocation - AEM
        StructField("EthaneSpecVolume", DecimalType(10, 1), True),
        
        # Propane Mix Volume - Decimal (10,1)
        # Volume of propane mix (m3)
        # Source: Allocation - AEM
        StructField("PropaneMixVolume", DecimalType(10, 1), True),
        
        # Propane Spec Volume - Decimal (10,1)
        # Volume of propane spec (m3)
        # Source: Allocation - AEM
        StructField("PropaneSpecVolume", DecimalType(10, 1), True),
        
        # Butane Mix Volume - Decimal (10,1)
        # Volume of butane mix (m3)
        # Source: Allocation - AEM
        StructField("ButaneMixVolume", DecimalType(10, 1), True),
        
        # Butane Spec Volume - Decimal (10,1)
        # Volume of butane spec (m3)
        # Source: Allocation - AEM
        StructField("ButaneSpecVolume", DecimalType(10, 1), True),
        
        # Pentane Mix Volume - Decimal (10,1)
        # Volume of pentane mix (m3)
        # Source: Allocation - AEM
        StructField("PentaneMixVolume", DecimalType(10, 1), True),
        
        # Pentane Spec Volume - Decimal (10,1)
        # Volume of pentane spec (m3)
        # Source: Allocation - AEM
        StructField("PentaneSpecVolume", DecimalType(10, 1), True),
        
        # Lite Mix Volume - Decimal (10,1)
        # Volume of lite mix (m3)
        # Source: Allocation - AEM
        StructField("LiteMixVolume", DecimalType(10, 1), True),
    ])


def get_schema_for_data_type(data_type: str):
    """
    Returns the appropriate schema for the given data type.
    
    Parameters
    ----------
    data_type : str
        One of: "Vol" (Volumetrics) or "NGL" (NGL and Marketable Gas Volumes)
    
    Returns
    -------
    pyspark.sql.types.StructType
        Schema for the specified data type
        
    Raises
    ------
    ValueError
        If data_type is not supported
    """
    if data_type == "Vol":
        return get_volumetrics_schema()
    elif data_type == "NGL":
        return get_ngl_schema()
    else:
        raise ValueError(
            f"Unsupported data_type: {data_type}. "
            f"Supported types: 'Vol', 'NGL'"
        )

