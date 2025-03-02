import pytest
from pyspark.sql import SparkSession, Row
from fa_capstone_zillow_merge import update_zillow_home_value_df

# Mock Spark Session
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("pytest").getOrCreate()
    yield spark
    spark.stop()

def test_update_zillow_home_value_df(spark):
    data = [
        Row(
            RegionID=1,
            SizeRank=1,
            RegionName=12345,
            RegionType="Zip",
            StateName="CA",
            City="Los Angeles",
            Metro="Los Angeles-Long Beach-Anaheim",
            CountyName="Los Angeles County",
            `2023-01-31`=100000.0,
            `2023-02-28`=101000.0
        ),
        Row(
            RegionID=2,
            SizeRank=2,
            RegionName=67890,
            RegionType="Zip",
            StateName="NY",
            City="New York",
            Metro="New York-Newark-Jersey City",
            CountyName="New York County",
            `2023-01-31`=200000.0,
            `2023-02-28`=202000.0
        ),
        Row(
            RegionID=1,
            SizeRank=1,
            RegionName=12345,
            RegionType="Zip",
            StateName="CA",
            City="Los Angeles",
            Metro="Los Angeles-Long Beach-Anaheim",
            CountyName="Los Angeles County",
            `2023-01-31`=100000.0,
            `2023-02-28`=101000.0
        )
    ]

    df = spark.createDataFrame(data)
    updated_df = update_zillow_home_value_df(df)

    assert updated_df.count() == 4

    expected_columns = [
        "RegionID", "SizeRank", "RegionName", "RegionType", "StateName", "state_id",
        "City", "Metro", "county_name", "Date", "Value"
    ]
    actual_columns = updated_df.columns
    assert sorted(actual_columns) == sorted(expected_columns)

    # Check for correct data types
    assert updated_df.schema["RegionID"].dataType.simpleString() == "int"
    assert updated_df.schema["Value"].dataType.simpleString() == "double"
    assert updated_df.schema["state_id"].dataType.simpleString() == "string"
    assert updated_df.select("county_name").collect()[0][0] == "los angeles"
    assert updated_df.select("state_id").collect()[0][0] == "ca"
