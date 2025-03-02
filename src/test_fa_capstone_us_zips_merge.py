import pytest
from pyspark.sql import SparkSession, Row
from fa_capstone_us_zips_merge import update_us_zips_df

# Mock Spark Session
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("pytest").getOrCreate()
    yield spark
    spark.stop()

def test_update_us_zips_df(spark):
    data = [
        Row(
            zip=12345,
            lat=34.0522,
            lng=-118.2437,
            city="Los Angeles",
            state_id="CA",
            state_name="California",
            population=10000,
            density=100.0,
            county_name="Los Angeles County",
            zcta="1",
            parent_zcta="2",
            imprecise="3",
            military="4",
            timezone="5",
            county_weights="6",
            county_names_all="7",
            county_fips_all="8",
            county_fips="9"
        ),
        Row(
            zip=67890,
            lat=40.7128,
            lng=-74.0060,
            city="New York",
            state_id="NY",
            state_name="New York",
            population=20000,
            density=200.0,
            county_name="New York County",
            zcta="1",
            parent_zcta="2",
            imprecise="3",
            military="4",
            timezone="5",
            county_weights="6",
            county_names_all="7",
            county_fips_all="8",
            county_fips="9"
        ),
        Row(
            zip=12345,
            lat=34.0522,
            lng=-118.2437,
            city="Los Angeles",
            state_id="CA",
            state_name="California",
            population=10000,
            density=100.0,
            county_name="Los Angeles County",
            zcta="1",
            parent_zcta="2",
            imprecise="3",
            military="4",
            timezone="5",
            county_weights="6",
            county_names_all="7",
            county_fips_all="8",
            county_fips="9"
        ),
    ]

    df = spark.createDataFrame(data)
    updated_df = update_us_zips_df(df)

    assert updated_df.count() == 2

    expected_columns = [
        "zip", "lat", "lng", "city", "state_id", "state_name", "population", "density", "county_name", "processed_date"
    ]
    actual_columns = updated_df.columns
    assert sorted(actual_columns) == sorted(expected_columns)

    # Check for correct data types
    assert updated_df.schema["zip"].dataType.simpleString() == "int"
    assert updated_df.schema["county_name"].dataType.simpleString() == "string"
    assert updated_df.schema["processed_date"].dataType.simpleString() == "date"
    assert updated_df.select("county_name").collect()[0][0] == "los angeles"
    assert updated_df.select("state_id").collect()[0][0] == "ca"
