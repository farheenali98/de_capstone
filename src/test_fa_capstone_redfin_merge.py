import pytest
from pyspark.sql import SparkSession, Row
from fa_capstone_redfin_merge import update_redfin_weekly_df

# Mock Spark Session
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("pytest").getOrCreate()
    yield spark
    spark.stop()

def test_update_redfin_weekly_df(spark):
    data = [
        Row(
            period_begin="2023-01-01",
            period_end="2023-01-07",
            region_type_id=5,
            region_name="King County, WA",
            region_type="county",
            duration="1week"
        ),
        Row(
            period_begin="2023-01-08",
            period_end="2023-01-14",
            region_type_id=5,
            region_name="Los Angeles County, CA",
            region_type="county",
            duration="1week"
        ),
        Row(
            period_begin="2023-01-01",
            period_end="2023-01-07",
            region_type_id=5,
            region_name="King County, WA",
            region_type="county",
            duration="1week"
        ),
        Row(
            period_begin="2023-01-15",
            period_end="2023-01-21",
            region_type_id=6,
            region_name="Seattle, WA",
            region_type="city",
            duration="1week"
        ),
    ]

    df = spark.createDataFrame(data)
    updated_df = update_redfin_weekly_df(df)

    assert updated_df.count() == 2

    expected_columns = [
        "period_begin", "period_end", "duration", "county_name", "state_id", "processed_date"
    ]
    actual_columns = updated_df.columns
    assert sorted(actual_columns) == sorted(expected_columns)

    # Check for correct data types
    assert updated_df.schema["period_begin"].dataType.simpleString() == "date"
    assert updated_df.schema["county_name"].dataType.simpleString() == "string"
    assert updated_df.schema["processed_date"].dataType.simpleString() == "date"
    assert updated_df.select("county_name").collect()[0][0] == "king"
    assert updated_df.select("state_id").collect()[0][0] == "wa"
