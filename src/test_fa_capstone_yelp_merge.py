import pytest
from pyspark.sql import SparkSession, Row
from fa_capstone_yelp_merge import update_yelp_business_df

# Mock Spark Session
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("pytest").getOrCreate()
    yield spark
    spark.stop()

def test_update_yelp_business_df(spark):
    data = [
        Row(
            address="123 Main St",
            categories="Restaurants, Italian",
            city="New York",
            latitude=40.7128,
            longitude=-74.0060,
            name="Italian Place",
            postal_code="10001",
            review_count=100,
            stars=4.5,
            state="NY",
            attributes={"RestaurantsTakeOut": True},
            business_id="1",
            hours={"Monday": "9:00-22:00"},
            is_open=1
        ),
        Row(
            address="456 Oak Ave",
            categories="Shopping, Books",
            city="Los Angeles",
            latitude=34.0522,
            longitude=-118.2437,
            name="Book Store",
            postal_code="90001",
            review_count=50,
            stars=3.0,
            state="CA",
            attributes={"WiFi": "free"},
            business_id="2",
            hours={"Tuesday": "10:00-20:00"},
            is_open=1
        ),
        Row(
            address="123 Main St",
            categories="Restaurants, Italian",
            city="New York",
            latitude=40.7128,
            longitude=-74.0060,
            name="Italian Place",
            postal_code="10001",
            review_count=100,
            stars=4.5,
            state="NY",
            attributes={"RestaurantsTakeOut": True},
            business_id="1",
            hours={"Monday": "9:00-22:00"},
            is_open=1
        ),
        Row(
            address="789 Pine Ln",
            categories="Cafe",
            city="Seattle",
            latitude=47.6062,
            longitude=-122.3321,
            name="Coffee Shop",
            postal_code="abc",
            review_count=75,
            stars=4.0,
            state="WA",
            attributes={"OutdoorSeating": True},
            business_id="3",
            hours={"Wednesday": "7:00-18:00"},
            is_open=1
        ),
    ]

    df = spark.createDataFrame(data)
    updated_df = update_yelp_business_df(df)

    assert updated_df.count() == 2

    expected_columns = [
        "address", "categories", "city", "latitude", "longitude", "name", "postal_code",
        "review_count", "stars", "state", "processed_date", "zip_int"
    ]
    actual_columns = updated_df.columns
    assert sorted(actual_columns) == sorted(expected_columns)

    # Check for correct data types
    assert updated_df.schema["address"].dataType.simpleString() == "string"
    assert updated_df.schema["stars"].dataType.simpleString() == "double"
    assert updated_df.schema["zip_int"].dataType.simpleString() == "int"
    assert updated_df.schema["processed_date"].dataType.simpleString() == "date"
    assert updated_df.select("city").collect()[0][0] == "new york"
    assert updated_df.select("state").collect()[0][0] == "ny"
