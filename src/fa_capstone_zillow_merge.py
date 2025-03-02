# Databricks notebook source
# This notebook downloads, processes, and loads Zillow home value index data into a Delta table.
# It unpivots the data, cleans it, and merges it with the existing table.

from pyspark.sql.functions import col, lower, lit, regexp_replace, trim
from pyspark.sql.functions import explode, array, struct

# COMMAND ----------

import shutil
import os

processed_data_dir = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Zillow/processed_data/"

if os.path.exists(processed_data_dir):
    shutil.rmtree(processed_data_dir)

os.makedirs(processed_data_dir)

# COMMAND ----------

CSV_FILE_LOCAL_PATH = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Zillow/processed_data/zillow_data.csv"
CSV_FILE_URL = "https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"

# COMMAND ----------

import requests
import os

def download_csv_file():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    }
    response = requests.get(CSV_FILE_URL, headers=headers,  stream=True)
    if response.status_code == 200:
        with open(CSV_FILE_LOCAL_PATH, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        print("Download completed.")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

# COMMAND ----------

download_csv_file()

# COMMAND ----------

from pyspark.sql.functions import col, split, regexp_replace, to_date, current_date, lower, trim, array, struct, lit, explode

def update_zillow_home_value_df(zillow_home_value_index_df):    
    date_columns_hvi = [c for c in zillow_home_value_index_df.columns if c.startswith('20')]
    identifier_columns_hvi = [c for c in zillow_home_value_index_df.columns if not c.startswith('20')]

    # Create an array of structs, where each struct contains the date and value
    array_of_structs = array([
        struct(lit(c).alias("Date"), col(c).cast("double").alias("Value")) for c in date_columns_hvi
    ])

    # Explode the array of structs to create the new rows
    zillow_home_value_index_unpivoted_df = zillow_home_value_index_df.select(*identifier_columns_hvi, explode(array_of_structs).alias("date_value")) \
        .select(*identifier_columns_hvi, col("date_value.Date"), col("date_value.Value"))

    zillow_home_value_index_unpivoted_df = zillow_home_value_index_unpivoted_df.withColumnRenamed("State", "state_id")

    zillow_home_value_index_unpivoted_df = zillow_home_value_index_unpivoted_df.withColumn("state_id", lower(trim(col("state_id")))) 

    zillow_home_value_index_unpivoted_df = zillow_home_value_index_unpivoted_df.withColumnRenamed("CountyName", "county_name")

    zillow_home_value_index_unpivoted_df = zillow_home_value_index_unpivoted_df.withColumn("county_name", 
    lower(trim(regexp_replace(col("county_name"), "(?i) county$", ""))))

    zillow_home_value_index_df.dropDuplicates(["regionID", "state_id", "Date"])
    
    return zillow_home_value_index_unpivoted_df

zillow_home_value_index_df = spark.read.csv(CSV_FILE_LOCAL_PATH, header=True, inferSchema=True)
zillow_home_value_index_df = update_zillow_home_value_df(zillow_home_value_index_df)

# Define the table name
ZILLOW_HOME_VALUE_INDEX_TABLE_NAME = "tabular.dataexpert.fa_zillow_data"

# Merge the new data into the existing Delta table with schema evolution
zillow_home_value_index_df.createOrReplaceTempView("new_zillow_home_value_index_data")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ZILLOW_HOME_VALUE_INDEX_TABLE_NAME} AS
SELECT * FROM new_zillow_home_value_index_data WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {ZILLOW_HOME_VALUE_INDEX_TABLE_NAME} AS target
USING new_zillow_home_value_index_data AS source
ON target.Date = source.Date AND target.regionID = source.regionID AND target.state_id = source.state_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

# COMMAND ----------
