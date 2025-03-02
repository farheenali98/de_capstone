# Databricks notebook source
# This notebook downloads, processes, and loads Redfin weekly housing market data into a Delta table.
# It handles data filtering, cleaning, and deduplication before merging with the existing table.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_date, lower, split, regexp_replace, trim

# COMMAND ----------

import shutil
import os

processed_data_dir = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Redfin/processed_data/"

if os.path.exists(processed_data_dir):
    shutil.rmtree(processed_data_dir)

os.makedirs(processed_data_dir)

# COMMAND ----------

TSV_FILE_LOCAL_PATH = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Redfin/processed_data/redfin_data.tsv"

# COMMAND ----------

TSV_FILE_URL = "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_covid19/weekly_housing_market_data_most_recent.tsv000"

# COMMAND ----------

import requests
import os

def download_tsv_file():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    }
    response = requests.get(TSV_FILE_URL, headers=headers,  stream=True)
    if response.status_code == 200:
        with open(TSV_FILE_LOCAL_PATH, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        print("Download completed.")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

# COMMAND ----------

download_tsv_file()

# COMMAND ----------

from pyspark.sql.functions import col, split, regexp_replace, to_date, current_date, lower, trim

# Define the function to generate Redfin weekly data
def update_redfin_weekly_df(df):
    df = df.filter(col("region_type_id") == 5)
    df = df.withColumn("county_name", split(col("region_name"), ",")[0]) \
           .withColumn("state_id", lower(trim(split(col("region_name"), ",")[1])))
    df = df.withColumn("county_name", lower(trim(regexp_replace(col("county_name"), "(?i) county$", ""))))
    df = df.withColumn("processed_date", to_date(current_date()))
    df = df.drop("region_type", "region_type_id", "region_name", "region_id")
    df = df.dropDuplicates(["period_begin", "period_end", "county_name", "state_id"])
    return df

redfin_weekly_df = spark.read.csv(TSV_FILE_LOCAL_PATH, sep='\t', header=True, inferSchema=True)
redfin_weekly_df = update_redfin_weekly_df(redfin_weekly_df)

# Define the table name
REDFIN_WEEKLY_TABLE_NAME = "tabular.dataexpert.fa_redfin_data"

# Merge the new data into the existing Delta table with schema evolution
redfin_weekly_df.createOrReplaceTempView("new_redfin_weekly_data")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {REDFIN_WEEKLY_TABLE_NAME} AS
SELECT * FROM new_redfin_weekly_data WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {REDFIN_WEEKLY_TABLE_NAME} AS target
USING new_redfin_weekly_data AS source
ON target.period_begin = source.period_begin AND target.period_end = source.period_end AND target.county_name = source.county_name AND target.state_id = source.state_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

# COMMAND ----------
