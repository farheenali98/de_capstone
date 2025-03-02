# Databricks notebook source
# This notebook downloads, extracts, processes, and loads US zip code data into a Delta table.
# It handles data cleaning and deduplication before merging with the existing table.

from pyspark.sql.functions import col, to_date, current_date, lower, regexp_replace, trim

# COMMAND ----------

import shutil
import os

raw_data_dir = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Zips/raw_data/"

if os.path.exists(raw_data_dir):
    shutil.rmtree(raw_data_dir)

os.makedirs(raw_data_dir)

# COMMAND ----------

processed_data_dir = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Zips/processed_data/"

if os.path.exists(processed_data_dir):
    shutil.rmtree(processed_data_dir)

os.makedirs(processed_data_dir)

# COMMAND ----------

ZIP_FILE_URL = "https://simplemaps.com/static/data/us-zips/1.90/basic/simplemaps_uszips_basicv1.90.zip"
LOCAL_ZIP_PATH = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Zips/raw_data/zips_data.zip"
EXTRACTED_PATH = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Zips/processed_data"

# COMMAND ----------

import requests
import os
import zipfile

def download_zip_file():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    }
    response = requests.get(ZIP_FILE_URL, headers=headers,  stream=True)
    if response.status_code == 200:
        with open(LOCAL_ZIP_PATH, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        print("Download completed.")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

# COMMAND ----------

download_zip_file()

# COMMAND ----------

# Step 2: Extract the zip file
def extract_zip_file():
    # TODO(fali) Need to test it out this function
    if not os.path.exists(EXTRACTED_PATH):
        os.makedirs(EXTRACTED_PATH)
    with zipfile.ZipFile(LOCAL_ZIP_PATH, "r") as zip_ref:
        zip_ref.extractall(EXTRACTED_PATH)
    print("Extraction completed.")

# COMMAND ----------

extract_zip_file()

# COMMAND ----------

def get_us_zips_df():
  us_zips_path = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Zips/processed_data/uszips.csv"
  df = spark.read.csv(us_zips_path, header=True, inferSchema=True)
  df = df.withColumn("county_name", lower(trim(regexp_replace(col("county_name"), "(?i) county$", ""))))
  df = df.withColumn("state_id", lower(trim(col("state_id"))))
  df = df.drop("zcta", "parent_zcta", "imprecise", "military", "timezone", "county_weights", "county_names_all", "county_fips_all", "county_fips")
  df = df.withColumn("processed_date", to_date(current_date()))
  df = df.dropDuplicates(["zip"])

  return df

us_zips_df = get_us_zips_df()

# Merge the new data into the existing Delta table with schema evolution
us_zips_df.createOrReplaceTempView("new_us_zips_data")

# Define the table name
US_ZIPS_TABLE_NAME = "tabular.dataexpert.fa_us_zip_data"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {US_ZIPS_TABLE_NAME} AS
SELECT * FROM new_us_zips_data WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {US_ZIPS_TABLE_NAME} AS target
USING new_us_zips_data AS source
ON target.zip = source.zip
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

# COMMAND ----------
