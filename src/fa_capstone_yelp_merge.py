# Databricks notebook source
# This notebook downloads, extracts, processes, and loads Yelp business data into a Delta table.
# It handles data cleaning, filtering, and deduplication before merging with the existing table.

from pyspark.sql.functions import col, to_date, current_date, lower, trim

# COMMAND ----------

import shutil
import os

raw_data_dir = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Yelp/raw_data/"

if os.path.exists(raw_data_dir):
    shutil.rmtree(raw_data_dir)

os.makedirs(raw_data_dir)

# COMMAND ----------

processed_data_dir = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Yelp/processed_data/"

if os.path.exists(processed_data_dir):
    shutil.rmtree(processed_data_dir)

os.makedirs(processed_data_dir)

# COMMAND ----------

ZIP_FILE_URL = "https://business.yelp.com/external-assets/files/Yelp-JSON.zip"
LOCAL_ZIP_PATH = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Yelp/raw_data/yelp_data.zip"
EXTRACTED_PATH = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Yelp/processed_data"

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

import tarfile
import os

# Define the path to the tar file and the extraction path
tar_path = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Yelp/processed_data/Yelp JSON/yelp_dataset.tar"
extracted_path = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Yelp/processed_data/Yelp JSON/extracted"

# Function to extract the tar file
def extract_tar_file():
    if not os.path.exists(extracted_path):
        os.makedirs(extracted_path)
    with tarfile.open(tar_path, "r") as tar_ref:
        tar_ref.extractall(extracted_path)
    print("Extraction completed.")

# Call the function to extract the tar file
extract_tar_file()

# COMMAND ----------

from pyspark.sql.functions import col, lower, trim, to_date, current_date, regexp_replace

# Define the path to the JSON file
json_path = "/Volumes/tabular/dataexpert/farheenali444/capstone_temp/Yelp/processed_data/Yelp JSON/extracted/yelp_academic_dataset_business.json"

# Define the function to generate Yelp business data
def get_yelp_business_df():
    # Read the JSON file into a DataFrame
    df = spark.read.json(json_path)
    
    # Process the DataFrame
    df = df.withColumn("processed_date", to_date(current_date()))
    df = df.withColumn("state", lower(trim(col("state"))))
    df = df.withColumn("city", lower(trim(col("city"))))
    df = df.filter(
        (col("name").isNotNull()) & 
        (col("stars") >= 1) & 
        (col("stars") <= 5) &
        (col("city").isNotNull()) &
        (col("state").isNotNull())
    )
    
    # Filter out rows where postal_code is not numeric
    df = df.filter(col("postal_code").rlike(r"^\d+$"))
    
    df = df.withColumn("zip_int", col("postal_code").cast("int"))
    df = df.drop("attributes", "business_id", "hours", "is_open")
    df = df.dropDuplicates(["address", "name", "postal_code"])
    
    return df

yelp_df = get_yelp_business_df()

# Define the table name
TMP_YELP_BUSINESS_TABLE_NAME = "tabular.dataexpert.fa_yelp_business_data"

# Merge the new data into the existing Delta table with schema evolution
yelp_df.createOrReplaceTempView("new_yelp_data")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TMP_YELP_BUSINESS_TABLE_NAME} AS
SELECT * FROM new_yelp_data WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {TMP_YELP_BUSINESS_TABLE_NAME} AS target
USING new_yelp_data AS source
ON target.address = source.address AND target.name = source.name AND target.postal_code = source.postal_code
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

# COMMAND ----------
