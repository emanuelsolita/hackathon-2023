# Databricks notebook source
spark.conf.set(
  "fs.azure.sas.peye3sourcedata@sedatahackathon2022src.blob.core.windows.net",
  dbutils.secrets.get(scope="source_data_storage", key="sas_token"))

#raw_data_location="wasbs://peye3sourcedata@sedatahackathon2022src.blob.core.windows.net/"

raw_data_location="/mnt/raw_images"
schema_location = "dbfs:/tmp/auto_loader_raw_capture/schema"
checkpoint_location = "dbfs:/tmp/auto_loader_raw_capture/checkpoint"
stg_picamera_raw_capture = "sentiment_db_prod.stg_picamera_raw_capture"

# COMMAND ----------

spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "binaryFile") \
  .option("cloudFiles.schemaLocation", schema_location) \
  .load(raw_data_location) \
  .writeStream \
  .format("delta") \
  .option("checkpointLocation", checkpoint_location) \
  .toTable(stg_picamera_raw_capture)

# COMMAND ----------

#delete autoloader metadata to reload everything in blob

#dbutils.fs.rm("dbfs:/tmp/auto_loader_raw_capture/", True)