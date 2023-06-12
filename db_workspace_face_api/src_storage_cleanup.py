# Databricks notebook source
from azure.storage.blob import ContainerClient
from pyspark.sql import functions

sas_url = dbutils.secrets.get(scope="source_data_storage", key="sas_url")
container = ContainerClient.from_container_url(sas_url)

counter = 0
files = spark.sql("SELECT SPLIT(path,'/')[3] as file_name FROM default.stg_picam_raw_image_capture")
blobs = container.list_blobs()

for blob in blobs:
    if files.filter(functions.col("file_name").contains(blob.name)):
        blob_client = container.get_blob_client(blob.name)
        #blob_client.delete_blob()
        counter += 1

print("Files deleted from src storage: " + str(counter))