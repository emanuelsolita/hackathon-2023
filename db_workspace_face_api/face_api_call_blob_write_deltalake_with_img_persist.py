# Databricks notebook source
from azure.storage.blob import ContainerClient
from msrest.authentication import CognitiveServicesCredentials
from azure.cognitiveservices.vision.face import FaceClient

sas_url = dbutils.secrets.get(scope="source_data_storage", key="sas_url")

sas_url_with_blob_placeholder = "https://sedatahackathon2022src.blob.core.windows.net/peye3sourcedata/{}?sp=racwdl&st=2022-06-08T11:31:27Z&se=2024-06-08T19:31:27Z&spr=https&sv=2020-08-04&sr=c&sig=JqRfNVS1h7qiVyD85CIHbohVLFUMeplGGYd8NoSV9Jg%3D"

face_api_endpoint = "https://sentiment-analysis-hackathon.cognitiveservices.azure.com/"
face_api_key = "56e642ff73df4de280880a622144205d"
credential = CognitiveServicesCredentials(face_api_key)

spark.conf.set(
  "fs.azure.sas.peye3sourcedata.sedatahackathon2022src.blob.core.windows.net", sas_url)

stg_face_api_response = "dbfs:/user/hive/warehouse/sentiment_db_prod.db/stg_face_api_response"


# COMMAND ----------

from pyspark.sql.functions import lit
from time import time
from datetime import datetime

container = ContainerClient.from_container_url(sas_url)
face_client = FaceClient(face_api_endpoint, credential)

attributes = ["emotion", "glasses", "smile", "age", "gender", "facialHair"]
include_id = True
include_landmarks = False
listblobs_iteration = 0
total_blobs_read = 0


while True:
    blob_list = container.list_blobs()
    blobs_per_listblobs_call = 0

    for blob in blob_list:
        blob_client = container.get_blob_client(blob.name)
        url = sas_url_with_blob_placeholder.format(blob.name)
        json_response = face_client.face.detect_with_url(url, include_id, include_landmarks, attributes, raw=True).response.json()
        spark.read.json(sc.parallelize([json_response])).withColumn("snapshot_timestamp", lit(datetime.fromtimestamp(int(blob.name)))).withColumn("api_response_timestamp", lit(datetime.fromtimestamp(round(time())))) \
             .write.format("delta").mode("append").save(stg_face_api_response)
        blob_client.delete_blob()
        blobs_per_listblobs_call += 1
        print("File written: " + str(blob.name))

    listblobs_iteration += 1
    total_blobs_read += blobs_per_listblobs_call
    print("Listblobs iteration: " + str(listblobs_iteration) + " | Blobs read this iteration: " + str(blobs_per_listblobs_call) + " | Total blobs read: " + str(total_blobs_read))

# COMMAND ----------

'''
spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "binaryFile") \
  .option("cloudFiles.schemaLocation", schema_location) \
  .load(raw_data_location) \
  .writeStream \
  .format("delta") \
  .option("checkpointLocation", checkpoint_location) \
  .toTable(target_delta_table_location)
'''
 

# COMMAND ----------

#delete autoloader metadata to reload everything in blob

dbutils.fs.rm("dbfs:/tmp/auto_loader", True)
#dbutils.fs.rm("dbfs:/tmp/delta/stg_face_api_response", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/sentiment_db_prod.db/stg_face_api_response", True)
#dbutils.fs.ls("dbfs:/databricks-datasets")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop/empty table 
# MAGIC 
# MAGIC DROP TABLE default.stg_face_api_response;
# MAGIC -- DELETE FROM default.stg_picam_raw_image_capture;