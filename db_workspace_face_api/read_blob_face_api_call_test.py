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

raw_data_location = "wasbs://peye3sourcedata@sedatahackathon2022src.blob.core.windows.net/"
target_delta_table_location = "dbfs:/tmp/delta/stg_face_api_response"
#target_delta_table_location = "default.stg_picam_raw_image_capture"
schema_location = "dbfs:/tmp/auto_loader/schema"
checkpoint_location = "dbfs:/tmp/auto_loader/checkpoint"

# COMMAND ----------

# Set up the stream to begin reading incoming files from the
# upload_path location.
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'binaryFile') \
  .load("/mnt/raw_images")



#df.writeStream.format('delta').outputMode("append").option("checkpointLocation", checkpoint_location).start("dbfs:/tmp/delta/tmp_face_api_response") #it works

#display(df2)

def process_row(batch_df,epoch_id):
    #print("df2: ", df2)
    data_collect = batch_df.collect()
    display(data_collect)
    
    #Send to FACE API
    #Send to stg_capture_raw
    #Empty blob

writer = df.writeStream.foreachBatch(process_row).start()

    
    
#final_df.writeStream.outputMode('append').foreach(foreach_batch_function).start().awaitTermination()




# COMMAND ----------

df = spark.read.format("delta").load("/tmp/delta/tmp_face_api_response")
display(df)

# COMMAND ----------

#Mount blob container to DBFS
dbutils.fs.mount(
  source = "wasbs://peye3sourcedata@sedatahackathon2022src.blob.core.windows.net",
  mount_point = "/mnt/raw_images",
  extra_configs = {"fs.azure.sas.peye3sourcedata.sedatahackathon2022src.blob.core.windows.net":dbutils.secrets.get(scope="source_data_storage", key="sas_url")})

# COMMAND ----------

dbutils.fs.ls ("dbfs:/mnt/")

# COMMAND ----------

dbutils.fs.ls ("dbfs:/mnt/sentiment_db_prod")

# COMMAND ----------

#Read from 