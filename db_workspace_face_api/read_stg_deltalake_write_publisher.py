# Databricks notebook source
schema_location = "dbfs:/tmp/auto_loader/schema"
checkpoint_location = "dbfs:/tmp/auto_loader/checkpoint"
stg_face_api_response = "dbfs:/user/hive/warehouse/sentiment_db_prod.db/stg_face_api_response"
stg_face_api_response_flat = "sentiment_db_prod.stg_face_api_response_flat"

# COMMAND ----------

spark.readStream.format("delta") \
  .load(stg_face_api_response) \
  .selectExpr("faceId", "faceRectangle.height", "faceRectangle.left", "faceRectangle.top", "faceRectangle.width", "faceAttributes.age", "faceAttributes.emotion.anger", "faceAttributes.emotion.contempt", "faceAttributes.emotion.disgust", "faceAttributes.emotion.fear", "faceAttributes.emotion.happiness", "faceAttributes.emotion.neutral", "faceAttributes.emotion.sadness", "faceAttributes.emotion.surprise", "faceAttributes.facialHair.beard", "faceAttributes.facialHair.moustache", "faceAttributes.facialHair.sideburns", "faceAttributes.gender", "faceAttributes.glasses", "faceAttributes.smile", "snapshot_timestamp", "api_response_timestamp", "current_timestamp() AS load_time") \
  .writeStream \
  .format("delta") \
  .option("checkpointLocation", checkpoint_location) \
  .toTable(stg_face_api_response_flat)
 

# COMMAND ----------

# MAGIC %sql
# MAGIC select snapshot_timestamp, load_time, datediff(second, snapshot_timestamp, load_time) AS lead_time, age, neutral, anger, happiness
# MAGIC from sentiment_db_prod.stg_face_api_response_flat
# MAGIC order by snapshot_timestamp desc
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sentiment_db_prod.stg_face_api_response_flat;

# COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/auto_loader", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/sentiment_db_prod.db/stg_face_api_response_flat", True)