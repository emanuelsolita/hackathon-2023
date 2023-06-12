# Databricks notebook source
#Mount blob container for raw campera capture to DBFS
dbutils.fs.mount(
  source = "wasbs://peye3sourcedata@sedatahackathon2022src.blob.core.windows.net",
  mount_point = "/mnt/raw_images",
  extra_configs = {"fs.azure.sas.peye3sourcedata.sedatahackathon2022src.blob.core.windows.net":dbutils.secrets.get(scope="source_data_storage", key="sas_url")})

# COMMAND ----------

#Mount blob container for persisted deltalake tables to DBFS
dbutils.fs.mount(
  source = "wasbs://sentiment-db-prod@sedatahackathon2022src.blob.core.windows.net",
  mount_point = "/mnt/sentiment_db_prod",
  extra_configs = {"fs.azure.sas.sentiment-db-prod.sedatahackathon2022src.blob.core.windows.net":dbutils.secrets.get(scope="sentiment_db_prod_storage", key="sas_url")})

# COMMAND ----------

dbutils.fs.unmount("/mnt/sentiment_db_prod")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create database sentiment_db_prod;
# MAGIC use database sentiment_db_prod;
# MAGIC show tables;