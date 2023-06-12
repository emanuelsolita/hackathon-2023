-- Databricks notebook source
select 1;
show tables;
show databases;
use database sentiment_db_prod;

-- COMMAND ----------

select current_timestamp() AS ;

-- COMMAND ----------

SELECT
  faceId,
  faceRectangle.height,
  faceRectangle.left,
  faceRectangle.top,
  faceRectangle.width,
  faceAttributes.age,
  faceAttributes.emotion.anger,
  faceAttributes.emotion.contempt,
  faceAttributes.emotion.disgust,
  faceAttributes.emotion.fear,
  faceAttributes.emotion.happiness,
  faceAttributes.emotion.neutral,
  faceAttributes.emotion.sadness,
  faceAttributes.emotion.surprise,
  faceAttributes.facialHair.beard,
  faceAttributes.facialHair.moustache,
  faceAttributes.facialHair.sideburns,
  faceAttributes.gender,
  faceAttributes.glasses,
  faceAttributes.smile,
  snapshot_timestamp,
  api_response_timestamp
FROM stg_face_api_response_flat
;

-- COMMAND ----------

SELECT
  faceAttributes.age,
  faceAttributes.emotion.anger,
  faceAttributes.emotion.contempt,
  faceAttributes.emotion.disgust,
  faceAttributes.emotion.fear,
  faceAttributes.emotion.happiness,
  faceAttributes.emotion.neutral,
  faceAttributes.emotion.sadness,
  faceAttributes.emotion.surprise,
  faceAttributes.facialHair.beard,
  faceAttributes.facialHair.moustache,
  faceAttributes.facialHair.sideburns,
  faceAttributes.gender,
  faceAttributes.glasses,
  faceAttributes.smile
  from stg_face_api_response_flat
  ;
-- faceAttributes.age, faceAttributes.emotion.anger, faceAttributes.emotion.contempt, faceAttributes.emotion.disgust, faceAttributes.emotion.fear, faceAttributes.emotion.happiness, faceAttributes.emotion.neutral, faceAttributes.emotion.sadness, faceAttributes.emotion.surprise, faceAttributes.facialHair.beard, faceAttributes.facialHair.moustache, faceAttributes.facialHair.sideburns, faceAttributes.gender, faceAttributes.glasses, faceAttributes.smile

-- COMMAND ----------

use database sentiment_db_prod;
CREATE TABLE test0 (id STRING, age INT);

-- COMMAND ----------

use database default;
show tables;
show databases;


-- COMMAND ----------

select factRectangle from stg_face_api_response;