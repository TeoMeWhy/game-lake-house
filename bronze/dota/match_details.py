# Databricks notebook source
import json
import time

from pyspark.sql.types import *

def import_schema(path):
    with open(path, 'r') as open_file:
        return json.load(open_file)
      
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# COMMAND ----------

schema_json = import_schema("match_schema.json")
schema = StructType.fromJson(schema_json)

# COMMAND ----------

origin_path = "/mnt/datalake/game-lake-house/raw/dota/match_details_raw"

target_path = "/mnt/datalake/game-lake-house/bronze/dota/matches"
checkpoint_path = "/mnt/datalake/game-lake-house/bronze/dota/match_details_checkpoint"


df_stream = (spark.readStream
                  .format('cloudFiles')
                  .option('cloudFiles.format', 'json')
                  .option("cloudFiles.maxFilesPerTrigger", 1000)
                  .schema(schema)
                  .load(origin_path))

# COMMAND ----------

stream = (df_stream.writeStream
                   .format('delta')
                   .option('checkpointLocation', checkpoint_path)
                   .start(target_path))

# COMMAND ----------

time.sleep(60*5)
stream.processAllAvailable()
stream.stop()
