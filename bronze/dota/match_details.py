# Databricks notebook source
# DBTITLE 1,Imports
import json
import time
import sys

sys.path.insert(0, "../../lib/")

import database  as db

from pyspark.sql.types import *
from delta.tables import *

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Schema
schema_json = db.import_schema("schemas/match_schema.json")
schema = StructType.fromJson(schema_json)

# COMMAND ----------

# DBTITLE 1,Setup
origin_path = "/mnt/datalake/game-lake-house/raw/dota/match_details_raw"

checkpoint_path = "/mnt/datalake/game-lake-house/bronze/dota/match_details_checkpoint"

database = 'bronze_gamelakehouse'
table = 'dota_match_details'

database_table = f'{database}.{table}'

# COMMAND ----------

# DBTITLE 1,Full Load
if db.table_exists(database, table, spark):
    print("Tabela já existente!")
else:
    print("Realizando a primeira carga da tabela...")
    df = spark.read.json(origin_path, schema=schema)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.{table}")
    dbutils.fs.rm(checkpoint_path, True)
    print("ok")

# COMMAND ----------

# DBTITLE 1,ReadStream
df_stream = (spark.readStream
                  .format('cloudFiles')
                  .option('cloudFiles.format', 'json')
                  .option("cloudFiles.maxFilesPerTrigger", 10000)
                  .schema(schema)
                  .load(origin_path))

# COMMAND ----------

# DBTITLE 1,WriteStream
def upsertDelta(batchId, df, delta_table):
    (delta_table.alias("d")
               .merge(df.alias("n"), "d.match_id = n.match_id")
               .whenMatchedUpdateAll()
               .whenNotMatchedInsertAll()
               .execute())

delta_table = DeltaTable.forName(spark, database_table)
    
stream = (df_stream.writeStream
                   .format('delta')
                   .foreachBatch(lambda df, batchId: upsertDelta(batchId, df, delta_table))
                   .option('checkpointLocation', checkpoint_path)
                   .start())

# COMMAND ----------

# DBTITLE 1,Stop Stream
time.sleep(60*2)
stream.processAllAvailable()
stream.stop()
