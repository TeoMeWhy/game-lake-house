# Databricks notebook source
# DBTITLE 1,Imports
import json
import time
import sys

sys.path.insert(0, "../../lib/")

import database  as db

from pyspark.sql.types import *
from pyspark.sql import window
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Schema
schema_json = db.import_schema("schemas/match_schema.json")
schema = StructType.fromJson(schema_json)

# COMMAND ----------

# DBTITLE 1,Setup
origin_path = "/mnt/datalake/game-lake-house/raw/dota/match_details_raw"

checkpoint_path = "/mnt/datalake/checkpoints/bronze/dota/match_details"

database = 'bronze.dota'
table = 'match_details'

database_table = f'{database}.{table}'

# COMMAND ----------

# DBTITLE 1,Full Load
if db.table_exists(database, table, spark):
    print("Tabela já existente!")

else:
    print("Realizando a primeira carga da tabela...")
    df = spark.createDataFrame(data=[], schema=schema)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.{table}")
    dbutils.fs.rm(checkpoint_path, True)
    print("ok")

# COMMAND ----------

# DBTITLE 1,ReadStream
df_stream = (spark.readStream
                  .format('cloudFiles')
                  .option('cloudFiles.format', 'json')
                  .schema(schema)
                  .load(origin_path))

# COMMAND ----------

# DBTITLE 1,WriteStream
def upsertDelta(batchId, df, delta_table):

    df.createOrReplaceGlobalTempView("dota_ingestion")
    
    query = '''
    
    select *
    from global_temp.dota_ingestion
    qualify row_number() over (partition by match_id order by start_time desc) = 1
    
    '''
    
    df_merge = spark.sql(query)

    (delta_table.alias("d")
               .merge(df_merge.alias("n"), "d.match_id = n.match_id")
               .whenMatchedUpdateAll()
               .whenNotMatchedInsertAll()
               .execute())

delta_table = DeltaTable.forName(spark, database_table)
    
stream = (df_stream.writeStream
                   .format('delta')
                   .foreachBatch(lambda df, batchId: upsertDelta(batchId, df, delta_table))
                   .option('checkpointLocation', checkpoint_path)
                   .trigger(once=True)
                   .start())
