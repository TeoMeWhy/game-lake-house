# Databricks notebook source
# DBTITLE 1,Install
# MAGIC %pip install mlflow lightgbm

# COMMAND ----------

# DBTITLE 1,Setup
import mlflow
import json
import requests
import delta
from pyspark.sql import functions as F
from pyspark.sql import types

def import_query(path):
    with open(path, 'r') as open_file:
        return open_file.read()
      

def import_features(path):
    with open(path, 'r') as open_file:
        return json.load(open_file)['features']
      

def import_schema(path):
    with open(path, 'r') as open_file:
        return types.StructType.fromJson(json.load(open_file))


def table_exists(table, database):
    count = (spark.sql(f"SHOW TABLES FROM {database}")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count > 0

def upsert(df, df_delta, table):

    df.createOrReplaceGlobalTempView(f"dota_{table}")
    
    query = f'''select *
                from global_temp.dota_{table}
                where match_id is not null
                qualify row_number() over (partition by match_id order by game_time desc) = 1'''
    
    df_upsert = spark.sql(query)
    (df_delta.alias("d")
             .merge(df_upsert.alias("u"), "d.match_id = u.match_id")
             .whenNotMatchedInsertAll()
             .whenMatchedUpdateAll()
             .execute())

# COMMAND ----------

# DBTITLE 1,Ingestão em bronze (CDC -> Delta)
table = "live_games"
database = "bronze.dota"

table_database = f"{database}.{table}"

raw_path = "/mnt/datalake/game-lake-house/raw/dota/live-games/*"
checkpoint_path = "/mnt/datalake/game-lake-house/raw/dota/checkpoint_live_games/"

schema = import_schema(f"{table}.json")

if not table_exists(table, database):
    dbutils.fs.rm(checkpoint_path, True)
    df = spark.createDataFrame([], schema=schema)
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table_database))
    
df_delta = delta.DeltaTable.forName(spark, table_database)

df_stream = (spark.readStream.format("cloudFiles")
                             .schema(schema)
                             .option("cloudFiles.format", "json")
                             .load(raw_path))

stream =(df_stream.writeStream
                  .trigger(once=True)
                  .option("checkpointLocation", checkpoint_path)
                  .foreachBatch(lambda df, batchId: upsert(df, df_delta, table))
                  .start())

stream.awaitTermination(timeout=120)

# COMMAND ----------

# DBTITLE 1,Score das partidas dos últimos 2 dias
model = mlflow.sklearn.load_model("models:/Dota2 Pre Match/production")

query = import_query("etl_predict_table.sql")
df_predict = spark.sql(query).toPandas()

features = import_features("features.json")
X_predict = df_predict[features]

df_predict['pred_radiant_win'] = model.predict(X_predict)
df_predict['proba_radiant_win'] = model.predict_proba(X_predict)[:,1]

# COMMAND ----------

layout_final = ['match_id',
                'league_id',
                'game_mode',
                'game_time',
                'match_datetime',
                'dire_score',
                'radiant_score',
                'team_id_dire',
                'team_id_radiant',
                'team_name_dire',
                'team_name_radiant',
                'proba_radiant_win',      
]

df_output = spark.createDataFrame(df_predict[layout_final])
df_output = df_output.withColumn("dt_predict", F.current_timestamp())

(df_output.coalesce(1)
          .write
          .format("delta")
          .mode("append")
          .option("overwriteSchema", "true")
          .saveAsTable("bronze.dota.history_live_predictions"))
