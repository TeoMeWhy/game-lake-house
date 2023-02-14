# Databricks notebook source
# MAGIC %pip install mlflow lightgbm

# COMMAND ----------

import mlflow
import json
import requests
from pyspark.sql import functions as F

model = mlflow.sklearn.load_model("models:/Dota2 Pre Match/production")

def import_query(path):
    with open(path) as open_file:
        return open_file.read()
      
def import_features(path):
    with open(path, 'r') as open_file:
        return json.load(open_file)['features']

# COMMAND ----------

API_KEY = dbutils.secrets.get("dota", "api_key")

url = "https://api.opendota.com/api/live"
data = requests.get(url, params={"api_key": API_KEY}).json()

# COMMAND ----------

df = spark.createDataFrame(data)
df.createOrReplaceTempView("dota2_live_games")

query = import_query("etl_predict_table.sql")
df_predict = spark.sql(query).toPandas()

# COMMAND ----------

features = import_features("features.json")
X_predict = df_predict[features]

# COMMAND ----------

df_predict['pred_radiant_win'] = model.predict(X_predict)
df_predict['proba_radiant_win'] = model.predict_proba(X_predict)[:,1]

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
