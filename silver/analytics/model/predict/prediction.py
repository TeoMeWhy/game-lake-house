# Databricks notebook source
import datetime
import requests
import sys

import mlflow

sys.path.insert(0, "../../../../lib")

import secrets_tools as sct
import database as db

API_KEY = sct.get_secret("dota", "api_key", dbutils)

def get_live_matches():
    url = "https://api.opendota.com/api/live"
    res = requests.get(url, params={"api_key":API_KEY})
    return res.json()

# COMMAND ----------

data = get_live_matches()
df = spark.createDataFrame(data)
df.createOrReplaceTempView("live_games")

query = db.import_query("etl.sql")

df_predict = spark.sql(query).toPandas()

# COMMAND ----------

loaded_model = mlflow.lightgbm.load_model("models:/Dota2 Pre Match/production")

# COMMAND ----------

info_columns = ["match_id", "dt_match_start", "dt_match_update", "team_id_radiant", "team_id_dire"]

df_predict["proba_radiant_win"] = loaded_model.predict_proba(df_predict[loaded_model.feature_name_])[:,1]

df_result = df_predict[info_columns+["proba_radiant_win"]].copy()
df_result['dt_score'] = datetime.datetime.now()

# COMMAND ----------

sdf = spark.createDataFrame(df_result)
(sdf.coalesce(1)
    .write
    .mode("append")
    .format("delta")
    .saveAsTable("bronze_gamelakehouse.dota_pre_match_prediction"))
