# Databricks notebook source
# MAGIC %pip install mlflow lightgbm

# COMMAND ----------

import mlflow
import requests
from pyspark.sql import functions as F

model = mlflow.sklearn.load_model("models:/Dota2 Pre Match/production")

def import_query(path):
    with open(path) as open_file:
        return open_file.read()

# COMMAND ----------

url = "https://api.opendota.com/api/live"
data = requests.get(url).json()

# COMMAND ----------

df = spark.createDataFrame(data)
df.createOrReplaceTempView("dota2_live_games")

query = import_query("etl_predict_table.sql")
df_predict = spark.sql(query).toPandas()

# COMMAND ----------

features = [ 'avgWinHeroRandiant1',
             'avgWinHeroRandiant2',
             'avgWinHeroRandiant3',
             'avgWinHeroRandiant4',
             'avgWinHeroRandiant5',
             'avgKdaHeroRandiant1',
             'avgKdaHeroRandiant2',
             'avgKdaHeroRandiant3',
             'avgKdaHeroRandiant4',
             'avgKdaHeroRandiant5',
             'avgDeathsHeroRandiant1',
             'avgDeathsHeroRandiant2',
             'avgDeathsHeroRandiant3',
             'avgDeathsHeroRandiant4',
             'avgDeathsHeroRandiant5',
             'avgXpPerMinHeroRandiant1',
             'avgXpPerMinHeroRandiant2',
             'avgXpPerMinHeroRandiant3',
             'avgXpPerMinHeroRandiant4',
             'avgXpPerMinHeroRandiant5',
             'avgGoldPerMinHeroRandiant1',
             'avgGoldPerMinHeroRandiant2',
             'avgGoldPerMinHeroRandiant3',
             'avgGoldPerMinHeroRandiant4',
             'avgGoldPerMinHeroRandiant5',
             'avgWinHeroDire1',
             'avgWinHeroDire2',
             'avgWinHeroDire3',
             'avgWinHeroDire4',
             'avgWinHeroDire5',
             'avgKdaHeroDire1',
             'avgKdaHeroDire2',
             'avgKdaHeroDire3',
             'avgKdaHeroDire4',
             'avgKdaHeroDire5',
             'avgDeathsHeroDire1',
             'avgDeathsHeroDire2',
             'avgDeathsHeroDire3',
             'avgDeathsHeroDire4',
             'avgDeathsHeroDire5',
             'avgXpPerMinHeroDire1',
             'avgXpPerMinHeroDire2',
             'avgXpPerMinHeroDire3',
             'avgXpPerMinHeroDire4',
             'avgXpPerMinHeroDire5',
             'avgGoldPerMinHeroDire1',
             'avgGoldPerMinHeroDire2',
             'avgGoldPerMinHeroDire3',
             'avgGoldPerMinHeroDire4',
             'avgGoldPerMinHeroDire5',
             'qtRecencyRadiant',
             'qtMatchesRadiant',
             'avgAssistsRadiant',
             'avgCampsStackedRadiant',
             'avgCreepsStackedRadiant',
             'avgDeathsRadiant',
             'avgDeniesRadiant',
             'avgFirstbloodClaimedRadiant',
             'avgGoldRadiant',
             'avgGoldPerMinRadiant',
             'avgGoldSpentRadiant',
             'avgHeroDamageRadiant',
             'avgHeroHealingRadiant',
             'avgKillsRadiant',
             'avgLastHitsRadiant',
             'avgLevelRadiant',
             'avgNetWorthRadiant',
             'avgRoshansKilledRadiant',
             'avgRunePickupsRadiant',
             'avgXpPerMinRadiant',
             'avgWinRadiant',
             'avgTotalGoldRadiant',
             'avgTotalXpRadiant',
             'avgKillsPerMinRadiant',
             'avgKdaRadiant',
             'avgNeutralKillsRadiant',
             'avgTowerKillsRadiant',
             'avgCourierKillsRadiant',
             'avgLaneKillsRadiant',
             'avgHeroKillsRadiant',
             'avgObserverKillsRadiant',
             'avgSentryKillsRadiant',
             'avgRoshanKillsRadiant',
             'avgNecronomiconKillsRadiant',
             'avgAncientKillsRadiant',
             'avgBuybackCountRadiant',
             'avgObserverUsesRadiant',
             'avgSentryUsesRadiant',
             'avgLaneEfficiencyRadiant',
             'qtRecencyDire',
             'qtMatchesDire',
             'avgAssistsDire',
             'avgCampsStackedDire',
             'avgCreepsStackedDire',
             'avgDeathsDire',
             'avgDeniesDire',
             'avgFirstbloodClaimedDire',
             'avgGoldDire',
             'avgGoldPerMinDire',
             'avgGoldSpentDire',
             'avgHeroDamageDire',
             'avgHeroHealingDire',
             'avgKillsDire',
             'avgLastHitsDire',
             'avgLevelDire',
             'avgNetWorthDire',
             'avgRoshansKilledDire',
             'avgRunePickupsDire',
             'avgXpPerMinDire',
             'avgWinDire',
             'avgTotalGoldDire',
             'avgTotalXpDire',
             'avgKillsPerMinDire',
             'avgKdaDire',
             'avgNeutralKillsDire',
             'avgTowerKillsDire',
             'avgCourierKillsDire',
             'avgLaneKillsDire',
             'avgHeroKillsDire',
             'avgObserverKillsDire',
             'avgSentryKillsDire',
             'avgRoshanKillsDire',
             'avgNecronomiconKillsDire',
             'avgAncientKillsDire',
             'avgBuybackCountDire',
             'avgObserverUsesDire',
             'avgSentryUsesDire',
             'avgLaneEfficiencyDire']

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
