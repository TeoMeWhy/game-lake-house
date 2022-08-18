# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import json
import os
import requests

import pandas as pd

from tqdm import tqdm

from delta.tables import *

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# COMMAND ----------

class Ingestor:
    
    def __init__(self, url, table_name, table_path):
        self.url = url
        self.table_name = table_name
        self.table_path = table_path
        
    def get_data(self, match_id, **params):
        url = self.url + str(match_id)
        res = requests.get(url, params=params)
        return res
    
    def save_data(self, data):
        match_id = data['match_id']
        path = "/dbfs" + self.table_path + f"_raw/{match_id}.json"
        json.dump(data, open(path, 'w'))
        
        data = pd.DataFrame({"match_id": [match_id]})
        df = (spark.createDataFrame(data)
                   .write
                   .format("delta")
                   .mode("append")
                   .save(self.table_path))
        
    def get_match_ids(self):
        
        (spark.read
              .format("delta")
              .load("/mnt/datalake/game-lake-house/raw/dota/match_history")
              .createOrReplaceTempView("match_history"))
        
        (spark.read
              .format("delta")
              .load(self.table_path)
              .createOrReplaceTempView("match_details"))
        
        query = '''
        select distinct t1.match_id as match_id
        from match_history as t1
        left join match_details as t2
        on t1.match_id = t2.match_id
        where t2.match_id is null        
        '''
        match_ids = spark.sql(query).toPandas()["match_id"].tolist()
        return match_ids
    
    def post_ingestion(self):
        deltaTable = DeltaTable.forPath(spark, self.table_path)
        deltaTable.optimize().executeCompaction()
        deltaTable.vacuum()

    def execute(self):
        match_ids = self.get_match_ids()
        
        for i in tqdm(match_ids):
            res = self.get_data(match_id=i)
            if 'match_id' in res.json():
                self.save_data(res.json())
            else:
                print("Partida não coletada.")
        
        self.post_ingestion()

# COMMAND ----------

url = "https://api.opendota.com/api/matches/"
table_name = "match_details"
table_path = f"/mnt/datalake/game-lake-house/raw/dota/{table_name}"

ingest = Ingestor(url, table_name, table_path)

# COMMAND ----------

ingest.execute()
