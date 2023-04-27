# Databricks notebook source
# DBTITLE 1,Imports
import requests
import datetime

import delta

import sys

from pyspark.sql import functions as F

sys.path.insert(0, "../../lib")

import secrets_tools as sct

API_KEY = sct.get_secret("dota", "api_key", dbutils)

# COMMAND ----------

# DBTITLE 1,Funções e classes
def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

class Ingestor:
    
    def __init__(self, url, table_name, table_path, date_stop, api_key):
        self.url = url
        self.table_path = table_path
        self.table_name = table_name
        self.date_stop = date_stop
        self.datetime_stop = (datetime.datetime
                                      .strptime(date_stop, "%Y-%m-%d")
                                      .date())
        self.api_key = api_key

    def get_data(self, **params):
        params['api_key'] = self.api_key
        res = requests.get(self.url, params=params)
        return res

    def etl(self, df):
        view_name = f"{self.table_name}_tmp"
        df.createOrReplaceTempView(view_name)
        
        path_name = f"{self.table_name}.sql"
        query = import_query(path_name)
        query = query.format(table = view_name)
        
        df = spark.sql(query )
        return df
        
    def save_data(self, df):
        (df.coalesce(1)
           .write
           .format("delta")
           .mode("append")
           .save(self.table_path))
    
    def get_and_save(self, **params):
        res = self.get_data(**params)
        df = spark.createDataFrame(res.json())
        df = self.etl(df)
        self.save_data(df)
        return res, df

    def get_min_date(self, df):
        return (df.select(F.min(F.to_date("dt_start")))
                  .collect()[0][0])
        
    def get_min_match_id(self, df):
        return (df.select(F.min("match_id"))
                  .collect()[0][0])
    
    def get_history_data(self, **params):
        res, df = self.get_and_save(**params)
        min_date = self.get_min_date(df)
        min_match_id = self.get_min_match_id(df)
        print(min_date)
        
        while min_date >= self.datetime_stop:
            res, df = self.get_and_save(less_than_match_id=min_match_id)
            min_date = self.get_min_date(df)
            min_match_id = self.get_min_match_id(df)
            print(min_date)

# COMMAND ----------

# DBTITLE 1,Setup
ingestion_mode = dbutils.widgets.get("mode")
date = dbutils.widgets.get("date")
delay_days = int(dbutils.widgets.get("delay"))

date_stop = datetime.datetime.strptime(date, "%Y-%m-%d") + datetime.timedelta(days=-delay_days)
date_stop = datetime.datetime.strftime(date_stop, "%Y-%m-%d")

url = "https://api.opendota.com/api/proMatches"
table_name = "match_history"
table_path = f"/mnt/datalake/game-lake-house/raw/dota/{table_name}"

match_ingestor = Ingestor(url, table_name, table_path, date_stop, api_key=API_KEY)

# COMMAND ----------

# DBTITLE 1,Execução
if ingestion_mode == "old":
    df = spark.read.format("delta").load(table_path)
    min_match_id = match_ingestor.get_min_match_id(df)
    match_ingestor.get_history_data(less_than_match_id=min_match_id)
    
elif ingestion_mode == "new":
    match_ingestor.get_history_data()

# COMMAND ----------

# DBTITLE 1,Limpeza de dados antigos
if date[-1] == '01':

    (spark.read
          .format("delta")
          .load(table_path)
          .distinct()
          .write
          .format("delta")
          .mode("overwrite")
          .save(table_path))

    df_delta = delta.DeltaTable.forPath(spark, table_path)
    df_delta.vacuum()
