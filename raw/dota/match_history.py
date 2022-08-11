# Databricks notebook source
# DBTITLE 1,Imports
import requests
import datetime

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Funções e classes
def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

class Ingestor:
    
    def __init__(self, url, table_name, table_path, date_stop):
        self.url = url
        self.table_path = table_path
        self.table_name = table_name
        self.date_stop = date_stop
        self.datetime_stop = (datetime.datetime
                                      .strptime(date_stop, '%Y-%m-%d')
                                      .date())

    def get_data(self, **params):
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

    def get_min_date(sefl, df):
        return (df.select(F.min(F.to_date("dt_start")))
                  .collect()[0][0])
        
    def get_min_match_id(sefl, df):
        return (df.select(F.min("match_id"))
                  .collect()[0][0])
    
    def get_history_data(self, **params):
        res, df = self.get_and_save(**params)
        min_date = self.get_min_date(df)
        min_match_id = self.get_min_match_id(df)
        
        while min_date >= self.datetime_stop:
            res, df = self.get_and_save(less_than_match_id=min_match_id)
            min_date = self.get_min_date(df)
            min_match_id = self.get_min_match_id(df)

# COMMAND ----------

# DBTITLE 1,Setup
ingestion_mode = dbutils.widgets.get("mode")
date = dbutils.widgets.get("date")
delay_days = int(dbutils.widgets.get("delay"))

date_stop = datetime.datetime.strptime(date, "%Y-%m-%d") + datetime.timedelta(days=-delay_days)
date_stop = datetime.datetime.strftime(date_stop, "%Y-%m-%d")

url = "https://api.opendota.com/api/proMatches"
table_path = "/mnt/datalake/game-lake-house/raw/dota/match_history"
table_name = "match_history"

match_ingestor = Ingestor(url, table_name, table_path, date_stop)

# COMMAND ----------

# DBTITLE 1,Execução
if ingestion_mode == "old":
    df = spark.read.format("delta").load(table_path)
    min_match_id = match_ingestor.get_min_match_id(df)
    match_ingestor.get_history_data(less_than_match_id=min_match_id)
    
elif ingestion_mode == "new":
    match_ingestor.get_history_data()
