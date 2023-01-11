# Databricks notebook source
# DBTITLE 1,Imports
import datetime

from databricks import feature_store

# COMMAND ----------

# DBTITLE 1,Funcoes
def import_query(path:str)->str:
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

def table_exists(schema:str, table:str)->bool:
    count = spark.sql(f"SHOW TABLES FROM {schema}").filter(f"tableName = '{table}'").count()
    return count > 0

def range_date(date_start:str, date_stop:str)->[str]:
    
    dt_start = datetime.datetime.strptime(date_start, "%Y-%m-%d")
    dt_stop = datetime.datetime.strptime(date_stop, "%Y-%m-%d")
    dates = []
    
    while dt_start <= dt_stop:
        dates.append(dt_start.strftime("%Y-%m-%d"))
        dt_start += datetime.timedelta(days=1)
        
    return dates

# COMMAND ----------

# DBTITLE 1,Setup
# schema = 'silver_gamelakehouse'
# fs_name = 'fs_player_summary'
# table_name = f'{schema}.{table}'
# dt_start = '2022-01-01'
# dt_stop = '2023-01-11'
# lag = 150

schema = 'silver_gamelakehouse'
fs_name = dbutils.widgets.get("fs_name")
table_name = f'{schema}.{table}'
dt_start = dbutils.widgets.get("dt_start")
dt_stop = dbutils.widgets.get("dt_stop")
lag = dbutils.widgets.get("lag")

fs_name += f"_lag{lag}"

# COMMAND ----------

# DBTITLE 1,Processo
fs_client = feature_store.FeatureStoreClient()
dates = range_date(dt_start, dt_stop)
query = import_query(f"sql/{fs_name}.sql")

if not table_exists(schema, fs_name):
    query_exec = query.format(date=date.pop(0), lag=lag)
    
    df = spark.sql(query_exec)
    
    fs_client.create_table(name=table_name, 
                           primary_keys=['dtReference','idAccount'],
                           df=df,
                           partition_columns= ['dtReference'])

for i in dates:
    query_exec = query.format(date=i, lag=lag)
    df = spark.sql(query_exec)
    fs_client.write_table(name=table_name, df=df, mode='merge')
    print(i, "- Done!")
    
