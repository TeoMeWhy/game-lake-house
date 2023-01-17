# Databricks notebook source
# DBTITLE 1,Imports
import datetime

# COMMAND ----------

# DBTITLE 1,Funcoes
def import_query(path:str)->str:
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

def table_exists(table_name:str)->bool:
    
    schema = ".".join(table_name.split(".")[:-1])
    table = table_name.split(".")[-1]
    query = f"SHOW TABLES FROM {schema}"
    count = spark.sql(query).filter(f"tableName = '{table}'").count()
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
catalog = 'silver'
schema = 'dota'
fs_name = dbutils.widgets.get("fs_name")
dt_start = dbutils.widgets.get("dt_start")
dt_stop = dbutils.widgets.get("dt_stop")

lag = 180

table_name = f'{catalog}.{schema}.{fs_name}'
table_name

# COMMAND ----------

# DBTITLE 1,Processo
dates = range_date(dt_start, dt_stop)

query = import_query(f"sql/{fs_name}.sql")

if not table_exists(table_name):
    query_exec = query.format(date=dates.pop(0), lag=lag)
    
    df = spark.sql(query_exec)
    
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .format("delta")
       .option("overwriteSchema", "true")
       .saveAsTable(table_name))

for i in dates:
    query_exec = query.format(date=i, lag=lag)
    df = spark.sql(query_exec)
    spark.sql(f"DELETE FROM {table_name} WHERE dtReference = '{i}'")
    df.write.mode("append").format("delta").saveAsTable(table_name)
    print(i, "- Done!")
