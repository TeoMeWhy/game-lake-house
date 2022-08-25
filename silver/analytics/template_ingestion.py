# Databricks notebook source
import sys

sys.path.insert(0, "../../lib")

import database as db
import date_utils as du

from delta import *

# COMMAND ----------

table = 'fs_dota_player_matches'
database = 'silver_gamelakehouse'

database_table = f"{database}.{table}"

date_start = '2022-08-01'
date_stop = '2022-08-25'

dates = du.date_range(date_start, date_stop)

query = db.import_query(f"{table}.sql")

# COMMAND ----------

def first_load(df, database_table, partition):
    (df.write
       .format("delta")
       .mode("overwrite")
       .partitionBy(partition)
       .saveAsTable(database_table))
    
def upsert(df, delta_table, id_columns):
    join = " and ".join([ f"d.{i} = n.{i}" for i in id_columns])
    (delta_table.alias("d")
                .merge(df.alias("n"), join)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())

# COMMAND ----------

df = spark.sql(query)

if not db.table_exists(database, table, spark):

    dt = dates.pop(0)
    query_exec = query.format(date=dt)
    df = spark.sql(query_exec)
    first_load(df, database_table, 'dtReference')


delta_table = DeltaTable.forName(spark, database_table)


for dt in dates:
    
    query_exec = query.format(date=dt)
    df = spark.sql(query_exec)
    upsert(df, delta_table, ['dtReference', 'account_id'])
    
delta_table.vacuum()
