# Databricks notebook source
# DBTITLE 1,Imports
import delta

def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

def table_exists(table, database):
    count = (spark.sql(f"SHOW TABLES FROM {database}")
                  .filter(f"tableName = '{table}' ")
                  .count())
    return count > 0

# COMMAND ----------

# DBTITLE 1,Setup
database = 'silver.dota'
table = dbutils.widgets.get("table")
database_table = f"{database}.{table}"
id_fields = dbutils.widgets.get("id_fields").split(",")

# COMMAND ----------

# DBTITLE 1,Execução
query = import_query(f"sql/{table}.sql")
df = spark.sql(query)

if table_exists(table, database):
    print("Atualizando dados da tabela...")
    delta_df = delta.DeltaTable.forName(spark, database_table)
    on_ids = " and ".join([f"d.{i} = u.{i}" for i in id_fields])
    
    (delta_df.alias("d")
             .merge(df.alias("u"), on_ids)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
    print("Ok.")

else:
    print("Criando tabela...")
    (df.coalesce(1)
       .write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(database_table))
    print("Ok.")
