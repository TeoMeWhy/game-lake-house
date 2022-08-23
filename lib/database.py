__all__ = ['import_query', 'import_schema', 'table_exists']

import json

def import_query(path):
    with open(path, 'r') as open_file:
        query = open_file.read()
    return query

def import_schema(path):
    with open(path, 'r') as open_file:
        return json.load(open_file)
    
def table_exists(database, table, spark):
    count = (spark.sql(f"show tables in {database}")
               .filter(f"tableName = '{table}'")
               .count())
    return count == 1