def get_secret(scopo, key, dbutils):
    keys = [k.key for k in dbutils.secrets.list(scopo)]

    if key not in keys:
        api_key = None
    else:    
        api_key = dbutils.secrets.get(scopo, key)
    
    return api_key