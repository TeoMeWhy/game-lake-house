# Databricks notebook source
import os
import sys

sys.path.insert(0, "../../../../lib")

import database as db

from sklearn import model_selection
from sklearn import tree
from sklearn import ensemble
from sklearn import metrics
from sklearn.utils import parallel_backend

import lightgbm as lgb

from joblibspark import register_spark

import pandas as pd

import mlflow

register_spark() # register spark backend

# COMMAND ----------

DATE_START = '2021-01-01'
DATE_STOP = '2022-08-25'

query = db.import_query("etl.sql")
query = query.format(dt_start=DATE_START, dt_stop=DATE_STOP)

# COMMAND ----------

df = spark.sql(query).toPandas()

# COMMAND ----------

id_columns = ['match_id']
target = 'radiant_win'
removed_columns = id_columns + [target]
features_columns = list(set(df.columns.tolist()) - set(removed_columns))

# COMMAND ----------

X_train, X_test, y_train, y_test = model_selection.train_test_split(df[features_columns],
                                                                    df[target],
                                                                    test_size=0.2,
                                                                    random_state=42,
                                                                    stratify=df[target])

# COMMAND ----------

print("Mean target Train:", y_train.mean())
print("Mean target Test:", y_test.mean())

# COMMAND ----------

mlflow.set_experiment("/Users/teo.bcalvo+db@gmail.com/dota/dota-pre-match")

# COMMAND ----------

model = lgb.LGBMClassifier(boosting_type='gbdt',
                         num_leaves=None,
                         max_depth=-1,
                         learning_rate=0.05,
                         n_estimators=350,
                         min_child_samples=180,
                         subsample=1,
                         random_state=42,
                         n_jobs=-1)

# COMMAND ----------

with mlflow.start_run():
    
    mlflow.lightgbm.autolog()
    
    metrics_ml = {}
    
    model.fit(X_train, y_train)
    
    y_pred_test = model.predict(X_test)
    y_prob_test = model.predict_proba(X_test)

    metrics_ml['acc_test'] = metrics.accuracy_score(y_test, y_pred_test)
    metrics_ml['auc_test'] = metrics.roc_auc_score(y_test, y_prob_test[:,1])
    
    mlflow.log_metrics(metrics_ml)

# COMMAND ----------


