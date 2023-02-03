# Databricks notebook source
# MAGIC %pip install scikit-plot lightgbm mlflow

# COMMAND ----------

from sklearn import ensemble
from sklearn import metrics
from sklearn import model_selection
from sklearn import tree
from sklearn import pipeline

import lightgbm

import pandas as pd

import matplotlib.pyplot as plt
import scikitplot as skplt

import mlflow

# COMMAND ----------

sdf = spark.table("silver.dota.abt_match_predict")
df = sdf.toPandas()

# COMMAND ----------

features = df.columns.tolist()[2:]
target = 'radiant_win'

# COMMAND ----------

X_train, X_test, y_train, y_test = model_selection.train_test_split(df[features], df[target],
                                                                    test_size=0.20,
                                                                    random_state=42)

print("Taxa de resposta TREINO:", y_train.mean())
print("Taxa de resposta TESTE:", y_test.mean())

# COMMAND ----------

X_train = X_train.fillna(-1)
X_test = X_test.fillna(-1)

clf = tree.DecisionTreeClassifier(random_state=42)
clf.fit(X_train, y_train)

# COMMAND ----------

feature_importance = pd.Series(clf.feature_importances_, index = features)
feature_importance = feature_importance.sort_values(ascending=False)

best_features = feature_importance[feature_importance.cumsum() <= 0.85].index.tolist()
len(best_features) / feature_importance.shape[0]

# COMMAND ----------

mlflow.set_experiment("/Users/teo.bcalvo+db@gmail.com-backup-1/dota/dota-pre-match")

with mlflow.start_run():
    
    mlflow.sklearn.autolog()

    model = lightgbm.LGBMClassifier(subsample=0.5,
                                    learning_rate=0.0025,
                                    max_depth=6,
                                    num_leaves=100,
                                    min_child_samples=200,
                                    n_estimators=2500,
                                    n_jobs=-1,
                                    random_state=42)

    model_pipeline = pipeline.Pipeline([('lgbm', model)])
    
    model_pipeline.fit(X_train, y_train)

    def test_model(model, X, y):

        pred_prob = model_pipeline.predict_proba(X)[:,1]
        pred = model_pipeline.predict(X)

        acc = metrics.accuracy_score(y, pred)
        auc = metrics.roc_auc_score(y, pred_prob)
        return {"acc": acc, "auc": auc}

    metrics_train = test_model(model_pipeline, X_train, y_train)
    metrics_test = test_model(model_pipeline, X_test, y_test)

    print(metrics_train)
    print(metrics_test)
