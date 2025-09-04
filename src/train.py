# Databricks notebook or job: Train & Register Model (MLflow)
import os, yaml
from pyspark.sql import SparkSession, functions as F
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score, average_precision_score, classification_report

spark = SparkSession.builder.getOrCreate()

base_dir = os.path.dirname(os.path.dirname(__file__))
with open(os.path.join(base_dir, "config", "params.yaml"), "r") as f:
    params = yaml.safe_load(f)

p = params["paths"]
m = params["modeling"]
label_col = m["label_col"]

features = spark.read.format("delta").load(p["features_table"].replace('delta.`','').replace('`',''))

# Join with labels from Silver if not already present
silver = spark.read.format("delta").load(p["silver_table"].replace('delta.`','').replace('`',''))
label_df = (silver
            .groupBy("user_id")
            .agg(F.max(F.col(label_col).cast("int")).alias(label_col))
           )

df = features.join(label_df, on="user_id", how="inner")

# To pandas
pdf = df.toPandas().dropna()
feature_cols = [c for c in pdf.columns if c not in ["user_id","week_start", label_col]]
X = pdf[feature_cols].values
y = pdf[label_col].values

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=m["test_size"], random_state=m["random_state"], stratify=y if len(set(y))>1 else None
)

pipeline = Pipeline([
    ("scaler", StandardScaler(with_mean=False)),
    ("clf", LogisticRegression(max_iter=1000))
])

mlflow.set_experiment("/Shared/lottery_fraud_experiment")
with mlflow.start_run():
    pipeline.fit(X_train, y_train)
    y_proba = pipeline.predict_proba(X_test)[:,1]
    auc = roc_auc_score(y_test, y_proba) if len(set(y_test))>1 else float("nan")
    ap = average_precision_score(y_test, y_proba) if len(set(y_test))>1 else float("nan")

    mlflow.log_metric("auc_roc", auc)
    mlflow.log_metric("avg_precision", ap)
    mlflow.sklearn.log_model(pipeline, "model")
    mlflow.log_param("features", feature_cols)

    run_id = mlflow.active_run().info.run_id
    print("Run:", run_id, "AUC:", auc, "AP:", ap)

    try:
        model_uri = f"runs:/{run_id}/model"
        mlflow.register_model(model_uri, "lottery_fraud_detector")
    except Exception as e:
        print("Model registry error:", e)

print("Training complete.")
